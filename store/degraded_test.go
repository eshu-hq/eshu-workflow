// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	"github.com/eshu-hq/eshu-workflow/store"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// degradedLog records one slog record for fail-open assertions.
type degradedLog struct {
	level  slog.Level
	msg    string
	reason string
	cause  string
}

// degradedHandler collects slog records with their reason/cause fields.
type degradedHandler struct {
	records *[]degradedLog
}

func (h *degradedHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *degradedHandler) Handle(_ context.Context, r slog.Record) error {
	var rec degradedLog
	rec.level = r.Level
	rec.msg = r.Message
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "reason":
			rec.reason, _ = a.Value.Any().(string)
		case "error":
			rec.cause, _ = a.Value.Any().(string)
		}
		return true
	})
	*h.records = append(*h.records, rec)
	return nil
}

func (h *degradedHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *degradedHandler) WithGroup(string) slog.Handler { return h }

// degradedStore wires a capturing logger and registry onto a fresh store.
func degradedStore(
	t *testing.T,
	cfg claimstore.Config,
) (*claimstore.Store, context.Context, *[]degradedLog, *prometheus.Registry) {
	t.Helper()
	st, ctx := openFreshStore(t)
	var records []degradedLog
	registry := prometheus.NewRegistry()
	st.Instruments = telemetry.NewInstruments(registry)
	cfg.Logger = slog.New(&degradedHandler{records: &records})
	if err := st.Configure(cfg); err != nil {
		t.Fatalf("configure: %v", err)
	}
	return st, ctx, &records, registry
}

// restoreMigration re-applies a migration dropped for degraded-mode proof.
func restoreMigration(t *testing.T, ctx context.Context, st *claimstore.Store, version string) {
	t.Helper()
	t.Cleanup(func() {
		if err := st.ForgetMigrationForTest(ctx, version); err != nil {
			t.Fatalf("forget migration: %v", err)
		}
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("restore migrate: %v", err)
		}
	})
}

func claimWithSelector(
	t *testing.T,
	ctx context.Context,
	st *claimstore.Store,
) workflowv1.ClaimNextResponse {
	t.Helper()
	resp, err := st.ClaimNext(ctx, workflowv1.ClaimSelector{
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: "jira-primary",
		OwnerID:             "owner-degraded",
		ClaimID:             "claim-degraded",
	}, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("degraded claim: %v", err)
	}
	return resp
}

func degradedCounter(t *testing.T, registry *prometheus.Registry, reason string, want int) {
	t.Helper()
	expected := `
# HELP workflow_claim_degraded_total Degraded-mode fail-open decisions by reason.
# TYPE workflow_claim_degraded_total counter
workflow_claim_degraded_total{reason="` + reason + `"} ` + strconv.Itoa(want) + `
`
	if err := testutil.GatherAndCompare(registry, strings.NewReader(expected),
		"workflow_claim_degraded_total"); err != nil {
		t.Fatalf("degraded counter mismatch: %v", err)
	}
}

// TestQuotaFloorSurfacesCause drops the quota windows table and requires
// the fail-open grant to carry its cause: a quota_floor reason on the
// response, a warn log with the underlying error, and a counter bump.
func TestQuotaFloorSurfacesCause(t *testing.T) {
	cfg := newTestConfig(t, map[string]int{"jira": 2})
	st, ctx, records, registry := degradedStore(t, cfg)
	runID := seedRun(t, ctx, st)
	if err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "floor-cause-0", false)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := st.DropTableForTest(ctx, "component_quota_windows"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	restoreMigration(t, ctx, st, "002_pacing_quota")

	resp := claimWithSelector(t, ctx, st)
	if !resp.Issued {
		t.Fatalf("floor claim did not issue under unreadable quota state")
	}
	if resp.DegradedReason != "quota_floor" {
		t.Fatalf("degraded reason = %q, want quota_floor", resp.DegradedReason)
	}
	found := false
	for _, rec := range *records {
		if rec.level == slog.LevelWarn && rec.reason == "quota_floor" && rec.cause != "" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no warn log with quota_floor cause in %+v", *records)
	}
	degradedCounter(t, registry, "quota_floor", 1)
}

// TestPacingLatchReadFailureFailsOpenWithSignal drops the pacing latch
// table and requires claiming to stay open while reporting the latch
// read as the degradation reason.
func TestPacingLatchReadFailureFailsOpenWithSignal(t *testing.T) {
	cfg := newTestConfig(t, nil)
	st, ctx, records, registry := degradedStore(t, cfg)
	runID := seedRun(t, ctx, st)
	if err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "latch-read-0", false)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := st.DropTableForTest(ctx, "workflow_claim_pacing"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	restoreMigration(t, ctx, st, "002_pacing_quota")

	resp := claimWithSelector(t, ctx, st)
	if !resp.Issued {
		t.Fatalf("latch-unreadable claim did not fail open")
	}
	if resp.DegradedReason != "pacing_latch_read" {
		t.Fatalf("degraded reason = %q, want pacing_latch_read", resp.DegradedReason)
	}
	found := false
	for _, rec := range *records {
		if rec.level == slog.LevelWarn && rec.reason == "pacing_latch_read" && rec.cause != "" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no warn log with pacing_latch_read cause in %+v", *records)
	}
	degradedCounter(t, registry, "pacing_latch_read", 1)
}

// TestPacingLatchAdvanceFailureSurfacesExecError breaks latch writes with a
// throwing trigger while reads still succeed, and requires the discarded
// Exec error path to surface as a counted, logged degradation on an
// otherwise issued grant.
func TestPacingLatchAdvanceFailureSurfacesExecError(t *testing.T) {
	cfg := newTestConfig(t, nil)
	st, ctx, records, registry := degradedStore(t, cfg)
	runID := seedRun(t, ctx, st)
	if err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "latch-advance-0", false)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := st.ExecForTest(ctx, `CREATE OR REPLACE FUNCTION pacing_break() RETURNS trigger AS
$$ BEGIN RAISE EXCEPTION 'latch writes broken for degraded-mode proof'; END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS pacing_break_trigger ON workflow_claim_pacing;
CREATE TRIGGER pacing_break_trigger BEFORE INSERT ON workflow_claim_pacing
FOR EACH ROW EXECUTE FUNCTION pacing_break()`); err != nil {
		t.Fatalf("create break trigger: %v", err)
	}
	t.Cleanup(func() {
		_ = st.ExecForTest(ctx, `DROP TRIGGER IF EXISTS pacing_break_trigger ON workflow_claim_pacing;
DROP FUNCTION IF EXISTS pacing_break()`)
	})

	resp := claimWithSelector(t, ctx, st)
	if !resp.Issued {
		t.Fatalf("latch-unwritable claim did not fail open")
	}
	if resp.DegradedReason != "pacing_latch_advance" {
		t.Fatalf("degraded reason = %q, want pacing_latch_advance", resp.DegradedReason)
	}
	found := false
	for _, rec := range *records {
		if rec.level == slog.LevelWarn && rec.reason == "pacing_latch_advance" && rec.cause != "" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no warn log with pacing_latch_advance cause in %+v", *records)
	}
	// Both the domain and the global latch advance fail, so the Exec
	// error surfaces twice while the grant still issues.
	degradedCounter(t, registry, "pacing_latch_advance", 2)
}
