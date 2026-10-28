// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil))
}

// capturedLog records one slog record for tick-path assertions.
type capturedLog struct {
	level slog.Level
	msg   string
}

// captureHandler collects slog records in memory.
type captureHandler struct {
	records *[]capturedLog
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	*h.records = append(*h.records, capturedLog{level: r.Level, msg: r.Message})
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

// fakeReaperStore stands in for the pgx store behind a narrow tick
// surface: it proves the reaper's error branch and observe calls without
// a database.
type fakeReaperStore struct {
	reapErr      error
	reaped       []workflowv1.Claim
	reconcileErr error
	transitioned int
	depthErr     error
	snapshot     workflowv1.QueueDepthSnapshot
	calls        []string
}

func (f *fakeReaperStore) Reap(
	_ context.Context,
	_ time.Time,
	_ int,
	_ time.Duration,
) ([]workflowv1.Claim, error) {
	f.calls = append(f.calls, "Reap")
	if f.reapErr != nil {
		return nil, f.reapErr
	}
	return f.reaped, nil
}

func (f *fakeReaperStore) ReconcileRuns(_ context.Context, _ time.Time) (int, error) {
	f.calls = append(f.calls, "ReconcileRuns")
	if f.reconcileErr != nil {
		return 0, f.reconcileErr
	}
	return f.transitioned, nil
}

func (f *fakeReaperStore) QueueDepth(_ context.Context, _ time.Time) (workflowv1.QueueDepthSnapshot, error) {
	f.calls = append(f.calls, "QueueDepth")
	if f.depthErr != nil {
		return workflowv1.QueueDepthSnapshot{}, f.depthErr
	}
	return f.snapshot, nil
}

func TestParseQuotas(t *testing.T) {
	quotas, err := parseQuotas("")
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if len(quotas) != 0 {
		t.Fatalf("empty quotas = %v", quotas)
	}
	quotas, err = parseQuotas("jira=100,package_registry=25")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if quotas["jira"] != 100 || quotas["package_registry"] != 25 {
		t.Fatalf("quotas = %v", quotas)
	}
	for _, raw := range []string{"jira", "jira=-1", "jira=abc", "=5"} {
		if _, err := parseQuotas(raw); err == nil {
			t.Fatalf("quota %q parsed without error", raw)
		}
	}
}

func TestParseFlagsRequiresDatabase(t *testing.T) {
	if _, err := parseFlags([]string{}); err == nil {
		t.Fatalf("missing database URL parsed without error")
	}
	opts, err := parseFlags([]string{
		"--database-url", "postgres://localhost:5432/x?sslmode=disable",
		"--quotas", "jira=10",
		"--pacing-domain-high", "100",
		"--pacing-domain-low", "50",
		"--pacing-global-high", "1000",
		"--pacing-global-low", "500",
		"--pacing-grant-delay", "5s",
		"--pacing-max-engagement", "1m",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if opts.listen != ":8080" || opts.adminListen != ":8081" {
		t.Fatalf("default listeners = %q %q", opts.listen, opts.adminListen)
	}
	if opts.pacing.DomainHigh != 100 || opts.pacing.GrantDelay != 5*time.Second {
		t.Fatalf("pacing = %+v", opts.pacing)
	}
	if _, err := parseFlags([]string{"--database-url", "x", "--reap-limit", "0"}); err == nil {
		t.Fatalf("zero reap limit parsed without error")
	}
}

func TestRunRejectsBadQuotasBeforeDialing(t *testing.T) {
	opts, err := parseFlags([]string{
		"--database-url", "postgres://localhost:5432/x?sslmode=disable",
		"--quotas", "jira",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := run(context.Background(), testLogger(), opts); err == nil {
		t.Fatalf("run with bad quotas succeeded")
	}
}

func TestParseQuotasRejectsTrailingComma(t *testing.T) {
	// A trailing comma must be rejected rather than silently granting an
	// empty kind.
	if _, err := parseQuotas("jira=10,"); err == nil {
		t.Fatalf("trailing comma parsed without error")
	}
}

// TestReaperTickLogsAndReturnsOnReapError requires a failing reap to log
// at error level and return before the reconcile and depth stages run.
func TestReaperTickLogsAndReturnsOnReapError(t *testing.T) {
	var records []capturedLog
	logger := slog.New(&captureHandler{records: &records})
	fake := &fakeReaperStore{reapErr: errors.New("connection reset")}
	tick := newReaper(fake, telemetry.NewInstruments(prometheus.NewRegistry()), logger,
		options{reapInterval: time.Minute, reapLimit: 10, requeueDelay: time.Minute})
	tick.tick(context.Background(), time.Now().UTC())
	if len(fake.calls) != 1 || fake.calls[0] != "Reap" {
		t.Fatalf("tick calls = %v, want exactly [Reap]", fake.calls)
	}
	for _, rec := range records {
		if rec.level == slog.LevelError && rec.msg == "reap tick" {
			return
		}
	}
	t.Fatalf("no error-level %q log in %v", "reap tick", records)
}

// TestReaperTickObservesReapAndDepth requires a successful tick to record
// the reaped count and every family depth gauge.
func TestReaperTickObservesReapAndDepth(t *testing.T) {
	registry := prometheus.NewRegistry()
	instruments := telemetry.NewInstruments(registry)
	fake := &fakeReaperStore{
		reaped:       []workflowv1.Claim{{ClaimID: "c-1"}, {ClaimID: "c-2"}},
		transitioned: 1,
		snapshot: workflowv1.QueueDepthSnapshot{
			AsOf:          time.Now().UTC(),
			GlobalPending: 3,
			GlobalClaimed: 1,
			Families: []workflowv1.FamilyQueueDepth{{
				CollectorKind: "jira", Pending: 3, Claimed: 1,
			}},
		},
	}
	var records []capturedLog
	tick := newReaper(fake, instruments, slog.New(&captureHandler{records: &records}),
		options{reapInterval: time.Minute, reapLimit: 10, requeueDelay: time.Minute})
	tick.tick(context.Background(), time.Now().UTC())
	wantCalls := []string{"Reap", "ReconcileRuns", "QueueDepth"}
	if strings.Join(fake.calls, ",") != strings.Join(wantCalls, ",") {
		t.Fatalf("tick calls = %v, want %v", fake.calls, wantCalls)
	}
	expected := `
# HELP workflow_claim_reaps_total Claims expired by the reaper.
# TYPE workflow_claim_reaps_total counter
workflow_claim_reaps_total 2
# HELP workflow_queue_depth Outstanding work items by family and state.
# TYPE workflow_queue_depth gauge
workflow_queue_depth{collector_kind="jira",state="claimed"} 1
workflow_queue_depth{collector_kind="jira",state="pending"} 3
`
	if err := testutil.GatherAndCompare(registry, strings.NewReader(expected),
		"workflow_claim_reaps_total", "workflow_queue_depth"); err != nil {
		t.Fatalf("reap/depth metrics mismatch: %v", err)
	}
}
