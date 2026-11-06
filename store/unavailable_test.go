// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/store"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// closedStore returns a store whose pool is already closed: every query
// fails the way a lost database does, without touching shared state.
func closedStore(t *testing.T) *claimstore.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, err := claimstore.Open(ctx, testDSN(t))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	st.Close()
	return st
}

func validMutation() workflowv1.ClaimMutation {
	return workflowv1.ClaimMutation{
		WorkItemID: "wi-1", ClaimID: "claim-1", FencingToken: 1, OwnerID: "owner-1",
		ObservedAt: time.Now().UTC(), LeaseDuration: time.Minute,
	}
}

// TestDatabaseLossSurfacesAsErrors requires every store operation to
// return an error, never a zero value that reads as success, when the
// database is unreachable.
func TestDatabaseLossSurfacesAsErrors(t *testing.T) {
	ctx := context.Background()
	st := closedStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	run := workflowv1.Run{
		RunID: "run-1", TriggerKind: workflowv1.TriggerKindSchedule,
		Status: workflowv1.RunStatusCollectionActive, CreatedAt: now, UpdatedAt: now,
	}
	selector := workflowv1.ClaimSelector{
		CollectorKind: "jira", CollectorInstanceID: "jira-primary", OwnerID: "o-1", ClaimID: "c-1",
	}
	mutation := validMutation()
	tests := []struct {
		name string
		call func() error
	}{
		{"Migrate", func() error { return st.Migrate(ctx) }},
		{"PingTable", func() error { return st.PingTable(ctx, "workflow_claims") }},
		{"TruncateForTest", func() error { return st.TruncateForTest(ctx) }},
		{"CreateRun", func() error { return st.CreateRun(ctx, run) }},
		{"GetRun", func() error { _, err := st.GetRun(ctx, "run-1"); return err }},
		{"ListRuns", func() error { _, err := st.ListRuns(ctx, workflowv1.ListRunsRequest{}); return err }},
		{"ReconcileRuns", func() error { _, err := st.ReconcileRuns(ctx, now); return err }},
		{"Enqueue", func() error {
			_, err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem("run-1", "wi-1", false)})
			return err
		}},
		{"ReconcileInstances", func() error {
			return st.ReconcileInstances(ctx, now, []workflowv1.DesiredCollectorInstance{{
				InstanceID: "i-1", CollectorKind: "jira", Mode: workflowv1.CollectorModeManual,
			}})
		}},
		{"ListInstances", func() error { _, err := st.ListInstances(ctx); return err }},
		{"UpsertCompleteness", func() error {
			return st.UpsertCompleteness(ctx, []workflowv1.CompletenessState{{
				RunID: "run-1", CollectorKind: "jira", Keyspace: "k", PhaseName: "p",
				Status: "complete", ObservedAt: now, UpdatedAt: now,
			}})
		}},
		{"GetCompleteness", func() error { _, err := st.GetCompleteness(ctx, "run-1"); return err }},
		{"ReportProgress", func() error {
			return st.ReportProgress(ctx, workflowv1.ReportRunProgressRequest{
				RunID: "run-1", ObservedAt: now,
				Progress: []workflowv1.CollectorRunProgress{{CollectorKind: "jira"}},
			})
		}},
		{"GetProgress", func() error { _, err := st.GetProgress(ctx, "run-1"); return err }},
		{"QueueDepth", func() error { _, err := st.QueueDepth(ctx, now); return err }},
		{"GetRunStatus", func() error { _, err := st.GetRunStatus(ctx, "run-1", now); return err }},
		{"ClaimNext", func() error { _, err := st.ClaimNext(ctx, selector, now, time.Minute); return err }},
		{"Heartbeat", func() error { return st.Heartbeat(ctx, mutation) }},
		{"Complete", func() error { return st.Complete(ctx, mutation) }},
		{"Release", func() error { return st.Release(ctx, mutation) }},
		{"FailRetryable", func() error { return st.FailRetryable(ctx, mutation) }},
		{"FailTerminal", func() error { return st.FailTerminal(ctx, mutation) }},
		{"Reap", func() error { _, err := st.Reap(ctx, now, 10, time.Second); return err }},
		{"PinForCommit", func() error {
			_, err := st.PinForCommit(ctx, workflowv1.CommitPinRequest{
				ClaimID: "c-1", FencingToken: 1, ScopeID: "s", SourceRunID: "sr",
				GenerationID: "g", CommitDeadline: now.Add(time.Minute),
			})
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatal("returned nil on a closed database pool")
			}
			if isCode(err, workflowv1.CodeInvalid) || isCode(err, workflowv1.CodeStaleFence) {
				t.Fatalf("database loss misreported as a caller error: %v", err)
			}
		})
	}
}

// TestDatabaseLossWithPacingAndQuotasStillErrors requires the degraded
// fail-open branches to hold only for the pacing and quota reads: the
// claim statement itself failing must still surface as an error.
func TestDatabaseLossWithPacingAndQuotasStillErrors(t *testing.T) {
	ctx := context.Background()
	st := closedStore(t)
	if err := st.Configure(newTestConfig(t, map[string]int{"jira": 5})); err != nil {
		t.Fatalf("configure: %v", err)
	}
	now := time.Now().UTC()
	selector := workflowv1.ClaimSelector{
		CollectorKind: "jira", CollectorInstanceID: "jira-primary", OwnerID: "o-1", ClaimID: "c-1",
	}
	// The presence lookup runs before the pacing, quota, and claim reads,
	// so a dead database surfaces there first. Either way the failure is
	// an error, never idle.
	if _, err := st.ClaimNext(ctx, selector, now, time.Minute); err == nil {
		t.Fatal("ClaimNext returned nil with pacing, quota, and claim reads all failing")
	} else if !strings.Contains(err.Error(), "read instance presence") {
		t.Fatalf("ClaimNext error = %v, want the instance-presence failure", err)
	}
	if _, err := st.CheckPacing(ctx, "jira", "jira-primary", now); err == nil {
		t.Fatal("CheckPacing returned nil with the depth read failing")
	} else if !strings.Contains(err.Error(), "pacing depths") {
		t.Fatalf("CheckPacing error = %v, want a pacing depths error", err)
	}
}

func TestOpenRejectsBadConfigurations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, err := claimstore.Open(ctx, "postgres://%zz"); err == nil ||
		!strings.Contains(err.Error(), "parse claim-plane database config") {
		t.Fatalf("malformed DSN error = %v, want a parse error", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	if _, err := claimstore.Open(ctx, "postgres://u:p@"+addr+"/d?sslmode=disable&connect_timeout=1"); err == nil ||
		!strings.Contains(err.Error(), "ping claim-plane database") {
		t.Fatalf("unreachable database error = %v, want a ping error", err)
	}
}

func TestConfigureRejectsInvalidPolicy(t *testing.T) {
	st, _ := openFreshStore(t)
	err := st.Configure(claimstore.Config{
		Pacing: workflowv1.ClaimPacingConfig{DomainHigh: 5},
	})
	if err == nil || !strings.Contains(err.Error(), "claim pacing config") {
		t.Fatalf("half-configured pacing error = %v, want a claim pacing config error", err)
	}
	err = st.Configure(claimstore.Config{Quotas: map[string]int{"jira": -1}})
	if err == nil || !strings.Contains(err.Error(), `quota for "jira"`) {
		t.Fatalf("negative quota error = %v, want it to name the kind", err)
	}
}

func TestPingTableReportsMissingTable(t *testing.T) {
	st, ctx := openFreshStore(t)
	if err := st.PingTable(ctx, "workflow_claims"); err != nil {
		t.Fatalf("migrated table reported missing: %v", err)
	}
	err := st.PingTable(ctx, "no_such_table_here")
	if err == nil || !strings.Contains(err.Error(), "is missing") {
		t.Fatalf("missing table error = %v, want it to say the table is missing", err)
	}
}
