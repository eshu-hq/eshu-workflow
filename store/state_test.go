// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"testing"
	"time"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestRunLifecycle covers run create/get/list pagination and the drain
// reconciliation that completes a run whose queue emptied.
func TestRunLifecycle(t *testing.T) {
	st, ctx := openFreshStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, id := range []string{"list-0", "list-1", "list-2"} {
		if err := st.CreateRun(ctx, workflowv1.Run{
			RunID: id, TriggerKind: workflowv1.TriggerKindSchedule,
			Status:    workflowv1.RunStatusCollectionActive,
			CreatedAt: now, UpdatedAt: now,
		}); err != nil {
			t.Fatalf("create run: %v", err)
		}
	}
	if _, err := st.GetRun(ctx, "missing"); !isCode(err, workflowv1.CodeNotFound) {
		t.Fatalf("get missing run err = %v, want not_found", err)
	}
	run, err := st.GetRun(ctx, "list-0")
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.RunID != "list-0" {
		t.Fatalf("run = %+v", run)
	}
	if err := st.CreateRun(ctx, workflowv1.Run{}); !isCode(err, workflowv1.CodeInvalid) {
		t.Fatalf("create invalid run err = %v, want invalid", err)
	}

	page, err := st.ListRuns(ctx, workflowv1.ListRunsRequest{Limit: 2})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if len(page.Runs) != 2 || page.NextCursor == "" {
		t.Fatalf("page = %+v, want 2 runs and a cursor", page)
	}
	rest, err := st.ListRuns(ctx, workflowv1.ListRunsRequest{Limit: 2, Cursor: page.NextCursor})
	if err != nil {
		t.Fatalf("list runs page 2: %v", err)
	}
	if len(rest.Runs) != 1 || rest.NextCursor != "" {
		t.Fatalf("page 2 = %+v, want 1 run and no cursor", rest)
	}
	if _, err := st.ListRuns(ctx, workflowv1.ListRunsRequest{Limit: -1}); !isCode(err, workflowv1.CodeInvalid) {
		t.Fatalf("negative limit err = %v, want invalid", err)
	}

	moved, err := st.ReconcileRuns(ctx, now)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if moved != 3 {
		t.Fatalf("reconciled = %d, want 3 drained runs", moved)
	}
	// A run with live work stays open.
	runID := seedRun(t, ctx, st)
	if err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "live-0", false)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	moved, err = st.ReconcileRuns(ctx, now)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if moved != 0 {
		t.Fatalf("reconciled = %d, want 0 with live work", moved)
	}
}

// TestInstanceAndCompleteness covers instance reconciliation (including
// stale deactivation), completeness upserts, and progress reporting with
// the assembled run status.
func TestInstanceAndCompleteness(t *testing.T) {
	st, ctx := openFreshStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	runID := seedRun(t, ctx, st)

	desired := []workflowv1.DesiredCollectorInstance{{
		InstanceID: "jira-primary", CollectorKind: "jira",
		Mode: workflowv1.CollectorModeScheduled, Enabled: true, ClaimsEnabled: true,
		Configuration: `{"key":"value"}`,
	}, {
		InstanceID: "jira-retired", CollectorKind: "jira",
		Mode: workflowv1.CollectorModeManual, Enabled: true,
	}}
	if err := st.ReconcileInstances(ctx, now, desired); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if err := st.ReconcileInstances(ctx, now, desired[:1]); err != nil {
		t.Fatalf("reconcile shrink: %v", err)
	}
	instances, err := st.ListInstances(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(instances) != 2 {
		t.Fatalf("instances = %d, want 2", len(instances))
	}
	for _, inst := range instances {
		if inst.InstanceID == "jira-retired" && inst.Enabled {
			t.Fatalf("retired instance still enabled: %+v", inst)
		}
	}
	if err := st.ReconcileInstances(ctx, now, []workflowv1.DesiredCollectorInstance{{
		InstanceID: "bad json", CollectorKind: "jira",
		Mode:          workflowv1.CollectorModeManual,
		Configuration: "{nope",
	}}); !isCode(err, workflowv1.CodeInvalid) {
		t.Fatalf("bad instance err = %v, want invalid", err)
	}

	state := workflowv1.CompletenessState{
		RunID: runID, CollectorKind: "jira", Keyspace: "code_entities_uid",
		PhaseName: "snapshot", Required: true, Status: "complete",
		ObservedAt: now, UpdatedAt: now,
	}
	if err := st.UpsertCompleteness(ctx, []workflowv1.CompletenessState{state}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	states, err := st.GetCompleteness(ctx, runID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(states) != 1 || states[0].PhaseName != "snapshot" {
		t.Fatalf("states = %+v", states)
	}
	if err := st.UpsertCompleteness(ctx, []workflowv1.CompletenessState{{}}); !isCode(err, workflowv1.CodeInvalid) {
		t.Fatalf("bad completeness err = %v, want invalid", err)
	}

	if err := st.ReportProgress(ctx, workflowv1.ReportRunProgressRequest{
		RunID: runID, ObservedAt: now,
		Progress: []workflowv1.CollectorRunProgress{{
			CollectorKind: "jira", TotalWorkItems: 1, CompletedWorkItems: 1,
		}},
	}); err != nil {
		t.Fatalf("report: %v", err)
	}
	if err := st.ReportProgress(ctx, workflowv1.ReportRunProgressRequest{
		RunID: "missing", ObservedAt: now,
		Progress: []workflowv1.CollectorRunProgress{{CollectorKind: "jira"}},
	}); !isCode(err, workflowv1.CodeNotFound) {
		t.Fatalf("report missing run err = %v, want not_found", err)
	}
	status, err := st.GetRunStatus(ctx, runID, now)
	if err != nil {
		t.Fatalf("run status: %v", err)
	}
	if len(status.Progress) != 1 || len(status.Completeness) != 1 {
		t.Fatalf("status = %+v", status)
	}
	if _, err := st.GetRunStatus(ctx, "missing", now); !isCode(err, workflowv1.CodeNotFound) {
		t.Fatalf("missing status err = %v, want not_found", err)
	}

	depth, err := st.QueueDepth(ctx, now)
	if err != nil {
		t.Fatalf("depth: %v", err)
	}
	if depth.GlobalPending != 0 {
		t.Fatalf("depth = %+v, want empty", depth)
	}
}

// TestClaimAliasesAndTerminalFail exercises the ControlStore-shaped
// aliases, terminal failure, and pin classification on unknown claims.
func TestClaimAliasesAndTerminalFail(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)
	if err := st.Enqueue(ctx, []workflowv1.WorkItem{
		seedItem(runID, "alias-0", false),
		seedItem(runID, "alias-1", false),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	grant := claimOne(t, ctx, st, "claim-alias", "owner-alias", time.Minute)
	m := mutationFor(grant.Grant, time.Minute)
	if err := st.HeartbeatClaim(ctx, m); err != nil {
		t.Fatalf("HeartbeatClaim alias: %v", err)
	}
	if err := st.ReleaseClaim(ctx, m); err != nil {
		t.Fatalf("ReleaseClaim alias: %v", err)
	}
	grant2 := claimOne(t, ctx, st, "claim-alias-2", "owner-alias", time.Minute)
	m2 := mutationFor(grant2.Grant, time.Minute)
	if err := st.CompleteClaim(ctx, m2); err != nil {
		t.Fatalf("CompleteClaim alias: %v", err)
	}

	third := claimOne(t, ctx, st, "claim-term", "owner-term", time.Minute)
	tm := mutationFor(third.Grant, time.Minute)
	tm.FailureClass = "provider_error"
	tm.FailureTerminal = true
	if err := st.FailTerminal(ctx, tm); err != nil {
		t.Fatalf("fail terminal: %v", err)
	}
	if err := st.FailTerminal(ctx, tm); err != nil {
		t.Fatalf("fail terminal replay: %v", err)
	}
	if err := st.Heartbeat(ctx, tm); !isCode(err, workflowv1.CodeStaleFence) {
		t.Fatalf("heartbeat on terminal err = %v, want stale_fence", err)
	}
	if _, err := st.Reap(ctx, time.Now().UTC(), 0, 0); !isCode(err, workflowv1.CodeInvalid) {
		t.Fatalf("zero-limit reap err = %v, want invalid", err)
	}

	if _, err := st.PinForCommit(ctx, workflowv1.CommitPinRequest{
		ClaimID: "nope", FencingToken: 1, ScopeID: "s", SourceRunID: "sr",
		GenerationID: "gen", CommitDeadline: time.Now().UTC().Add(time.Hour),
	}); !isCode(err, workflowv1.CodeNotFound) {
		t.Fatalf("pin unknown err = %v, want not_found", err)
	}
}

// TestQuotaFloorGrantsWhenQuotaStateUnreadable drops the quota windows
// table and requires claims to continue bounded by the degraded-mode floor,
// then restores the schema.
func TestQuotaFloorGrantsWhenQuotaStateUnreadable(t *testing.T) {
	st, ctx := openFreshStore(t)
	if err := st.Configure(newTestConfig(t, map[string]int{"jira": 2})); err != nil {
		t.Fatalf("configure: %v", err)
	}
	runID := seedRun(t, ctx, st)
	if err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "floor-0", false)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := st.DropTableForTest(ctx, "component_quota_windows"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	t.Cleanup(func() {
		if err := st.ForgetMigrationForTest(ctx, "002_pacing_quota"); err != nil {
			t.Fatalf("forget migration: %v", err)
		}
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("restore migrate: %v", err)
		}
	})
	resp, err := st.ClaimNext(ctx, workflowv1.ClaimSelector{
		CollectorKind: "jira", CollectorInstanceID: "jira-primary",
		OwnerID: "owner-floor", ClaimID: "claim-floor",
	}, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("floor claim: %v", err)
	}
	if !resp.Issued {
		t.Fatalf("floor claim did not issue under unreadable quota state")
	}
}

// TestPacingProbeReadsVerdict engages the latch through claims and requires
// the read-only probe to report paced without granting.
func TestPacingProbeReadsVerdict(t *testing.T) {
	st, ctx := openFreshStore(t)
	if err := st.Configure(newTestConfig(t, nil)); err != nil {
		t.Fatalf("configure: %v", err)
	}
	runID := seedRun(t, ctx, st)
	if err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "probe-0", false)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	probe, err := st.CheckPacing(ctx, "jira", "jira-primary", time.Now().UTC())
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if probe.Paced {
		t.Fatalf("probe paced below the high watermark")
	}
	if err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "probe-1", false)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	probe, err = st.CheckPacing(ctx, "jira", "jira-primary", time.Now().UTC())
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !probe.Paced || probe.Signal != workflowv1.ClaimPacingSignalDomain {
		t.Fatalf("probe = %+v, want paced on domain", probe)
	}
}
