// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/store"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// strandedItem builds a sched-shaped pending item for one instance,
// mirroring a reconciler-seeded row that outlived its instance.
func strandedItem(runID, instanceID, scope string) workflowv1.WorkItem {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return workflowv1.WorkItem{
		WorkItemID:          "sched-jira-" + instanceID + "-w0/" + scope,
		RunID:               runID,
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: instanceID,
		SourceSystem:        "jira",
		ScopeID:             scope,
		AcceptanceUnitID:    "au-" + scope,
		SourceRunID:         "sr-" + scope,
		GenerationID:        "gen-" + scope,
		FairnessKey:         "jira:" + instanceID + ":scheduled",
		Status:              workflowv1.WorkItemStatusPending,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
}

func declareInstance(enabled, claimsEnabled bool) workflowv1.DesiredCollectorInstance {
	return workflowv1.DesiredCollectorInstance{
		InstanceID: "drill-instance-1", CollectorKind: workflowv1.CollectorKind("jira"),
		Mode: workflowv1.CollectorModeScheduled, Enabled: enabled, ClaimsEnabled: claimsEnabled,
	}
}

func mustClaim(
	t *testing.T,
	ctx context.Context,
	st *claimstore.Store,
	selector workflowv1.ClaimSelector,
	now time.Time,
) workflowv1.ClaimNextResponse {
	t.Helper()
	resp, err := st.ClaimNext(ctx, selector, now, time.Minute)
	if err != nil {
		t.Fatalf("claim next: %v", err)
	}
	return resp
}

// TestDecommissionedInstanceStaysIdle requires the stranded shape to go
// quiet: an item seeded while its instance was enabled, reaped after its
// lease expired, then decommissioned out of the desired set, must not be
// issued again. Before the fix the claim path had no instance-state
// predicate, so the dry claim re-issued the orphan and this test failed.
func TestDecommissionedInstanceStaysIdle(t *testing.T) {
	st, ctx := openFreshStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := st.ReconcileInstances(ctx, now, []workflowv1.DesiredCollectorInstance{
		declareInstance(true, true),
	}); err != nil {
		t.Fatalf("reconcile instances: %v", err)
	}
	runID := seedRun(t, ctx, st)
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{
		strandedItem(runID, "drill-instance-1", "scope-a"),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	selector := workflowv1.ClaimSelector{
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: "drill-instance-1",
		OwnerID:             "owner-a",
		ClaimID:             "claim-a",
	}
	first := mustClaim(t, ctx, st, selector, now)
	if !first.Issued {
		t.Fatalf("enabled instance issued nothing, want the seeded item")
	}
	reaped, err := st.Reap(ctx, now.Add(2*time.Minute), 10, 0)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(reaped) != 1 {
		t.Fatalf("reaped %d claims, want 1 (the expired epoch)", len(reaped))
	}
	// The revert drops the instance from the desired set: the row is
	// deactivated but the pending item survives. That is the orphan.
	if err := st.ReconcileInstances(ctx, now, nil); err != nil {
		t.Fatalf("decommission instances: %v", err)
	}
	instances, err := st.ListInstances(ctx)
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	if len(instances) != 1 || instances[0].Enabled || instances[0].ClaimsEnabled {
		t.Fatalf("instance after decommission = %+v, want one disabled row", instances)
	}
	selector.OwnerID = "owner-b"
	selector.ClaimID = "claim-b"
	second := mustClaim(t, ctx, st, selector, now.Add(2*time.Minute))
	if second.Issued {
		t.Fatalf("decommissioned instance issued %q, want idle", second.Grant.WorkItem.WorkItemID)
	}
}

// TestClaimsDisabledInstanceStaysIdle requires a declared but
// claims-disabled instance (the live rehearsal shape: desired, yet
// claims_enabled=false) to issue nothing.
func TestClaimsDisabledInstanceStaysIdle(t *testing.T) {
	st, ctx := openFreshStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := st.ReconcileInstances(ctx, now, []workflowv1.DesiredCollectorInstance{
		declareInstance(true, false),
	}); err != nil {
		t.Fatalf("reconcile instances: %v", err)
	}
	runID := seedRun(t, ctx, st)
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{
		strandedItem(runID, "drill-instance-1", "scope-a"),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	resp := mustClaim(t, ctx, st, workflowv1.ClaimSelector{
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: "drill-instance-1",
		OwnerID:             "owner-a",
		ClaimID:             "claim-a",
	}, now)
	if resp.Issued {
		t.Fatalf("claims-disabled instance issued %q, want idle", resp.Grant.WorkItem.WorkItemID)
	}
}

// TestGatedInstanceIdlesOnQuotaPath pins the quota statement's gate: with
// a quota configured and exhausted, a claims-disabled instance must answer
// idle (no grant and no deferral), not quota-deferred. Before the probe
// fix the throttle probe ignored instance state, so this claim deferred
// and the test failed.
func TestGatedInstanceIdlesOnQuotaPath(t *testing.T) {
	st, ctx := openFreshStore(t)
	if err := st.Configure(claimstore.Config{Quotas: map[string]int{"jira": 1}}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := st.ReconcileInstances(ctx, now, []workflowv1.DesiredCollectorInstance{
		declareInstance(true, true),
	}); err != nil {
		t.Fatalf("reconcile instances: %v", err)
	}
	runID := seedRun(t, ctx, st)
	first := strandedItem(runID, "drill-instance-1", "scope-q")
	second := strandedItem(runID, "drill-instance-1", "scope-q")
	second.WorkItemID = "sched-jira-drill-instance-1-w1/scope-q"
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{first, second}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	selector := workflowv1.ClaimSelector{
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: "drill-instance-1",
		OwnerID:             "owner-q0",
		ClaimID:             "claim-q0",
	}
	if first := mustClaim(t, ctx, st, selector, now); !first.Issued {
		t.Fatalf("enabled instance issued nothing, want the first item (quota now exhausted)")
	}
	if err := st.ReconcileInstances(ctx, now, []workflowv1.DesiredCollectorInstance{
		declareInstance(true, false),
	}); err != nil {
		t.Fatalf("disable claims: %v", err)
	}
	selector.OwnerID = "owner-q1"
	selector.ClaimID = "claim-q1"
	resp := mustClaim(t, ctx, st, selector, now)
	if resp.Issued {
		t.Fatalf("claims-disabled instance issued %q on the quota path, want idle", resp.Grant.WorkItem.WorkItemID)
	}
	if resp.Deferral != (workflowv1.ClaimPacingDeferral{}) {
		t.Fatalf("claims-disabled instance deferred %+v on the quota path, want idle", resp.Deferral)
	}
}

// TestDecommissionedInstanceIdlesOnQuotaPath is the decommissioned twin of
// the gate above: the instance row survives the revert as disabled while
// the pending item and the exhausted quota survive with it.
func TestDecommissionedInstanceIdlesOnQuotaPath(t *testing.T) {
	st, ctx := openFreshStore(t)
	if err := st.Configure(claimstore.Config{Quotas: map[string]int{"jira": 1}}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := st.ReconcileInstances(ctx, now, []workflowv1.DesiredCollectorInstance{
		declareInstance(true, true),
	}); err != nil {
		t.Fatalf("reconcile instances: %v", err)
	}
	runID := seedRun(t, ctx, st)
	first := strandedItem(runID, "drill-instance-1", "scope-q")
	second := strandedItem(runID, "drill-instance-1", "scope-q")
	second.WorkItemID = "sched-jira-drill-instance-1-w1/scope-q"
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{first, second}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	selector := workflowv1.ClaimSelector{
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: "drill-instance-1",
		OwnerID:             "owner-q0",
		ClaimID:             "claim-q0",
	}
	if first := mustClaim(t, ctx, st, selector, now); !first.Issued {
		t.Fatalf("enabled instance issued nothing, want the first item (quota now exhausted)")
	}
	if err := st.ReconcileInstances(ctx, now, nil); err != nil {
		t.Fatalf("decommission instances: %v", err)
	}
	selector.OwnerID = "owner-q1"
	selector.ClaimID = "claim-q1"
	resp := mustClaim(t, ctx, st, selector, now)
	if resp.Issued {
		t.Fatalf("decommissioned instance issued %q on the quota path, want idle", resp.Grant.WorkItem.WorkItemID)
	}
	if resp.Deferral != (workflowv1.ClaimPacingDeferral{}) {
		t.Fatalf("decommissioned instance deferred %+v on the quota path, want idle", resp.Deferral)
	}
}

// TestGatedInstanceIdlesWithQuotaAvailable isolates the quota statement's
// issuing gate: quota is fresh (grants remain), so only the statement gate
// stands between the pending item and a grant. Removing the statement gate
// issues here, while the exhausted-quota twins above would still pass.
func TestGatedInstanceIdlesWithQuotaAvailable(t *testing.T) {
	st, ctx := openFreshStore(t)
	if err := st.Configure(claimstore.Config{Quotas: map[string]int{"jira": 5}}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := st.ReconcileInstances(ctx, now, []workflowv1.DesiredCollectorInstance{
		declareInstance(true, false),
	}); err != nil {
		t.Fatalf("reconcile instances: %v", err)
	}
	runID := seedRun(t, ctx, st)
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{
		strandedItem(runID, "drill-instance-1", "scope-q"),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	resp := mustClaim(t, ctx, st, workflowv1.ClaimSelector{
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: "drill-instance-1",
		OwnerID:             "owner-q",
		ClaimID:             "claim-q",
	}, now)
	if resp.Issued {
		t.Fatalf("claims-disabled instance issued %q with quota available, want idle", resp.Grant.WorkItem.WorkItemID)
	}
	if resp.Deferral != (workflowv1.ClaimPacingDeferral{}) {
		t.Fatalf("claims-disabled instance deferred %+v with quota available, want idle", resp.Deferral)
	}
}

// TestGatedInstanceIdlesWithFloorExhausted covers the quota-floor error
// corner: the quota statement fails and the floor budget is spent, so only
// the instance gate stands between the pending item and a deferral. Before
// the floor fix the exhausted branch deferred; it must answer idle with
// the floor reason instead, without burning budget it never grants.
func TestGatedInstanceIdlesWithFloorExhausted(t *testing.T) {
	st, ctx := openFreshStore(t)
	if err := st.Configure(claimstore.Config{Quotas: map[string]int{"jira": 1}}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	// Anchor mid-hour so the floor-burn claims and the final claim share
	// one hourly floor window; a run starting in the last seconds of an
	// hour would otherwise straddle the window and pass vacuously.
	now := time.Now().UTC().Truncate(time.Hour).Add(5 * time.Minute)
	if err := st.ReconcileInstances(ctx, now, []workflowv1.DesiredCollectorInstance{
		declareInstance(true, true),
	}); err != nil {
		t.Fatalf("reconcile instances: %v", err)
	}
	runID := seedRun(t, ctx, st)
	var items []workflowv1.WorkItem
	for i := 0; i < claimstore.DefaultQuotaFloorGrantsPerHour+1; i++ {
		item := strandedItem(runID, "drill-instance-1", "scope-f")
		item.WorkItemID = "sched-jira-drill-instance-1-wfloor-" + string(rune('a'+i)) + "/floor-scope"
		item.ScopeID = "floor-scope"
		item.AcceptanceUnitID = "au-floor-scope"
		item.SourceRunID = "sr-floor-scope"
		item.GenerationID = "gen-floor-scope"
		items = append(items, item)
	}
	if _, err := st.Enqueue(ctx, items); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	// Fail the quota statement so every claim runs the floor path, then
	// spend the whole floor budget through the still-enabled instance.
	if err := st.DropTableForTest(ctx, "component_quota_windows"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	restoreMigration(t, ctx, st, "002_pacing_quota")
	for i := 0; i < claimstore.DefaultQuotaFloorGrantsPerHour; i++ {
		resp, err := st.ClaimNext(ctx, workflowv1.ClaimSelector{
			CollectorKind:       workflowv1.CollectorKind("jira"),
			CollectorInstanceID: "drill-instance-1",
			OwnerID:             "owner-f",
			ClaimID:             "claim-floor-burn-" + string(rune('a'+i)),
		}, now.Add(time.Duration(i)*time.Second), time.Minute)
		if err != nil {
			t.Fatalf("floor burn claim %d: %v", i, err)
		}
		if !resp.Issued {
			t.Fatalf("floor burn claim %d issued nothing, want a floor grant", i)
		}
	}
	if err := st.ReconcileInstances(ctx, now, []workflowv1.DesiredCollectorInstance{
		declareInstance(true, false),
	}); err != nil {
		t.Fatalf("disable claims: %v", err)
	}
	resp := mustClaim(t, ctx, st, workflowv1.ClaimSelector{
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: "drill-instance-1",
		OwnerID:             "owner-g",
		ClaimID:             "claim-gated-floor",
	}, now)
	if resp.Issued {
		t.Fatalf("claims-disabled instance issued %q on the exhausted floor, want idle", resp.Grant.WorkItem.WorkItemID)
	}
	if resp.Deferral != (workflowv1.ClaimPacingDeferral{}) {
		t.Fatalf("claims-disabled instance deferred %+v on the exhausted floor, want idle", resp.Deferral)
	}
	if resp.DegradedReason != "quota_floor" {
		t.Fatalf("degraded reason = %q, want quota_floor", resp.DegradedReason)
	}
}

// TestEnabledInstanceStillIssues guards the other side of the gate: an
// enabled, claims-enabled instance keeps issuing its pending items.
func TestEnabledInstanceStillIssues(t *testing.T) {
	st, ctx := openFreshStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := st.ReconcileInstances(ctx, now, []workflowv1.DesiredCollectorInstance{
		declareInstance(true, true),
	}); err != nil {
		t.Fatalf("reconcile instances: %v", err)
	}
	runID := seedRun(t, ctx, st)
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{
		strandedItem(runID, "drill-instance-1", "scope-a"),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	resp := mustClaim(t, ctx, st, workflowv1.ClaimSelector{
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: "drill-instance-1",
		OwnerID:             "owner-a",
		ClaimID:             "claim-a",
	}, now)
	if !resp.Issued {
		t.Fatalf("enabled instance issued nothing, want the seeded item")
	}
}
