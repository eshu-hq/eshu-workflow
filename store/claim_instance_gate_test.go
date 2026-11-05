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
// issued again. Today the claim path has no instance-state predicate, so
// the dry claim re-issues the orphan and this test fails.
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
