// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"testing"
	"time"
)

// TestControlPlaneOperationsAreRouted requires the six coordinator seam
// operations to resolve to real protocol routes: adding an operation is a
// minor protocol change, so the route table must carry every one.
func TestControlPlaneOperationsAreRouted(t *testing.T) {
	want := []Operation{
		OpCreateRun,
		OpEnqueueWorkItems,
		OpReconcileCollectorInstances,
		OpUpsertCompletenessStates,
		OpReconcileWorkflowRuns,
		OpReapExpiredClaims,
	}
	for _, op := range want {
		route, ok := RouteFor(op)
		if !ok {
			t.Fatalf("operation %q has no protocol route", op)
		}
		if route.Method != "POST" {
			t.Fatalf("operation %q method = %q, want POST", op, route.Method)
		}
		if route.Path == "" {
			t.Fatalf("operation %q has an empty path", op)
		}
	}
	if len(RouteTable) != 20 {
		t.Fatalf("route table holds %d routes, want 20 (14 claim/report/read plus 6 control)", len(RouteTable))
	}
}

func TestCreateRunRequestValidate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	valid := CreateRunRequest{Run: Run{
		RunID:       "run-1",
		TriggerKind: TriggerKindSchedule,
		Status:      RunStatusCollectionActive,
		CreatedAt:   now,
		UpdatedAt:   now,
	}}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := valid
	bad.Run.RunID = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("blank run_id validated")
	}
}

func TestEnqueueWorkItemsRequestValidate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	item := WorkItem{
		WorkItemID: "item-1", RunID: "run-1", CollectorKind: "jira",
		CollectorInstanceID: "jira-primary", SourceSystem: "jira",
		ScopeID: "scope-1", AcceptanceUnitID: "au-1",
		SourceRunID: "sr-1", GenerationID: "gen-1",
		Status:    WorkItemStatusPending,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := (EnqueueWorkItemsRequest{Items: []WorkItem{item}}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (EnqueueWorkItemsRequest{}).Validate(); err == nil {
		t.Fatal("empty enqueue validated")
	}
	bad := item
	bad.WorkItemID = ""
	if err := (EnqueueWorkItemsRequest{Items: []WorkItem{bad}}).Validate(); err == nil {
		t.Fatal("blank work_item_id validated")
	}
}

func TestReconcileCollectorInstancesRequestValidate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	valid := ReconcileCollectorInstancesRequest{
		ObservedAt: now,
		Instances: []DesiredCollectorInstance{{
			InstanceID: "i-1", CollectorKind: "jira", Mode: CollectorModeManual,
		}},
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	// Empty declarations are valid: they deactivate everything absent.
	if err := (ReconcileCollectorInstancesRequest{ObservedAt: now}).Validate(); err != nil {
		t.Fatalf("empty declaration rejected: %v", err)
	}
	bad := valid
	bad.ObservedAt = time.Time{}
	if err := bad.Validate(); err == nil {
		t.Fatal("zero observed_at validated")
	}
	bad = valid
	bad.Instances[0].InstanceID = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("blank instance_id validated")
	}
}

func TestUpsertCompletenessStatesRequestValidate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	state := CompletenessState{
		RunID: "run-1", CollectorKind: "jira", Keyspace: "facts",
		PhaseName: "publish", Status: "complete",
		ObservedAt: now, UpdatedAt: now,
	}
	if err := (UpsertCompletenessStatesRequest{States: []CompletenessState{state}}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (UpsertCompletenessStatesRequest{}).Validate(); err == nil {
		t.Fatal("empty upsert validated")
	}
}

func TestReconcileWorkflowRunsRequestValidate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	if err := (ReconcileWorkflowRunsRequest{ObservedAt: now}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (ReconcileWorkflowRunsRequest{}).Validate(); err == nil {
		t.Fatal("zero observed_at validated")
	}
}

func TestReapExpiredClaimsRequestValidate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	valid := ReapExpiredClaimsRequest{ObservedAt: now, Limit: 10, RequeueDelay: time.Minute}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := valid
	bad.Limit = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("zero limit validated")
	}
	bad = valid
	bad.RequeueDelay = -time.Second
	if err := bad.Validate(); err == nil {
		t.Fatal("negative requeue delay validated")
	}
}
