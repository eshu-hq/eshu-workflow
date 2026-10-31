// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/server"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestReadPlaneServesRealState pins the behavioral contract of the five
// protocol routes core never calls: every read/report route below resolves
// through the service's own database, reflects writes the report plane
// took, and (for reads) changes no state. This is the removal-gate pin
// for #9376: the routes must hold this shape after the satellite owns
// them, or the in-tree removal in eshu#8875 cannot land.
func TestReadPlaneServesRealState(t *testing.T) {
	st, _ := liveStore(t)
	srv, err := server.New(st, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	now := time.Now().UTC().Truncate(time.Millisecond)
	const runID = "pin-5r1"
	const instanceID = "pin-5r1-i1"

	must := func(path string, reqBody, respBody any) {
		t.Helper()
		if status := call(t, httpSrv, path, reqBody, respBody); status != http.StatusOK {
			t.Fatalf("POST %s status = %d, want 200", path, status)
		}
	}

	var admitted workflowv1.CreateRunResponse
	must("/workflow.v1/CreateRun", workflowv1.CreateRunRequest{Run: workflowv1.Run{
		RunID: runID, TriggerKind: workflowv1.TriggerKindSchedule,
		Status:    workflowv1.RunStatusCollectionActive,
		CreatedAt: now, UpdatedAt: now,
	}}, &admitted)
	if admitted.RunID != runID {
		t.Fatalf("CreateRun run_id = %q, want %q", admitted.RunID, runID)
	}

	item := func(id, scope string) workflowv1.WorkItem {
		return workflowv1.WorkItem{
			WorkItemID: id, RunID: runID,
			CollectorKind: "jira", CollectorInstanceID: instanceID,
			SourceSystem: "jira", ScopeID: scope,
			TenantID: "tenant-pin", WorkspaceID: "workspace-pin",
			SubjectClass: "project", PolicyRevisionHash: "policy-pin",
			AcceptanceUnitID: "au-pin", SourceRunID: "sr-pin", GenerationID: "gen-pin",
			Status:    workflowv1.WorkItemStatusPending,
			CreatedAt: now, UpdatedAt: now,
		}
	}
	var enqueued workflowv1.EnqueueWorkItemsResponse
	must("/workflow.v1/EnqueueWorkItems", workflowv1.EnqueueWorkItemsRequest{
		Items: []workflowv1.WorkItem{item("pin-5r1-a", "scope-a"), item("pin-5r1-b", "scope-b")},
	}, &enqueued)
	if enqueued.Accepted != 2 || enqueued.InsertedWorkItems != 2 {
		t.Fatalf("Enqueue = %+v, want accepted=2 inserted=2", enqueued)
	}

	must("/workflow.v1/ReconcileCollectorInstances",
		workflowv1.ReconcileCollectorInstancesRequest{
			ObservedAt: now,
			Instances: []workflowv1.DesiredCollectorInstance{{
				InstanceID: instanceID, CollectorKind: "jira", Mode: "manual",
			}},
		}, &map[string]any{})

	must("/workflow.v1/UpsertCompletenessStates",
		workflowv1.UpsertCompletenessStatesRequest{States: []workflowv1.CompletenessState{{
			RunID: runID, CollectorKind: "jira", Keyspace: "facts",
			PhaseName: "publish", Status: "complete",
			ObservedAt: now, UpdatedAt: now,
		}}}, &map[string]any{})

	var completeness workflowv1.GetRunCompletenessResponse
	must("/workflow.v1/GetRunCompleteness",
		workflowv1.GetRunCompletenessRequest{RunID: runID}, &completeness)
	if completeness.RunID != runID || len(completeness.States) != 1 ||
		completeness.States[0].PhaseName != "publish" {
		t.Fatalf("GetRunCompleteness = %+v, want the publish checkpoint", completeness)
	}

	must("/workflow.v1/ReportRunProgress", workflowv1.ReportRunProgressRequest{
		RunID: runID, ObservedAt: now,
		Progress: []workflowv1.CollectorRunProgress{{
			CollectorKind: "jira", TotalWorkItems: 2,
			PublishedPhaseCounts: map[string]int{},
		}},
	}, &map[string]any{})

	var statusResp workflowv1.GetRunStatusResponse
	must("/workflow.v1/GetRunStatus",
		workflowv1.GetRunStatusRequest{RunID: runID}, &statusResp)
	if statusResp.Run.RunID != runID {
		t.Fatalf("GetRunStatus run = %q, want %q", statusResp.Run.RunID, runID)
	}
	if len(statusResp.Progress) != 1 || statusResp.Progress[0].TotalWorkItems != 2 {
		t.Fatalf("GetRunStatus progress = %+v, want the reported total=2", statusResp.Progress)
	}
	if len(statusResp.Completeness) != 1 {
		t.Fatalf("GetRunStatus completeness = %+v, want the upserted checkpoint", statusResp.Completeness)
	}

	depthOf := func() workflowv1.QueueDepthSnapshot {
		t.Helper()
		var depth workflowv1.QueueDepthSnapshot
		must("/workflow.v1/QueueDepthSnapshot", nil, &depth)
		return depth
	}
	depth := depthOf()
	if depth.GlobalPending != 2 || depth.GlobalClaimed != 0 {
		t.Fatalf("depth = %+v, want pending=2 claimed=0", depth)
	}

	// Reads change nothing: the same snapshot before and after the full
	// read plane.
	var runs workflowv1.ListRunsResponse
	must("/workflow.v1/ListRuns", workflowv1.ListRunsRequest{}, &runs)
	if len(runs.Runs) != 1 || runs.Runs[0].RunID != runID {
		t.Fatalf("ListRuns = %+v, want the one run", runs)
	}
	var instances workflowv1.ListCollectorInstancesResponse
	must("/workflow.v1/ListCollectorInstances", nil, &instances)
	found := false
	for _, inst := range instances.Instances {
		if inst.InstanceID == instanceID {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListCollectorInstances = %+v, want %q declared", instances, instanceID)
	}
	after := depthOf()
	if after.GlobalPending != depth.GlobalPending || after.GlobalClaimed != depth.GlobalClaimed {
		t.Fatalf("reads moved depth %+v -> %+v", depth, after)
	}

	// Drain through the claim plane; the depth arithmetic must track it.
	claim := func(owner, claimID string) workflowv1.ClaimNextResponse {
		t.Helper()
		var out workflowv1.ClaimNextResponse
		must("/workflow.v1/ClaimNext", workflowv1.ClaimNextRequest{
			Selector: workflowv1.ClaimSelector{
				CollectorKind: "jira", CollectorInstanceID: instanceID,
				OwnerID: owner, ClaimID: claimID,
			},
			ObservedAt: now, LeaseTTL: time.Minute,
		}, &out)
		if !out.Issued {
			t.Fatalf("ClaimNext %s issued = false", claimID)
		}
		return out
	}
	finish := func(grant workflowv1.ClaimNextResponse, owner, claimID string) {
		t.Helper()
		mut := workflowv1.ClaimMutation{
			WorkItemID: grant.Grant.WorkItem.WorkItemID, ClaimID: claimID,
			FencingToken: grant.Grant.Claim.FencingToken, OwnerID: owner,
			ObservedAt: now, LeaseDuration: time.Minute,
		}
		must("/workflow.v1/HeartbeatClaim", mut, &map[string]any{})
		must("/workflow.v1/ValidateAndPinClaimForCommit", workflowv1.CommitPinRequest{
			ClaimID: claimID, FencingToken: grant.Grant.Claim.FencingToken,
			ScopeID:        grant.Grant.WorkItem.ScopeID,
			SourceRunID:    grant.Grant.WorkItem.SourceRunID,
			GenerationID:   grant.Grant.WorkItem.GenerationID,
			CommitDeadline: now.Add(time.Hour),
		}, &map[string]any{})
		must("/workflow.v1/CompleteClaim", mut, &map[string]any{})
	}
	finish(claim("pin-owner", "pin-claim-1"), "pin-owner", "pin-claim-1")
	mid := depthOf()
	if mid.GlobalPending != 1 || mid.GlobalClaimed != 0 {
		t.Fatalf("mid depth = %+v, want pending=1 claimed=0", mid)
	}
	finish(claim("pin-owner", "pin-claim-2"), "pin-owner", "pin-claim-2")
	empty := depthOf()
	if empty.GlobalPending != 0 || empty.GlobalClaimed != 0 {
		t.Fatalf("final depth = %+v, want drained", empty)
	}
}
