// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package client_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/client"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestClientSpeaksEveryRoute requires every client method to survive a
// wire round trip: the coordinator's whole durable surface travels HTTP,
// so each method proves its request encodes and its response decodes.
func TestClientSpeaksEveryRoute(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var out any
		switch r.URL.Path {
		case "/workflow.v1/ClaimNext":
			out = map[string]any{"issued": false, "deferral": map[string]any{"signal": "domain", "domain": "jira/i-1"}}
		case "/workflow.v1/ValidateAndPinClaimForCommit":
			out = workflowv1.CommitPinResponse{Pinned: true, CommitID: "commit-1"}
		case "/workflow.v1/GetRunStatus":
			out = workflowv1.GetRunStatusResponse{
				Run: workflowv1.Run{
					RunID: "run-1", TriggerKind: workflowv1.TriggerKindSchedule,
					Status: workflowv1.RunStatusCollectionActive, CreatedAt: now, UpdatedAt: now,
				},
				AsOf: now,
			}
		case "/workflow.v1/ListRuns":
			out = workflowv1.ListRunsResponse{Runs: []workflowv1.RunSummary{}, AsOf: now}
		case "/workflow.v1/GetRunCompleteness":
			out = workflowv1.GetRunCompletenessResponse{RunID: "run-1", States: []workflowv1.CompletenessState{}, AsOf: now}
		case "/workflow.v1/ListCollectorInstances":
			out = workflowv1.ListCollectorInstancesResponse{Instances: []workflowv1.CollectorInstance{}, AsOf: now}
		case "/workflow.v1/QueueDepthSnapshot":
			out = workflowv1.QueueDepthSnapshot{AsOf: now}
		default:
			out = map[string]bool{"ok": true}
		}
		_ = json.NewEncoder(w).Encode(out)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	plane, err := client.New(srv.URL)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	ctx := t.Context()
	claimMutation := workflowv1.ClaimMutation{
		WorkItemID: "item-1", ClaimID: "c-1", FencingToken: 1,
		OwnerID: "o-1", ObservedAt: now, LeaseDuration: time.Minute,
	}
	if _, err := plane.ClaimNext(ctx, workflowv1.ClaimSelector{
		CollectorKind: "jira", CollectorInstanceID: "i-1", OwnerID: "o-1", ClaimID: "c-1",
	}, now, time.Minute); err != nil {
		t.Fatalf("ClaimNext: %v", err)
	}
	if _, err := plane.CheckClaimPacing(ctx, "jira", "i-1", now); err != nil {
		t.Fatalf("CheckClaimPacing: %v", err)
	}
	for name, call := range map[string]func() error{
		"Heartbeat":     func() error { return plane.Heartbeat(ctx, claimMutation) },
		"Complete":      func() error { return plane.Complete(ctx, claimMutation) },
		"Release":       func() error { return plane.Release(ctx, claimMutation) },
		"FailRetryable": func() error { return plane.FailRetryable(ctx, claimMutation) },
		"FailTerminal":  func() error { return plane.FailTerminal(ctx, claimMutation) },
	} {
		if err := call(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if _, err := plane.PinForCommit(ctx, workflowv1.CommitPinRequest{
		ClaimID: "c-1", FencingToken: 1, ScopeID: "scope-1",
		SourceRunID: "sr-1", GenerationID: "gen-1", CommitDeadline: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("PinForCommit: %v", err)
	}
	if err := plane.ReportRunProgress(ctx, workflowv1.ReportRunProgressRequest{
		RunID: "run-1", ObservedAt: now,
		Progress: []workflowv1.CollectorRunProgress{{CollectorKind: "jira"}},
	}); err != nil {
		t.Fatalf("ReportRunProgress: %v", err)
	}
	if _, err := plane.GetRunStatus(ctx, "run-1"); err != nil {
		t.Fatalf("GetRunStatus: %v", err)
	}
	if _, err := plane.ListRuns(ctx, workflowv1.ListRunsRequest{}); err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	if _, err := plane.GetRunCompleteness(ctx, "run-1"); err != nil {
		t.Fatalf("GetRunCompleteness: %v", err)
	}
	if _, err := plane.ListCollectorInstances(ctx); err != nil {
		t.Fatalf("ListCollectorInstances: %v", err)
	}
	if _, err := plane.QueueDepthSnapshot(ctx); err != nil {
		t.Fatalf("QueueDepthSnapshot: %v", err)
	}
}
