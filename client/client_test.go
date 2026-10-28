// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package client_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/client"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestClientControlPlaneRoundTrip requires the six coordinator seams to
// travel real routes: the client must implement every one over HTTP,
// never answer ErrClaimPlaneUnsupported.
func TestClientControlPlaneRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	var gotVersion string
	mux := http.NewServeMux()
	mux.HandleFunc("POST /workflow.v1/CreateRun", func(w http.ResponseWriter, r *http.Request) {
		gotVersion = r.Header.Get(workflowv1.VersionHeader)
		var body workflowv1.CreateRunRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode create run: %v", err)
		}
		_ = json.NewEncoder(w).Encode(workflowv1.CreateRunResponse{RunID: body.Run.RunID})
	})
	mux.HandleFunc("POST /workflow.v1/EnqueueWorkItems", func(w http.ResponseWriter, r *http.Request) {
		var body workflowv1.EnqueueWorkItemsRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode enqueue: %v", err)
		}
		_ = json.NewEncoder(w).Encode(workflowv1.EnqueueWorkItemsResponse{
			Accepted: len(body.Items), InsertedWorkItems: len(body.Items),
		})
	})
	mux.HandleFunc("POST /workflow.v1/ReconcileCollectorInstances", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /workflow.v1/UpsertCompletenessStates", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /workflow.v1/ReconcileWorkflowRuns", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(workflowv1.ReconcileWorkflowRunsResponse{Transitioned: 3})
	})
	mux.HandleFunc("POST /workflow.v1/ReapExpiredClaims", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(workflowv1.ReapExpiredClaimsResponse{Claims: []workflowv1.Claim{}})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	plane, err := client.New(srv.URL)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	ctx := t.Context()
	run := workflowv1.Run{
		RunID: "run-1", TriggerKind: workflowv1.TriggerKindSchedule,
		Status: workflowv1.RunStatusCollectionActive, CreatedAt: now, UpdatedAt: now,
	}
	if err := plane.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	item := workflowv1.WorkItem{
		WorkItemID: "item-1", RunID: "run-1", CollectorKind: "jira",
		CollectorInstanceID: "i-1", SourceSystem: "jira", ScopeID: "scope-1",
		AcceptanceUnitID: "au-1", SourceRunID: "sr-1", GenerationID: "gen-1",
		Status: workflowv1.WorkItemStatusPending, CreatedAt: now, UpdatedAt: now,
	}
	inserted, err := plane.EnqueueWorkItems(ctx, []workflowv1.WorkItem{item, item})
	if err != nil {
		t.Fatalf("EnqueueWorkItems: %v", err)
	}
	if inserted != 2 {
		t.Fatalf("EnqueueWorkItems inserted = %d, want 2", inserted)
	}
	if err := plane.ReconcileCollectorInstances(ctx, now, []workflowv1.DesiredCollectorInstance{{
		InstanceID: "i-1", CollectorKind: "jira", Mode: workflowv1.CollectorModeManual,
	}}); err != nil {
		t.Fatalf("ReconcileCollectorInstances: %v", err)
	}
	if err := plane.UpsertCompletenessStates(ctx, []workflowv1.CompletenessState{{
		RunID: "run-1", CollectorKind: "jira", Keyspace: "facts",
		PhaseName: "publish", Status: "complete", ObservedAt: now, UpdatedAt: now,
	}}); err != nil {
		t.Fatalf("UpsertCompletenessStates: %v", err)
	}
	transitioned, err := plane.ReconcileWorkflowRuns(ctx, now)
	if err != nil {
		t.Fatalf("ReconcileWorkflowRuns: %v", err)
	}
	if transitioned != 3 {
		t.Fatalf("transitioned = %d, want 3", transitioned)
	}
	claims, err := plane.ReapExpiredClaims(ctx, now, 10, time.Minute)
	if err != nil {
		t.Fatalf("ReapExpiredClaims: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("claims = %d, want 0", len(claims))
	}
	if gotVersion != "1" {
		t.Fatalf("version header = %q, want 1", gotVersion)
	}
}

// TestClientMapsProtocolErrors requires wire error envelopes to surface
// as coded protocol errors the coordinator can switch on, so a stale
// fence never retries the same mutation.
func TestClientMapsProtocolErrors(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /workflow.v1/HeartbeatClaim", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(workflowv1.ErrorEnvelope{Code: workflowv1.CodeStaleFence, Message: "epoch superseded"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	plane, err := client.New(srv.URL)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	now := time.Now().UTC()
	err = plane.Heartbeat(t.Context(), workflowv1.ClaimMutation{
		WorkItemID: "item-1", ClaimID: "c-1", FencingToken: 1,
		OwnerID: "o-1", ObservedAt: now, LeaseDuration: time.Minute,
	})
	var proto *workflowv1.ProtocolError
	if !errors.As(err, &proto) {
		t.Fatalf("heartbeat error type = %T, want *ProtocolError", err)
	}
	if proto.Code != workflowv1.CodeStaleFence {
		t.Fatalf("heartbeat error code = %q, want stale_fence", proto.Code)
	}
}

// TestClientRequiresBaseURL requires an empty service address to fail
// fast instead of posting claims at an accidental target.
func TestClientRequiresBaseURL(t *testing.T) {
	if _, err := client.New(""); err == nil {
		t.Fatal("empty base URL accepted")
	}
	if _, err := client.New("://bad-url"); err == nil {
		t.Fatal("malformed base URL accepted")
	}
}
