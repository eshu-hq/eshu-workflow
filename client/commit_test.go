// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/client"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

func validClientCommitRequest() workflowv1.CommitClaimFactsRequest {
	return workflowv1.CommitClaimFactsRequest{
		ClaimMutation: workflowv1.ClaimMutation{
			WorkItemID: "item-1", ClaimID: "c-1", FencingToken: 7,
			OwnerID: "o-1", ObservedAt: time.Now().UTC(),
			LeaseDuration: time.Minute,
		},
		IdempotencyKey: "idem-1",
		ResultJSON:     json.RawMessage(`{"fact_rows":[]}`),
		ScopeID:        "scope-1",
		SourceRunID:    "sr-1",
		GenerationID:   "gen-1",
	}
}

// TestClientCommitClaimFactsPostsEnvelope requires the commit call to
// travel the proxy route with the version header and accept the ack.
func TestClientCommitClaimFactsPostsEnvelope(t *testing.T) {
	var gotPath, gotVersion string
	var gotBody workflowv1.CommitClaimFactsRequest
	srv := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter, request *http.Request,
	) {
		gotPath = request.URL.Path
		gotVersion = request.Header.Get(workflowv1.VersionHeader)
		if err := json.NewDecoder(request.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode commit: %v", err)
		}
		_ = json.NewEncoder(writer).Encode(map[string]bool{"ok": true})
	}))
	defer srv.Close()

	plane, err := client.New(srv.URL)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	sent := validClientCommitRequest()
	if err := plane.CommitClaimFacts(context.Background(), sent); err != nil {
		t.Fatalf("CommitClaimFacts: %v", err)
	}
	if gotPath != "/workflow.v1/CommitClaimFacts" {
		t.Fatalf("path = %q, want the commit route", gotPath)
	}
	if gotVersion != "1" {
		t.Fatalf("version header = %q, want 1", gotVersion)
	}
	if string(gotBody.ResultJSON) != string(sent.ResultJSON) {
		t.Fatalf("result_json = %s, want %s", gotBody.ResultJSON, sent.ResultJSON)
	}
}

// TestClientCommitClaimFactsSurfacesFenceRejection requires a 409 from
// the proxy (copied verbatim from core) to surface as a ProtocolError
// the caller switches on, never as a nil error.
func TestClientCommitClaimFactsSurfacesFenceRejection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter, _ *http.Request,
	) {
		writer.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(writer).Encode(workflowv1.ErrorEnvelope{
			Code: workflowv1.CodeStaleFence, Message: "epoch superseded",
		})
	}))
	defer srv.Close()

	plane, err := client.New(srv.URL)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	err = plane.CommitClaimFacts(context.Background(), validClientCommitRequest())
	var proto *workflowv1.ProtocolError
	if !errors.As(err, &proto) || proto.Code != workflowv1.CodeStaleFence {
		t.Fatalf("CommitClaimFacts error = %v, want stale_fence ProtocolError", err)
	}
}

// TestClientCommitClaimFactsRejectsOversizeResult requires the client to
// fail an over-cap result before any HTTP: it is terminal, never sent.
func TestClientCommitClaimFactsRejectsOversizeResult(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter, _ *http.Request,
	) {
		hits++
		_ = json.NewEncoder(writer).Encode(map[string]bool{"ok": true})
	}))
	defer srv.Close()

	plane, err := client.New(srv.URL)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	req := validClientCommitRequest()
	req.ResultJSON = json.RawMessage(strings.Repeat("x", workflowv1.MaxCommitResultBytes+1))
	if err := plane.CommitClaimFacts(context.Background(), req); err == nil {
		t.Fatal("oversize result committed without error")
	}
	if hits != 0 {
		t.Fatalf("oversize result sent %d requests, want 0", hits)
	}
}
