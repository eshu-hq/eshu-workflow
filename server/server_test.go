// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/server"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// fakeStore implements server.ClaimStore in memory for handler tests.
type fakeStore struct {
	claimErr error
	pinResp  workflowv1.CommitPinResponse
	paced    bool
	idle     bool
	calls    []string
}

func (f *fakeStore) note(op string) {
	f.calls = append(f.calls, op)
}

func (f *fakeStore) ClaimNext(
	_ context.Context,
	sel workflowv1.ClaimSelector,
	_ time.Time,
	_ time.Duration,
) (workflowv1.ClaimNextResponse, error) {
	f.note("ClaimNext")
	if f.claimErr != nil {
		return workflowv1.ClaimNextResponse{}, f.claimErr
	}
	if f.idle {
		return workflowv1.ClaimNextResponse{}, nil
	}
	if f.paced {
		return workflowv1.ClaimNextResponse{Deferral: workflowv1.ClaimPacingDeferral{
			Signal:     workflowv1.ClaimPacingSignalDomain,
			Domain:     string(sel.CollectorKind) + "/" + sel.CollectorInstanceID,
			RetryAfter: 5 * time.Second,
		}}, nil
	}
	now := time.Now().UTC()
	return workflowv1.ClaimNextResponse{Issued: true, Grant: workflowv1.ClaimedWorkItem{
		WorkItem: workflowv1.WorkItem{
			WorkItemID: "item-1", RunID: "run-1",
			CollectorKind:       sel.CollectorKind,
			CollectorInstanceID: sel.CollectorInstanceID,
			SourceSystem:        "jira", ScopeID: "scope-1",
			AcceptanceUnitID: "au-1", SourceRunID: "sr-1", GenerationID: "gen-1",
			Status:              workflowv1.WorkItemStatusClaimed,
			CurrentClaimID:      sel.ClaimID,
			CurrentFencingToken: 1, CurrentOwnerID: sel.OwnerID,
			CreatedAt: now, UpdatedAt: now,
		},
		Claim: workflowv1.Claim{
			ClaimID: sel.ClaimID, WorkItemID: "item-1", FencingToken: 1,
			OwnerID: sel.OwnerID, Status: workflowv1.ClaimStatusActive,
			ClaimedAt: now, HeartbeatAt: now,
			LeaseExpiresAt: now.Add(time.Minute),
			CreatedAt:      now, UpdatedAt: now,
		},
	}}, nil
}

func (f *fakeStore) Heartbeat(_ context.Context, _ workflowv1.ClaimMutation) error {
	f.note("Heartbeat")
	return f.claimErr
}

func (f *fakeStore) Complete(_ context.Context, _ workflowv1.ClaimMutation) error {
	f.note("Complete")
	return f.claimErr
}

func (f *fakeStore) Release(_ context.Context, _ workflowv1.ClaimMutation) error {
	f.note("Release")
	return f.claimErr
}

func (f *fakeStore) FailRetryable(_ context.Context, _ workflowv1.ClaimMutation) error {
	f.note("FailRetryable")
	return f.claimErr
}

func (f *fakeStore) FailTerminal(_ context.Context, _ workflowv1.ClaimMutation) error {
	f.note("FailTerminal")
	return f.claimErr
}

func (f *fakeStore) PinForCommit(
	_ context.Context,
	req workflowv1.CommitPinRequest,
) (workflowv1.CommitPinResponse, error) {
	f.note("PinForCommit")
	if f.claimErr != nil {
		return workflowv1.CommitPinResponse{}, f.claimErr
	}
	return f.pinResp, nil
}

func (f *fakeStore) CheckPacing(
	_ context.Context,
	_, _ string,
	_ time.Time,
) (workflowv1.ClaimPacingDecision, error) {
	f.note("CheckPacing")
	return workflowv1.ClaimPacingDecision{}, f.claimErr
}

func (f *fakeStore) ReportProgress(_ context.Context, _ workflowv1.ReportRunProgressRequest) error {
	f.note("ReportProgress")
	return f.claimErr
}

func (f *fakeStore) GetRunStatus(
	_ context.Context,
	runID string,
	now time.Time,
) (workflowv1.GetRunStatusResponse, error) {
	f.note("GetRunStatus")
	if f.claimErr != nil {
		return workflowv1.GetRunStatusResponse{}, f.claimErr
	}
	return workflowv1.GetRunStatusResponse{
		Run: workflowv1.Run{
			RunID: runID, TriggerKind: workflowv1.TriggerKindSchedule,
			Status:    workflowv1.RunStatusCollectionActive,
			CreatedAt: now, UpdatedAt: now,
		},
		AsOf: now,
	}, nil
}

func (f *fakeStore) ListRuns(
	_ context.Context,
	_ workflowv1.ListRunsRequest,
) (workflowv1.ListRunsResponse, error) {
	f.note("ListRuns")
	if f.claimErr != nil {
		return workflowv1.ListRunsResponse{}, f.claimErr
	}
	return workflowv1.ListRunsResponse{Runs: []workflowv1.RunSummary{}, AsOf: time.Now().UTC()}, nil
}

func (f *fakeStore) GetCompleteness(
	_ context.Context,
	runID string,
) ([]workflowv1.CompletenessState, error) {
	f.note("GetCompleteness")
	return nil, f.claimErr
}

func (f *fakeStore) ListInstances(_ context.Context) ([]workflowv1.CollectorInstance, error) {
	f.note("ListInstances")
	return []workflowv1.CollectorInstance{}, f.claimErr
}

func (f *fakeStore) QueueDepth(
	_ context.Context,
	now time.Time,
) (workflowv1.QueueDepthSnapshot, error) {
	f.note("QueueDepth")
	if f.claimErr != nil {
		return workflowv1.QueueDepthSnapshot{}, f.claimErr
	}
	return workflowv1.QueueDepthSnapshot{AsOf: now}, nil
}

func newTestServer(t *testing.T, st *fakeStore) *httptest.Server {
	t.Helper()
	srv, err := server.New(st, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	return httptest.NewServer(srv.Handler())
}

func post(t *testing.T, srv *httptest.Server, path, body string) *http.Response {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(workflowv1.VersionHeader, "1")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func decodeEnvelope(t *testing.T, resp *http.Response) workflowv1.ErrorEnvelope {
	t.Helper()
	defer resp.Body.Close()
	var envelope workflowv1.ErrorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	return envelope
}

// TestVersionMismatchRejected requires the version header on every route:
// a missing or foreign major is a 400 version_mismatch, never a claim.
func TestVersionMismatchRejected(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(t, st)
	defer srv.Close()

	for _, header := range []string{"", "2", "v1"} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/workflow.v1/ClaimNext",
			bytes.NewReader([]byte(`{}`)))
		if header != "" {
			req.Header.Set(workflowv1.VersionHeader, header)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("do: %v", err)
		}
		envelope := decodeEnvelope(t, resp)
		if resp.StatusCode != http.StatusBadRequest || envelope.Code != workflowv1.CodeVersionMismatch {
			t.Fatalf("header %q: status %d code %q, want 400 version_mismatch",
				header, resp.StatusCode, envelope.Code)
		}
	}
	if len(st.calls) != 0 {
		t.Fatalf("version-mismatched requests reached the store: %v", st.calls)
	}
}

// TestErrorCodesMapToHTTP requires store-level protocol errors to surface
// with their contract status and code.
func TestErrorCodesMapToHTTP(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{&workflowv1.ProtocolError{Code: workflowv1.CodeStaleFence, Message: "old"}, 409, workflowv1.CodeStaleFence},
		{&workflowv1.ProtocolError{Code: workflowv1.CodeNotFound, Message: "gone"}, 404, workflowv1.CodeNotFound},
		{&workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: "bad"}, 400, workflowv1.CodeInvalid},
	}
	for _, tc := range cases {
		st := &fakeStore{claimErr: tc.err}
		srv := newTestServer(t, st)
		resp := post(t, srv, "/workflow.v1/HeartbeatClaim",
			fmt.Sprintf(`{"work_item_id":"item-1","claim_id":"c-1","fencing_token":1,"owner_id":"o-1","observed_at":%q,"lease_duration_ns":60000000000}`,
				time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)))
		envelope := decodeEnvelope(t, resp)
		if resp.StatusCode != tc.status || envelope.Code != tc.code {
			t.Fatalf("err %v: status %d code %q, want %d %s",
				tc.err, resp.StatusCode, envelope.Code, tc.status, tc.code)
		}
		srv.Close()
	}
}

// TestIdleClaimIsNotFound requires an empty queue to answer 404 not_found
// (never a 200 with an undeferrable empty body, which the contract type
// forbids: an unissued response must carry a deferral).
func TestIdleClaimIsNotFound(t *testing.T) {
	st := &fakeStore{idle: true}
	srv := newTestServer(t, st)
	defer srv.Close()

	resp := post(t, srv, "/workflow.v1/ClaimNext",
		fmt.Sprintf(`{"selector":{"collector_kind":"jira","collector_instance_id":"i-1","owner_id":"o-1","claim_id":"c-1"},"observed_at":%q,"lease_ttl_ns":60000000000}`,
			time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)))
	envelope := decodeEnvelope(t, resp)
	if resp.StatusCode != http.StatusNotFound || envelope.Code != workflowv1.CodeNotFound {
		t.Fatalf("idle claim: status %d code %q, want 404 not_found", resp.StatusCode, envelope.Code)
	}
}

// TestPacedClaimCarriesDeferral requires a paced grant to answer 200 with
// the inline deferral the runner backs off on.
func TestPacedClaimCarriesDeferral(t *testing.T) {
	st := &fakeStore{paced: true}
	srv := newTestServer(t, st)
	defer srv.Close()

	resp := post(t, srv, "/workflow.v1/ClaimNext",
		fmt.Sprintf(`{"selector":{"collector_kind":"jira","collector_instance_id":"i-1","owner_id":"o-1","claim_id":"c-1"},"observed_at":%q,"lease_ttl_ns":60000000000}`,
			time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)))
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("paced claim status = %d, want 200", resp.StatusCode)
	}
	var body workflowv1.ClaimNextResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Issued {
		t.Fatalf("paced claim issued a grant")
	}
	if err := body.Validate(); err != nil {
		t.Fatalf("paced response invalid: %v", err)
	}
}

// TestReadRoutesServeBodies requires every read and report route to answer
// 200 with a decodable body through the fake store.
func TestReadRoutesServeBodies(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(t, st)
	defer srv.Close()

	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	cases := []struct {
		path string
		body string
		out  any
	}{
		{
			"/workflow.v1/CheckClaimPacing",
			fmt.Sprintf(`{"collector_kind":"jira","collector_instance_id":"i-1","observed_at":%q}`, now),
			&workflowv1.ClaimPacingDecision{},
		},
		{
			"/workflow.v1/ReportRunProgress",
			fmt.Sprintf(`{"run_id":"run-1","observed_at":%q,"progress":[{"collector_kind":"jira"}]}`, now),
			&map[string]any{},
		},
		{"/workflow.v1/GetRunStatus", `{"run_id":"run-1"}`, &workflowv1.GetRunStatusResponse{}},
		{"/workflow.v1/ListRuns", `{}`, &workflowv1.ListRunsResponse{}},
		{"/workflow.v1/GetRunCompleteness", `{"run_id":"run-1"}`, &workflowv1.GetRunCompletenessResponse{}},
		{"/workflow.v1/ListCollectorInstances", ``, &workflowv1.ListCollectorInstancesResponse{}},
		{"/workflow.v1/QueueDepthSnapshot", ``, &workflowv1.QueueDepthSnapshot{}},
	}
	for _, tc := range cases {
		resp := post(t, srv, tc.path, tc.body)
		func() {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("POST %s status = %d, want 200", tc.path, resp.StatusCode)
			}
			if err := json.NewDecoder(resp.Body).Decode(tc.out); err != nil {
				t.Fatalf("POST %s decode: %v", tc.path, err)
			}
		}()
	}
}

// TestInternalErrorStaysGeneric requires a store failure without a
// protocol code to surface as a 500 that carries no internals.
func TestInternalErrorStaysGeneric(t *testing.T) {
	st := &fakeStore{claimErr: fmt.Errorf("pq: connection reset")}
	srv := newTestServer(t, st)
	defer srv.Close()

	resp := post(t, srv, "/workflow.v1/GetRunStatus", `{"run_id":"run-1"}`)
	envelope := decodeEnvelope(t, resp)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	if strings.Contains(envelope.Message, "connection reset") {
		t.Fatalf("envelope leaks internals: %q", envelope.Message)
	}
}

// TestWrongMethodRejected requires GET on a protocol route to miss the
// POST-only registration.
func TestWrongMethodRejected(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(t, st)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/workflow.v1/ClaimNext", nil)
	req.Header.Set(workflowv1.VersionHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		t.Fatalf("GET claimed a 200 on a POST-only route")
	}
	if len(st.calls) != 0 {
		t.Fatalf("GET reached the store: %v", st.calls)
	}
}

// TestInvalidBodyRejected requires a malformed mutation to fail 400
// without touching the store.
func TestInvalidBodyRejected(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(t, st)
	defer srv.Close()

	resp := post(t, srv, "/workflow.v1/CompleteClaim", `{"claim_id":"","fencing_token":0}`)
	envelope := decodeEnvelope(t, resp)
	if resp.StatusCode != http.StatusBadRequest || envelope.Code != workflowv1.CodeInvalid {
		t.Fatalf("invalid body: status %d code %q, want 400 invalid", resp.StatusCode, envelope.Code)
	}
	if len(st.calls) != 0 {
		t.Fatalf("invalid body reached the store: %v", st.calls)
	}
}
