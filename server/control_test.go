// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestControlRoutesServeRealState requires the six coordinator seams to
// resolve to real routes: each answers 200 through the store instead of
// the claim-plane client's ErrClaimPlaneUnsupported.
func TestControlRoutesServeRealState(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	st := &fakeStore{
		transitioned: 2,
		reaped:       []workflowv1.Claim{},
		inserted:     2,
	}
	srv := newTestServer(t, st)
	defer srv.Close()

	runBody := fmt.Sprintf(`{"run":{"run_id":"run-1","trigger_kind":"schedule","status":"collection_active","created_at":%q,"updated_at":%q}}`, now, now)
	resp := post(t, srv, "/workflow.v1/CreateRun", runBody)
	func() {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("CreateRun status = %d, want 200", resp.StatusCode)
		}
		var out workflowv1.CreateRunResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("CreateRun decode: %v", err)
		}
		if out.RunID != "run-1" {
			t.Fatalf("CreateRun run_id = %q, want run-1", out.RunID)
		}
	}()

	itemBody := fmt.Sprintf(`{"items":[
{"work_item_id":"item-1","run_id":"run-1","collector_kind":"jira","collector_instance_id":"i-1","source_system":"jira","scope_id":"scope-1","acceptance_unit_id":"au-1","source_run_id":"sr-1","generation_id":"gen-1","status":"pending","created_at":%q,"updated_at":%q},
{"work_item_id":"item-2","run_id":"run-1","collector_kind":"jira","collector_instance_id":"i-1","source_system":"jira","scope_id":"scope-2","acceptance_unit_id":"au-2","source_run_id":"sr-2","generation_id":"gen-2","status":"pending","created_at":%q,"updated_at":%q}
]}`, now, now, now, now)
	resp = post(t, srv, "/workflow.v1/EnqueueWorkItems", itemBody)
	func() {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("EnqueueWorkItems status = %d, want 200", resp.StatusCode)
		}
		var out workflowv1.EnqueueWorkItemsResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("EnqueueWorkItems decode: %v", err)
		}
		if out.Accepted != 2 {
			t.Fatalf("EnqueueWorkItems accepted = %d, want 2", out.Accepted)
		}
		if out.InsertedWorkItems != 2 {
			t.Fatalf("EnqueueWorkItems inserted = %d, want 2", out.InsertedWorkItems)
		}
	}()

	for _, tc := range []struct {
		path string
		body string
		out  any
	}{
		{
			"/workflow.v1/ReconcileCollectorInstances",
			fmt.Sprintf(`{"observed_at":%q,"instances":[{"instance_id":"i-1","collector_kind":"jira","mode":"manual"}]}`, now),
			&map[string]any{},
		},
		{
			"/workflow.v1/UpsertCompletenessStates",
			fmt.Sprintf(`{"states":[{"run_id":"run-1","collector_kind":"jira","keyspace":"facts","phase_name":"publish","status":"complete","observed_at":%q,"updated_at":%q}]}`, now, now),
			&map[string]any{},
		},
		{
			"/workflow.v1/ReconcileWorkflowRuns",
			fmt.Sprintf(`{"observed_at":%q}`, now),
			&workflowv1.ReconcileWorkflowRunsResponse{},
		},
		{
			"/workflow.v1/ReapExpiredClaims",
			fmt.Sprintf(`{"observed_at":%q,"limit":10,"requeue_delay_ns":60000000000}`, now),
			&workflowv1.ReapExpiredClaimsResponse{},
		},
	} {
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

	wantOps := []string{
		"CreateRun", "Enqueue", "ReconcileInstances", "UpsertCompleteness",
		"ReconcileRuns", "Reap",
	}
	if len(st.calls) != len(wantOps) {
		t.Fatalf("store calls = %v, want %v", st.calls, wantOps)
	}
	for i := range wantOps {
		if st.calls[i] != wantOps[i] {
			t.Fatalf("control route %d: store call = %q, want %q", i, st.calls[i], wantOps[i])
		}
	}
}

// TestControlRoutesRejectInvalidBodies requires malformed control requests
// to fail 400 without touching the store.
func TestControlRoutesRejectInvalidBodies(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(t, st)
	defer srv.Close()

	cases := map[string]string{
		"/workflow.v1/CreateRun":                   `{"run":{}}`,
		"/workflow.v1/EnqueueWorkItems":            `{"items":[]}`,
		"/workflow.v1/ReconcileCollectorInstances": `{"observed_at":"0001-01-01T00:00:00Z"}`,
		"/workflow.v1/UpsertCompletenessStates":    `{"states":[]}`,
		"/workflow.v1/ReconcileWorkflowRuns":       `{}`,
		"/workflow.v1/ReapExpiredClaims":           `{"limit":0}`,
	}
	for path, body := range cases {
		resp := post(t, srv, path, body)
		envelope := decodeEnvelope(t, resp)
		if resp.StatusCode != http.StatusBadRequest || envelope.Code != workflowv1.CodeInvalid {
			t.Fatalf("POST %s: status %d code %q, want 400 invalid", path, resp.StatusCode, envelope.Code)
		}
	}
	if len(st.calls) != 0 {
		t.Fatalf("invalid control bodies reached the store: %v", st.calls)
	}
}
