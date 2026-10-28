// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestAllRoutesAreServed walks the merged route table and requires every
// operation to answer (never 404) with a well-formed body or envelope.
func TestAllRoutesAreServed(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(t, st)
	defer srv.Close()

	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	bodies := map[workflowv1.Operation]string{
		workflowv1.OpClaimNext: fmt.Sprintf(
			`{"selector":{"collector_kind":"jira","collector_instance_id":"i-1","owner_id":"o-1","claim_id":"c-1"},"observed_at":%q,"lease_ttl_ns":60000000000}`,
			now),
		workflowv1.OpCheckClaimPacing: fmt.Sprintf(
			`{"collector_kind":"jira","collector_instance_id":"i-1","observed_at":%q}`, now),
		workflowv1.OpHeartbeatClaim: fmt.Sprintf(
			`{"work_item_id":"item-1","claim_id":"c-1","fencing_token":1,"owner_id":"o-1","observed_at":%q,"lease_duration_ns":60000000000}`,
			now),
		workflowv1.OpCompleteClaim: fmt.Sprintf(
			`{"work_item_id":"item-1","claim_id":"c-1","fencing_token":1,"owner_id":"o-1","observed_at":%q,"lease_duration_ns":60000000000}`,
			now),
		workflowv1.OpReleaseClaim: fmt.Sprintf(
			`{"work_item_id":"item-1","claim_id":"c-1","fencing_token":1,"owner_id":"o-1","observed_at":%q,"lease_duration_ns":60000000000}`,
			now),
		workflowv1.OpFailClaimRetryable: fmt.Sprintf(
			`{"work_item_id":"item-1","claim_id":"c-1","fencing_token":1,"owner_id":"o-1","observed_at":%q,"lease_duration_ns":60000000000}`,
			now),
		workflowv1.OpFailClaimTerminal: fmt.Sprintf(
			`{"work_item_id":"item-1","claim_id":"c-1","fencing_token":1,"owner_id":"o-1","observed_at":%q,"lease_duration_ns":60000000000}`,
			now),
		workflowv1.OpValidateAndPinClaimForCommit: fmt.Sprintf(
			`{"claim_id":"c-1","fencing_token":1,"scope_id":"scope-1","source_run_id":"sr-1","generation_id":"gen-1","commit_deadline":%q}`,
			time.Now().UTC().Add(time.Hour).Truncate(time.Second).Format(time.RFC3339Nano)),
		workflowv1.OpReportRunProgress: fmt.Sprintf(
			`{"run_id":"run-1","observed_at":%q,"progress":[{"collector_kind":"jira"}]}`, now),
		workflowv1.OpGetRunStatus:           `{"run_id":"run-1"}`,
		workflowv1.OpListRuns:               `{}`,
		workflowv1.OpGetRunCompleteness:     `{"run_id":"run-1"}`,
		workflowv1.OpListCollectorInstances: ``,
		workflowv1.OpQueueDepthSnapshot:     ``,
	}
	// routeStoreOp maps each protocol operation to the store method its
	// handler must invoke. Handler names differ from store method names
	// (HeartbeatClaim served by Heartbeat, ValidateAndPinClaimForCommit
	// by PinForCommit), so the map pins identity, not spelling.
	routeStoreOp := map[workflowv1.Operation]string{
		workflowv1.OpClaimNext:                    "ClaimNext",
		workflowv1.OpCheckClaimPacing:             "CheckPacing",
		workflowv1.OpHeartbeatClaim:               "Heartbeat",
		workflowv1.OpCompleteClaim:                "Complete",
		workflowv1.OpReleaseClaim:                 "Release",
		workflowv1.OpFailClaimRetryable:           "FailRetryable",
		workflowv1.OpFailClaimTerminal:            "FailTerminal",
		workflowv1.OpValidateAndPinClaimForCommit: "PinForCommit",
		workflowv1.OpReportRunProgress:            "ReportProgress",
		workflowv1.OpGetRunStatus:                 "GetRunStatus",
		workflowv1.OpListRuns:                     "ListRuns",
		workflowv1.OpGetRunCompleteness:           "GetCompleteness",
		workflowv1.OpListCollectorInstances:       "ListInstances",
		workflowv1.OpQueueDepthSnapshot:           "QueueDepth",
	}
	for _, route := range workflowv1.RouteTable {
		body, ok := bodies[route.Operation]
		if !ok {
			t.Fatalf("no test body for %s", route.Operation)
		}
		resp := post(t, srv, route.Path, body)
		resp.Body.Close()
		// Every valid body here must answer exactly 200: idle and paced
		// shapes have their own tests, so no other status is valid on
		// this path.
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s %s: status %d, want 200", route.Method, route.Path, resp.StatusCode)
		}
	}
	wantOps := make([]string, 0, len(workflowv1.RouteTable))
	for _, route := range workflowv1.RouteTable {
		op, ok := routeStoreOp[route.Operation]
		if !ok {
			t.Fatalf("no expected store op for %s", route.Operation)
		}
		wantOps = append(wantOps, op)
	}
	// Requests post in RouteTable order against a synchronous fake, so
	// the recorded calls must match positionally: a Heartbeat<->Complete
	// handler swap keeps the multiset (and the count) intact while
	// serving the wrong operation, and only an ordered comparison
	// catches it.
	if len(st.calls) != len(wantOps) {
		t.Fatalf("store calls = %v, want %v (one per route)", st.calls, wantOps)
	}
	for i := range wantOps {
		if st.calls[i] != wantOps[i] {
			t.Fatalf("route %d (%s): store call = %q, want %q (handler serves the wrong operation)",
				i, workflowv1.RouteTable[i].Operation, st.calls[i], wantOps[i])
		}
	}
}
