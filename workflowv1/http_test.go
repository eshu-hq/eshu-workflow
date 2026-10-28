// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRouteTableCoversAllPlanes(t *testing.T) {
	if len(RouteTable) != 20 {
		t.Fatalf("route table has %d entries, want 20", len(RouteTable))
	}
	seen := map[string]bool{}
	for _, route := range RouteTable {
		if route.Method != "POST" {
			t.Fatalf("%s uses %s, want POST", route.Operation, route.Method)
		}
		if !strings.HasPrefix(route.Path, "/workflow.v1/") {
			t.Fatalf("%s path %q misses the protocol prefix", route.Operation, route.Path)
		}
		if seen[route.Path] {
			t.Fatalf("duplicate path %q", route.Path)
		}
		seen[route.Path] = true
		if _, ok := RouteFor(route.Operation); !ok {
			t.Fatalf("RouteFor(%s) misses", route.Operation)
		}
	}
	if _, ok := RouteFor("RetireTheCoordinator"); ok {
		t.Fatal("unknown operation resolved a route")
	}
	// Every verdict-named operation has a route.
	for _, op := range []Operation{
		OpClaimNext, OpCheckClaimPacing, OpHeartbeatClaim, OpCompleteClaim,
		OpReleaseClaim, OpFailClaimRetryable, OpFailClaimTerminal,
		OpValidateAndPinClaimForCommit, OpCreateRun, OpEnqueueWorkItems,
		OpReconcileCollectorInstances, OpUpsertCompletenessStates,
		OpReconcileWorkflowRuns, OpReapExpiredClaims, OpReportRunProgress,
		OpGetRunStatus, OpListRuns, OpGetRunCompleteness,
		OpListCollectorInstances, OpQueueDepthSnapshot,
	} {
		if _, ok := RouteFor(op); !ok {
			t.Fatalf("verdict operation %s has no route", op)
		}
	}
}

func TestVersionCheckRejectsMismatch(t *testing.T) {
	if err := CheckVersion("1"); err != nil {
		t.Fatal(err)
	}
	for _, header := range []string{"", "2", "v1", "1.0"} {
		err := CheckVersion(header)
		protoErr, ok := err.(*ProtocolError)
		if !ok {
			t.Fatalf("CheckVersion(%q) error type = %T, want *ProtocolError", header, err)
		}
		if protoErr.Code != CodeVersionMismatch || protoErr.HTTPStatus() != 400 {
			t.Fatalf("CheckVersion(%q) = %v/%d, want version_mismatch/400", header, protoErr.Code, protoErr.HTTPStatus())
		}
	}
}

func TestErrorCodesMapToStatuses(t *testing.T) {
	cases := map[string]int{
		CodeInvalid: 400, CodeNotFound: 404, CodeClaimConflict: 409,
		CodeStaleFence: 409, CodeLeaseExpired: 409, CodePaced: 429,
	}
	for code, status := range cases {
		if got := (&ProtocolError{Code: code}).HTTPStatus(); got != status {
			t.Fatalf("%s -> %d, want %d", code, got, status)
		}
	}
	// Paced errors carry the backoff in the envelope, never in prose.
	paced := &ProtocolError{Code: CodePaced, Message: "slow down", RetryAfterSeconds: 5}
	var decoded ErrorEnvelope
	raw, _ := json.Marshal(paced.Envelope())
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.RetryAfterSeconds != 5 || decoded.Code != CodePaced {
		t.Fatalf("envelope = %+v", decoded)
	}
}

func TestListRunsLimitBounds(t *testing.T) {
	if got := (ListRunsRequest{}).EffectiveLimit(); got != defaultListRunsLimit {
		t.Fatalf("default limit = %d", got)
	}
	if got := (ListRunsRequest{Limit: 25}).EffectiveLimit(); got != 25 {
		t.Fatalf("limit = %d, want 25", got)
	}
	if err := (ListRunsRequest{Limit: maxListRunsLimit + 1}).Validate(); err == nil {
		t.Fatal("over-max limit validated")
	}
}

func TestReadPlaneRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	snapshot := QueueDepthSnapshot{
		Families:      []FamilyQueueDepth{{CollectorKind: "jira", Pending: 12, Claimed: 3}},
		GlobalPending: 40,
		GlobalClaimed: 9,
		AsOf:          now,
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var decoded QueueDepthSnapshot
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatal(err)
	}
	if decoded.Families[0].Pending != 12 || decoded.GlobalClaimed != 9 {
		t.Fatalf("round trip = %+v", decoded)
	}
	status := GetRunStatusResponse{Run: validRun(), AsOf: now}
	if err := status.Validate(); err != nil {
		t.Fatal(err)
	}
}
