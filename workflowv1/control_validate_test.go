// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"testing"
	"time"
)

func TestCreateRunResponseValidate(t *testing.T) {
	if err := (CreateRunResponse{RunID: "run-1"}).Validate(); err != nil {
		t.Fatal(err)
	}
	wantErrContaining(t, (CreateRunResponse{}).Validate(), "run_id")
}

func TestEnqueueWorkItemsResponseValidate(t *testing.T) {
	// A replay accepts the whole batch while inserting nothing: valid.
	if err := (EnqueueWorkItemsResponse{Accepted: 3, InsertedWorkItems: 0}).Validate(); err != nil {
		t.Fatal(err)
	}
	wantErrContaining(t, (EnqueueWorkItemsResponse{Accepted: -1}).Validate(), "accepted must not be negative")
	wantErrContaining(t, (EnqueueWorkItemsResponse{InsertedWorkItems: -1}).Validate(), "inserted_work_items must not be negative")
	wantErrContaining(t, (EnqueueWorkItemsResponse{Accepted: 1, InsertedWorkItems: 2}).Validate(), "must not exceed accepted")
}

func TestUpsertCompletenessStatesRequestValidateRejections(t *testing.T) {
	valid := UpsertCompletenessStatesRequest{States: []CompletenessState{validCompleteness()}}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	wantErrContaining(t, (UpsertCompletenessStatesRequest{}).Validate(), "states must not be empty")
	bad := UpsertCompletenessStatesRequest{States: []CompletenessState{validCompleteness(), {}}}
	wantErrContaining(t, bad.Validate(), "states[1]")
}

func TestReconcileWorkflowRunsResponseValidate(t *testing.T) {
	if err := (ReconcileWorkflowRunsResponse{Transitioned: 0}).Validate(); err != nil {
		t.Fatal(err)
	}
	wantErrContaining(t, (ReconcileWorkflowRunsResponse{Transitioned: -1}).Validate(), "transitioned")
}

func TestReapExpiredClaimsRequestValidateRejections(t *testing.T) {
	valid := ReapExpiredClaimsRequest{ObservedAt: time.Now(), Limit: 10, RequeueDelay: time.Second}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	noTime := valid
	noTime.ObservedAt = time.Time{}
	wantErrContaining(t, noTime.Validate(), "observed_at")
	noLimit := valid
	noLimit.Limit = 0
	wantErrContaining(t, noLimit.Validate(), "limit")
	negDelay := valid
	negDelay.RequeueDelay = -time.Second
	wantErrContaining(t, negDelay.Validate(), "requeue_delay_ns")
}

func TestReapExpiredClaimsResponseValidate(t *testing.T) {
	if err := (ReapExpiredClaimsResponse{}).Validate(); err != nil {
		t.Fatalf("empty reap rejected: %v", err)
	}
	if err := (ReapExpiredClaimsResponse{Claims: []Claim{validClaim()}}).Validate(); err != nil {
		t.Fatal(err)
	}
	bad := ReapExpiredClaimsResponse{Claims: []Claim{validClaim(), {}}}
	wantErrContaining(t, bad.Validate(), "claims[1]")
}

func TestReconcileCollectorInstancesRequestRejectsBadMode(t *testing.T) {
	req := ReconcileCollectorInstancesRequest{
		ObservedAt: time.Now(),
		Instances:  []DesiredCollectorInstance{validInstanceDeclaration(), {}},
	}
	wantErrContaining(t, req.Validate(), "instances[1]")
}
