// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"strings"
	"testing"
	"time"
)

func TestClaimSelectorValidate(t *testing.T) {
	valid := ClaimSelector{
		CollectorKind:       "jira",
		CollectorInstanceID: "jira-primary",
		OwnerID:             "owner-1",
		ClaimID:             "claim-1",
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := valid
	bad.ClaimID = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("blank claim_id validated")
	}
}

func TestClaimNextRequestValidate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	valid := ClaimNextRequest{
		Selector: ClaimSelector{
			CollectorKind:       "jira",
			CollectorInstanceID: "jira-primary",
			OwnerID:             "owner-1",
			ClaimID:             "claim-1",
		},
		ObservedAt: now,
		LeaseTTL:   time.Minute,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := valid
	bad.LeaseTTL = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("zero lease TTL validated")
	}
}

func TestCheckClaimPacingRequestValidate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	valid := CheckClaimPacingRequest{CollectorKind: "jira", CollectorInstanceID: "i-1", ObservedAt: now}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := valid
	bad.CollectorInstanceID = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("blank instance validated")
	}
}

func TestDefaultsArePositive(t *testing.T) {
	if DefaultClaimLeaseTTL() <= 0 || DefaultHeartbeatInterval() <= 0 || DefaultReapInterval() <= 0 {
		t.Fatal("durations must be positive")
	}
	if DefaultExpiredClaimLimit() <= 0 || DefaultExpiredClaimRequeueDelay() <= 0 || DefaultClaimMaxAttempts() <= 0 {
		t.Fatal("budgets must be positive")
	}
	if DefaultHeartbeatInterval() >= DefaultClaimLeaseTTL() {
		t.Fatal("heartbeat must fit inside the lease it renews")
	}
}

func TestProtocolErrorMessage(t *testing.T) {
	err := &ProtocolError{Code: CodeStaleFence, Message: "epoch superseded"}
	if !strings.Contains(err.Error(), CodeStaleFence) {
		t.Fatalf("error = %q, want the code", err.Error())
	}
}

func TestFairnessCandidateValidate(t *testing.T) {
	valid := FairnessCandidate{CollectorKind: "jira", CollectorInstanceID: "a", Weight: 2}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	// Zero weight means default, not invalid, on the candidate itself:
	// the scheduler applies the default.
	if err := (FairnessCandidate{CollectorKind: "jira", CollectorInstanceID: "a"}).Validate(); err != nil {
		t.Fatal(err)
	}
	bad := valid
	bad.Weight = -1
	if err := bad.Validate(); err == nil {
		t.Fatal("negative weight validated")
	}
}

func TestQuotaValidate(t *testing.T) {
	if err := (ClaimQuota{MaxGenerationsPerHour: 100}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (ClaimQuota{MaxGenerationsPerHour: -1}).Validate(); err == nil {
		t.Fatal("negative quota validated")
	}
	if err := (QuotaAssignment{CollectorKind: "jira", Quota: ClaimQuota{}}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestInstanceDocumentValidation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	blank := DesiredCollectorInstance{InstanceID: "i-1", CollectorKind: "jira", Mode: CollectorModeManual}
	if err := blank.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := blank
	bad.Configuration = "{not json"
	if err := bad.Validate(); err == nil {
		t.Fatal("invalid configuration JSON validated")
	}
	stored := CollectorInstance{
		InstanceID: "i-1", CollectorKind: "jira", Mode: CollectorModeManual,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := stored.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestReadRequestValidation(t *testing.T) {
	if err := (GetRunStatusRequest{RunID: "run-1"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (GetRunStatusRequest{}).Validate(); err == nil {
		t.Fatal("blank run status request validated")
	}
	if err := (GetRunCompletenessRequest{RunID: "run-1"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (ListRunsRequest{Status: "nope"}).Validate(); err == nil {
		t.Fatal("unknown list filter status validated")
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := (ListCollectorInstancesResponse{AsOf: now}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (GetRunCompletenessResponse{RunID: "run-1", AsOf: now}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (ListRunsResponse{Runs: []RunSummary{{
		RunID: "run-1", TriggerKind: TriggerKindSchedule, Status: RunStatusComplete,
		CreatedAt: now, UpdatedAt: now,
	}}, AsOf: now}).Validate(); err != nil {
		t.Fatal(err)
	}
}
