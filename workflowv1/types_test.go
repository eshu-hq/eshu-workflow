// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"testing"
	"time"
)

func validRun() Run {
	now := time.Now().UTC().Truncate(time.Second)
	return Run{
		RunID:       "run-1",
		TriggerKind: TriggerKindSchedule,
		Status:      RunStatusCollectionActive,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
}

func TestRunValidate(t *testing.T) {
	if err := validRun().Validate(); err != nil {
		t.Fatal(err)
	}
	bad := validRun()
	bad.FinishedAt = bad.CreatedAt.Add(-time.Second)
	if err := bad.Validate(); err == nil {
		t.Fatal("finished_at before created_at validated")
	}
	bad = validRun()
	bad.RunID = " "
	if err := bad.Validate(); err == nil {
		t.Fatal("blank run_id validated")
	}
}

func validWorkItem() WorkItem {
	now := time.Now().UTC().Truncate(time.Second)
	return WorkItem{
		WorkItemID:          "wi-1",
		RunID:               "run-1",
		CollectorKind:       "jira",
		CollectorInstanceID: "jira-primary",
		SourceSystem:        "jira",
		ScopeID:             "scope-1",
		AcceptanceUnitID:    "au-1",
		SourceRunID:         "sr-1",
		GenerationID:        "gen-1",
		Status:              WorkItemStatusPending,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
}

func TestWorkItemValidate(t *testing.T) {
	if err := validWorkItem().Validate(); err != nil {
		t.Fatal(err)
	}
	// Tenant tuple is all-or-none.
	partial := validWorkItem()
	partial.TenantID = "t-1"
	if err := partial.Validate(); err == nil {
		t.Fatal("partial tenant tuple validated")
	}
	full := validWorkItem()
	full.TenantID, full.WorkspaceID, full.SubjectClass, full.PolicyRevisionHash = "t", "w", "s", "h"
	if err := full.Validate(); err != nil {
		t.Fatal(err)
	}
	negative := validWorkItem()
	negative.AttemptCount = -1
	if err := negative.Validate(); err == nil {
		t.Fatal("negative attempt_count validated")
	}
}

func validClaim() Claim {
	now := time.Now().UTC().Truncate(time.Second)
	return Claim{
		ClaimID:        "claim-1",
		WorkItemID:     "wi-1",
		FencingToken:   7,
		OwnerID:        "owner-1",
		Status:         ClaimStatusActive,
		ClaimedAt:      now,
		HeartbeatAt:    now,
		LeaseExpiresAt: now.Add(time.Minute),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func TestClaimFencingTokenMustBePositive(t *testing.T) {
	if err := validClaim().Validate(); err != nil {
		t.Fatal(err)
	}
	for _, token := range []int64{0, -3} {
		bad := validClaim()
		bad.FencingToken = token
		if err := bad.Validate(); err == nil {
			t.Fatalf("fencing_token %d validated", token)
		}
	}
}

func TestClaimLeaseOrdering(t *testing.T) {
	bad := validClaim()
	bad.LeaseExpiresAt = bad.HeartbeatAt.Add(-time.Second)
	if err := bad.Validate(); err == nil {
		t.Fatal("lease expiring before heartbeat validated")
	}
}

func TestRunAdmissionCounts(t *testing.T) {
	if err := (RunAdmission{EligibleTargets: 5, InsertedWorkItems: 3}).Validate(); err != nil {
		t.Fatal(err)
	}
	// Inserted can never exceed eligible: the gap is the lost-work signal.
	if err := (RunAdmission{EligibleTargets: 3, InsertedWorkItems: 5}).Validate(); err == nil {
		t.Fatal("inserted exceeding eligible validated")
	}
}

func TestCompletenessStateValidate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	state := CompletenessState{
		RunID:         "run-1",
		CollectorKind: "jira",
		Keyspace:      "default",
		PhaseName:     "collect",
		Required:      true,
		Status:        "complete",
		ObservedAt:    now,
		UpdatedAt:     now,
	}
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
	state.Status = ""
	if err := state.Validate(); err == nil {
		t.Fatal("blank completeness status validated")
	}
}
