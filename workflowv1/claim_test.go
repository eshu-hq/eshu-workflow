// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"testing"
	"time"
)

func TestClaimMutationFencing(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	mutation := ClaimMutation{
		WorkItemID:    "wi-1",
		ClaimID:       "claim-1",
		FencingToken:  3,
		OwnerID:       "owner-1",
		ObservedAt:    now,
		LeaseDuration: time.Minute,
	}
	if err := mutation.Validate(); err != nil {
		t.Fatal(err)
	}
	mutation.FencingToken = 0
	if err := mutation.Validate(); err == nil {
		t.Fatal("zero fencing token validated")
	}
}

func TestClaimMutationFailureStatusBounds(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	base := ClaimMutation{
		WorkItemID:    "wi-1",
		ClaimID:       "claim-1",
		FencingToken:  3,
		OwnerID:       "owner-1",
		ObservedAt:    now,
		LeaseDuration: time.Minute,
	}
	ok := base
	ok.FailureStatusCode = 503
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := base
	bad.FailureStatusCode = 999
	if err := bad.Validate(); err == nil {
		t.Fatal("out-of-range failure status validated")
	}
}

func TestClaimNextResponseIsExactlyOneOutcome(t *testing.T) {
	grant := ClaimNextResponse{Issued: true, Grant: ClaimedWorkItem{WorkItem: validWorkItem(), Claim: validClaim()}}
	// Align the pair: the claim must reference the work item.
	grant.Grant.Claim.WorkItemID = grant.Grant.WorkItem.WorkItemID
	if err := grant.Validate(); err != nil {
		t.Fatal(err)
	}
	both := grant
	both.Deferral = ClaimPacingDeferral{Signal: ClaimPacingSignalDomain, RetryAfter: time.Second}
	if err := both.Validate(); err == nil {
		t.Fatal("grant+deferral response validated")
	}
	neither := ClaimNextResponse{}
	if err := neither.Validate(); err == nil {
		t.Fatal("empty unissued response validated")
	}
	mismatch := grant
	mismatch.Grant.Claim.WorkItemID = "other"
	if err := mismatch.Validate(); err == nil {
		t.Fatal("claim/item id mismatch validated")
	}
}

func TestCommitPinRequiresPositiveFenceAndDeadline(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	req := CommitPinRequest{
		ClaimID:        "claim-1",
		FencingToken:   9,
		ScopeID:        "scope-1",
		SourceRunID:    "sr-1",
		GenerationID:   "gen-1",
		CommitDeadline: now.Add(time.Minute),
	}
	if err := req.Validate(); err != nil {
		t.Fatal(err)
	}
	req.FencingToken = 0
	if err := req.Validate(); err == nil {
		t.Fatal("zero fencing token pin validated")
	}
	if err := (CommitPinResponse{Pinned: true, CommitID: "commit-1"}).Validate(); err != nil {
		t.Fatal(err)
	}
	// An unpinned pin must surface as a stale-fence error, never a body.
	if err := (CommitPinResponse{}).Validate(); err == nil {
		t.Fatal("unpinned commit response validated")
	}
}
