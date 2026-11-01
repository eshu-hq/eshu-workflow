// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"testing"
	"time"
)

func validSelector() ClaimSelector {
	return ClaimSelector{
		CollectorKind:       "jira",
		CollectorInstanceID: "jira-primary",
		OwnerID:             "owner-1",
		ClaimID:             "claim-1",
	}
}

func validMutation() ClaimMutation {
	return ClaimMutation{
		WorkItemID:    "wi-1",
		ClaimID:       "claim-1",
		FencingToken:  3,
		OwnerID:       "owner-1",
		ObservedAt:    time.Now().UTC(),
		LeaseDuration: time.Minute,
	}
}

func TestClaimSelectorValidateRejections(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ClaimSelector)
		want   string
	}{
		{"blank collector kind", func(s *ClaimSelector) { s.CollectorKind = "" }, "collector_kind"},
		{"blank instance", func(s *ClaimSelector) { s.CollectorInstanceID = "" }, "collector_instance_id"},
		{"blank owner", func(s *ClaimSelector) { s.OwnerID = "" }, "owner_id"},
		{"blank claim", func(s *ClaimSelector) { s.ClaimID = "" }, "claim_id"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sel := validSelector()
			tc.mutate(&sel)
			wantErrContaining(t, sel.Validate(), tc.want)
		})
	}
}

func TestClaimMutationValidateRejections(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ClaimMutation)
		want   string
	}{
		{"blank work_item_id", func(m *ClaimMutation) { m.WorkItemID = "" }, "work_item_id"},
		{"blank claim_id", func(m *ClaimMutation) { m.ClaimID = "" }, "claim_id"},
		{"zero fencing token", func(m *ClaimMutation) { m.FencingToken = 0 }, "fencing_token"},
		{"blank owner_id", func(m *ClaimMutation) { m.OwnerID = "" }, "owner_id"},
		{"zero observed_at", func(m *ClaimMutation) { m.ObservedAt = time.Time{} }, "observed_at"},
		{"zero lease", func(m *ClaimMutation) { m.LeaseDuration = 0 }, "lease_duration_ns"},
		{"negative lease", func(m *ClaimMutation) { m.LeaseDuration = -time.Second }, "lease_duration_ns"},
		{"partial tenant tuple", func(m *ClaimMutation) { m.TenantID = "t" }, "set together"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := validMutation()
			tc.mutate(&m)
			wantErrContaining(t, m.Validate(), tc.want)
		})
	}
}

func TestClaimedWorkItemValidate(t *testing.T) {
	valid := ClaimedWorkItem{WorkItem: validWorkItem(), Claim: validClaim()}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}

	badItem := valid
	badItem.WorkItem.RunID = ""
	wantErrContaining(t, badItem.Validate(), "run_id")

	badClaim := valid
	badClaim.Claim.OwnerID = ""
	wantErrContaining(t, badClaim.Validate(), "owner_id")

	// A claim that belongs to a different work item must never validate:
	// pairing them would let a holder act on work it does not own.
	mismatch := valid
	mismatch.Claim.WorkItemID = "wi-other"
	wantErrContaining(t, mismatch.Validate(), "does not match")
}

func TestClaimNextRequestValidateRejections(t *testing.T) {
	valid := ClaimNextRequest{Selector: validSelector(), ObservedAt: time.Now(), LeaseTTL: time.Minute}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	badSelector := valid
	badSelector.Selector.OwnerID = ""
	wantErrContaining(t, badSelector.Validate(), "owner_id")

	noTime := valid
	noTime.ObservedAt = time.Time{}
	wantErrContaining(t, noTime.Validate(), "observed_at")

	noTTL := valid
	noTTL.LeaseTTL = 0
	wantErrContaining(t, noTTL.Validate(), "lease_ttl_ns")
}

func TestClaimNextResponseValidateOutcomes(t *testing.T) {
	grant := ClaimedWorkItem{WorkItem: validWorkItem(), Claim: validClaim()}

	if err := (ClaimNextResponse{Issued: true, Grant: grant}).Validate(); err != nil {
		t.Fatalf("grant rejected: %v", err)
	}
	invalidGrant := grant
	invalidGrant.Claim.FencingToken = 0
	wantErrContaining(t, (ClaimNextResponse{Issued: true, Grant: invalidGrant}).Validate(), "fencing_token")

	deferral := ClaimPacingDeferral{Signal: ClaimPacingSignalGlobal, RetryAfter: time.Second}
	if err := (ClaimNextResponse{Deferral: deferral}).Validate(); err != nil {
		t.Fatalf("deferral rejected: %v", err)
	}
	// Not issued and no deferral is an empty answer, which is never valid.
	wantErrContaining(t, (ClaimNextResponse{}).Validate(), "claim pacing signal")
}

func TestCheckClaimPacingRequestValidateRejections(t *testing.T) {
	valid := CheckClaimPacingRequest{CollectorKind: "jira", CollectorInstanceID: "jira-primary", ObservedAt: time.Now()}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	noKind := valid
	noKind.CollectorKind = ""
	wantErrContaining(t, noKind.Validate(), "collector_kind")

	noInstance := valid
	noInstance.CollectorInstanceID = ""
	wantErrContaining(t, noInstance.Validate(), "collector_instance_id")

	noTime := valid
	noTime.ObservedAt = time.Time{}
	wantErrContaining(t, noTime.Validate(), "observed_at")
}

func TestCommitPinRequestValidateRejections(t *testing.T) {
	valid := CommitPinRequest{
		ClaimID: "claim-1", FencingToken: 2, ScopeID: "scope-1",
		SourceRunID: "sr-1", GenerationID: "gen-1", CommitDeadline: time.Now().Add(time.Minute),
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*CommitPinRequest)
		want   string
	}{
		{"blank claim_id", func(r *CommitPinRequest) { r.ClaimID = "" }, "claim_id"},
		{"zero fencing token", func(r *CommitPinRequest) { r.FencingToken = 0 }, "fencing_token"},
		{"blank scope_id", func(r *CommitPinRequest) { r.ScopeID = "" }, "scope_id"},
		{"blank source_run_id", func(r *CommitPinRequest) { r.SourceRunID = "" }, "source_run_id"},
		{"blank generation_id", func(r *CommitPinRequest) { r.GenerationID = "" }, "generation_id"},
		{"zero deadline", func(r *CommitPinRequest) { r.CommitDeadline = time.Time{} }, "commit_deadline"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := valid
			tc.mutate(&req)
			wantErrContaining(t, req.Validate(), tc.want)
		})
	}
}

func TestCommitPinResponseValidate(t *testing.T) {
	if err := (CommitPinResponse{Pinned: true, CommitID: "commit-1"}).Validate(); err != nil {
		t.Fatal(err)
	}
	wantErrContaining(t, (CommitPinResponse{Pinned: false, CommitID: "commit-1"}).Validate(), "stale-fence")
	wantErrContaining(t, (CommitPinResponse{Pinned: true}).Validate(), "commit_id")
}
