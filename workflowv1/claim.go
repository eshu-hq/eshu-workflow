// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"fmt"
	"time"
)

// ClaimSelector identifies which collector actor is attempting to claim work.
type ClaimSelector struct {
	CollectorKind       CollectorKind `json:"collector_kind"`
	CollectorInstanceID string        `json:"collector_instance_id"`
	OwnerID             string        `json:"owner_id"`
	ClaimID             string        `json:"claim_id"`
	// RotationSalt seeds the fair-claim rotation draw for this call.
	// Callers that need reproducible picks (tests) pass a fixed salt;
	// production callers leave it empty and the server draws randomness
	// per head row, keeping every claim an independent shuffle.
	RotationSalt string `json:"rotation_salt,omitempty"`
}

// Validate checks the claim selector identity.
func (s ClaimSelector) Validate() error {
	if err := s.CollectorKind.Validate(); err != nil {
		return err
	}
	if err := validateIdentifier("collector_instance_id", s.CollectorInstanceID); err != nil {
		return err
	}
	if err := validateIdentifier("owner_id", s.OwnerID); err != nil {
		return err
	}
	if err := validateIdentifier("claim_id", s.ClaimID); err != nil {
		return err
	}
	return nil
}

// ClaimMutation carries the fenced mutation arguments for one claim epoch.
type ClaimMutation struct {
	WorkItemID    string        `json:"work_item_id"`
	ClaimID       string        `json:"claim_id"`
	FencingToken  int64         `json:"fencing_token"`
	OwnerID       string        `json:"owner_id"`
	ObservedAt    time.Time     `json:"observed_at"`
	LeaseDuration time.Duration `json:"lease_duration_ns"`
	// Tenant boundary fields are optional for legacy shared-mode work, but
	// hosted claim-aware collectors set all four so the fact commit
	// boundary can re-check and lock the active grant before persisting.
	TenantID           string `json:"tenant_id,omitempty"`
	WorkspaceID        string `json:"workspace_id,omitempty"`
	SubjectClass       string `json:"subject_class,omitempty"`
	PolicyRevisionHash string `json:"policy_revision_hash,omitempty"`
	FailureClass       string `json:"failure_class,omitempty"`
	FailureMessage     string `json:"failure_message,omitempty"`
	// FailureOperation is the bounded claim-stage or provider operation
	// that failed (for example "collect_failure"). It carries only a
	// sanitized [a-z0-9_] token: never a URL, response body, or
	// credential. Blank means the operation was not recorded.
	FailureOperation string `json:"failure_operation,omitempty"`
	// FailureStatusCode is the bounded provider HTTP status preserved
	// from the failure cause, or 0 when no HTTP status is known.
	FailureStatusCode int `json:"failure_status_code,omitempty"`
	// FailureTerminal mirrors the FailClaimTerminal (true) versus
	// FailClaimRetryable (false) route alongside the error's own
	// terminal flag, so terminal rows stay queryable without joining
	// status.
	FailureTerminal bool      `json:"failure_terminal,omitempty"`
	VisibleAt       time.Time `json:"visible_at,omitempty"`
	// Resolved* fields optionally replace a planned work-item phase
	// identity when a collector only knows the final reducer checkpoint
	// tuple after opening the source.
	ResolvedScopeID          string `json:"resolved_scope_id,omitempty"`
	ResolvedAcceptanceUnitID string `json:"resolved_acceptance_unit_id,omitempty"`
	ResolvedSourceRunID      string `json:"resolved_source_run_id,omitempty"`
	ResolvedGenerationID     string `json:"resolved_generation_id,omitempty"`
}

// Validate checks the fenced mutation arguments.
func (m ClaimMutation) Validate() error {
	if err := validateIdentifier("work_item_id", m.WorkItemID); err != nil {
		return err
	}
	if err := validateIdentifier("claim_id", m.ClaimID); err != nil {
		return err
	}
	if m.FencingToken <= 0 {
		return fmt.Errorf("fencing_token must be positive")
	}
	if err := validateIdentifier("owner_id", m.OwnerID); err != nil {
		return err
	}
	if err := validateTime("observed_at", m.ObservedAt); err != nil {
		return err
	}
	if m.LeaseDuration <= 0 {
		return fmt.Errorf("lease_duration_ns must be positive")
	}
	if err := validateTenantBoundary(m.TenantID, m.WorkspaceID, m.SubjectClass, m.PolicyRevisionHash); err != nil {
		return err
	}
	if m.FailureStatusCode < 0 || m.FailureStatusCode > 599 {
		return fmt.Errorf("failure_status_code must be a valid HTTP status or 0")
	}
	return nil
}

// ClaimedWorkItem returns the currently owned work item and claim epoch.
type ClaimedWorkItem struct {
	WorkItem WorkItem `json:"work_item"`
	Claim    Claim    `json:"claim"`
}

// Validate checks the claimed pair.
func (c ClaimedWorkItem) Validate() error {
	if err := c.WorkItem.Validate(); err != nil {
		return err
	}
	if err := c.Claim.Validate(); err != nil {
		return err
	}
	if c.Claim.WorkItemID != c.WorkItem.WorkItemID {
		return fmt.Errorf("claim work_item_id %q does not match work item %q", c.Claim.WorkItemID, c.WorkItem.WorkItemID)
	}
	return nil
}

// ClaimNextRequest asks the server for the next eligible work item. The
// response carries the inline pacing verdict: either a claimed item or a
// deferral, never both.
type ClaimNextRequest struct {
	Selector   ClaimSelector `json:"selector"`
	ObservedAt time.Time     `json:"observed_at"`
	LeaseTTL   time.Duration `json:"lease_ttl_ns"`
}

// Validate checks the claim-next request.
func (r ClaimNextRequest) Validate() error {
	if err := r.Selector.Validate(); err != nil {
		return err
	}
	if err := validateTime("observed_at", r.ObservedAt); err != nil {
		return err
	}
	if r.LeaseTTL <= 0 {
		return fmt.Errorf("lease_ttl_ns must be positive")
	}
	return nil
}

// ClaimNextResponse is exactly one of a grant or a pacing deferral.
type ClaimNextResponse struct {
	Issued   bool                `json:"issued"`
	Grant    ClaimedWorkItem     `json:"grant,omitempty"`
	Deferral ClaimPacingDeferral `json:"deferral,omitempty"`
	// DegradedReason names a degraded-mode fail-open decision behind
	// this answer (pacing_depths, pacing_latch_read,
	// pacing_latch_advance, quota_floor), or empty when the claim path
	// was healthy. Server-only: it is never serialized, so the wire
	// contract is unchanged.
	DegradedReason string `json:"-"`
}

// Validate checks that the response is exactly one outcome.
func (r ClaimNextResponse) Validate() error {
	if r.Issued {
		if r.Deferral.Signal != "" {
			return fmt.Errorf("claim response must not carry both a grant and a deferral")
		}
		return r.Grant.Validate()
	}
	return r.Deferral.Validate()
}

// CheckClaimPacingRequest probes the pacing latch without claiming.
type CheckClaimPacingRequest struct {
	CollectorKind       CollectorKind `json:"collector_kind"`
	CollectorInstanceID string        `json:"collector_instance_id"`
	ObservedAt          time.Time     `json:"observed_at"`
}

// Validate checks the pacing probe.
func (r CheckClaimPacingRequest) Validate() error {
	if err := r.CollectorKind.Validate(); err != nil {
		return err
	}
	if err := validateIdentifier("collector_instance_id", r.CollectorInstanceID); err != nil {
		return err
	}
	if err := validateTime("observed_at", r.ObservedAt); err != nil {
		return err
	}
	return nil
}

// CommitPinRequest asks the server to pin a claim for a fact commit: the
// reaper must not expire the claim before CommitDeadline so the
// in-transaction heartbeat the in-tree path relied on has a protocol
// replacement.
type CommitPinRequest struct {
	ClaimID        string    `json:"claim_id"`
	FencingToken   int64     `json:"fencing_token"`
	ScopeID        string    `json:"scope_id"`
	SourceRunID    string    `json:"source_run_id"`
	GenerationID   string    `json:"generation_id"`
	CommitDeadline time.Time `json:"commit_deadline"`
}

// Validate checks the commit-pin request.
func (r CommitPinRequest) Validate() error {
	if err := validateIdentifier("claim_id", r.ClaimID); err != nil {
		return err
	}
	if r.FencingToken <= 0 {
		return fmt.Errorf("fencing_token must be positive")
	}
	if err := validateIdentifier("scope_id", r.ScopeID); err != nil {
		return err
	}
	if err := validateIdentifier("source_run_id", r.SourceRunID); err != nil {
		return err
	}
	if err := validateIdentifier("generation_id", r.GenerationID); err != nil {
		return err
	}
	if err := validateTime("commit_deadline", r.CommitDeadline); err != nil {
		return err
	}
	return nil
}

// CommitPinResponse carries the server-side pin: the commit tag core
// stamps on the generation. Core must never activate a generation below
// the highest activated fencing token.
type CommitPinResponse struct {
	Pinned   bool   `json:"pinned"`
	CommitID string `json:"commit_id"`
}

// Validate checks the commit-pin response.
func (r CommitPinResponse) Validate() error {
	if !r.Pinned {
		return fmt.Errorf("unpinned commit response must surface as a stale-fence error, not a body")
	}
	return validateIdentifier("commit_id", r.CommitID)
}
