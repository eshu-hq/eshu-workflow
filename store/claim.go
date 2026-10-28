// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/eshu-hq/eshu-workflow/server"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// The pgx store serves the full HTTP plane surface.
var _ server.ClaimStore = (*Store)(nil)

// ClaimNext grants the next eligible work item or returns the inline pacing
// verdict. Fairness, the pacing latch, and quota accounting execute inside
// this call; an idle queue reports (zero response, nil) and the server maps
// it to not_found.
func (s *Store) ClaimNext(
	ctx context.Context,
	selector workflowv1.ClaimSelector,
	now time.Time,
	leaseTTL time.Duration,
) (workflowv1.ClaimNextResponse, error) {
	if err := selector.Validate(); err != nil {
		return workflowv1.ClaimNextResponse{}, staleAsInvalid(err)
	}
	if leaseTTL <= 0 {
		leaseTTL = workflowv1.DefaultClaimLeaseTTL()
	}
	now = now.UTC()
	if deferral, paced := s.checkPacing(ctx, now, string(selector.CollectorKind), selector.CollectorInstanceID); paced {
		return workflowv1.ClaimNextResponse{Deferral: deferral}, nil
	}
	if limit, ok := s.quotas[string(selector.CollectorKind)]; ok {
		return s.claimNextWithQuota(ctx, selector, now, leaseTTL, limit)
	}
	grant, err := s.runClaimStatement(ctx, claimNextQuery, []any{
		string(selector.CollectorKind),
		selector.CollectorInstanceID,
		now,
		selector.OwnerID,
		selector.ClaimID,
		now.Add(leaseTTL),
		selector.RotationSalt,
	})
	if err != nil {
		return workflowv1.ClaimNextResponse{}, err
	}
	if grant == nil {
		return workflowv1.ClaimNextResponse{}, nil
	}
	return workflowv1.ClaimNextResponse{Issued: true, Grant: *grant}, nil
}

// HeartbeatClaim extends the live ownership epoch. Repeating a heartbeat
// with identical epoch arguments is idempotent: a live claim renews, and a
// claim that already moved on reports stale_fence.
func (s *Store) HeartbeatClaim(ctx context.Context, mutation workflowv1.ClaimMutation) error {
	return s.Heartbeat(ctx, mutation)
}

// Heartbeat renews the lease on the live epoch.
func (s *Store) Heartbeat(ctx context.Context, mutation workflowv1.ClaimMutation) error {
	if err := mutation.Validate(); err != nil {
		return staleAsInvalid(err)
	}
	ttl := mutation.LeaseDuration
	if ttl <= 0 {
		ttl = workflowv1.DefaultClaimLeaseTTL()
	}
	tag, err := s.pool.Exec(ctx, heartbeatClaimQuery,
		mutation.ObservedAt.UTC(),
		mutation.ObservedAt.UTC().Add(ttl),
		mutation.FencingToken,
		mutation.OwnerID,
		mutation.ClaimID,
		mutation.WorkItemID,
	)
	if err != nil {
		return fmt.Errorf("heartbeat claim: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	// A heartbeat replayed after its own commit is indistinguishable from
	// a lost race by rows alone: consult the epoch before crying stale.
	return s.mapEmptyMutation(ctx, mutation, "heartbeat")
}

// CompleteClaim marks the live epoch and work item complete. Repeating a
// complete with identical epoch arguments succeeds: the commit already
// landed, so the replay is an acknowledgement, not a second completion.
func (s *Store) CompleteClaim(ctx context.Context, mutation workflowv1.ClaimMutation) error {
	return s.Complete(ctx, mutation)
}

// Complete finishes the live epoch and its work item.
func (s *Store) Complete(ctx context.Context, mutation workflowv1.ClaimMutation) error {
	if err := mutation.Validate(); err != nil {
		return staleAsInvalid(err)
	}
	tag, err := s.pool.Exec(ctx, completeClaimQuery,
		mutation.ObservedAt.UTC(),
		mutation.ObservedAt.UTC().Add(mutation.LeaseDuration),
		mutation.FencingToken,
		mutation.OwnerID,
		mutation.ClaimID,
		mutation.WorkItemID,
		mutation.ResolvedScopeID,
		mutation.ResolvedAcceptanceUnitID,
		mutation.ResolvedSourceRunID,
		mutation.ResolvedGenerationID,
	)
	if err != nil {
		return fmt.Errorf("complete claim: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	return s.mapEmptyMutation(ctx, mutation, "complete")
}

// ReleaseClaim gives up the live epoch without recording a failure.
// Repeating a release with identical epoch arguments succeeds.
func (s *Store) ReleaseClaim(ctx context.Context, mutation workflowv1.ClaimMutation) error {
	return s.Release(ctx, mutation)
}

// Release abandons the live epoch; the item returns to pending.
func (s *Store) Release(ctx context.Context, mutation workflowv1.ClaimMutation) error {
	if err := mutation.Validate(); err != nil {
		return staleAsInvalid(err)
	}
	tag, err := s.pool.Exec(ctx, releaseClaimQuery,
		mutation.ObservedAt.UTC(),
		mutation.ObservedAt.UTC(),
		mutation.FencingToken,
		mutation.OwnerID,
		mutation.ClaimID,
		mutation.WorkItemID,
	)
	if err != nil {
		return fmt.Errorf("release claim: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	return s.mapEmptyMutation(ctx, mutation, "release")
}

// FailRetryable ends the epoch retryable and requeues the item. Repeating a
// fail with identical epoch arguments succeeds.
func (s *Store) FailRetryable(ctx context.Context, mutation workflowv1.ClaimMutation) error {
	if err := mutation.Validate(); err != nil {
		return staleAsInvalid(err)
	}
	visibleAt := mutation.VisibleAt
	if visibleAt.IsZero() {
		visibleAt = mutation.ObservedAt
	}
	tag, err := s.pool.Exec(ctx, failClaimRetryableQuery,
		mutation.ObservedAt.UTC(),
		visibleAt.UTC(),
		mutation.FencingToken,
		mutation.OwnerID,
		mutation.ClaimID,
		mutation.WorkItemID,
		mutation.FailureClass,
		mutation.FailureMessage,
		mutation.FailureOperation,
		mutation.FailureStatusCode,
		mutation.FailureTerminal,
	)
	if err != nil {
		return fmt.Errorf("fail claim retryable: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	return s.mapEmptyMutation(ctx, mutation, "fail_retryable")
}

// FailTerminal ends the epoch terminal without requeueing. Repeating a
// fail with identical epoch arguments succeeds.
func (s *Store) FailTerminal(ctx context.Context, mutation workflowv1.ClaimMutation) error {
	if err := mutation.Validate(); err != nil {
		return staleAsInvalid(err)
	}
	tag, err := s.pool.Exec(ctx, failClaimTerminalQuery,
		mutation.ObservedAt.UTC(),
		mutation.FencingToken,
		mutation.OwnerID,
		mutation.ClaimID,
		mutation.WorkItemID,
		mutation.FailureClass,
		mutation.FailureMessage,
		mutation.FailureOperation,
		mutation.FailureStatusCode,
		mutation.FailureTerminal,
	)
	if err != nil {
		return fmt.Errorf("fail claim terminal: %w", err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	return s.mapEmptyMutation(ctx, mutation, "fail_terminal")
}

// Reap expires stale active claims past asOf and requeues their work,
// skipping claims a commit pin still covers. It returns the expired claims.
func (s *Store) Reap(
	ctx context.Context,
	asOf time.Time,
	limit int,
	requeueDelay time.Duration,
) ([]workflowv1.Claim, error) {
	if limit <= 0 {
		return nil, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: "expired claim limit must be positive"}
	}
	if requeueDelay < 0 {
		requeueDelay = 0
	}
	rows, err := s.pool.Query(ctx, reapExpiredClaimsQuery, asOf.UTC(), limit, asOf.UTC().Add(requeueDelay))
	if err != nil {
		return nil, fmt.Errorf("reap expired claims: %w", err)
	}
	defer rows.Close()
	claims := make([]workflowv1.Claim, 0)
	for rows.Next() {
		var claim workflowv1.Claim
		if err := rows.Scan(
			&claim.ClaimID, &claim.WorkItemID, &claim.FencingToken, &claim.OwnerID,
			&claim.Status, &claim.ClaimedAt, &claim.HeartbeatAt, &claim.LeaseExpiresAt,
			&claim.CreatedAt, &claim.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("reap expired claims scan: %w", err)
		}
		claims = append(claims, claim)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reap expired claims: %w", err)
	}
	return claims, nil
}

// epochStatus reads the claim row a mutation just missed.
type epochStatus struct {
	found        bool
	status       string
	fencingToken int64
	ownerID      string
}

func (s *Store) readEpoch(ctx context.Context, claimID string) (epochStatus, error) {
	var st epochStatus
	err := s.pool.QueryRow(ctx, `SELECT status, fencing_token, owner_id
FROM workflow_claims WHERE claim_id = $1`, claimID).Scan(&st.status, &st.fencingToken, &st.ownerID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return epochStatus{}, nil
		}
		return st, fmt.Errorf("read claim epoch: %w", err)
	}
	st.found = true
	return st, nil
}

// mapEmptyMutation classifies a mutation that changed no rows: a replay of
// an already-applied epoch succeeds only while the item has not moved on
// to a newer epoch, anything else is a stale fence. The item check is what
// keeps a replayed release from acknowledging away a successor's live
// claim: the claim row alone cannot tell a replay from a superseded epoch
// once the item is reclaimed.
func (s *Store) mapEmptyMutation(ctx context.Context, mutation workflowv1.ClaimMutation, op string) error {
	epoch, err := s.readEpoch(ctx, mutation.ClaimID)
	if err != nil {
		return err
	}
	if !epoch.found {
		return &workflowv1.ProtocolError{
			Code:    workflowv1.CodeStaleFence,
			Message: fmt.Sprintf("%s: unknown claim %q", op, mutation.ClaimID),
		}
	}
	if epoch.fencingToken == mutation.FencingToken && epoch.ownerID == mutation.OwnerID {
		if op == "heartbeat" {
			// A heartbeat that lands after its own epoch finished is a
			// late duplicate, not a renewal: the epoch is gone.
			return &workflowv1.ProtocolError{
				Code:    workflowv1.CodeStaleFence,
				Message: fmt.Sprintf("heartbeat: claim already %s", epoch.status),
			}
		}
		itemFencing, err := s.readItemFencing(ctx, mutation.WorkItemID)
		if err != nil {
			return err
		}
		if itemFencing == mutation.FencingToken {
			switch op {
			case "complete":
				if epoch.status == string(workflowv1.ClaimStatusCompleted) {
					return nil
				}
			case "release":
				if epoch.status == string(workflowv1.ClaimStatusReleased) {
					return nil
				}
			case "fail_retryable":
				if epoch.status == string(workflowv1.ClaimStatusFailedRetryable) {
					return nil
				}
			case "fail_terminal":
				if epoch.status == string(workflowv1.ClaimStatusFailedTerminal) {
					return nil
				}
			}
		}
	}
	return &workflowv1.ProtocolError{
		Code:    workflowv1.CodeStaleFence,
		Message: fmt.Sprintf("%s: epoch superseded (claim %s)", op, epoch.status),
	}
}

// readItemFencing returns the work item's current fencing token, or -1 when
// the item is gone (a gone item always reads as moved on).
func (s *Store) readItemFencing(ctx context.Context, workItemID string) (int64, error) {
	var fencing int64
	err := s.pool.QueryRow(ctx, `SELECT current_fencing_token
FROM workflow_work_items WHERE work_item_id = $1`, workItemID).Scan(&fencing)
	if err != nil {
		if err == pgx.ErrNoRows {
			return -1, nil
		}
		return -1, fmt.Errorf("read work item fencing: %w", err)
	}
	return fencing, nil
}

func staleAsInvalid(err error) error {
	return &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()}
}

// runClaimStatement runs one atomic claim statement. No rows means no
// eligible work, never an error.
func (s *Store) runClaimStatement(ctx context.Context, query string, args []any) (*workflowv1.ClaimedWorkItem, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("claim work item: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("claim work item: %w", err)
		}
		return nil, nil
	}
	grant, err := scanClaimedItem(rows)
	if err != nil {
		return nil, fmt.Errorf("claim work item scan: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim work item: %w", err)
	}
	return &grant, nil
}
