// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// Degradation reasons reported on fail-open claim paths. The reason rides
// the response to the server for logging and counting; it never reaches
// the wire body.
const (
	degradedPacingDepths       = "pacing_depths"
	degradedPacingLatchRead    = "pacing_latch_read"
	degradedPacingLatchAdvance = "pacing_latch_advance"
	degradedQuotaFloor         = "quota_floor"
)

// degraded records one fail-open decision: a structured warn log carrying
// the cause plus a counter bump by reason. Claiming stays open; only the
// silence is removed. Instruments is nil-safe, so unconfigured stores
// still log.
func (s *Store) degraded(reason string, cause error, attrs ...any) {
	args := append([]any{
		slog.String("reason", reason),
		slog.String("error", cause.Error()),
	}, attrs...)
	s.logger.Warn("claim-plane degraded", args...)
	if s.Instruments != nil {
		s.Instruments.ObserveDegraded(reason)
	}
}

// checkPacing evaluates the shared hysteresis latch for one claim attempt.
// It returns the deferral and true when the grant must wait. Storage
// failures fail open to a deferral-free pass and report the reason: depth
// reads must never wedge claiming, so the degradation — not an error —
// rides back for the response while the cause lands in logs and metrics.
// The latch row stays authoritative in storage and is always read fresh;
// only the depth numbers may lag by DepthCacheTTL.
func (s *Store) checkPacing(
	ctx context.Context,
	now time.Time,
	kind, instance string,
) (workflowv1.ClaimPacingDeferral, bool, string) {
	if !s.pacing.Enabled() {
		return workflowv1.ClaimPacingDeferral{}, false, ""
	}
	domain := kind + "/" + instance
	depths, err := s.cachedDepths(ctx, now, kind, instance)
	if err != nil {
		s.degraded(degradedPacingDepths, err,
			slog.String("collector_kind", kind),
			slog.String("collector_instance_id", instance))
		return workflowv1.ClaimPacingDeferral{}, false, degradedPacingDepths
	}
	degraded := ""
	for _, signal := range []workflowv1.ClaimPacingSignal{
		workflowv1.ClaimPacingSignalDomain,
		workflowv1.ClaimPacingSignalGlobal,
	} {
		latch, err := s.readLatch(ctx, signal, domainFor(signal, domain))
		if err != nil {
			s.degraded(degradedPacingLatchRead, err,
				slog.String("signal", string(signal)),
				slog.String("domain", domain))
			return workflowv1.ClaimPacingDeferral{}, false, degradedPacingLatchRead
		}
		var engagedFor time.Duration
		if latch.Engaged && latch.EngagedAt != nil {
			engagedFor = now.Sub(*latch.EngagedAt)
		}
		decision := workflowv1.DecideClaimPacing(s.pacing, depths, workflowv1.ClaimPacingLatch{
			Engaged: latch.Engaged,
			Signal:  latch.Signal,
		}, engagedFor)
		if !decision.Paced {
			if err := s.advanceLatch(ctx, now, signal, domainFor(signal, domain), false); err != nil {
				s.degraded(degradedPacingLatchAdvance, err,
					slog.String("signal", string(signal)),
					slog.String("domain", domain))
				degraded = degradedPacingLatchAdvance
			}
			continue
		}
		if err := s.advanceLatch(ctx, now, signal, domainFor(signal, domain), true); err != nil {
			s.degraded(degradedPacingLatchAdvance, err,
				slog.String("signal", string(signal)),
				slog.String("domain", domain))
			degraded = degradedPacingLatchAdvance
		}
		return workflowv1.ClaimPacingDeferral{
			Signal:     decision.Signal,
			Domain:     domainForDeferral(decision.Signal, domain),
			RetryAfter: decision.RetryAfter,
			Stuck:      decision.Stuck,
		}, true, degraded
	}
	return workflowv1.ClaimPacingDeferral{}, false, degraded
}

func domainFor(signal workflowv1.ClaimPacingSignal, domain string) string {
	if signal == workflowv1.ClaimPacingSignalGlobal {
		return ""
	}
	return domain
}

func domainForDeferral(signal workflowv1.ClaimPacingSignal, domain string) string {
	if signal == workflowv1.ClaimPacingSignalGlobal {
		return ""
	}
	return domain
}

type storedLatch struct {
	Engaged   bool
	Signal    workflowv1.ClaimPacingSignal
	EngagedAt *time.Time
}

func (s *Store) readLatch(ctx context.Context, signal workflowv1.ClaimPacingSignal, domain string) (storedLatch, error) {
	var latch storedLatch
	var signalText string
	err := s.pool.QueryRow(ctx, `SELECT engaged, signal, engaged_at
FROM workflow_claim_pacing WHERE signal = $1 AND domain = $2`, string(signal), domain).
		Scan(&latch.Engaged, &signalText, &latch.EngagedAt)
	if err != nil {
		if errNoRows(err) {
			return storedLatch{}, nil
		}
		return storedLatch{}, err
	}
	latch.Signal = workflowv1.ClaimPacingSignal(signalText)
	return latch, nil
}

// errNoRows reports a missing row without importing driver internals at
// the call site.
func errNoRows(err error) bool {
	return err != nil && err.Error() == "no rows in result set"
}

// advanceLatch persists the hysteresis latch. A write failure never
// wedges claiming: the error returns to checkPacing, which logs and
// counts the degradation and keeps the previous latch state.
func (s *Store) advanceLatch(
	ctx context.Context,
	now time.Time,
	signal workflowv1.ClaimPacingSignal,
	domain string,
	engaged bool,
) error {
	if engaged {
		_, err := s.pool.Exec(ctx, `INSERT INTO workflow_claim_pacing
(signal, domain, engaged, engaged_at, updated_at)
VALUES ($1, $2, TRUE, COALESCE((SELECT engaged_at FROM workflow_claim_pacing
WHERE signal = $1 AND domain = $2), $3), $3)
ON CONFLICT (signal, domain) DO UPDATE
SET engaged = TRUE,
    engaged_at = COALESCE(workflow_claim_pacing.engaged_at, EXCLUDED.engaged_at),
    updated_at = EXCLUDED.updated_at`,
			string(signal), domain, now)
		if err != nil {
			return fmt.Errorf("engage pacing latch: %w", err)
		}
		return nil
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO workflow_claim_pacing
(signal, domain, engaged, engaged_at, updated_at)
VALUES ($1, $2, FALSE, NULL, $3)
ON CONFLICT (signal, domain) DO UPDATE
SET engaged = FALSE, engaged_at = NULL, updated_at = EXCLUDED.updated_at`,
		string(signal), domain, now)
	if err != nil {
		return fmt.Errorf("release pacing latch: %w", err)
	}
	return nil
}

func (s *Store) cachedDepths(
	ctx context.Context,
	now time.Time,
	kind, instance string,
) (workflowv1.ClaimPacingDepths, error) {
	ttl := s.pacing.DepthCacheTTL
	domain := kind + "/" + instance
	s.mu.Lock()
	cached := s.depthCache
	s.mu.Unlock()
	if ttl > 0 && now.Sub(cached.at) < ttl {
		if depth, ok := cached.domain[domain]; ok {
			return workflowv1.ClaimPacingDepths{Domain: depth, Global: cached.global}, nil
		}
	}
	var depths workflowv1.ClaimPacingDepths
	err := s.pool.QueryRow(ctx, `SELECT
COALESCE(SUM(CASE WHEN collector_kind = $1 AND collector_instance_id = $2 THEN 1 ELSE 0 END), 0)::BIGINT,
COUNT(*)::BIGINT
FROM workflow_work_items WHERE status = 'pending'`, kind, instance).Scan(&depths.Domain, &depths.Global)
	if err != nil {
		return depths, err
	}
	if ttl > 0 {
		s.mu.Lock()
		if s.depthCache.domain == nil {
			s.depthCache.domain = map[string]int64{}
		}
		s.depthCache.at = now
		s.depthCache.ttl = ttl
		s.depthCache.domain[domain] = depths.Domain
		s.depthCache.global = depths.Global
		s.mu.Unlock()
	}
	return depths, nil
}

// CheckPacing probes the pacing verdict for one family without claiming.
// The probe never advances the latch: only a real ClaimNext engages or
// releases it.
func (s *Store) CheckPacing(
	ctx context.Context,
	kind, instance string,
	now time.Time,
) (workflowv1.ClaimPacingDecision, error) {
	if !s.pacing.Enabled() {
		return workflowv1.ClaimPacingDecision{}, nil
	}
	now = now.UTC()
	domain := kind + "/" + instance
	depths, err := s.cachedDepths(ctx, now, kind, instance)
	if err != nil {
		return workflowv1.ClaimPacingDecision{}, fmt.Errorf("pacing depths: %w", err)
	}
	for _, signal := range []workflowv1.ClaimPacingSignal{
		workflowv1.ClaimPacingSignalDomain,
		workflowv1.ClaimPacingSignalGlobal,
	} {
		latch, err := s.readLatch(ctx, signal, domainFor(signal, domain))
		if err != nil {
			return workflowv1.ClaimPacingDecision{}, fmt.Errorf("pacing latch: %w", err)
		}
		var engagedFor time.Duration
		if latch.Engaged && latch.EngagedAt != nil {
			engagedFor = now.Sub(*latch.EngagedAt)
		}
		decision := workflowv1.DecideClaimPacing(s.pacing, depths, workflowv1.ClaimPacingLatch{
			Engaged: latch.Engaged,
			Signal:  latch.Signal,
		}, engagedFor)
		if decision.Paced {
			decision.Domain = domainForDeferral(decision.Signal, domain)
			return decision, nil
		}
	}
	return workflowv1.ClaimPacingDecision{}, nil
}

// claimNextWithQuota grants through the quota-gated statement: the
// reservation upsert in the same statement is the gate, so concurrent
// replicas cannot overshoot. A quota-blocked head returns a domain
// deferral whose backoff reaches the next hour window.
func (s *Store) claimNextWithQuota(
	ctx context.Context,
	selector workflowv1.ClaimSelector,
	now time.Time,
	leaseTTL time.Duration,
	limit int,
) (workflowv1.ClaimNextResponse, error) {
	grant, err := s.runClaimStatement(ctx, claimNextQuotaQuery, []any{
		string(selector.CollectorKind),
		selector.CollectorInstanceID,
		now,
		selector.OwnerID,
		selector.ClaimID,
		now.Add(leaseTTL),
		selector.RotationSalt,
		limit,
	})
	if err != nil {
		return s.claimWithFloor(ctx, selector, now, leaseTTL, err)
	}
	if grant != nil {
		return workflowv1.ClaimNextResponse{Issued: true, Grant: *grant}, nil
	}
	throttled, err := s.quotaThrottled(ctx, selector, now, limit)
	if err != nil {
		return workflowv1.ClaimNextResponse{}, err
	}
	if throttled {
		window := now.Truncate(time.Hour).Add(time.Hour)
		return workflowv1.ClaimNextResponse{Deferral: workflowv1.ClaimPacingDeferral{
			Signal:     workflowv1.ClaimPacingSignalDomain,
			Domain:     string(selector.CollectorKind) + "/" + selector.CollectorInstanceID,
			RetryAfter: window.Sub(now),
		}}, nil
	}
	return workflowv1.ClaimNextResponse{}, nil
}

func (s *Store) quotaThrottled(
	ctx context.Context,
	selector workflowv1.ClaimSelector,
	now time.Time,
	limit int,
) (bool, error) {
	var hasWork, throttled bool
	err := s.pool.QueryRow(ctx, quotaThrottleProbeQuery,
		string(selector.CollectorKind),
		selector.CollectorInstanceID,
		now,
		limit,
	).Scan(&hasWork, &throttled)
	if err != nil {
		return false, fmt.Errorf("quota throttle probe: %w", err)
	}
	return hasWork && throttled, nil
}

// claimWithFloor is the degraded-mode path: when the quota statement
// itself fails, grants continue bounded by an in-memory per-component
// hourly budget instead of wedging the family. Entry logs and counts the
// quota cause, and every non-error answer carries the quota_floor reason
// so the server can tell floor grants from quota grants. The floor is
// deliberately local to one server: it bounds damage, it never grants
// authority.
func (s *Store) claimWithFloor(
	ctx context.Context,
	selector workflowv1.ClaimSelector,
	now time.Time,
	leaseTTL time.Duration,
	cause error,
) (workflowv1.ClaimNextResponse, error) {
	s.degraded(degradedQuotaFloor, cause,
		slog.String("collector_kind", string(selector.CollectorKind)),
		slog.String("collector_instance_id", selector.CollectorInstanceID))
	window := now.Truncate(time.Hour)
	key := floorKey{component: string(selector.CollectorKind), window: window}
	s.mu.Lock()
	if s.floorWindow != window {
		s.floorWindow = window
		s.floorUsed = map[floorKey]int{}
	}
	used := s.floorUsed[key]
	if used >= s.floor {
		s.mu.Unlock()
		return workflowv1.ClaimNextResponse{
			Deferral: workflowv1.ClaimPacingDeferral{
				Signal:     workflowv1.ClaimPacingSignalDomain,
				Domain:     string(selector.CollectorKind) + "/" + selector.CollectorInstanceID,
				RetryAfter: window.Add(time.Hour).Sub(now),
				Stuck:      true,
			},
			DegradedReason: degradedQuotaFloor,
		}, nil
	}
	s.floorUsed[key] = used + 1
	s.mu.Unlock()
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
		return workflowv1.ClaimNextResponse{},
			fmt.Errorf("quota-gated claim failed (%v) and floor claim failed: %w", cause, err)
	}
	if grant == nil {
		return workflowv1.ClaimNextResponse{DegradedReason: degradedQuotaFloor}, nil
	}
	return workflowv1.ClaimNextResponse{Issued: true, Grant: *grant, DegradedReason: degradedQuotaFloor}, nil
}

// PinForCommit pins a live claim for a fact commit: the reaper must not
// expire it before the commit deadline. Repeating a pin with identical
// arguments returns the same commit id: the pin is the idempotency key for
// an ambiguous commit timeout.
func (s *Store) PinForCommit(
	ctx context.Context,
	req workflowv1.CommitPinRequest,
) (workflowv1.CommitPinResponse, error) {
	if err := req.Validate(); err != nil {
		return workflowv1.CommitPinResponse{}, staleAsInvalid(err)
	}
	commitID := fmt.Sprintf("commit-%s-%d", req.ClaimID, req.FencingToken)
	var pinned bool
	var returnedID string
	err := s.pool.QueryRow(ctx, pinClaimQuery,
		req.ClaimID,
		req.FencingToken,
		req.ScopeID,
		req.SourceRunID,
		req.GenerationID,
		req.CommitDeadline.UTC(),
		commitID,
	).Scan(&pinned, &returnedID)
	if err != nil {
		return workflowv1.CommitPinResponse{}, fmt.Errorf("pin claim for commit: %w", err)
	}
	if !pinned {
		return workflowv1.CommitPinResponse{}, s.classifyPinMiss(ctx, req)
	}
	return workflowv1.CommitPinResponse{Pinned: true, CommitID: returnedID}, nil
}

func (s *Store) classifyPinMiss(ctx context.Context, req workflowv1.CommitPinRequest) error {
	epoch, err := s.readEpoch(ctx, req.ClaimID)
	if err != nil {
		return err
	}
	if !epoch.found {
		return &workflowv1.ProtocolError{
			Code:    workflowv1.CodeNotFound,
			Message: fmt.Sprintf("pin: unknown claim %q", req.ClaimID),
		}
	}
	return &workflowv1.ProtocolError{
		Code: workflowv1.CodeStaleFence,
		Message: fmt.Sprintf("pin: claim %s is %s at fencing token %d",
			req.ClaimID, epoch.status, epoch.fencingToken),
	}
}

// pinClaimQuery pins the live epoch for a commit. The update fires only
// while the claim is still the item's current live epoch and the phase
// identity matches the request, so a pin can never anchor a superseded
// generation. COALESCE keeps the first commit id stable across replays;
// GREATEST only ever extends the deadline.
const pinClaimQuery = `
WITH candidate AS (
    SELECT item.work_item_id
    FROM workflow_work_items AS item
    JOIN workflow_claims AS claim
      ON claim.claim_id = $1
     AND claim.work_item_id = item.work_item_id
    WHERE item.current_claim_id = $1
      AND item.current_fencing_token = $2
      AND item.status = 'claimed'
      AND item.scope_id = $3
      AND item.source_run_id = $4
      AND item.generation_id = $5
      AND claim.fencing_token = $2
      AND claim.status = 'active'
    FOR UPDATE OF item, claim
),
pinned AS (
    UPDATE workflow_claims AS claim
    SET commit_id = COALESCE(claim.commit_id, $7),
        commit_pinned_until = GREATEST(
            COALESCE(claim.commit_pinned_until, $6),
            $6
        ),
        updated_at = now()
    FROM candidate
    WHERE claim.claim_id = $1
      AND claim.work_item_id = candidate.work_item_id
    RETURNING claim.commit_id
)
SELECT EXISTS(SELECT 1 FROM pinned) AS pinned,
    COALESCE((SELECT commit_id FROM pinned), '') AS commit_id
`
