// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/store"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

type mutationOp struct {
	name string
	call func(context.Context, *claimstore.Store, workflowv1.ClaimMutation) error
}

func mutationOps() []mutationOp {
	return []mutationOp{
		{"Heartbeat", func(ctx context.Context, s *claimstore.Store, m workflowv1.ClaimMutation) error {
			return s.Heartbeat(ctx, m)
		}},
		{"Complete", func(ctx context.Context, s *claimstore.Store, m workflowv1.ClaimMutation) error {
			return s.Complete(ctx, m)
		}},
		{"Release", func(ctx context.Context, s *claimstore.Store, m workflowv1.ClaimMutation) error {
			return s.Release(ctx, m)
		}},
		{"FailRetryable", func(ctx context.Context, s *claimstore.Store, m workflowv1.ClaimMutation) error {
			return s.FailRetryable(ctx, m)
		}},
		{"FailTerminal", func(ctx context.Context, s *claimstore.Store, m workflowv1.ClaimMutation) error {
			return s.FailTerminal(ctx, m)
		}},
	}
}

// TestMutationsRejectUnknownClaimAsStale requires a mutation naming a claim
// that never existed to be a stale fence, never a silent success.
func TestMutationsRejectUnknownClaimAsStale(t *testing.T) {
	st, ctx := openFreshStore(t)
	for _, op := range mutationOps() {
		t.Run(op.name, func(t *testing.T) {
			m := validMutation()
			m.ClaimID = "claim-that-never-existed"
			err := op.call(ctx, st, m)
			if !isCode(err, workflowv1.CodeStaleFence) {
				t.Fatalf("unknown claim error = %v, want stale_fence", err)
			}
			if !strings.Contains(err.Error(), "unknown claim") {
				t.Fatalf("error %q does not say the claim is unknown", err)
			}
		})
	}
}

// TestMutationsRejectInvalidRequests requires validation to run before any
// database work, so a malformed mutation is invalid, not stale.
func TestMutationsRejectInvalidRequests(t *testing.T) {
	st, ctx := openFreshStore(t)
	for _, op := range mutationOps() {
		t.Run(op.name, func(t *testing.T) {
			m := validMutation()
			m.FencingToken = 0
			if err := op.call(ctx, st, m); !isCode(err, workflowv1.CodeInvalid) {
				t.Fatalf("zero fencing token error = %v, want invalid", err)
			}
		})
	}
}

// TestMutationsRejectWrongOwnerOnLiveClaim requires a holder that presents
// the right token with the wrong owner to be fenced out and leave the
// claim live.
func TestMutationsRejectWrongOwnerOnLiveClaim(t *testing.T) {
	for _, op := range mutationOps() {
		t.Run(op.name, func(t *testing.T) {
			st, ctx := openFreshStore(t)
			runID := seedRun(t, ctx, st)
			if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "wo-1", false)}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			grant := claimOne(t, ctx, st, "wo-claim", "owner-real", time.Minute).Grant
			m := mutationFor(grant, time.Minute)
			m.OwnerID = "owner-impostor"
			err := op.call(ctx, st, m)
			if !isCode(err, workflowv1.CodeStaleFence) {
				t.Fatalf("wrong owner error = %v, want stale_fence", err)
			}
			// The rightful holder still owns the claim and can heartbeat.
			if err := st.Heartbeat(ctx, mutationFor(grant, time.Minute)); err != nil {
				t.Fatalf("rightful heartbeat after impostor attempt: %v", err)
			}
		})
	}
}

// TestLateHeartbeatOnFinishedEpochIsStale requires a heartbeat that lands
// after its own epoch ended to be fenced, not treated as a renewal.
func TestLateHeartbeatOnFinishedEpochIsStale(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "late-1", false)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	grant := claimOne(t, ctx, st, "late-claim", "owner-1", time.Minute).Grant
	m := mutationFor(grant, time.Minute)
	if err := st.Complete(ctx, m); err != nil {
		t.Fatalf("complete: %v", err)
	}
	err := st.Heartbeat(ctx, m)
	if !isCode(err, workflowv1.CodeStaleFence) || !strings.Contains(err.Error(), "already completed") {
		t.Fatalf("late heartbeat error = %v, want stale_fence naming the finished epoch", err)
	}
}

// TestReplayedTerminalMutationsAcknowledge requires each fail-style
// mutation to be idempotent for its own epoch, while a different terminal
// op against that epoch is fenced.
func TestReplayedTerminalMutationsAcknowledge(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)
	cases := []struct {
		name  string
		apply func(workflowv1.ClaimMutation) error
		other func(workflowv1.ClaimMutation) error
	}{
		{
			"release", func(m workflowv1.ClaimMutation) error { return st.Release(ctx, m) },
			func(m workflowv1.ClaimMutation) error { return st.Complete(ctx, m) },
		},
		{
			"fail_retryable", func(m workflowv1.ClaimMutation) error { return st.FailRetryable(ctx, m) },
			func(m workflowv1.ClaimMutation) error { return st.Release(ctx, m) },
		},
		{
			"fail_terminal", func(m workflowv1.ClaimMutation) error { return st.FailTerminal(ctx, m) },
			func(m workflowv1.ClaimMutation) error { return st.FailRetryable(ctx, m) },
		},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			itemID := fmt.Sprintf("replay-%d", i)
			if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, itemID, false)}); err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			grant := claimOne(t, ctx, st, "replay-claim-"+itemID, "owner-1", time.Minute).Grant
			m := mutationFor(grant, time.Minute)
			m.FailureClass = "provider_error"
			m.FailureMessage = "boom"
			if err := tc.apply(m); err != nil {
				t.Fatalf("first apply: %v", err)
			}
			if err := tc.apply(m); err != nil {
				t.Fatalf("replay must acknowledge, got: %v", err)
			}
			if err := tc.other(m); !isCode(err, workflowv1.CodeStaleFence) {
				t.Fatalf("different op on a finished epoch error = %v, want stale_fence", err)
			}
		})
	}
}

// TestPinClassifiesMissesPrecisely requires a pin that cannot land to say
// whether the claim is unknown or superseded.
func TestPinClassifiesMissesPrecisely(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "pin-1", false)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	grant := claimOne(t, ctx, st, "pin-claim", "owner-1", time.Minute).Grant
	pin := workflowv1.CommitPinRequest{
		ClaimID: grant.Claim.ClaimID, FencingToken: grant.Claim.FencingToken,
		ScopeID: grant.WorkItem.ScopeID, SourceRunID: grant.WorkItem.SourceRunID,
		GenerationID: grant.WorkItem.GenerationID, CommitDeadline: time.Now().UTC().Add(time.Minute),
	}

	unknown := pin
	unknown.ClaimID = "pin-claim-unknown"
	if _, err := st.PinForCommit(ctx, unknown); !isCode(err, workflowv1.CodeNotFound) {
		t.Fatalf("unknown claim pin error = %v, want not_found", err)
	}

	wrongToken := pin
	wrongToken.FencingToken = grant.Claim.FencingToken + 5
	if _, err := st.PinForCommit(ctx, wrongToken); !isCode(err, workflowv1.CodeStaleFence) {
		t.Fatalf("wrong token pin error = %v, want stale_fence", err)
	}

	first, err := st.PinForCommit(ctx, pin)
	if err != nil {
		t.Fatalf("pin: %v", err)
	}
	again, err := st.PinForCommit(ctx, pin)
	if err != nil || again.CommitID != first.CommitID {
		t.Fatalf("replayed pin = %+v, %v, want the same commit id %q", again, err, first.CommitID)
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("pin response violates its contract: %v", err)
	}

	if _, err := st.PinForCommit(ctx, workflowv1.CommitPinRequest{}); !isCode(err, workflowv1.CodeInvalid) {
		t.Fatalf("empty pin error = %v, want invalid", err)
	}
}

// TestReapRejectsNonPositiveLimitAndClampsNegativeDelay requires the
// reaper bounds to be enforced at the store, not only at the HTTP edge.
func TestReapRejectsNonPositiveLimitAndClampsNegativeDelay(t *testing.T) {
	st, ctx := openFreshStore(t)
	now := time.Now().UTC()
	if _, err := st.Reap(ctx, now, 0, time.Second); !isCode(err, workflowv1.CodeInvalid) {
		t.Fatalf("zero limit error = %v, want invalid", err)
	}
	claims, err := st.Reap(ctx, now, 5, -time.Hour)
	if err != nil {
		t.Fatalf("negative delay must clamp, got: %v", err)
	}
	if claims == nil || len(claims) != 0 {
		t.Fatalf("reap on an empty store = %#v, want an empty non-nil slice", claims)
	}
}

// TestClaimNextLeaseTTLDefaultsWhenUnset requires a zero lease TTL to fall
// back to the protocol default rather than granting an instantly expired
// lease.
func TestClaimNextLeaseTTLDefaultsWhenUnset(t *testing.T) {
	st, ctx := openFreshStore(t)
	declareJiraPrimary(t, ctx, st)
	runID := seedRun(t, ctx, st)
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "ttl-1", false)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	now := time.Now().UTC()
	resp, err := st.ClaimNext(ctx, workflowv1.ClaimSelector{
		CollectorKind: "jira", CollectorInstanceID: "jira-primary", OwnerID: "o-1", ClaimID: "ttl-claim",
	}, now, 0)
	if err != nil || !resp.Issued {
		t.Fatalf("claim = %+v, %v, want a grant", resp, err)
	}
	if lease := resp.Grant.Claim.LeaseExpiresAt.Sub(now); lease < workflowv1.DefaultClaimLeaseTTL()-time.Second {
		t.Fatalf("lease = %s, want about the default %s", lease, workflowv1.DefaultClaimLeaseTTL())
	}
	if _, err := st.ClaimNext(ctx, workflowv1.ClaimSelector{}, now, time.Minute); !isCode(err, workflowv1.CodeInvalid) {
		t.Fatalf("blank selector error = %v, want invalid", err)
	}
}
