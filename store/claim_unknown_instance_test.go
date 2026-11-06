// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/store"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// ghostSelector names a collector instance the plane never declared. It is
// syntactically valid, so validation passes and the claim path itself must
// decide: idle, never a grant and never an error.
func ghostSelector(claimID string) workflowv1.ClaimSelector {
	return workflowv1.ClaimSelector{
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: "ghost-instance",
		OwnerID:             "owner-ghost",
		ClaimID:             claimID,
	}
}

// seedGhostItem enqueues one pending item for the ghost instance, the
// stranded shape a renamed instance leaves behind: work rows survive while
// the plane knows no such instance.
func seedGhostItem(t *testing.T, ctx context.Context, st *claimstore.Store) {
	t.Helper()
	runID := seedRun(t, ctx, st)
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{
		strandedItem(runID, "ghost-instance", "scope-g"),
	}); err != nil {
		t.Fatalf("enqueue ghost item: %v", err)
	}
}

// requireIdle requires the clean idle answer: nil error, no grant, no
// deferral, and no degraded reason for the server to explain.
func requireIdle(
	t *testing.T,
	resp workflowv1.ClaimNextResponse,
	err error,
) {
	t.Helper()
	if err != nil {
		t.Fatalf("unknown-instance claim error = %v, want clean idle (nil)", err)
	}
	if resp.Issued {
		t.Fatalf("unknown instance issued %q, want idle", resp.Grant.WorkItem.WorkItemID)
	}
	if resp.Deferral.Signal != "" {
		t.Fatalf("unknown-instance deferral = %+v, want idle (no deferral)", resp.Deferral)
	}
	if resp.DegradedReason != "" {
		t.Fatalf("unknown-instance degraded reason = %q, want clean idle", resp.DegradedReason)
	}
}

// TestUnknownInstanceClaimsIdle requires ClaimNext for a syntactically
// valid selector naming an instance the plane never declared to answer
// clean idle (no grant, no deferral) with a nil error, on every claim
// path. Before the fix the stranded-item cases issued the ghost row to an
// instance that does not exist; the empty case already idled and pins that
// boundary.
func TestUnknownInstanceClaimsIdle(t *testing.T) {
	t.Run("empty queue", func(t *testing.T) {
		st, ctx := openFreshStore(t)
		resp, err := st.ClaimNext(ctx, ghostSelector("claim-ghost-empty"), time.Now().UTC(), time.Minute)
		requireIdle(t, resp, err)
	})

	t.Run("plain path with stranded item", func(t *testing.T) {
		st, ctx := openFreshStore(t)
		seedGhostItem(t, ctx, st)
		resp, err := st.ClaimNext(ctx, ghostSelector("claim-ghost-plain"), time.Now().UTC(), time.Minute)
		requireIdle(t, resp, err)
	})

	t.Run("quota path with stranded item", func(t *testing.T) {
		st, ctx := openFreshStore(t)
		if err := st.Configure(claimstore.Config{Quotas: map[string]int{"jira": 5}}); err != nil {
			t.Fatalf("configure: %v", err)
		}
		seedGhostItem(t, ctx, st)
		resp, err := st.ClaimNext(ctx, ghostSelector("claim-ghost-quota"), time.Now().UTC(), time.Minute)
		requireIdle(t, resp, err)
	})
}
