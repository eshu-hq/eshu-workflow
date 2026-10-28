// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"testing"
	"time"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestStaleFenceAfterRelease pins the stale-fence contract: after release
// and reclaim, the superseded epoch's mutations fail with stale_fence while
// the new epoch proceeds.
func TestStaleFenceAfterRelease(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "fence-0", false)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	first := claimOne(t, ctx, st, "claim-f1", "owner-f1", time.Minute)
	if err := st.Release(ctx, mutationFor(first.Grant, time.Minute)); err != nil {
		t.Fatalf("release: %v", err)
	}
	second := claimOne(t, ctx, st, "claim-f2", "owner-f2", time.Minute)
	if second.Grant.Claim.FencingToken != 2 {
		t.Fatalf("second fencing token = %d, want 2", second.Grant.Claim.FencingToken)
	}

	stale := mutationFor(first.Grant, time.Minute)
	for name, call := range map[string]func() error{
		"heartbeat": func() error { return st.Heartbeat(ctx, stale) },
		"complete":  func() error { return st.Complete(ctx, stale) },
		"release":   func() error { return st.Release(ctx, stale) },
		"fail":      func() error { return st.FailRetryable(ctx, stale) },
	} {
		if err := call(); !isCode(err, workflowv1.CodeStaleFence) {
			t.Fatalf("stale %s err = %v, want stale_fence", name, err)
		}
	}
	if err := st.Complete(ctx, mutationFor(second.Grant, time.Minute)); err != nil {
		t.Fatalf("current-epoch complete: %v", err)
	}
}

// TestAmbiguousTimeoutReplay proves every mutation is safe to retry after
// an ambiguous timeout: repeating a heartbeat, complete, release, fail, or
// pin with the same epoch arguments succeeds instead of double-applying.
func TestAmbiguousTimeoutReplay(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{
		seedItem(runID, "replay-hb", false),
		seedItem(runID, "replay-complete", false),
		seedItem(runID, "replay-release", false),
		seedItem(runID, "replay-fail", false),
		seedItem(runID, "replay-pin", false),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	hb := claimOne(t, ctx, st, "claim-hb", "owner-hb", time.Minute)
	m := mutationFor(hb.Grant, time.Minute)
	if err := st.Heartbeat(ctx, m); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if err := st.Heartbeat(ctx, m); err != nil {
		t.Fatalf("heartbeat replay: %v", err)
	}

	cp := claimOne(t, ctx, st, "claim-cp", "owner-cp", time.Minute)
	cm := mutationFor(cp.Grant, time.Minute)
	if err := st.Complete(ctx, cm); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := st.Complete(ctx, cm); err != nil {
		t.Fatalf("complete replay: %v", err)
	}

	rel := claimOne(t, ctx, st, "claim-rel", "owner-rel", time.Minute)
	rm := mutationFor(rel.Grant, time.Minute)
	if err := st.Release(ctx, rm); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := st.Release(ctx, rm); err != nil {
		t.Fatalf("release replay: %v", err)
	}

	fl := claimOne(t, ctx, st, "claim-fl", "owner-fl", time.Minute)
	fm := mutationFor(fl.Grant, time.Minute)
	fm.FailureClass = "provider_error"
	if err := st.FailRetryable(ctx, fm); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if err := st.FailRetryable(ctx, fm); err != nil {
		t.Fatalf("fail replay: %v", err)
	}

	pinGrant := claimOne(t, ctx, st, "claim-pin", "owner-pin", time.Minute)
	pinReq := workflowv1.CommitPinRequest{
		ClaimID:        pinGrant.Grant.Claim.ClaimID,
		FencingToken:   pinGrant.Grant.Claim.FencingToken,
		ScopeID:        pinGrant.Grant.WorkItem.ScopeID,
		SourceRunID:    pinGrant.Grant.WorkItem.SourceRunID,
		GenerationID:   pinGrant.Grant.WorkItem.GenerationID,
		CommitDeadline: time.Now().UTC().Add(time.Hour),
	}
	first, err := st.PinForCommit(ctx, pinReq)
	if err != nil {
		t.Fatalf("pin: %v", err)
	}
	second, err := st.PinForCommit(ctx, pinReq)
	if err != nil {
		t.Fatalf("pin replay: %v", err)
	}
	if first.CommitID != second.CommitID || first.CommitID == "" {
		t.Fatalf("pin replay commit ids %q vs %q, want one stable id", first.CommitID, second.CommitID)
	}
}

// TestPinBlocksReaper is the CommitPin contract: a pinned claim survives
// reap past its lease expiry, and becomes reapable only after the commit
// deadline passes.
func TestPinBlocksReaper(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "pin-0", false)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	grant := claimOne(t, ctx, st, "claim-pin-reap", "owner-pin-reap", 50*time.Millisecond)
	deadline := time.Now().UTC().Add(time.Hour)
	if _, err := st.PinForCommit(ctx, workflowv1.CommitPinRequest{
		ClaimID:        grant.Grant.Claim.ClaimID,
		FencingToken:   grant.Grant.Claim.FencingToken,
		ScopeID:        grant.Grant.WorkItem.ScopeID,
		SourceRunID:    grant.Grant.WorkItem.SourceRunID,
		GenerationID:   grant.Grant.WorkItem.GenerationID,
		CommitDeadline: deadline,
	}); err != nil {
		t.Fatalf("pin: %v", err)
	}

	afterLease := time.Now().UTC().Add(time.Second)
	reaped, err := st.Reap(ctx, afterLease, 10, time.Second)
	if err != nil {
		t.Fatalf("reap under pin: %v", err)
	}
	if len(reaped) != 0 {
		t.Fatalf("reaped %d claims under an active pin, want 0", len(reaped))
	}

	reaped, err = st.Reap(ctx, deadline.Add(time.Second), 10, time.Second)
	if err != nil {
		t.Fatalf("reap after pin expiry: %v", err)
	}
	if len(reaped) != 1 {
		t.Fatalf("reaped %d claims after pin expiry, want 1", len(reaped))
	}
}

// TestTenantScopedMutationsNeedNoGrantJoin is the extraction-seam
// regression: heartbeat and complete on a fully tenant-scoped item must
// succeed even though this database owns no tenant/producer grant tables.
// Grant authority lives in the CommitPin plus core admission.
func TestTenantScopedMutationsNeedNoGrantJoin(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "tenant-0", true)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	grant := claimOne(t, ctx, st, "claim-tenant", "owner-tenant", time.Minute)
	if grant.Grant.WorkItem.TenantID == "" {
		t.Fatalf("seeded item lost its tenant boundary")
	}
	if err := st.Heartbeat(ctx, mutationFor(grant.Grant, time.Minute)); err != nil {
		t.Fatalf("tenant heartbeat: %v", err)
	}
	if err := st.Complete(ctx, mutationFor(grant.Grant, time.Minute)); err != nil {
		t.Fatalf("tenant complete: %v", err)
	}
}
