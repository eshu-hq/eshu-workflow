// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestClaimContentionGrantsEachItemOnce fires eight concurrent claimers at
// four pending items. Every item must be granted exactly once, fencing
// tokens must start at 1, and no two grants may share a work item.
func TestClaimContentionGrantsEachItemOnce(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)
	items := make([]workflowv1.WorkItem, 0, 4)
	for i := range 4 {
		items = append(items, seedItem(runID, fmt.Sprintf("contention-%d", i), false))
	}
	if err := st.Enqueue(ctx, items); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	const claimers = 8
	type result struct {
		resp workflowv1.ClaimNextResponse
		err  error
	}
	results := make([]result, claimers)
	var wg sync.WaitGroup
	for i := range claimers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Same-head collisions shed as empty attempts by design
			// (SKIP LOCKED): collectors absorb them with a poll retry,
			// so the test retries the same way, bounded.
			for attempt := range 50 {
				resp, err := st.ClaimNext(ctx, workflowv1.ClaimSelector{
					CollectorKind:       workflowv1.CollectorKind("jira"),
					CollectorInstanceID: "jira-primary",
					OwnerID:             fmt.Sprintf("owner-%d", i),
					ClaimID:             fmt.Sprintf("claim-%d-try-%d", i, attempt),
				}, time.Now().UTC(), time.Minute)
				if err != nil {
					results[i] = result{err: err}
					return
				}
				if resp.Issued {
					results[i] = result{resp: resp}
					return
				}
				time.Sleep(time.Millisecond)
			}
		}(i)
	}
	wg.Wait()

	grants := map[string]workflowv1.ClaimedWorkItem{}
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("claimer %d: %v", i, r.err)
		}
		if !r.resp.Issued {
			continue
		}
		id := r.resp.Grant.WorkItem.WorkItemID
		if _, dup := grants[id]; dup {
			t.Fatalf("work item %q granted twice", id)
		}
		grants[id] = r.resp.Grant
	}
	if len(grants) != len(items) {
		t.Fatalf("grants = %d, want %d (every item exactly once)", len(grants), len(items))
	}
	for id, grant := range grants {
		if grant.Claim.FencingToken != 1 {
			t.Fatalf("item %q fencing token = %d, want 1", id, grant.Claim.FencingToken)
		}
		if err := grant.Validate(); err != nil {
			t.Fatalf("item %q grant invalid: %v", id, err)
		}
	}
}

// TestReclaimAfterExpiry walks the full reclaim arc: claim, lease expiry,
// reap, reclaim by a new owner. The old epoch's heartbeat must fail with
// stale_fence; the new epoch must heartbeat cleanly with fencing token 2.
func TestReclaimAfterExpiry(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)
	if err := st.Enqueue(ctx, []workflowv1.WorkItem{seedItem(runID, "reclaim-0", false)}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	first := claimOne(t, ctx, st, "claim-first", "owner-first", 50*time.Millisecond)
	if first.Grant.Claim.FencingToken != 1 {
		t.Fatalf("first fencing token = %d, want 1", first.Grant.Claim.FencingToken)
	}

	// Let the lease lapse, then reap at the current clock with no requeue
	// delay so the item is immediately visible to the next claim.
	time.Sleep(100 * time.Millisecond)
	reaped, err := st.Reap(ctx, time.Now().UTC(), 10, 0)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(reaped) != 1 || reaped[0].ClaimID != "claim-first" {
		t.Fatalf("reaped = %+v, want the expired first claim", reaped)
	}

	second := claimOne(t, ctx, st, "claim-second", "owner-second", time.Minute)
	if second.Grant.Claim.FencingToken != 2 {
		t.Fatalf("second fencing token = %d, want 2", second.Grant.Claim.FencingToken)
	}

	stale := mutationFor(first.Grant, time.Minute)
	if err := st.Heartbeat(ctx, stale); !isCode(err, workflowv1.CodeStaleFence) {
		t.Fatalf("old-epoch heartbeat err = %v, want stale_fence", err)
	}
	if err := st.Complete(ctx, stale); !isCode(err, workflowv1.CodeStaleFence) {
		t.Fatalf("old-epoch complete err = %v, want stale_fence", err)
	}
	if err := st.Heartbeat(ctx, mutationFor(second.Grant, time.Minute)); err != nil {
		t.Fatalf("new-epoch heartbeat: %v", err)
	}
}

func isCode(err error, code string) bool {
	if err == nil {
		return false
	}
	var proto *workflowv1.ProtocolError
	if errors.As(err, &proto) {
		return proto.Code == code
	}
	return false
}
