// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/store"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

func enqueueN(t *testing.T, st *claimstore.Store, ctx context.Context, runID, prefix string, n int) {
	t.Helper()
	items := make([]workflowv1.WorkItem, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, seedItem(runID, fmt.Sprintf("%s-%d", prefix, i), false))
	}
	if _, err := st.Enqueue(ctx, items); err != nil {
		t.Fatalf("enqueue %s: %v", prefix, err)
	}
}

// TestGlobalPacingDeferralCarriesNoDomain requires the global ceiling to
// pace every family and to report no saturated domain, in both the probe
// and the real claim path.
func TestGlobalPacingDeferralCarriesNoDomain(t *testing.T) {
	st, ctx := openFreshStore(t)
	cfg := claimstore.Config{Pacing: workflowv1.ClaimPacingConfig{
		DomainHigh: 100, DomainLow: 50, GlobalHigh: 2, GlobalLow: 1,
		GrantDelay: 7 * time.Second, MaxEngagement: time.Minute,
	}}
	if err := st.Configure(cfg); err != nil {
		t.Fatalf("configure: %v", err)
	}
	runID := seedRun(t, ctx, st)
	enqueueN(t, st, ctx, runID, "global", 3)
	now := time.Now().UTC()

	decision, err := st.CheckPacing(ctx, "jira", "jira-primary", now)
	if err != nil {
		t.Fatalf("check pacing: %v", err)
	}
	if !decision.Paced || decision.Signal != workflowv1.ClaimPacingSignalGlobal || decision.Domain != "" {
		t.Fatalf("decision = %+v, want global-paced with no domain", decision)
	}

	resp, err := st.ClaimNext(ctx, workflowv1.ClaimSelector{
		CollectorKind: "jira", CollectorInstanceID: "jira-primary", OwnerID: "o-1", ClaimID: "g-claim",
	}, now, time.Minute)
	if err != nil {
		t.Fatalf("claim next: %v", err)
	}
	if resp.Issued || resp.Deferral.Signal != workflowv1.ClaimPacingSignalGlobal ||
		resp.Deferral.Domain != "" || resp.Deferral.RetryAfter != 7*time.Second {
		t.Fatalf("response = %+v, want a global deferral of 7s with no domain", resp)
	}
	if err := resp.Validate(); err != nil {
		t.Fatalf("deferral violates its contract: %v", err)
	}
}

// TestDepthCacheHoldsWithinTTLThenRefreshes requires the pacing depth
// observation to be reused inside DepthCacheTTL and re-read after it, so a
// drained queue stops pacing only once the cached number expires.
func TestDepthCacheHoldsWithinTTLThenRefreshes(t *testing.T) {
	st, ctx := openFreshStore(t)
	cfg := claimstore.Config{Pacing: workflowv1.ClaimPacingConfig{
		DomainHigh: 2, DomainLow: 1, GlobalHigh: 1000, GlobalLow: 900,
		GrantDelay: 5 * time.Second, MaxEngagement: time.Minute,
		DepthCacheTTL: time.Hour,
	}}
	if err := st.Configure(cfg); err != nil {
		t.Fatalf("configure: %v", err)
	}
	runID := seedRun(t, ctx, st)
	enqueueN(t, st, ctx, runID, "cache", 3)
	t0 := time.Now().UTC()

	first, err := st.CheckPacing(ctx, "jira", "jira-primary", t0)
	if err != nil || !first.Paced {
		t.Fatalf("first probe = %+v, %v, want paced on depth 3 > high 2", first, err)
	}

	// Drain the queue behind the cache's back.
	if err := st.ExecForTest(ctx, `DELETE FROM workflow_work_items`); err != nil {
		t.Fatalf("drain: %v", err)
	}
	cached, err := st.CheckPacing(ctx, "jira", "jira-primary", t0.Add(time.Minute))
	if err != nil || !cached.Paced {
		t.Fatalf("probe inside TTL = %+v, %v, want the cached depth to keep pacing", cached, err)
	}
	other, err := st.CheckPacing(ctx, "git", "git-primary", t0.Add(time.Minute))
	if err != nil {
		t.Fatalf("probe of an uncached family: %v", err)
	}
	if other.Paced {
		t.Fatalf("uncached family on a drained queue paced: %+v", other)
	}

	refreshed, err := st.CheckPacing(ctx, "jira", "jira-primary", t0.Add(2*time.Hour))
	if err != nil {
		t.Fatalf("probe after TTL: %v", err)
	}
	if refreshed.Paced {
		t.Fatalf("probe after TTL = %+v, want the drained depth to release pacing", refreshed)
	}
}

// TestQuotaOnEmptyQueueIsIdleNotThrottled requires a quota-gated family
// with no work to answer idle (no grant, no deferral), so the server maps
// it to not_found instead of telling the runner to back off an hour.
func TestQuotaOnEmptyQueueIsIdleNotThrottled(t *testing.T) {
	st, ctx := openFreshStore(t)
	if err := st.Configure(newTestConfig(t, map[string]int{"jira": 1})); err != nil {
		t.Fatalf("configure: %v", err)
	}
	resp, err := st.ClaimNext(ctx, workflowv1.ClaimSelector{
		CollectorKind: "jira", CollectorInstanceID: "jira-primary", OwnerID: "o-1", ClaimID: "idle-claim",
	}, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("claim next: %v", err)
	}
	if resp.Issued || resp.Deferral.Signal != "" {
		t.Fatalf("response = %+v, want idle (no grant, no deferral)", resp)
	}
}
