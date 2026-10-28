// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/store"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestMigrateIsIdempotent applies the fresh-install migrations twice and
// verifies every claim-plane table exists.
func TestMigrateIsIdempotent(t *testing.T) {
	st, ctx := openFreshStore(t)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	for _, table := range []string{
		"workflow_runs", "workflow_work_items", "workflow_claims",
		"workflow_claim_pacing", "component_quota_windows",
		"collector_instances", "workflow_run_completeness",
		"workflow_run_progress", "schema_migrations",
	} {
		if err := st.PingTable(ctx, table); err != nil {
			t.Fatalf("table %s: %v", table, err)
		}
	}
}

// TestQuotaBlocksThirdGrantInScope seeds three items in one scope with two
// generations per hour: the first two claim, the third defers on the domain
// signal with a backoff reaching the next hour window.
func TestQuotaBlocksThirdGrantInScope(t *testing.T) {
	st, ctx := openFreshStore(t)
	if err := st.Configure(claimstore.Config{Quotas: map[string]int{"jira": 2}}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	runID := seedRun(t, ctx, st)
	items := []workflowv1.WorkItem{
		seedItem(runID, "quota-0", false),
		seedItem(runID, "quota-1", false),
		seedItem(runID, "quota-2", false),
	}
	for i := range items {
		items[i].ScopeID = "scope-shared"
	}
	if err := st.Enqueue(ctx, items); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimOne(t, ctx, st, "claim-q0", "owner-q0", time.Minute)
	claimOne(t, ctx, st, "claim-q1", "owner-q1", time.Minute)

	resp, err := st.ClaimNext(ctx, workflowv1.ClaimSelector{
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: "jira-primary",
		OwnerID:             "owner-q2",
		ClaimID:             "claim-q2",
	}, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("quota-blocked claim: %v", err)
	}
	if resp.Issued {
		t.Fatalf("quota-blocked claim issued a third grant in one scope")
	}
	if err := resp.Deferral.Validate(); err != nil {
		t.Fatalf("deferral invalid: %v", err)
	}
	if resp.Deferral.Signal != workflowv1.ClaimPacingSignalDomain {
		t.Fatalf("deferral signal = %q, want domain", resp.Deferral.Signal)
	}
	if resp.Deferral.RetryAfter <= 0 {
		t.Fatalf("deferral retry_after = %s, want a positive backoff", resp.Deferral.RetryAfter)
	}
}

// TestPacingDefersOverHighWater seeds depth above the domain high watermark
// and requires ClaimNext to defer instead of granting.
func TestPacingDefersOverHighWater(t *testing.T) {
	st, ctx := openFreshStore(t)
	if err := st.Configure(claimstore.Config{Pacing: workflowv1.ClaimPacingConfig{
		DomainHigh: 1, DomainLow: 0,
		GlobalHigh: 1000, GlobalLow: 900,
		GrantDelay:    5 * time.Second,
		MaxEngagement: time.Minute,
	}}); err != nil {
		t.Fatalf("configure: %v", err)
	}
	runID := seedRun(t, ctx, st)
	if err := st.Enqueue(ctx, []workflowv1.WorkItem{
		seedItem(runID, "pace-0", false),
		seedItem(runID, "pace-1", false),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	resp, err := st.ClaimNext(ctx, workflowv1.ClaimSelector{
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: "jira-primary",
		OwnerID:             "owner-pace",
		ClaimID:             "claim-pace",
	}, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatalf("paced claim: %v", err)
	}
	if resp.Issued {
		t.Fatalf("paced claim issued a grant above the high watermark")
	}
	if err := resp.Deferral.Validate(); err != nil {
		t.Fatalf("deferral invalid: %v", err)
	}
	if resp.Deferral.Signal != workflowv1.ClaimPacingSignalDomain {
		t.Fatalf("deferral signal = %q, want domain", resp.Deferral.Signal)
	}
}
