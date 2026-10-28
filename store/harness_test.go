// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/store"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

func testDSN(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("CLAIMPLANE_TEST_DATABASE_URL"); dsn != "" {
		return dsn
	}
	return "postgres://claimplane_test:claimplane_test@localhost:5432/claimplane_test_store?sslmode=disable"
}

// openFreshStore migrates a clean database handle for one test. Tables are
// truncated (child-first) so tests share one test database without
// cross-talk.
func openFreshStore(t *testing.T) (*claimstore.Store, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	st, err := claimstore.Open(ctx, testDSN(t))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := st.TruncateForTest(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return st, ctx
}

var testRunSeq int

func seedRun(t *testing.T, ctx context.Context, st *claimstore.Store) string {
	t.Helper()
	testRunSeq++
	runID := fmt.Sprintf("run-%d-%d", time.Now().UnixNano(), testRunSeq)
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := st.CreateRun(ctx, workflowv1.Run{
		RunID:       runID,
		TriggerKind: workflowv1.TriggerKindSchedule,
		Status:      workflowv1.RunStatusCollectionActive,
		CreatedAt:   now,
		UpdatedAt:   now,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	return runID
}

func seedItem(runID, itemID string, tenant bool) workflowv1.WorkItem {
	now := time.Now().UTC().Truncate(time.Millisecond)
	item := workflowv1.WorkItem{
		WorkItemID:          itemID,
		RunID:               runID,
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: "jira-primary",
		SourceSystem:        "jira",
		ScopeID:             "scope-" + itemID,
		AcceptanceUnitID:    "au-" + itemID,
		SourceRunID:         "sr-" + itemID,
		GenerationID:        "gen-" + itemID,
		FairnessKey:         "jira:jira-primary:bulk",
		Status:              workflowv1.WorkItemStatusPending,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	if tenant {
		item.TenantID = "tenant-1"
		item.WorkspaceID = "workspace-1"
		item.SubjectClass = "project"
		item.PolicyRevisionHash = "policy-hash-1"
	}
	return item
}

func claimOne(t *testing.T, ctx context.Context, st *claimstore.Store, claimID, owner string, ttl time.Duration) workflowv1.ClaimNextResponse {
	t.Helper()
	resp, err := st.ClaimNext(ctx, workflowv1.ClaimSelector{
		CollectorKind:       workflowv1.CollectorKind("jira"),
		CollectorInstanceID: "jira-primary",
		OwnerID:             owner,
		ClaimID:             claimID,
	}, time.Now().UTC(), ttl)
	if err != nil {
		t.Fatalf("claim next: %v", err)
	}
	if !resp.Issued {
		t.Fatalf("expected a grant, got deferral %+v", resp.Deferral)
	}
	return resp
}

func newTestConfig(t *testing.T, quotas map[string]int) claimstore.Config {
	t.Helper()
	return claimstore.Config{
		Quotas: quotas,
		Pacing: workflowv1.ClaimPacingConfig{
			DomainHigh: 1, DomainLow: 0,
			GlobalHigh: 1000, GlobalLow: 900,
			GrantDelay: 5 * time.Second, MaxEngagement: time.Minute,
		},
	}
}

func mutationFor(grant workflowv1.ClaimedWorkItem, ttl time.Duration) workflowv1.ClaimMutation {
	return workflowv1.ClaimMutation{
		WorkItemID:    grant.WorkItem.WorkItemID,
		ClaimID:       grant.Claim.ClaimID,
		FencingToken:  grant.Claim.FencingToken,
		OwnerID:       grant.Claim.OwnerID,
		ObservedAt:    time.Now().UTC(),
		LeaseDuration: ttl,
	}
}
