// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package coordinator_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/client"
	"github.com/eshu-hq/eshu-workflow/coordinator"
	"github.com/eshu-hq/eshu-workflow/server"
	claimstore "github.com/eshu-hq/eshu-workflow/store"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// portedStack wires the real claim-plane server over its own Postgres
// database to the ported coordinator through the HTTP client: every
// state change below travels a protocol route, never a shared table.
type portedStack struct {
	plane *client.Client
	svc   coordinator.Service
}

func openPortedStack(t *testing.T, instances ...workflowv1.DesiredCollectorInstance) portedStack {
	t.Helper()
	// The coordinator integration suite owns its own test database:
	// the server suite asserts a globally drained queue, so sharing
	// its database would couple the two packages under parallel runs.
	dsn := os.Getenv("CLAIMPLANE_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://claimplane_test:claimplane_test@localhost:5432/claimplane_test_coordinator?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	st, err := claimstore.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := st.TruncateForTest(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	srv, err := server.New(st, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	plane, err := client.New(httpSrv.URL)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	return portedStack{
		plane: plane,
		svc: coordinator.Service{
			Config: coordinator.Config{
				DeploymentMode: "active", ClaimsEnabled: true,
				CollectorInstances: instances,
			},
			Plane:    plane,
			Planners: coordinator.DefaultRegistry(),
			Logger:   slog.New(slog.DiscardHandler),
		},
	}
}

func singleTargetInstance(id, kind string) workflowv1.DesiredCollectorInstance {
	return workflowv1.DesiredCollectorInstance{
		InstanceID: id, CollectorKind: workflowv1.CollectorKind(kind),
		Mode: workflowv1.CollectorModeScheduled, Enabled: true, ClaimsEnabled: true,
		Configuration: `{"targets":[{"scope_id":"scope-only"}]}`,
	}
}

func claimAs(
	t *testing.T,
	plane *client.Client,
	kind, instance, owner, claimID string,
	at time.Time,
	ttl time.Duration,
) workflowv1.ClaimNextResponse {
	t.Helper()
	resp, err := plane.ClaimNext(context.Background(), workflowv1.ClaimSelector{
		CollectorKind: workflowv1.CollectorKind(kind), CollectorInstanceID: instance,
		OwnerID: owner, ClaimID: claimID,
	}, at, ttl)
	if err != nil {
		t.Fatalf("claim next: %v", err)
	}
	if !resp.Issued {
		t.Fatalf("expected a grant, got deferral %+v", resp.Deferral)
	}
	return resp
}

// TestReclaimIssuesMonotonicFencing requires the reclaim path: a grant
// whose lease lapses reaps back to the queue, and the next grant on the
// same item carries a higher fencing token.
func TestReclaimIssuesMonotonicFencing(t *testing.T) {
	stack := openPortedStack(t, singleTargetInstance("jira-primary", "jira"))
	ctx := context.Background()
	if err := stack.svc.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	// The reap observes past the grant's lease: asOf doubles as the
	// requeued item's next visible time, so the reclaim observes just
	// after it, the way a runner polling past the reap tick would.
	grantedAt := time.Now().UTC()
	first := claimAs(t, stack.plane, "jira", "jira-primary", "owner-1", "claim-1", grantedAt, time.Minute)
	reapedAt := grantedAt.Add(2 * time.Minute)
	reaped, err := stack.plane.ReapExpiredClaims(ctx, reapedAt, 10, 0)
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(reaped) != 1 {
		t.Fatalf("reaped = %d, want 1", len(reaped))
	}
	second := claimAs(t, stack.plane, "jira", "jira-primary", "owner-1", "claim-2", reapedAt.Add(time.Second), time.Minute)
	if second.Grant.Claim.FencingToken <= first.Grant.Claim.FencingToken {
		t.Fatalf("reclaim token = %d, want above %d",
			second.Grant.Claim.FencingToken, first.Grant.Claim.FencingToken)
	}
	if second.Grant.WorkItem.WorkItemID != first.Grant.WorkItem.WorkItemID {
		t.Fatalf("reclaim item = %q, want %q", second.Grant.WorkItem.WorkItemID, first.Grant.WorkItem.WorkItemID)
	}
}

// TestStaleFenceRejectsSupersededEpoch requires fencing on the ported
// path: a mutation on the epoch the reaper already replaced fails
// stale_fence instead of touching the successor's live claim.
func TestStaleFenceRejectsSupersededEpoch(t *testing.T) {
	stack := openPortedStack(t, singleTargetInstance("jira-primary", "jira"))
	ctx := context.Background()
	if err := stack.svc.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	grantedAt := time.Now().UTC()
	first := claimAs(t, stack.plane, "jira", "jira-primary", "owner-1", "claim-1", grantedAt, time.Minute)
	reapedAt := grantedAt.Add(2 * time.Minute)
	if _, err := stack.plane.ReapExpiredClaims(ctx, reapedAt, 10, 0); err != nil {
		t.Fatalf("reap: %v", err)
	}
	second := claimAs(t, stack.plane, "jira", "jira-primary", "owner-1", "claim-2", reapedAt.Add(time.Second), time.Minute)

	stale := workflowv1.ClaimMutation{
		WorkItemID: first.Grant.WorkItem.WorkItemID, ClaimID: first.Grant.Claim.ClaimID,
		FencingToken: first.Grant.Claim.FencingToken, OwnerID: "owner-1",
		ObservedAt: time.Now().UTC(), LeaseDuration: time.Minute,
	}
	err := stack.plane.Heartbeat(ctx, stale)
	var proto *workflowv1.ProtocolError
	if !errors.As(err, &proto) {
		t.Fatalf("stale heartbeat error type = %T, want *ProtocolError", err)
	}
	if proto.Code != workflowv1.CodeStaleFence {
		t.Fatalf("stale heartbeat code = %q, want stale_fence", proto.Code)
	}
	// The successor epoch still heartbeats cleanly: the stale mutation
	// touched nothing live.
	live := stale
	live.ClaimID = second.Grant.Claim.ClaimID
	live.FencingToken = second.Grant.Claim.FencingToken
	if err := stack.plane.Heartbeat(ctx, live); err != nil {
		t.Fatalf("live heartbeat: %v", err)
	}
}

// TestTerminalFailureDeadLetters requires the dead-letter path: a
// terminal failure never returns the item to the queue, and the
// report/read planes reflect the terminal count core pushed.
func TestTerminalFailureDeadLetters(t *testing.T) {
	stack := openPortedStack(t, singleTargetInstance("jira-primary", "jira"))
	ctx := context.Background()
	if err := stack.svc.Reconcile(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	grant := claimAs(t, stack.plane, "jira", "jira-primary", "owner-1", "claim-1", time.Now().UTC(), time.Minute)
	terminal := workflowv1.ClaimMutation{
		WorkItemID: grant.Grant.WorkItem.WorkItemID, ClaimID: grant.Grant.Claim.ClaimID,
		FencingToken: grant.Grant.Claim.FencingToken, OwnerID: "owner-1",
		ObservedAt: time.Now().UTC(), LeaseDuration: time.Minute,
		FailureClass: "provider_rejected", FailureTerminal: true,
	}
	if err := stack.plane.FailTerminal(ctx, terminal); err != nil {
		t.Fatalf("fail terminal: %v", err)
	}
	// Nothing left to claim: the terminal item never requeues.
	_, err := stack.plane.ClaimNext(ctx, workflowv1.ClaimSelector{
		CollectorKind: "jira", CollectorInstanceID: "jira-primary",
		OwnerID: "owner-1", ClaimID: "claim-2",
	}, time.Now().UTC(), time.Minute)
	var idle *workflowv1.ProtocolError
	if !errors.As(err, &idle) || idle.Code != workflowv1.CodeNotFound {
		t.Fatalf("post-terminal claim error = %v, want not_found", err)
	}

	now := time.Now().UTC()
	if err := stack.plane.ReportRunProgress(ctx, workflowv1.ReportRunProgressRequest{
		RunID:      grant.Grant.WorkItem.RunID,
		ObservedAt: now,
		Progress: []workflowv1.CollectorRunProgress{{
			CollectorKind: "jira", TotalWorkItems: 1, FailedTerminalItems: 1,
			TerminalDeadLetterCounts: map[string]int{"facts/publish": 1},
		}},
	}); err != nil {
		t.Fatalf("report progress: %v", err)
	}
	status, err := stack.plane.GetRunStatus(ctx, grant.Grant.WorkItem.RunID)
	if err != nil {
		t.Fatalf("run status: %v", err)
	}
	if len(status.Progress) != 1 || status.Progress[0].FailedTerminalItems != 1 {
		t.Fatalf("read-plane progress = %+v, want one family with a terminal item", status.Progress)
	}
	if status.Progress[0].TerminalDeadLetterCounts["facts/publish"] != 1 {
		t.Fatalf("dead-letter counts = %+v, want facts/publish = 1",
			status.Progress[0].TerminalDeadLetterCounts)
	}
}
