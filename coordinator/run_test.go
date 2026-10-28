// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package coordinator_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/coordinator"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

var errTestDepth = errors.New("depth unavailable")

// TestRunTicksThenStops requires the service loop to reconcile on
// start, tick its active maintenance, and return nil on cancellation.
func TestRunTicksThenStops(t *testing.T) {
	plane := newFakePlane()
	planners := coordinator.NewRegistry()
	planners.Register("jira", &recordingPlanner{targets: []coordinator.PlanTarget{{ScopeID: "scope-a"}}})
	cfg := activeConfig(schedulableInstance("jira-primary", "jira"))
	cfg.ReconcileInterval = 5 * time.Millisecond
	cfg.ReapInterval = 5 * time.Millisecond
	cfg.RunReconcileInterval = 5 * time.Millisecond
	svc := coordinator.Service{Config: cfg, Plane: plane, Planners: planners}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	var runErr error
	go func() {
		defer wg.Done()
		runErr = svc.Run(ctx)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		plane.mu.Lock()
		reaped := false
		reconciled := false
		for _, call := range plane.calls {
			if call == "ReapExpiredClaims" {
				reaped = true
			}
			if call == "ReconcileWorkflowRuns" {
				reconciled = true
			}
		}
		plane.mu.Unlock()
		if reaped && reconciled {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			wg.Wait()
			t.Fatal("active maintenance never ticked")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("run: %v", runErr)
	}
}

// TestRunRejectsBadWiring requires construction failures to surface
// before any tick: no plane, no registry, or a bad config.
func TestRunRejectsBadWiring(t *testing.T) {
	plane := newFakePlane()
	planners := coordinator.NewRegistry()
	if err := (coordinator.Service{Planners: planners}).Run(context.Background()); err == nil {
		t.Fatal("nil plane accepted")
	}
	if err := (coordinator.Service{Plane: plane}).Run(context.Background()); err == nil {
		t.Fatal("nil registry accepted")
	}
	cfg := validConfig()
	cfg.DeploymentMode = "nocturnal"
	if err := (coordinator.Service{Config: cfg, Plane: plane, Planners: planners}).Run(context.Background()); err == nil {
		t.Fatal("bad config accepted")
	}
}

// TestDarkMaintenanceIsNoOp requires reap and run reconciliation to do
// nothing outside active mode.
func TestDarkMaintenanceIsNoOp(t *testing.T) {
	plane := newFakePlane()
	cfg := activeConfig()
	cfg.DeploymentMode = "dark"
	svc := coordinator.Service{Config: cfg, Plane: plane, Planners: coordinator.NewRegistry()}
	if err := svc.Reap(context.Background()); err != nil {
		t.Fatalf("dark reap: %v", err)
	}
	if err := svc.ReconcileRuns(context.Background()); err != nil {
		t.Fatalf("dark run reconcile: %v", err)
	}
	plane.mu.Lock()
	defer plane.mu.Unlock()
	for _, call := range plane.calls {
		if call == "ReapExpiredClaims" || call == "ReconcileWorkflowRuns" {
			t.Fatalf("dark maintenance reached the plane: %v", plane.calls)
		}
	}
}

// TestFamilyPlanningFailuresAreReported requires every planning
// failure mode to name its family: an unregistered family, a
// malformed target document, and a failing guard read.
func TestFamilyPlanningFailuresAreReported(t *testing.T) {
	plane := newFakePlane()
	svc := coordinator.Service{
		Config: activeConfig(
			schedulableInstance("unknown-primary", "unknown"),
			workflowv1.DesiredCollectorInstance{
				InstanceID: "broken-primary", CollectorKind: "jira",
				Mode: workflowv1.CollectorModeScheduled, Enabled: true, ClaimsEnabled: true,
				Configuration: `{"targets": "not-a-list"}`,
			},
		),
		Plane:    plane,
		Planners: coordinator.DefaultRegistry(),
	}
	err := svc.Reconcile(context.Background())
	if err == nil {
		t.Fatal("unplannable families reconciled clean")
	}
	if !strings.Contains(err.Error(), "unknown") || !strings.Contains(err.Error(), "broken-primary") {
		t.Fatalf("reconcile error = %v, want both families named", err)
	}
}

// TestGuardPageCapBoundsTheWalk requires the admission guard to fail
// loudly instead of paging forever when the read plane never ends.
func TestGuardPageCapBoundsTheWalk(t *testing.T) {
	plane := newFakePlane()
	plane.loopCursor = true
	planners := coordinator.NewRegistry()
	planners.Register("jira", &recordingPlanner{targets: []coordinator.PlanTarget{{ScopeID: "scope-a"}}})
	svc := coordinator.Service{
		Config:   activeConfig(schedulableInstance("jira-primary", "jira")),
		Plane:    plane,
		Planners: planners,
	}
	err := svc.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "pages") {
		t.Fatalf("reconcile error = %v, want the page-cap bound", err)
	}
}

// TestDepthReadFailureKeepsThePass requires a failed queue-depth
// observation to log and continue: depth is observability, never the
// pass's fate.
func TestDepthReadFailureKeepsThePass(t *testing.T) {
	plane := newFakePlane()
	plane.depthErr = errTestDepth
	planners := coordinator.NewRegistry()
	planners.Register("jira", &recordingPlanner{targets: []coordinator.PlanTarget{{ScopeID: "scope-a"}}})
	svc := coordinator.Service{
		Config:   activeConfig(schedulableInstance("jira-primary", "jira")),
		Plane:    plane,
		Planners: planners,
	}
	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile with failed depth: %v", err)
	}
	if got := plane.itemCount(); got != 1 {
		t.Fatalf("work items = %d, want 1 (depth failure must not stop admission)", got)
	}
}
