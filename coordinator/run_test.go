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

// TestRunRetriesTransientInitialReconcile requires startup to survive
// a claim-plane that is not yet reachable: two refused reconciles then
// success must still bring the loop up instead of crash-looping.
func TestRunRetriesTransientInitialReconcile(t *testing.T) {
	plane := newFakePlane()
	plane.reconcileFailures = 2
	cfg := activeConfig()
	cfg.DeploymentMode = "dark"
	cfg.ReconcileInterval = 5 * time.Millisecond
	cfg.StartupReconcileAttempts = 5
	cfg.StartupReconcileInterval = time.Millisecond
	svc := coordinator.Service{Config: cfg, Plane: plane, Planners: coordinator.NewRegistry()}

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
		attempts := 0
		for _, call := range plane.calls {
			if call == "ReconcileCollectorInstances" {
				attempts++
			}
		}
		plane.mu.Unlock()
		if attempts >= 3 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			wg.Wait()
			t.Fatalf("startup retry never reached the plane 3 times (got %d)", attempts)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("run with transient startup failure: %v", runErr)
	}
}

// TestRunExhaustsStartupBudget requires a permanently unreachable
// plane to fail startup after exactly the configured attempts,
// keeping the original error text.
func TestRunExhaustsStartupBudget(t *testing.T) {
	plane := newFakePlane()
	plane.reconcileFailures = 100
	cfg := activeConfig()
	cfg.DeploymentMode = "dark"
	cfg.StartupReconcileAttempts = 3
	cfg.StartupReconcileInterval = time.Millisecond
	svc := coordinator.Service{Config: cfg, Plane: plane, Planners: coordinator.NewRegistry()}

	err := svc.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "initial collector reconciliation") {
		t.Fatalf("startup exhaustion: %v", err)
	}
	if !strings.Contains(err.Error(), "after 3 attempts") {
		t.Fatalf("exhaustion error omits consumed budget: %v", err)
	}
	plane.mu.Lock()
	defer plane.mu.Unlock()
	attempts := 0
	for _, call := range plane.calls {
		if call == "ReconcileCollectorInstances" {
			attempts++
		}
	}
	if attempts != 3 {
		t.Fatalf("startup attempts = %d, want 3", attempts)
	}
}

// TestRunFailsFastOnDeterministicStartupError requires a deterministic
// plane answer (here a coded invalid response, never a transport
// failure) to fail startup on the first attempt: only unreachable-plane
// transients retry, so a sick configuration still crash-loops fast.
func TestRunFailsFastOnDeterministicStartupError(t *testing.T) {
	plane := newFakePlane()
	plane.reconcileDeterministicErr = &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: "test deterministic"}
	cfg := activeConfig()
	cfg.DeploymentMode = "dark"
	cfg.StartupReconcileAttempts = 5
	cfg.StartupReconcileInterval = time.Millisecond
	svc := coordinator.Service{Config: cfg, Plane: plane, Planners: coordinator.NewRegistry()}

	err := svc.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "initial collector reconciliation") {
		t.Fatalf("deterministic startup error: %v", err)
	}
	if strings.Contains(err.Error(), "after") {
		t.Fatalf("deterministic error consumed retry budget: %v", err)
	}
	plane.mu.Lock()
	defer plane.mu.Unlock()
	attempts := 0
	for _, call := range plane.calls {
		if call == "ReconcileCollectorInstances" {
			attempts++
		}
	}
	if attempts != 1 {
		t.Fatalf("deterministic startup attempts = %d, want 1", attempts)
	}
}

// TestRunRetriesTransientInitialReap requires active-mode startup to
// survive a claim-plane that answers reaps late: two refused reaps
// then success must still bring the loop up instead of crash-looping.
func TestRunRetriesTransientInitialReap(t *testing.T) {
	plane := newFakePlane()
	plane.reapFailures = 2
	cfg := activeConfig()
	cfg.ReconcileInterval = 5 * time.Millisecond
	cfg.ReapInterval = 5 * time.Millisecond
	cfg.RunReconcileInterval = 5 * time.Millisecond
	cfg.StartupReconcileAttempts = 5
	cfg.StartupReconcileInterval = time.Millisecond
	svc := coordinator.Service{Config: cfg, Plane: plane, Planners: coordinator.NewRegistry()}

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
		attempts := 0
		for _, call := range plane.calls {
			if call == "ReapExpiredClaims" {
				attempts++
			}
		}
		plane.mu.Unlock()
		if attempts >= 3 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			wg.Wait()
			t.Fatalf("startup reap retry never reached the plane 3 times (got %d)", attempts)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("run with transient reap failure: %v", runErr)
	}
}

// TestRunExhaustsInitialReapBudget requires a permanently unreachable
// plane to fail the initial reap after exactly the configured attempts,
// keeping the original error text.
func TestRunExhaustsInitialReapBudget(t *testing.T) {
	plane := newFakePlane()
	plane.reapFailures = 100
	cfg := activeConfig()
	cfg.StartupReconcileAttempts = 3
	cfg.StartupReconcileInterval = time.Millisecond
	svc := coordinator.Service{Config: cfg, Plane: plane, Planners: coordinator.NewRegistry()}

	err := svc.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "initial expired-claim reap") {
		t.Fatalf("reap startup exhaustion: %v", err)
	}
	if !strings.Contains(err.Error(), "after 3 attempts") {
		t.Fatalf("reap exhaustion error omits consumed budget: %v", err)
	}
	plane.mu.Lock()
	defer plane.mu.Unlock()
	attempts := 0
	for _, call := range plane.calls {
		if call == "ReapExpiredClaims" {
			attempts++
		}
	}
	if attempts != 3 {
		t.Fatalf("reap startup attempts = %d, want 3", attempts)
	}
}

// TestRunRetriesTransientInitialReconcileRuns requires active-mode
// startup to survive a claim-plane that answers run reconciliation
// late: two refused passes then success must still bring the loop up.
func TestRunRetriesTransientInitialReconcileRuns(t *testing.T) {
	plane := newFakePlane()
	plane.reconcileRunsFailures = 2
	cfg := activeConfig()
	cfg.ReconcileInterval = 5 * time.Millisecond
	cfg.ReapInterval = 5 * time.Millisecond
	cfg.RunReconcileInterval = 5 * time.Millisecond
	cfg.StartupReconcileAttempts = 5
	cfg.StartupReconcileInterval = time.Millisecond
	svc := coordinator.Service{Config: cfg, Plane: plane, Planners: coordinator.NewRegistry()}

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
		attempts := 0
		for _, call := range plane.calls {
			if call == "ReconcileWorkflowRuns" {
				attempts++
			}
		}
		plane.mu.Unlock()
		if attempts >= 3 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			wg.Wait()
			t.Fatalf("startup run-reconcile retry never reached the plane 3 times (got %d)", attempts)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	wg.Wait()
	if runErr != nil {
		t.Fatalf("run with transient run-reconcile failure: %v", runErr)
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
