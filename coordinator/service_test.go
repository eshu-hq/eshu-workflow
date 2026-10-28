// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package coordinator_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/coordinator"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// recordingPlanner plans a fixed target set and records its calls.
type recordingPlanner struct {
	mu      sync.Mutex
	targets []coordinator.PlanTarget
	calls   int
	fail    error
}

func (p *recordingPlanner) Plan(
	_ context.Context,
	req coordinator.PlanRequest,
) (workflowv1.Run, []workflowv1.WorkItem, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.fail != nil {
		return workflowv1.Run{}, nil, p.fail
	}
	now := req.ObservedAt
	scopes := make([]string, 0, len(p.targets))
	for _, target := range p.targets {
		scopes = append(scopes, target.ScopeID)
	}
	sort.Strings(scopes)
	encoded, _ := json.Marshal(scopes)
	run := workflowv1.Run{
		RunID: req.PlanKey, TriggerKind: workflowv1.TriggerKindSchedule,
		Status:            workflowv1.RunStatusCollectionActive,
		RequestedScopeSet: string(encoded), RequestedCollector: req.Instance.InstanceID,
		CreatedAt: now, UpdatedAt: now,
	}
	items := make([]workflowv1.WorkItem, 0, len(p.targets))
	for _, target := range p.targets {
		items = append(items, workflowv1.WorkItem{
			WorkItemID: req.PlanKey + "/" + target.ScopeID, RunID: req.PlanKey,
			CollectorKind: req.Family, CollectorInstanceID: req.Instance.InstanceID,
			SourceSystem: string(req.Family), ScopeID: target.ScopeID,
			AcceptanceUnitID: "au-" + target.ScopeID, SourceRunID: "sr-" + target.ScopeID,
			GenerationID: "gen-" + target.ScopeID, Status: workflowv1.WorkItemStatusPending,
			CreatedAt: now, UpdatedAt: now,
		})
	}
	return run, items, nil
}

func activeConfig(instances ...workflowv1.DesiredCollectorInstance) coordinator.Config {
	return coordinator.Config{
		DeploymentMode:       "active",
		ClaimsEnabled:        true,
		ReconcileInterval:    time.Second,
		RunReconcileInterval: time.Second,
		ReapInterval:         time.Second,
		CollectorInstances:   instances,
	}
}

func schedulableInstance(id, kind string) workflowv1.DesiredCollectorInstance {
	return workflowv1.DesiredCollectorInstance{
		InstanceID: id, CollectorKind: workflowv1.CollectorKind(kind),
		Mode: workflowv1.CollectorModeScheduled, Enabled: true, ClaimsEnabled: true,
		Configuration: `{"targets":[{"scope_id":"scope-a"},{"scope_id":"scope-b"}]}`,
	}
}

// TestConcurrentReconcileAdmitsOnce requires scheduler contention to
// converge: eight concurrent reconciles planning the same targets insert
// each work item exactly once through idempotent admission.
func TestConcurrentReconcileAdmitsOnce(t *testing.T) {
	plane := newFakePlane()
	planners := coordinator.NewRegistry()
	planner := &recordingPlanner{targets: []coordinator.PlanTarget{{ScopeID: "scope-a"}, {ScopeID: "scope-b"}}}
	planners.Register("jira", planner)
	svc := coordinator.Service{
		Config:   activeConfig(schedulableInstance("jira-primary", "jira")),
		Plane:    plane,
		Planners: planners,
		Logger:   slog.New(slog.DiscardHandler),
		Clock:    func() time.Time { return time.Now().UTC() },
	}

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = svc.Reconcile(context.Background())
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}
	if got := plane.itemCount(); got != 2 {
		t.Fatalf("work items = %d, want 2 (exactly-once admission under contention)", got)
	}
	plane.mu.Lock()
	inserted := plane.inserted
	plane.mu.Unlock()
	if inserted != 2 {
		t.Fatalf("total inserted = %d, want 2", inserted)
	}
}

// TestEveryFamilyPlannedEachPass requires the starvation bound: one
// reconcile pass plans every schedulable family, with the weighted
// round-robin order deciding who plans first.
func TestEveryFamilyPlannedEachPass(t *testing.T) {
	plane := newFakePlane()
	planners := coordinator.NewRegistry()
	kinds := []string{"jira", "loki", "pagerduty"}
	recorders := map[string]*recordingPlanner{}
	var instances []workflowv1.DesiredCollectorInstance
	for _, kind := range kinds {
		recorders[kind] = &recordingPlanner{targets: []coordinator.PlanTarget{{ScopeID: "scope-a"}}}
		planners.Register(workflowv1.CollectorKind(kind), recorders[kind])
		instances = append(instances, schedulableInstance(kind+"-primary", kind))
	}
	svc := coordinator.Service{
		Config:   activeConfig(instances...),
		Plane:    plane,
		Planners: planners,
		Logger:   slog.New(slog.DiscardHandler),
	}

	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, kind := range kinds {
		recorders[kind].mu.Lock()
		calls := recorders[kind].calls
		recorders[kind].mu.Unlock()
		if calls != 1 {
			t.Fatalf("family %s planned %d times in one pass, want 1", kind, calls)
		}
	}
	if got := plane.itemCount(); got != len(kinds) {
		t.Fatalf("work items = %d, want %d (one per family)", got, len(kinds))
	}
}

// TestFailingFamilyDoesNotStarveOthers requires family isolation: a
// planner that errors must not stop the pass for healthy families. The
// reconcile still reports the failure so the operator sees the sick
// family instead of silent drops.
func TestFailingFamilyDoesNotStarveOthers(t *testing.T) {
	plane := newFakePlane()
	planners := coordinator.NewRegistry()
	sick := &recordingPlanner{fail: fmt.Errorf("provider outage")}
	healthy := &recordingPlanner{targets: []coordinator.PlanTarget{{ScopeID: "scope-a"}}}
	planners.Register("jira", sick)
	planners.Register("loki", healthy)
	svc := coordinator.Service{
		Config: activeConfig(
			schedulableInstance("jira-primary", "jira"),
			schedulableInstance("loki-primary", "loki"),
		),
		Plane:    plane,
		Planners: planners,
		Logger:   slog.New(slog.DiscardHandler),
	}

	err := svc.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "jira") {
		t.Fatalf("reconcile error = %v, want the sick family named", err)
	}
	healthy.mu.Lock()
	calls := healthy.calls
	healthy.mu.Unlock()
	if calls != 1 {
		t.Fatalf("healthy family planned %d times, want 1 (isolation from the sick family)", calls)
	}
	if got := plane.itemCount(); got != 1 {
		t.Fatalf("work items = %d, want 1 (only the healthy family admitted)", got)
	}
}

// TestDarkModeReconcilesInstancesOnly requires dark mode to reconcile
// durable instance state without planning or admitting any work.
func TestDarkModeReconcilesInstancesOnly(t *testing.T) {
	plane := newFakePlane()
	planners := coordinator.NewRegistry()
	planner := &recordingPlanner{targets: []coordinator.PlanTarget{{ScopeID: "scope-a"}}}
	planners.Register("jira", planner)
	cfg := activeConfig(schedulableInstance("jira-primary", "jira"))
	cfg.DeploymentMode = "dark"
	svc := coordinator.Service{
		Config: cfg, Plane: plane, Planners: planners,
		Logger: slog.New(slog.DiscardHandler),
	}

	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	instances, err := plane.ListCollectorInstances(context.Background())
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("durable instances = %d, want 1", len(instances))
	}
	planner.mu.Lock()
	calls := planner.calls
	planner.mu.Unlock()
	if calls != 0 {
		t.Fatalf("dark mode planned %d times, want 0", calls)
	}
	if got := plane.itemCount(); got != 0 {
		t.Fatalf("dark mode admitted %d work items, want 0", got)
	}
}

// TestOpenRunSkipsDuplicatePlan requires the admission guard: a plan whose
// collector and scope set an open run already covers admits nothing, even
// across ListRuns pages.
func TestOpenRunSkipsDuplicatePlan(t *testing.T) {
	plane := newFakePlane()
	now := time.Now().UTC().Truncate(time.Second)
	// The cover is older than the decoys, so with a two-row page it
	// lands on page two: the guard must walk, not just read page one.
	plane.runs["sched-jira-jira-primary-w0"] = workflowv1.Run{
		RunID: "sched-jira-jira-primary-w0", TriggerKind: workflowv1.TriggerKindSchedule,
		Status: workflowv1.RunStatusCollectionActive, RequestedCollector: "jira-primary",
		RequestedScopeSet: `["scope-a","scope-b"]`, CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
	}
	plane.runs["other-1"] = workflowv1.Run{
		RunID: "other-1", TriggerKind: workflowv1.TriggerKindSchedule,
		Status: workflowv1.RunStatusCollectionActive, CreatedAt: now, UpdatedAt: now,
	}
	plane.runs["other-2"] = workflowv1.Run{
		RunID: "other-2", TriggerKind: workflowv1.TriggerKindSchedule,
		Status: workflowv1.RunStatusCollectionActive, CreatedAt: now, UpdatedAt: now,
	}
	planners := coordinator.NewRegistry()
	planner := &recordingPlanner{targets: []coordinator.PlanTarget{{ScopeID: "scope-a"}, {ScopeID: "scope-b"}}}
	planners.Register("jira", planner)
	svc := coordinator.Service{
		Config:   activeConfig(schedulableInstance("jira-primary", "jira")),
		Plane:    plane,
		Planners: planners,
		Logger:   slog.New(slog.DiscardHandler),
		Clock:    func() time.Time { return now },
	}

	// The guard matches on (requested collector, requested scope set),
	// not on run id: the seeded run covers the plan whatever its id, so
	// the pass must admit nothing even though the planner still runs.
	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := plane.itemCount(); got != 0 {
		t.Fatalf("guarded plan admitted %d work items, want 0", got)
	}
}
