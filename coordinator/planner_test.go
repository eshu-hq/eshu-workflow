// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package coordinator_test

import (
	"context"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/coordinator"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestDefaultRegistryPlansEveryPortedFamily requires the registry to
// cover the ported family set: every family the in-tree runner
// scheduled plans through its declared targets.
func TestDefaultRegistryPlansEveryPortedFamily(t *testing.T) {
	registry := coordinator.DefaultRegistry()
	families := []workflowv1.CollectorKind{
		"terraform_state", "oci_registry", "package_registry",
		"vulnerability_intelligence", "sbom_attestation", "security_alert",
		"ci_cd_run", "pagerduty", "jira", "scanner_worker", "aws", "gcp",
		"prometheus_mimir", "tempo", "grafana", "loki", "vault_live",
	}
	now := time.Now().UTC().Truncate(time.Second)
	for _, family := range families {
		planner, ok := registry.Lookup(family)
		if !ok {
			t.Fatalf("family %q has no planner", family)
		}
		run, items, err := planner.Plan(context.Background(), coordinator.PlanRequest{
			Family:   family,
			Instance: workflowv1.CollectorInstance{InstanceID: "i-1", CollectorKind: family},
			Targets: []coordinator.PlanTarget{{
				ScopeID: "scope-a", TenantID: "t-1", WorkspaceID: "w-1",
				SubjectClass: "project", PolicyRevisionHash: "p-1",
			}},
			PlanKey:    "plan-1",
			ObservedAt: now,
		})
		if err != nil {
			t.Fatalf("family %q plan: %v", family, err)
		}
		if len(items) != 1 {
			t.Fatalf("family %q planned %d items, want 1", family, len(items))
		}
		if err := run.Validate(); err != nil {
			t.Fatalf("family %q run invalid: %v", family, err)
		}
		if err := items[0].Validate(); err != nil {
			t.Fatalf("family %q item invalid: %v", family, err)
		}
		if items[0].TenantID != "t-1" || items[0].PolicyRevisionHash != "p-1" {
			t.Fatalf("family %q dropped the tenant tuple: %+v", family, items[0])
		}
	}
}

// TestTargetPlannerSkipsEmptyTargets requires an empty declaration to
// plan nothing: no run, no items, no error.
func TestTargetPlannerSkipsEmptyTargets(t *testing.T) {
	planner := coordinator.NewTargetPlanner("jira")
	run, items, err := planner.Plan(context.Background(), coordinator.PlanRequest{
		Family:     "jira",
		Instance:   workflowv1.CollectorInstance{InstanceID: "i-1", CollectorKind: "jira"},
		PlanKey:    "plan-empty",
		ObservedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("empty plan: %v", err)
	}
	if len(items) != 0 || (run != workflowv1.Run{}) {
		t.Fatalf("empty plan = %+v, %+v, want zero run and no items", run, items)
	}
}

// TestTargetPlannerRejectsBadRequests requires malformed invocations to
// fail before any planning.
func TestTargetPlannerRejectsBadRequests(t *testing.T) {
	planner := coordinator.NewTargetPlanner("jira")
	now := time.Now().UTC()
	valid := coordinator.PlanRequest{
		Family:     "jira",
		Instance:   workflowv1.CollectorInstance{InstanceID: "i-1", CollectorKind: "jira"},
		Targets:    []coordinator.PlanTarget{{ScopeID: "scope-a"}},
		PlanKey:    "plan-1",
		ObservedAt: now,
	}
	if _, _, err := planner.Plan(context.Background(), valid); err != nil {
		t.Fatalf("valid plan rejected: %v", err)
	}
	bad := valid
	bad.Family = ""
	if _, _, err := planner.Plan(context.Background(), bad); err == nil {
		t.Fatal("blank family planned")
	}
	bad = valid
	bad.Targets = []coordinator.PlanTarget{{ScopeID: "  "}}
	if _, _, err := planner.Plan(context.Background(), bad); err == nil {
		t.Fatal("blank scope planned")
	}
}

// TestPlanKeyWindowsByScanInterval requires deterministic windowing:
// two passes inside one scan window derive the same run identity, so
// the replay upserts one run and inserts nothing twice; a later window
// admits a fresh run once the earlier run is terminal and releases the
// guard.
func TestPlanKeyWindowsByScanInterval(t *testing.T) {
	registry := coordinator.NewRegistry()
	planner := &recordingPlanner{targets: []coordinator.PlanTarget{{ScopeID: "scope-a"}}}
	registry.Register("jira", planner)
	plane := newFakePlane()
	window := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	cfg := activeConfig(schedulableInstance("jira-primary", "jira"))
	cfg.ScanInterval = 5 * time.Minute
	// Pass minutes land two scan windows: 12:01 and 12:02 share one,
	// 12:06 opens the next.
	minutes := []int{1, 2, 6}
	calls := 0
	svc := coordinator.Service{
		Config:   cfg,
		Plane:    plane,
		Planners: registry,
		Clock: func() time.Time {
			at := window.Add(time.Duration(minutes[calls]) * time.Minute)
			calls++
			return at
		},
	}
	// Passes one and two share a window: one run admitted, the replay
	// inserting nothing.
	for range 2 {
		if err := svc.Reconcile(context.Background()); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}
	plane.mu.Lock()
	if len(plane.runs) != 1 || plane.inserted != 1 {
		t.Fatalf("replay admitted runs=%d inserted=%d, want 1 and 1", len(plane.runs), plane.inserted)
	}
	for id, run := range plane.runs {
		run.Status = workflowv1.RunStatusComplete
		plane.runs[id] = run
	}
	plane.mu.Unlock()
	// Pass three lands a new window with the earlier run terminal: a
	// second run admits its item.
	if err := svc.Reconcile(context.Background()); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	plane.mu.Lock()
	defer plane.mu.Unlock()
	if len(plane.runs) != 2 || plane.inserted != 2 {
		t.Fatalf("new window admitted runs=%d inserted=%d, want 2 and 2", len(plane.runs), plane.inserted)
	}
}
