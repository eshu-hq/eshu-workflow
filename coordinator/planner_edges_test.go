// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package coordinator_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/coordinator"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

func validPlanRequest() coordinator.PlanRequest {
	return coordinator.PlanRequest{
		Family:     "jira",
		Instance:   workflowv1.CollectorInstance{InstanceID: "jira-primary", CollectorKind: "jira"},
		Targets:    []coordinator.PlanTarget{{ScopeID: "scope-b"}, {ScopeID: "scope-a"}},
		PlanKey:    "sched-jira-jira-primary-100",
		ObservedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
}

func TestPlanRequestValidateRejections(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*coordinator.PlanRequest)
		want   string
	}{
		{"blank family", func(r *coordinator.PlanRequest) { r.Family = "" }, "collector_kind"},
		{"blank instance", func(r *coordinator.PlanRequest) { r.Instance.InstanceID = " " }, "instance id"},
		{"blank plan key", func(r *coordinator.PlanRequest) { r.PlanKey = "" }, "plan key"},
		{"zero observed_at", func(r *coordinator.PlanRequest) { r.ObservedAt = time.Time{} }, "observed_at"},
		{"blank target scope", func(r *coordinator.PlanRequest) { r.Targets[1].ScopeID = "" }, "target 1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validPlanRequest()
			req.Targets = append([]coordinator.PlanTarget(nil), req.Targets...)
			tc.mutate(&req)
			err := req.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate = %v, want an error containing %q", err, tc.want)
			}
			if _, _, planErr := coordinator.NewTargetPlanner("jira").Plan(context.Background(), req); planErr == nil {
				t.Fatal("Plan accepted a request Validate rejects")
			}
		})
	}
	if err := validPlanRequest().Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
}

// TestTargetPlannerBuildsDeterministicRunAndItems requires duplicate
// scopes to collapse to one item, ids to default from the scope, explicit
// ids to survive, and the run to carry the sorted scope set so replays
// and racing coordinators converge on identical rows.
func TestTargetPlannerBuildsDeterministicRunAndItems(t *testing.T) {
	req := validPlanRequest()
	req.Targets = []coordinator.PlanTarget{
		{ScopeID: "scope-b"},
		{ScopeID: "scope-a", AcceptanceUnitID: "au-explicit", SourceRunID: "sr-explicit", GenerationID: "gen-explicit"},
		{ScopeID: "scope-b", AcceptanceUnitID: "ignored-duplicate"},
	}
	run, items, err := coordinator.NewTargetPlanner("jira").Plan(context.Background(), req)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("planned %d items from two distinct scopes, want 2", len(items))
	}
	byScope := map[string]workflowv1.WorkItem{}
	for _, item := range items {
		byScope[item.ScopeID] = item
		if err := item.Validate(); err != nil {
			t.Fatalf("item %s invalid: %v", item.WorkItemID, err)
		}
		if item.RunID != req.PlanKey || item.WorkItemID != req.PlanKey+"/"+item.ScopeID {
			t.Fatalf("item identity = %s in run %s, want %s/<scope> in %s", item.WorkItemID, item.RunID, req.PlanKey, req.PlanKey)
		}
		if item.FairnessKey != "jira:jira-primary:scheduled" || item.Status != workflowv1.WorkItemStatusPending {
			t.Fatalf("item scheduling fields = %+v", item)
		}
	}
	if got := byScope["scope-b"]; got.AcceptanceUnitID != "au-scope-b" || got.SourceRunID != "sr-scope-b" || got.GenerationID != "gen-scope-b" {
		t.Fatalf("defaulted ids = %s/%s/%s, want the scope-derived defaults (the duplicate's explicit id must not leak)",
			got.AcceptanceUnitID, got.SourceRunID, got.GenerationID)
	}
	if got := byScope["scope-a"]; got.AcceptanceUnitID != "au-explicit" || got.SourceRunID != "sr-explicit" || got.GenerationID != "gen-explicit" {
		t.Fatalf("explicit ids were overwritten: %+v", got)
	}

	var scopes []string
	if err := json.Unmarshal([]byte(run.RequestedScopeSet), &scopes); err != nil {
		t.Fatalf("scope set is not JSON: %v", err)
	}
	if strings.Join(scopes, ",") != "scope-a,scope-b" {
		t.Fatalf("scope set = %v, want sorted [scope-a scope-b]", scopes)
	}
	if run.RunID != req.PlanKey || run.RequestedCollector != "jira-primary" ||
		run.TriggerKind != workflowv1.TriggerKindSchedule || !run.CreatedAt.Equal(req.ObservedAt) {
		t.Fatalf("run = %+v", run)
	}
	if err := run.Validate(); err != nil {
		t.Fatalf("run invalid: %v", err)
	}
}

type stubPlanner struct{}

func (stubPlanner) Plan(context.Context, coordinator.PlanRequest) (workflowv1.Run, []workflowv1.WorkItem, error) {
	return workflowv1.Run{}, nil, nil
}

func TestRegistryRegisterReplacesAndRemoves(t *testing.T) {
	registry := coordinator.NewRegistry()
	if _, ok := registry.Lookup("jira"); ok {
		t.Fatal("empty registry resolved a family")
	}
	registry.Register("jira", stubPlanner{})
	first, ok := registry.Lookup("jira")
	if !ok || first != (stubPlanner{}) {
		t.Fatalf("lookup after register = %v, %v", first, ok)
	}
	replacement := coordinator.NewTargetPlanner("jira")
	registry.Register("jira", replacement)
	if got, _ := registry.Lookup("jira"); got != coordinator.Planner(replacement) {
		t.Fatalf("lookup after replace = %v, want the replacement", got)
	}
	registry.Register("jira", nil)
	if _, ok := registry.Lookup("jira"); ok {
		t.Fatal("registering a nil planner did not remove the family")
	}
}

func TestPlanTargetValidate(t *testing.T) {
	if err := (coordinator.PlanTarget{ScopeID: "s"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (coordinator.PlanTarget{ScopeID: "  "}).Validate(); err == nil {
		t.Fatal("blank scope validated")
	}
}
