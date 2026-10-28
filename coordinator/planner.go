// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// portedFamilies is the family set the port plans: every kind the
// in-tree runner scheduled, keyed by its protocol collector kind. The
// generic component-extension scheduler has no kind scope and stays in
// core until its activation documents cross the boundary.
var portedFamilies = []workflowv1.CollectorKind{
	"terraform_state",
	"oci_registry",
	"package_registry",
	"vulnerability_intelligence",
	"sbom_attestation",
	"security_alert",
	"ci_cd_run",
	"pagerduty",
	"jira",
	"scanner_worker",
	"aws",
	"gcp",
	"prometheus_mimir",
	"tempo",
	"grafana",
	"loki",
	"vault_live",
}

// PlanTarget is one declared collection target. The tenant tuple is
// core-owned input: planners carry it onto the work item so the fact
// commit boundary can re-check it, and the service never reads a grant
// table.
type PlanTarget struct {
	ScopeID            string `json:"scope_id"`
	AcceptanceUnitID   string `json:"acceptance_unit_id,omitempty"`
	SourceRunID        string `json:"source_run_id,omitempty"`
	GenerationID       string `json:"generation_id,omitempty"`
	TenantID           string `json:"tenant_id,omitempty"`
	WorkspaceID        string `json:"workspace_id,omitempty"`
	SubjectClass       string `json:"subject_class,omitempty"`
	PolicyRevisionHash string `json:"policy_revision_hash,omitempty"`
}

// Validate checks the target identity.
func (t PlanTarget) Validate() error {
	if strings.TrimSpace(t.ScopeID) == "" {
		return fmt.Errorf("scope_id must not be blank")
	}
	return nil
}

// PlanRequest is one family planning invocation for one instance.
type PlanRequest struct {
	Family     workflowv1.CollectorKind
	Instance   workflowv1.CollectorInstance
	Targets    []PlanTarget
	PlanKey    string
	ObservedAt time.Time
}

// Validate checks the planning invocation identity.
func (r PlanRequest) Validate() error {
	if err := r.Family.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(r.Instance.InstanceID) == "" {
		return fmt.Errorf("instance id must not be blank")
	}
	if strings.TrimSpace(r.PlanKey) == "" {
		return fmt.Errorf("plan key must not be blank")
	}
	if r.ObservedAt.IsZero() {
		return fmt.Errorf("observed_at must not be zero")
	}
	for i := range r.Targets {
		if err := r.Targets[i].Validate(); err != nil {
			return fmt.Errorf("target %d: %w", i, err)
		}
	}
	return nil
}

// Planner plans one family run and its work items from declared targets.
// Provider-specific target discovery stays in core; what crosses the
// boundary is the declared list.
type Planner interface {
	Plan(context.Context, PlanRequest) (workflowv1.Run, []workflowv1.WorkItem, error)
}

// Registry maps collector families to their planners.
type Registry struct {
	mu       sync.RWMutex
	planners map[workflowv1.CollectorKind]Planner
}

// NewRegistry builds an empty planner registry.
func NewRegistry() *Registry {
	return &Registry{planners: map[workflowv1.CollectorKind]Planner{}}
}

// Register binds one family to its planner. A nil planner removes the
// family.
func (r *Registry) Register(kind workflowv1.CollectorKind, planner Planner) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if planner == nil {
		delete(r.planners, kind)
		return
	}
	r.planners[kind] = planner
}

// Lookup returns the planner for one family.
func (r *Registry) Lookup(kind workflowv1.CollectorKind) (Planner, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	planner, ok := r.planners[kind]
	return planner, ok
}

// DefaultRegistry plans every ported family from its declared targets.
func DefaultRegistry() *Registry {
	registry := NewRegistry()
	for _, family := range portedFamilies {
		registry.Register(family, NewTargetPlanner(family))
	}
	return registry
}

// TargetPlanner is the boundary-crossing shape of the in-tree family
// planners: it turns declared targets into a deterministic run and
// idempotent work items. Deterministic ids make replays converge: the
// same plan key upserts the same run and conflict-skips the same items.
type TargetPlanner struct {
	family workflowv1.CollectorKind
}

// NewTargetPlanner builds the planner for one family.
func NewTargetPlanner(family workflowv1.CollectorKind) *TargetPlanner {
	return &TargetPlanner{family: family}
}

// Plan builds the deterministic run and items for the declared targets.
// No targets plans nothing: it returns the zero run and no items, and
// the service skips admission.
func (p *TargetPlanner) Plan(_ context.Context, req PlanRequest) (workflowv1.Run, []workflowv1.WorkItem, error) {
	if err := req.Validate(); err != nil {
		return workflowv1.Run{}, nil, err
	}
	if len(req.Targets) == 0 {
		return workflowv1.Run{}, nil, nil
	}
	scopes := make([]string, 0, len(req.Targets))
	seen := map[string]bool{}
	items := make([]workflowv1.WorkItem, 0, len(req.Targets))
	for _, target := range req.Targets {
		if seen[target.ScopeID] {
			continue
		}
		seen[target.ScopeID] = true
		scopes = append(scopes, target.ScopeID)
		items = append(items, workflowv1.WorkItem{
			WorkItemID:          req.PlanKey + "/" + target.ScopeID,
			RunID:               req.PlanKey,
			CollectorKind:       req.Family,
			CollectorInstanceID: req.Instance.InstanceID,
			SourceSystem:        string(req.Family),
			ScopeID:             target.ScopeID,
			TenantID:            target.TenantID,
			WorkspaceID:         target.WorkspaceID,
			SubjectClass:        target.SubjectClass,
			PolicyRevisionHash:  target.PolicyRevisionHash,
			AcceptanceUnitID:    defaultID("au", target.AcceptanceUnitID, target.ScopeID),
			SourceRunID:         defaultID("sr", target.SourceRunID, target.ScopeID),
			GenerationID:        defaultID("gen", target.GenerationID, target.ScopeID),
			FairnessKey:         string(req.Family) + ":" + req.Instance.InstanceID + ":scheduled",
			Status:              workflowv1.WorkItemStatusPending,
			CreatedAt:           req.ObservedAt,
			UpdatedAt:           req.ObservedAt,
		})
	}
	sort.Strings(scopes)
	encoded, err := json.Marshal(scopes)
	if err != nil {
		return workflowv1.Run{}, nil, fmt.Errorf("encode scope set: %w", err)
	}
	return workflowv1.Run{
		RunID:              req.PlanKey,
		TriggerKind:        workflowv1.TriggerKindSchedule,
		Status:             workflowv1.RunStatusCollectionActive,
		RequestedScopeSet:  string(encoded),
		RequestedCollector: req.Instance.InstanceID,
		CreatedAt:          req.ObservedAt,
		UpdatedAt:          req.ObservedAt,
	}, items, nil
}

func defaultID(prefix, value, scope string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return prefix + "-" + scope
}

// instanceTargets parses the declared target list from an instance
// configuration document. A missing or empty list plans nothing; a
// malformed document fails the family so the operator sees it.
func instanceTargets(configuration string) ([]PlanTarget, error) {
	trimmed := strings.TrimSpace(configuration)
	if trimmed == "" || trimmed == "{}" {
		return nil, nil
	}
	var document struct {
		Targets []PlanTarget `json:"targets"`
	}
	if err := json.Unmarshal([]byte(trimmed), &document); err != nil {
		return nil, fmt.Errorf("parse instance targets: %w", err)
	}
	for i := range document.Targets {
		if err := document.Targets[i].Validate(); err != nil {
			return nil, fmt.Errorf("target %d: %w", i, err)
		}
	}
	return document.Targets, nil
}

// planKeyFor windows deterministic plan identity: two passes inside one
// scan window plan the same run id, so ambiguous-timeout replays and
// racing coordinators converge instead of doubling.
func planKeyFor(family workflowv1.CollectorKind, instanceID string, observedAt time.Time, scan time.Duration) string {
	return fmt.Sprintf("sched-%s-%s-%d",
		string(family), instanceID, observedAt.UTC().Truncate(scan).Unix())
}
