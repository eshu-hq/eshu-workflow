// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"fmt"
	"strings"
	"time"
)

// Run is the durable root record for one workflow execution.
type Run struct {
	RunID              string      `json:"run_id"`
	TriggerKind        TriggerKind `json:"trigger_kind"`
	Status             RunStatus   `json:"status"`
	RequestedScopeSet  string      `json:"requested_scope_set,omitempty"`
	RequestedCollector string      `json:"requested_collector,omitempty"`
	CreatedAt          time.Time   `json:"created_at"`
	UpdatedAt          time.Time   `json:"updated_at"`
	FinishedAt         time.Time   `json:"finished_at,omitempty"`
}

// Validate checks that the run carries the minimum durable identity.
func (r Run) Validate() error {
	if err := validateIdentifier("run_id", r.RunID); err != nil {
		return err
	}
	if err := r.TriggerKind.Validate(); err != nil {
		return err
	}
	if err := r.Status.Validate(); err != nil {
		return err
	}
	if err := validateTime("created_at", r.CreatedAt); err != nil {
		return err
	}
	if err := validateTime("updated_at", r.UpdatedAt); err != nil {
		return err
	}
	if r.UpdatedAt.Before(r.CreatedAt) {
		return fmt.Errorf("updated_at must not be before created_at")
	}
	if !r.FinishedAt.IsZero() && r.FinishedAt.Before(r.CreatedAt) {
		return fmt.Errorf("finished_at must not be before created_at")
	}
	return nil
}

// WorkItem is the durable unit claimed by one collector instance.
type WorkItem struct {
	WorkItemID          string         `json:"work_item_id"`
	RunID               string         `json:"run_id"`
	CollectorKind       CollectorKind  `json:"collector_kind"`
	CollectorInstanceID string         `json:"collector_instance_id"`
	SourceSystem        string         `json:"source_system"`
	ScopeID             string         `json:"scope_id"`
	TenantID            string         `json:"tenant_id,omitempty"`
	WorkspaceID         string         `json:"workspace_id,omitempty"`
	SubjectClass        string         `json:"subject_class,omitempty"`
	PolicyRevisionHash  string         `json:"policy_revision_hash,omitempty"`
	AcceptanceUnitID    string         `json:"acceptance_unit_id"`
	SourceRunID         string         `json:"source_run_id"`
	GenerationID        string         `json:"generation_id"`
	FairnessKey         string         `json:"fairness_key,omitempty"`
	ClaimPriority       int            `json:"claim_priority"`
	ClaimEcosystem      string         `json:"claim_ecosystem,omitempty"`
	Status              WorkItemStatus `json:"status"`
	AttemptCount        int            `json:"attempt_count"`
	CurrentClaimID      string         `json:"current_claim_id,omitempty"`
	CurrentFencingToken int64          `json:"current_fencing_token"`
	CurrentOwnerID      string         `json:"current_owner_id,omitempty"`
	LeaseExpiresAt      time.Time      `json:"lease_expires_at,omitempty"`
	VisibleAt           time.Time      `json:"visible_at,omitempty"`
	LastClaimedAt       time.Time      `json:"last_claimed_at,omitempty"`
	LastCompletedAt     time.Time      `json:"last_completed_at,omitempty"`
	LastFailureClass    string         `json:"last_failure_class,omitempty"`
	LastFailureMessage  string         `json:"last_failure_message,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

// Validate checks that the work item has durable identity and lifecycle shape.
func (w WorkItem) Validate() error {
	if err := validateIdentifier("work_item_id", w.WorkItemID); err != nil {
		return err
	}
	if err := validateIdentifier("run_id", w.RunID); err != nil {
		return err
	}
	if err := w.CollectorKind.Validate(); err != nil {
		return err
	}
	if err := validateIdentifier("collector_instance_id", w.CollectorInstanceID); err != nil {
		return err
	}
	if err := validateIdentifier("source_system", w.SourceSystem); err != nil {
		return err
	}
	if err := validateIdentifier("scope_id", w.ScopeID); err != nil {
		return err
	}
	if err := validateTenantBoundary(w.TenantID, w.WorkspaceID, w.SubjectClass, w.PolicyRevisionHash); err != nil {
		return err
	}
	if err := validateIdentifier("acceptance_unit_id", w.AcceptanceUnitID); err != nil {
		return err
	}
	if err := validateIdentifier("source_run_id", w.SourceRunID); err != nil {
		return err
	}
	if err := validateIdentifier("generation_id", w.GenerationID); err != nil {
		return err
	}
	if err := w.Status.Validate(); err != nil {
		return err
	}
	if w.AttemptCount < 0 {
		return fmt.Errorf("attempt_count must not be negative")
	}
	if w.CurrentFencingToken < 0 {
		return fmt.Errorf("current_fencing_token must not be negative")
	}
	if err := validateTime("created_at", w.CreatedAt); err != nil {
		return err
	}
	if err := validateTime("updated_at", w.UpdatedAt); err != nil {
		return err
	}
	if w.UpdatedAt.Before(w.CreatedAt) {
		return fmt.Errorf("updated_at must not be before created_at")
	}
	return nil
}

// validateTenantBoundary enforces the all-or-none tenant tuple: absent for
// legacy shared-mode work, complete for claim-aware collectors.
func validateTenantBoundary(tenantID, workspaceID, subjectClass, policyRevisionHash string) error {
	values := []string{tenantID, workspaceID, subjectClass, policyRevisionHash}
	present := 0
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			present++
		}
	}
	if present == 0 {
		return nil
	}
	if present != len(values) {
		return fmt.Errorf("tenant_id, workspace_id, subject_class, and policy_revision_hash must be set together")
	}
	return nil
}

// Claim is one durable ownership epoch for a workflow work item.
type Claim struct {
	ClaimID        string      `json:"claim_id"`
	WorkItemID     string      `json:"work_item_id"`
	FencingToken   int64       `json:"fencing_token"`
	OwnerID        string      `json:"owner_id"`
	Status         ClaimStatus `json:"status"`
	ClaimedAt      time.Time   `json:"claimed_at"`
	HeartbeatAt    time.Time   `json:"heartbeat_at"`
	LeaseExpiresAt time.Time   `json:"lease_expires_at"`
	FinishedAt     time.Time   `json:"finished_at,omitempty"`
	FailureClass   string      `json:"failure_class,omitempty"`
	FailureMessage string      `json:"failure_message,omitempty"`
	CreatedAt      time.Time   `json:"created_at"`
	UpdatedAt      time.Time   `json:"updated_at"`
}

// Validate checks that the claim carries durable ownership identity.
func (c Claim) Validate() error {
	if err := validateIdentifier("claim_id", c.ClaimID); err != nil {
		return err
	}
	if err := validateIdentifier("work_item_id", c.WorkItemID); err != nil {
		return err
	}
	if c.FencingToken <= 0 {
		return fmt.Errorf("fencing_token must be positive")
	}
	if err := validateIdentifier("owner_id", c.OwnerID); err != nil {
		return err
	}
	if err := c.Status.Validate(); err != nil {
		return err
	}
	if err := validateTime("claimed_at", c.ClaimedAt); err != nil {
		return err
	}
	if err := validateTime("heartbeat_at", c.HeartbeatAt); err != nil {
		return err
	}
	if err := validateTime("lease_expires_at", c.LeaseExpiresAt); err != nil {
		return err
	}
	if err := validateTime("created_at", c.CreatedAt); err != nil {
		return err
	}
	if err := validateTime("updated_at", c.UpdatedAt); err != nil {
		return err
	}
	if c.HeartbeatAt.Before(c.ClaimedAt) {
		return fmt.Errorf("heartbeat_at must not be before claimed_at")
	}
	if c.LeaseExpiresAt.Before(c.HeartbeatAt) {
		return fmt.Errorf("lease_expires_at must not be before heartbeat_at")
	}
	if c.UpdatedAt.Before(c.CreatedAt) {
		return fmt.Errorf("updated_at must not be before created_at")
	}
	if !c.FinishedAt.IsZero() && c.FinishedAt.Before(c.ClaimedAt) {
		return fmt.Errorf("finished_at must not be before claimed_at")
	}
	return nil
}

// DesiredCollectorInstance is the operator-declared shape of one collector
// instance the service reconciles into durable state.
type DesiredCollectorInstance struct {
	InstanceID    string        `json:"instance_id"`
	CollectorKind CollectorKind `json:"collector_kind"`
	Mode          CollectorMode `json:"mode"`
	Enabled       bool          `json:"enabled"`
	Bootstrap     bool          `json:"bootstrap"`
	ClaimsEnabled bool          `json:"claims_enabled"`
	DisplayName   string        `json:"display_name,omitempty"`
	Configuration string        `json:"configuration,omitempty"`
}

// Validate checks that the desired collector instance is well formed.
func (d DesiredCollectorInstance) Validate() error {
	if err := validateIdentifier("instance_id", d.InstanceID); err != nil {
		return err
	}
	if err := d.CollectorKind.Validate(); err != nil {
		return err
	}
	if err := d.Mode.Validate(); err != nil {
		return err
	}
	if err := validateJSONDocument("configuration", d.Configuration); err != nil {
		return err
	}
	return nil
}

// CollectorInstance is the durable reconciled collector instance.
type CollectorInstance struct {
	InstanceID     string        `json:"instance_id"`
	CollectorKind  CollectorKind `json:"collector_kind"`
	Mode           CollectorMode `json:"mode"`
	Enabled        bool          `json:"enabled"`
	Bootstrap      bool          `json:"bootstrap"`
	ClaimsEnabled  bool          `json:"claims_enabled"`
	DisplayName    string        `json:"display_name,omitempty"`
	Configuration  string        `json:"configuration,omitempty"`
	LastObservedAt time.Time     `json:"last_observed_at,omitempty"`
	DeactivatedAt  time.Time     `json:"deactivated_at,omitempty"`
	CreatedAt      time.Time     `json:"created_at"`
	UpdatedAt      time.Time     `json:"updated_at"`
}

// Validate checks that the stored collector instance has durable identity.
func (i CollectorInstance) Validate() error {
	if err := validateIdentifier("instance_id", i.InstanceID); err != nil {
		return err
	}
	if err := i.CollectorKind.Validate(); err != nil {
		return err
	}
	if err := i.Mode.Validate(); err != nil {
		return err
	}
	if err := validateJSONDocument("configuration", i.Configuration); err != nil {
		return err
	}
	if err := validateTime("created_at", i.CreatedAt); err != nil {
		return err
	}
	if err := validateTime("updated_at", i.UpdatedAt); err != nil {
		return err
	}
	return nil
}

// CompletenessState captures one reducer-facing completion checkpoint for a
// workflow run.
type CompletenessState struct {
	RunID         string        `json:"run_id"`
	CollectorKind CollectorKind `json:"collector_kind"`
	Keyspace      string        `json:"keyspace"`
	PhaseName     string        `json:"phase_name"`
	Required      bool          `json:"required"`
	Status        string        `json:"status"`
	Detail        string        `json:"detail,omitempty"`
	ObservedAt    time.Time     `json:"observed_at"`
	UpdatedAt     time.Time     `json:"updated_at"`
}

// Validate checks the completeness checkpoint identity.
func (s CompletenessState) Validate() error {
	if err := validateIdentifier("run_id", s.RunID); err != nil {
		return err
	}
	if err := s.CollectorKind.Validate(); err != nil {
		return err
	}
	if err := validateIdentifier("keyspace", s.Keyspace); err != nil {
		return err
	}
	if err := validateIdentifier("phase_name", s.PhaseName); err != nil {
		return err
	}
	if err := validateIdentifier("status", s.Status); err != nil {
		return err
	}
	if err := validateTime("observed_at", s.ObservedAt); err != nil {
		return err
	}
	if err := validateTime("updated_at", s.UpdatedAt); err != nil {
		return err
	}
	return nil
}

// RunAdmission reports what one guarded scheduled-run admission did.
// EligibleTargets and InsertedWorkItems answer different questions: the
// first counts planned work no non-terminal run already covers, the second
// counts rows the work-item table actually accepted. Collapsing them
// reports lost work as a harmless duplicate skip.
type RunAdmission struct {
	EligibleTargets   int `json:"eligible_targets"`
	InsertedWorkItems int `json:"inserted_work_items"`
}

// Validate checks the admission counts are sane.
func (a RunAdmission) Validate() error {
	if a.EligibleTargets < 0 {
		return fmt.Errorf("eligible_targets must not be negative")
	}
	if a.InsertedWorkItems < 0 {
		return fmt.Errorf("inserted_work_items must not be negative")
	}
	if a.InsertedWorkItems > a.EligibleTargets {
		return fmt.Errorf("inserted_work_items must not exceed eligible_targets")
	}
	return nil
}

func validateIdentifier(field, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s must not be blank", field)
	}
	return nil
}

func validateTime(field string, value time.Time) error {
	if value.IsZero() {
		return fmt.Errorf("%s must not be zero", field)
	}
	return nil
}
