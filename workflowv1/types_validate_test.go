// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"strings"
	"testing"
	"time"
)

// wantErrContaining fails unless err is non-nil and mentions want, so a
// rejection is pinned to the field that caused it and not to any error.
func wantErrContaining(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("validated, want error containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, want it to contain %q", err, want)
	}
}

func TestRunValidateRejections(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Run)
		want   string
	}{
		{"unknown trigger", func(r *Run) { r.TriggerKind = "cron" }, "trigger kind"},
		{"unknown status", func(r *Run) { r.Status = "paused" }, "run status"},
		{"zero created_at", func(r *Run) { r.CreatedAt = time.Time{} }, "created_at"},
		{"zero updated_at", func(r *Run) { r.UpdatedAt = time.Time{} }, "updated_at"},
		{"updated before created", func(r *Run) { r.UpdatedAt = r.CreatedAt.Add(-time.Second) }, "updated_at must not be before"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			run := validRun()
			tc.mutate(&run)
			wantErrContaining(t, run.Validate(), tc.want)
		})
	}
}

func TestWorkItemValidateRejections(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*WorkItem)
		want   string
	}{
		{"blank work_item_id", func(w *WorkItem) { w.WorkItemID = "" }, "work_item_id"},
		{"blank run_id", func(w *WorkItem) { w.RunID = " " }, "run_id"},
		{"blank collector kind", func(w *WorkItem) { w.CollectorKind = "" }, "collector_kind"},
		{"blank collector_instance_id", func(w *WorkItem) { w.CollectorInstanceID = "" }, "collector_instance_id"},
		{"blank source_system", func(w *WorkItem) { w.SourceSystem = "" }, "source_system"},
		{"blank scope_id", func(w *WorkItem) { w.ScopeID = "" }, "scope_id"},
		{"blank acceptance_unit_id", func(w *WorkItem) { w.AcceptanceUnitID = "" }, "acceptance_unit_id"},
		{"blank source_run_id", func(w *WorkItem) { w.SourceRunID = "" }, "source_run_id"},
		{"blank generation_id", func(w *WorkItem) { w.GenerationID = "" }, "generation_id"},
		{"unknown status", func(w *WorkItem) { w.Status = "stuck" }, "work-item status"},
		{"negative fencing token", func(w *WorkItem) { w.CurrentFencingToken = -1 }, "current_fencing_token"},
		{"zero created_at", func(w *WorkItem) { w.CreatedAt = time.Time{} }, "created_at"},
		{"zero updated_at", func(w *WorkItem) { w.UpdatedAt = time.Time{} }, "updated_at"},
		{"updated before created", func(w *WorkItem) { w.UpdatedAt = w.CreatedAt.Add(-time.Second) }, "updated_at must not be before"},
		{"tenant tuple partial", func(w *WorkItem) { w.WorkspaceID = "w" }, "set together"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			item := validWorkItem()
			tc.mutate(&item)
			wantErrContaining(t, item.Validate(), tc.want)
		})
	}
}

func TestClaimValidateRejections(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Claim)
		want   string
	}{
		{"blank claim_id", func(c *Claim) { c.ClaimID = "" }, "claim_id"},
		{"blank work_item_id", func(c *Claim) { c.WorkItemID = "" }, "work_item_id"},
		{"blank owner_id", func(c *Claim) { c.OwnerID = "" }, "owner_id"},
		{"unknown status", func(c *Claim) { c.Status = "lost" }, "claim status"},
		{"zero claimed_at", func(c *Claim) { c.ClaimedAt = time.Time{} }, "claimed_at"},
		{"zero heartbeat_at", func(c *Claim) { c.HeartbeatAt = time.Time{} }, "heartbeat_at"},
		{"zero lease_expires_at", func(c *Claim) { c.LeaseExpiresAt = time.Time{} }, "lease_expires_at"},
		{"zero created_at", func(c *Claim) { c.CreatedAt = time.Time{} }, "created_at"},
		{"zero updated_at", func(c *Claim) { c.UpdatedAt = time.Time{} }, "updated_at"},
		{"heartbeat before claim", func(c *Claim) { c.HeartbeatAt = c.ClaimedAt.Add(-time.Second) }, "heartbeat_at must not be before"},
		{"updated before created", func(c *Claim) { c.UpdatedAt = c.CreatedAt.Add(-time.Second) }, "updated_at must not be before"},
		{"finished before claimed", func(c *Claim) { c.FinishedAt = c.ClaimedAt.Add(-time.Second) }, "finished_at must not be before"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			claim := validClaim()
			tc.mutate(&claim)
			wantErrContaining(t, claim.Validate(), tc.want)
		})
	}
	finished := validClaim()
	finished.FinishedAt = finished.ClaimedAt.Add(time.Second)
	if err := finished.Validate(); err != nil {
		t.Fatalf("claim finished after it was claimed rejected: %v", err)
	}
}

func validInstanceDeclaration() DesiredCollectorInstance {
	return DesiredCollectorInstance{
		InstanceID:    "jira-primary",
		CollectorKind: "jira",
		Mode:          CollectorModeContinuous,
		Configuration: `{"site":"example"}`,
	}
}

func TestDesiredCollectorInstanceValidate(t *testing.T) {
	if err := validInstanceDeclaration().Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*DesiredCollectorInstance)
		want   string
	}{
		{"blank instance_id", func(d *DesiredCollectorInstance) { d.InstanceID = "" }, "instance_id"},
		{"blank collector kind", func(d *DesiredCollectorInstance) { d.CollectorKind = "" }, "collector_kind"},
		{"unknown mode", func(d *DesiredCollectorInstance) { d.Mode = "sometimes" }, "collector mode"},
		{"configuration not json", func(d *DesiredCollectorInstance) { d.Configuration = "{nope" }, "configuration"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decl := validInstanceDeclaration()
			tc.mutate(&decl)
			wantErrContaining(t, decl.Validate(), tc.want)
		})
	}
}

func TestCollectorInstanceValidate(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	valid := CollectorInstance{
		InstanceID:    "jira-primary",
		CollectorKind: "jira",
		Mode:          CollectorModeScheduled,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*CollectorInstance)
		want   string
	}{
		{"blank instance_id", func(i *CollectorInstance) { i.InstanceID = "" }, "instance_id"},
		{"blank collector kind", func(i *CollectorInstance) { i.CollectorKind = "" }, "collector_kind"},
		{"unknown mode", func(i *CollectorInstance) { i.Mode = "" }, "collector mode"},
		{"configuration not json", func(i *CollectorInstance) { i.Configuration = "[1," }, "configuration"},
		{"zero created_at", func(i *CollectorInstance) { i.CreatedAt = time.Time{} }, "created_at"},
		{"zero updated_at", func(i *CollectorInstance) { i.UpdatedAt = time.Time{} }, "updated_at"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inst := valid
			tc.mutate(&inst)
			wantErrContaining(t, inst.Validate(), tc.want)
		})
	}
}

func TestCompletenessStateValidateRejections(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	valid := CompletenessState{
		RunID: "run-1", CollectorKind: "jira", Keyspace: "default",
		PhaseName: "collect", Status: "complete", ObservedAt: now, UpdatedAt: now,
	}
	tests := []struct {
		name   string
		mutate func(*CompletenessState)
		want   string
	}{
		{"blank run_id", func(s *CompletenessState) { s.RunID = "" }, "run_id"},
		{"blank collector kind", func(s *CompletenessState) { s.CollectorKind = "" }, "collector_kind"},
		{"blank keyspace", func(s *CompletenessState) { s.Keyspace = "" }, "keyspace"},
		{"blank phase_name", func(s *CompletenessState) { s.PhaseName = "" }, "phase_name"},
		{"zero observed_at", func(s *CompletenessState) { s.ObservedAt = time.Time{} }, "observed_at"},
		{"zero updated_at", func(s *CompletenessState) { s.UpdatedAt = time.Time{} }, "updated_at"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := valid
			tc.mutate(&state)
			wantErrContaining(t, state.Validate(), tc.want)
		})
	}
}

func TestRunAdmissionRejectsNegativeCounts(t *testing.T) {
	wantErrContaining(t, (RunAdmission{EligibleTargets: -1}).Validate(), "eligible_targets")
	wantErrContaining(t, (RunAdmission{InsertedWorkItems: -1}).Validate(), "inserted_work_items must not be negative")
}
