// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"fmt"
	"time"
)

// RunSummary is the list-view of one workflow run.
type RunSummary struct {
	RunID              string      `json:"run_id"`
	TriggerKind        TriggerKind `json:"trigger_kind"`
	Status             RunStatus   `json:"status"`
	RequestedScopeSet  string      `json:"requested_scope_set,omitempty"`
	RequestedCollector string      `json:"requested_collector,omitempty"`
	CreatedAt          time.Time   `json:"created_at"`
	UpdatedAt          time.Time   `json:"updated_at"`
	FinishedAt         time.Time   `json:"finished_at,omitempty"`
}

// Validate checks the run summary.
func (s RunSummary) Validate() error {
	return Run(s).Validate()
}

// GetRunStatusRequest asks for the full status of one run.
type GetRunStatusRequest struct {
	RunID string `json:"run_id"`
}

// Validate checks the run-status request.
func (r GetRunStatusRequest) Validate() error {
	return validateIdentifier("run_id", r.RunID)
}

// GetRunStatusResponse is the read-plane run status: the run, per-family
// published progress, and completeness checkpoints.
type GetRunStatusResponse struct {
	Run          Run                    `json:"run"`
	Progress     []CollectorRunProgress `json:"progress,omitempty"`
	Completeness []CompletenessState    `json:"completeness,omitempty"`
	AsOf         time.Time              `json:"as_of"`
}

// Validate checks the run-status response.
func (r GetRunStatusResponse) Validate() error {
	if err := r.Run.Validate(); err != nil {
		return err
	}
	for i := range r.Progress {
		if err := r.Progress[i].Validate(); err != nil {
			return fmt.Errorf("progress[%d]: %w", i, err)
		}
	}
	for i := range r.Completeness {
		if err := r.Completeness[i].Validate(); err != nil {
			return fmt.Errorf("completeness[%d]: %w", i, err)
		}
	}
	return validateTime("as_of", r.AsOf)
}

const (
	defaultListRunsLimit = 100
	maxListRunsLimit     = 1000
)

// ListRunsRequest pages workflow runs, newest first.
type ListRunsRequest struct {
	Status RunStatus `json:"status,omitempty"`
	Limit  int       `json:"limit,omitempty"`
	Cursor string    `json:"cursor,omitempty"`
}

// Validate checks the list-runs request.
func (r ListRunsRequest) Validate() error {
	if r.Status != "" {
		if err := r.Status.Validate(); err != nil {
			return err
		}
	}
	if r.Limit < 0 || r.Limit > maxListRunsLimit {
		return fmt.Errorf("limit must be between 0 and %d", maxListRunsLimit)
	}
	return nil
}

// EffectiveLimit returns the server-applied page size.
func (r ListRunsRequest) EffectiveLimit() int {
	if r.Limit <= 0 {
		return defaultListRunsLimit
	}
	return r.Limit
}

// ListRunsResponse is one page of run summaries. NextCursor is empty on
// the last page.
type ListRunsResponse struct {
	Runs       []RunSummary `json:"runs"`
	NextCursor string       `json:"next_cursor,omitempty"`
	AsOf       time.Time    `json:"as_of"`
}

// Validate checks the list-runs response.
func (r ListRunsResponse) Validate() error {
	for i := range r.Runs {
		if err := r.Runs[i].Validate(); err != nil {
			return fmt.Errorf("runs[%d]: %w", i, err)
		}
	}
	return validateTime("as_of", r.AsOf)
}

// GetRunCompletenessRequest asks for the completeness checkpoints of one run.
type GetRunCompletenessRequest struct {
	RunID string `json:"run_id"`
}

// Validate checks the completeness request.
func (r GetRunCompletenessRequest) Validate() error {
	return validateIdentifier("run_id", r.RunID)
}

// GetRunCompletenessResponse carries the checkpoints.
type GetRunCompletenessResponse struct {
	RunID  string              `json:"run_id"`
	States []CompletenessState `json:"states,omitempty"`
	AsOf   time.Time           `json:"as_of"`
}

// Validate checks the completeness response.
func (r GetRunCompletenessResponse) Validate() error {
	if err := validateIdentifier("run_id", r.RunID); err != nil {
		return err
	}
	for i := range r.States {
		if err := r.States[i].Validate(); err != nil {
			return fmt.Errorf("states[%d]: %w", i, err)
		}
	}
	return validateTime("as_of", r.AsOf)
}

// ListCollectorInstancesResponse carries the durable collector instances.
// The request is empty: instances are read-only server state.
type ListCollectorInstancesResponse struct {
	Instances []CollectorInstance `json:"instances,omitempty"`
	AsOf      time.Time           `json:"as_of"`
}

// Validate checks the instances response.
func (r ListCollectorInstancesResponse) Validate() error {
	for i := range r.Instances {
		if err := r.Instances[i].Validate(); err != nil {
			return fmt.Errorf("instances[%d]: %w", i, err)
		}
	}
	return validateTime("as_of", r.AsOf)
}

// FamilyQueueDepth is the pending/claimed depth of one collector family.
type FamilyQueueDepth struct {
	CollectorKind CollectorKind `json:"collector_kind"`
	Pending       int64         `json:"pending"`
	Claimed       int64         `json:"claimed"`
}

// Validate checks the family depth.
func (d FamilyQueueDepth) Validate() error {
	if err := d.CollectorKind.Validate(); err != nil {
		return err
	}
	if d.Pending < 0 {
		return fmt.Errorf("pending must not be negative")
	}
	if d.Claimed < 0 {
		return fmt.Errorf("claimed must not be negative")
	}
	return nil
}

// QueueDepthSnapshot is the read-plane queue-depth observation that
// replaces direct work-item table reads for backpressure status.
type QueueDepthSnapshot struct {
	Families      []FamilyQueueDepth `json:"families,omitempty"`
	GlobalPending int64              `json:"global_pending"`
	GlobalClaimed int64              `json:"global_claimed"`
	AsOf          time.Time          `json:"as_of"`
}

// Validate checks the queue-depth snapshot.
func (s QueueDepthSnapshot) Validate() error {
	for i := range s.Families {
		if err := s.Families[i].Validate(); err != nil {
			return fmt.Errorf("families[%d]: %w", i, err)
		}
	}
	if s.GlobalPending < 0 {
		return fmt.Errorf("global_pending must not be negative")
	}
	if s.GlobalClaimed < 0 {
		return fmt.Errorf("global_claimed must not be negative")
	}
	return validateTime("as_of", s.AsOf)
}
