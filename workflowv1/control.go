// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"fmt"
	"time"
)

// Control-plane request and response bodies. These six operations are the
// coordinator seams: the claim-plane client used to answer every one with
// ErrClaimPlaneUnsupported, and each now resolves to a real protocol route
// served from the service's own database. No shared tables cross the
// boundary; tenant and producer grants stay core-owned and never appear
// here. Adding an operation is a minor protocol change; the closed enums
// elsewhere are untouched, so the protocol major stays 1.

// CreateRunRequest admits one durable workflow run by upsert on run id: an
// ambiguous-timeout replay of the same run converges instead of doubling.
type CreateRunRequest struct {
	Run Run `json:"run"`
}

// Validate checks the run identity.
func (r CreateRunRequest) Validate() error {
	return r.Run.Validate()
}

// CreateRunResponse acknowledges the admission.
type CreateRunResponse struct {
	RunID string `json:"run_id"`
}

// Validate checks the acknowledgement.
func (r CreateRunResponse) Validate() error {
	return validateIdentifier("run_id", r.RunID)
}

// EnqueueWorkItemsRequest admits work items idempotently: conflict rows
// are skipped on work_item_id, so an ambiguous-timeout replay of the same
// batch lands nothing twice.
type EnqueueWorkItemsRequest struct {
	Items []WorkItem `json:"items"`
}

// Validate checks every item is admissible.
func (r EnqueueWorkItemsRequest) Validate() error {
	if len(r.Items) == 0 {
		return fmt.Errorf("items must not be empty")
	}
	for i := range r.Items {
		if err := r.Items[i].Validate(); err != nil {
			return fmt.Errorf("items[%d]: %w", i, err)
		}
	}
	return nil
}

// EnqueueWorkItemsResponse reports what one idempotent batch did.
// Accepted counts the rows the batch carried; InsertedWorkItems counts
// the rows the table actually took. The two numbers answer different
// questions: an ambiguous-timeout replay accepts the whole batch again
// while inserting nothing, and collapsing them reports lost work as a
// harmless duplicate.
type EnqueueWorkItemsResponse struct {
	Accepted          int `json:"accepted"`
	InsertedWorkItems int `json:"inserted_work_items"`
}

// Validate checks the acknowledgement.
func (r EnqueueWorkItemsResponse) Validate() error {
	if r.Accepted < 0 {
		return fmt.Errorf("accepted must not be negative")
	}
	if r.InsertedWorkItems < 0 {
		return fmt.Errorf("inserted_work_items must not be negative")
	}
	if r.InsertedWorkItems > r.Accepted {
		return fmt.Errorf("inserted_work_items must not exceed accepted")
	}
	return nil
}

// ReconcileCollectorInstancesRequest declares the operator-desired
// collector instances. Instances absent from the declaration deactivate;
// an empty declaration deactivates everything absent, which is valid.
type ReconcileCollectorInstancesRequest struct {
	ObservedAt time.Time                  `json:"observed_at"`
	Instances  []DesiredCollectorInstance `json:"instances,omitempty"`
}

// Validate checks the observation time and every declaration.
func (r ReconcileCollectorInstancesRequest) Validate() error {
	if err := validateTime("observed_at", r.ObservedAt); err != nil {
		return err
	}
	for i := range r.Instances {
		if err := r.Instances[i].Validate(); err != nil {
			return fmt.Errorf("instances[%d]: %w", i, err)
		}
	}
	return nil
}

// UpsertCompletenessStatesRequest records reducer-facing completion
// checkpoints for workflow runs.
type UpsertCompletenessStatesRequest struct {
	States []CompletenessState `json:"states"`
}

// Validate checks every checkpoint.
func (r UpsertCompletenessStatesRequest) Validate() error {
	if len(r.States) == 0 {
		return fmt.Errorf("states must not be empty")
	}
	for i := range r.States {
		if err := r.States[i].Validate(); err != nil {
			return fmt.Errorf("states[%d]: %w", i, err)
		}
	}
	return nil
}

// ReconcileWorkflowRunsRequest asks the service to advance drained runs.
type ReconcileWorkflowRunsRequest struct {
	ObservedAt time.Time `json:"observed_at"`
}

// Validate checks the observation time.
func (r ReconcileWorkflowRunsRequest) Validate() error {
	return validateTime("observed_at", r.ObservedAt)
}

// ReconcileWorkflowRunsResponse reports how many runs transitioned.
type ReconcileWorkflowRunsResponse struct {
	Transitioned int `json:"transitioned"`
}

// Validate checks the transition count.
func (r ReconcileWorkflowRunsResponse) Validate() error {
	if r.Transitioned < 0 {
		return fmt.Errorf("transitioned must not be negative")
	}
	return nil
}

// ReapExpiredClaimsRequest asks the service to expire stale claims past
// the observation time, skipping claims a commit pin still covers.
type ReapExpiredClaimsRequest struct {
	ObservedAt   time.Time     `json:"observed_at"`
	Limit        int           `json:"limit"`
	RequeueDelay time.Duration `json:"requeue_delay_ns"`
}

// Validate checks the reap bounds.
func (r ReapExpiredClaimsRequest) Validate() error {
	if err := validateTime("observed_at", r.ObservedAt); err != nil {
		return err
	}
	if r.Limit <= 0 {
		return fmt.Errorf("limit must be positive")
	}
	if r.RequeueDelay < 0 {
		return fmt.Errorf("requeue_delay_ns must not be negative")
	}
	return nil
}

// ReapExpiredClaimsResponse carries the expired claims.
type ReapExpiredClaimsResponse struct {
	Claims []Claim `json:"claims,omitempty"`
}

// Validate checks every expired claim.
func (r ReapExpiredClaimsResponse) Validate() error {
	for i := range r.Claims {
		if err := r.Claims[i].Validate(); err != nil {
			return fmt.Errorf("claims[%d]: %w", i, err)
		}
	}
	return nil
}
