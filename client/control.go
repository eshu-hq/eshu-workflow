// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package client

import (
	"context"
	"time"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// Control-plane calls. These six methods are the coordinator seams: each
// used to answer ErrClaimPlaneUnsupported on the in-tree claim-plane
// client, and each now travels a real protocol route served from the
// service's own database.

// CreateRun admits one durable workflow run by upsert on run id.
func (c *Client) CreateRun(ctx context.Context, run workflowv1.Run) error {
	if err := run.Validate(); err != nil {
		return err
	}
	var out workflowv1.CreateRunResponse
	return c.call(ctx, workflowv1.OpCreateRun, workflowv1.CreateRunRequest{Run: run}, &out)
}

// EnqueueWorkItems admits work items idempotently on work_item_id and
// reports how many rows the table actually took. A replay of the same
// batch accepts everything while inserting nothing.
func (c *Client) EnqueueWorkItems(ctx context.Context, items []workflowv1.WorkItem) (int, error) {
	req := workflowv1.EnqueueWorkItemsRequest{Items: items}
	if err := req.Validate(); err != nil {
		return 0, err
	}
	var out workflowv1.EnqueueWorkItemsResponse
	if err := c.call(ctx, workflowv1.OpEnqueueWorkItems, req, &out); err != nil {
		return 0, err
	}
	return out.InsertedWorkItems, nil
}

// ReconcileCollectorInstances declares the operator-desired collector
// instances; absent instances deactivate.
func (c *Client) ReconcileCollectorInstances(
	ctx context.Context,
	observedAt time.Time,
	desired []workflowv1.DesiredCollectorInstance,
) error {
	req := workflowv1.ReconcileCollectorInstancesRequest{ObservedAt: observedAt, Instances: desired}
	if err := req.Validate(); err != nil {
		return err
	}
	var out struct {
		OK bool `json:"ok"`
	}
	return c.call(ctx, workflowv1.OpReconcileCollectorInstances, req, &out)
}

// UpsertCompletenessStates records reducer-facing completion checkpoints.
func (c *Client) UpsertCompletenessStates(
	ctx context.Context,
	states []workflowv1.CompletenessState,
) error {
	req := workflowv1.UpsertCompletenessStatesRequest{States: states}
	if err := req.Validate(); err != nil {
		return err
	}
	var out struct {
		OK bool `json:"ok"`
	}
	return c.call(ctx, workflowv1.OpUpsertCompletenessStates, req, &out)
}

// ReconcileWorkflowRuns advances drained runs and reports how many
// transitioned.
func (c *Client) ReconcileWorkflowRuns(ctx context.Context, observedAt time.Time) (int, error) {
	req := workflowv1.ReconcileWorkflowRunsRequest{ObservedAt: observedAt}
	if err := req.Validate(); err != nil {
		return 0, err
	}
	var out workflowv1.ReconcileWorkflowRunsResponse
	if err := c.call(ctx, workflowv1.OpReconcileWorkflowRuns, req, &out); err != nil {
		return 0, err
	}
	return out.Transitioned, nil
}

// ReapExpiredClaims expires stale claims past the observation time and
// returns them; pinned claims survive.
func (c *Client) ReapExpiredClaims(
	ctx context.Context,
	observedAt time.Time,
	limit int,
	requeueDelay time.Duration,
) ([]workflowv1.Claim, error) {
	req := workflowv1.ReapExpiredClaimsRequest{
		ObservedAt: observedAt, Limit: limit, RequeueDelay: requeueDelay,
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	var out workflowv1.ReapExpiredClaimsResponse
	if err := c.call(ctx, workflowv1.OpReapExpiredClaims, req, &out); err != nil {
		return nil, err
	}
	if out.Claims == nil {
		out.Claims = []workflowv1.Claim{}
	}
	return out.Claims, nil
}
