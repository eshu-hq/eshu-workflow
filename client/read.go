// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package client

import (
	"context"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// Report-plane call: core pushes run progress the service may not read
// from core tables. The coordinator binary forwards core's pushes over
// this route; it never reads fact tables itself.

// ReportRunProgress stores core's published per-family run progress.
func (c *Client) ReportRunProgress(ctx context.Context, req workflowv1.ReportRunProgressRequest) error {
	if err := req.Validate(); err != nil {
		return err
	}
	var out struct {
		OK bool `json:"ok"`
	}
	return c.call(ctx, workflowv1.OpReportRunProgress, req, &out)
}

// Read-plane calls: the coordinator reconciles from stored pushes and
// durable state, never from direct table reads.

// GetRunStatus reads the full status of one run.
func (c *Client) GetRunStatus(
	ctx context.Context,
	runID string,
) (workflowv1.GetRunStatusResponse, error) {
	var out workflowv1.GetRunStatusResponse
	err := c.call(ctx, workflowv1.OpGetRunStatus, workflowv1.GetRunStatusRequest{RunID: runID}, &out)
	return out, err
}

// ListRuns pages runs newest first.
func (c *Client) ListRuns(
	ctx context.Context,
	req workflowv1.ListRunsRequest,
) (workflowv1.ListRunsResponse, error) {
	var out workflowv1.ListRunsResponse
	err := c.call(ctx, workflowv1.OpListRuns, req, &out)
	return out, err
}

// GetRunCompleteness reads the checkpoints of one run.
func (c *Client) GetRunCompleteness(
	ctx context.Context,
	runID string,
) ([]workflowv1.CompletenessState, error) {
	var out workflowv1.GetRunCompletenessResponse
	err := c.call(ctx, workflowv1.OpGetRunCompleteness,
		workflowv1.GetRunCompletenessRequest{RunID: runID}, &out)
	if err != nil {
		return nil, err
	}
	if out.States == nil {
		out.States = []workflowv1.CompletenessState{}
	}
	return out.States, nil
}

// ListCollectorInstances reads every durable collector instance.
func (c *Client) ListCollectorInstances(ctx context.Context) ([]workflowv1.CollectorInstance, error) {
	var out workflowv1.ListCollectorInstancesResponse
	err := c.call(ctx, workflowv1.OpListCollectorInstances, struct{}{}, &out)
	if err != nil {
		return nil, err
	}
	if out.Instances == nil {
		out.Instances = []workflowv1.CollectorInstance{}
	}
	return out.Instances, nil
}

// QueueDepthSnapshot reads outstanding work per family plus globals for
// backpressure status.
func (c *Client) QueueDepthSnapshot(
	ctx context.Context,
) (workflowv1.QueueDepthSnapshot, error) {
	var out workflowv1.QueueDepthSnapshot
	err := c.call(ctx, workflowv1.OpQueueDepthSnapshot, struct{}{}, &out)
	return out, err
}
