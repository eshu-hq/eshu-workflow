// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// requestTimeout bounds one protocol call: the coordinator ticks past a
// hung server instead of wedging its reconcile loop on it.
const requestTimeout = 30 * time.Second

// Client drives one claim-plane service over HTTP. The zero value is
// unusable; build one with New.
type Client struct {
	base   string
	http   *http.Client
	header string
}

// New builds a client for the service at baseURL. An empty or malformed
// address fails fast so the coordinator never posts claims at an
// accidental target.
func New(baseURL string) (*Client, error) {
	trimmed := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if trimmed == "" {
		return nil, fmt.Errorf("claim-plane base URL is required")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid claim-plane base URL %q", baseURL)
	}
	return &Client{
		base:   trimmed,
		http:   &http.Client{Timeout: requestTimeout},
		header: "1",
	}, nil
}

// call posts one operation body and decodes the success value. Wire error
// envelopes surface as *workflowv1.ProtocolError so callers switch on the
// code: a stale fence re-claims, never retries the same mutation.
func (c *Client) call(ctx context.Context, op workflowv1.Operation, body, out any) error {
	route, ok := workflowv1.RouteFor(op)
	if !ok {
		return fmt.Errorf("operation %q has no protocol route", op)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode %s request: %w", op, err)
	}
	request, err := http.NewRequestWithContext(ctx, route.Method, c.base+route.Path, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("build %s request: %w", op, err)
	}
	request.Header.Set(workflowv1.VersionHeader, c.header)
	request.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(request)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var envelope workflowv1.ErrorEnvelope
		if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
			return fmt.Errorf("%s: status %d", op, resp.StatusCode)
		}
		return &workflowv1.ProtocolError{
			Code:              envelope.Code,
			Message:           envelope.Message,
			RetryAfterSeconds: envelope.RetryAfterSeconds,
		}
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode %s response: %w", op, err)
	}
	return nil
}

// mutation posts one fenced claim mutation and expects the acknowledgement.
func (c *Client) mutation(ctx context.Context, op workflowv1.Operation, body workflowv1.ClaimMutation) error {
	if err := body.Validate(); err != nil {
		return err
	}
	var out struct {
		OK bool `json:"ok"`
	}
	return c.call(ctx, op, body, &out)
}

// ClaimNext asks the server for the next eligible work item.
func (c *Client) ClaimNext(
	ctx context.Context,
	selector workflowv1.ClaimSelector,
	observedAt time.Time,
	leaseTTL time.Duration,
) (workflowv1.ClaimNextResponse, error) {
	var out workflowv1.ClaimNextResponse
	err := c.call(ctx, workflowv1.OpClaimNext, workflowv1.ClaimNextRequest{
		Selector: selector, ObservedAt: observedAt, LeaseTTL: leaseTTL,
	}, &out)
	return out, err
}

// CheckClaimPacing probes the pacing latch without claiming.
func (c *Client) CheckClaimPacing(
	ctx context.Context,
	kind workflowv1.CollectorKind,
	instanceID string,
	observedAt time.Time,
) (workflowv1.ClaimPacingDecision, error) {
	var out workflowv1.ClaimPacingDecision
	err := c.call(ctx, workflowv1.OpCheckClaimPacing, workflowv1.CheckClaimPacingRequest{
		CollectorKind: kind, CollectorInstanceID: instanceID, ObservedAt: observedAt,
	}, &out)
	return out, err
}

// Heartbeat renews one claim epoch.
func (c *Client) Heartbeat(ctx context.Context, mutation workflowv1.ClaimMutation) error {
	return c.mutation(ctx, workflowv1.OpHeartbeatClaim, mutation)
}

// Complete finishes one claim epoch.
func (c *Client) Complete(ctx context.Context, mutation workflowv1.ClaimMutation) error {
	return c.mutation(ctx, workflowv1.OpCompleteClaim, mutation)
}

// Release returns one claim epoch to the queue.
func (c *Client) Release(ctx context.Context, mutation workflowv1.ClaimMutation) error {
	return c.mutation(ctx, workflowv1.OpReleaseClaim, mutation)
}

// FailRetryable records a retryable failure on one claim epoch.
func (c *Client) FailRetryable(ctx context.Context, mutation workflowv1.ClaimMutation) error {
	return c.mutation(ctx, workflowv1.OpFailClaimRetryable, mutation)
}

// FailTerminal records a terminal failure on one claim epoch: the work
// item dead-letters instead of returning to the queue.
func (c *Client) FailTerminal(ctx context.Context, mutation workflowv1.ClaimMutation) error {
	return c.mutation(ctx, workflowv1.OpFailClaimTerminal, mutation)
}

// PinForCommit pins one claim for a fact commit so the reaper cannot
// expire it before the commit deadline.
func (c *Client) PinForCommit(
	ctx context.Context,
	req workflowv1.CommitPinRequest,
) (workflowv1.CommitPinResponse, error) {
	var out workflowv1.CommitPinResponse
	err := c.call(ctx, workflowv1.OpValidateAndPinClaimForCommit, req, &out)
	return out, err
}
