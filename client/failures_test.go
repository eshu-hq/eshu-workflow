// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package client_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/client"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

func okMutation() workflowv1.ClaimMutation {
	return workflowv1.ClaimMutation{
		WorkItemID: "item-1", ClaimID: "c-1", FencingToken: 1, OwnerID: "o-1",
		ObservedAt: time.Now().UTC(), LeaseDuration: time.Minute,
	}
}

// clientCall is one client method invoked with arguments its local
// validation accepts.
type clientCall struct {
	name string
	run  func(context.Context, *client.Client) error
	// invalid invokes the same method with arguments local validation
	// must reject before any network call. Nil means the method has no
	// local validation.
	invalid func(context.Context, *client.Client) error
}

func clientCalls() []clientCall {
	now := time.Now().UTC().Truncate(time.Second)
	run := workflowv1.Run{
		RunID: "run-1", TriggerKind: workflowv1.TriggerKindSchedule,
		Status: workflowv1.RunStatusCollectionActive, CreatedAt: now, UpdatedAt: now,
	}
	item := workflowv1.WorkItem{
		WorkItemID: "item-1", RunID: "run-1", CollectorKind: "jira", CollectorInstanceID: "i-1",
		SourceSystem: "jira", ScopeID: "s", AcceptanceUnitID: "au", SourceRunID: "sr",
		GenerationID: "g", Status: workflowv1.WorkItemStatusPending, CreatedAt: now, UpdatedAt: now,
	}
	selector := workflowv1.ClaimSelector{
		CollectorKind: "jira", CollectorInstanceID: "i-1", OwnerID: "o-1", ClaimID: "c-1",
	}
	badMutation := workflowv1.ClaimMutation{}
	mutate := func(name string, fn func(*client.Client, context.Context, workflowv1.ClaimMutation) error) clientCall {
		return clientCall{
			name: name,
			run:  func(ctx context.Context, c *client.Client) error { return fn(c, ctx, okMutation()) },
			invalid: func(ctx context.Context, c *client.Client) error {
				return fn(c, ctx, badMutation)
			},
		}
	}
	return []clientCall{
		{name: "ClaimNext", run: func(ctx context.Context, c *client.Client) error {
			_, err := c.ClaimNext(ctx, selector, now, time.Minute)
			return err
		}},
		{name: "CheckClaimPacing", run: func(ctx context.Context, c *client.Client) error {
			_, err := c.CheckClaimPacing(ctx, "jira", "i-1", now)
			return err
		}},
		mutate("Heartbeat", (*client.Client).Heartbeat),
		mutate("Complete", (*client.Client).Complete),
		mutate("Release", (*client.Client).Release),
		mutate("FailRetryable", (*client.Client).FailRetryable),
		mutate("FailTerminal", (*client.Client).FailTerminal),
		{name: "PinForCommit", run: func(ctx context.Context, c *client.Client) error {
			_, err := c.PinForCommit(ctx, workflowv1.CommitPinRequest{
				ClaimID: "c-1", FencingToken: 1, ScopeID: "s", SourceRunID: "sr",
				GenerationID: "g", CommitDeadline: now.Add(time.Hour),
			})
			return err
		}},
		{name: "CreateRun",
			run: func(ctx context.Context, c *client.Client) error { return c.CreateRun(ctx, run) },
			invalid: func(ctx context.Context, c *client.Client) error {
				return c.CreateRun(ctx, workflowv1.Run{})
			}},
		{name: "EnqueueWorkItems",
			run: func(ctx context.Context, c *client.Client) error {
				_, err := c.EnqueueWorkItems(ctx, []workflowv1.WorkItem{item})
				return err
			},
			invalid: func(ctx context.Context, c *client.Client) error {
				_, err := c.EnqueueWorkItems(ctx, nil)
				return err
			}},
		{name: "ReconcileCollectorInstances",
			run: func(ctx context.Context, c *client.Client) error {
				return c.ReconcileCollectorInstances(ctx, now, nil)
			},
			invalid: func(ctx context.Context, c *client.Client) error {
				return c.ReconcileCollectorInstances(ctx, time.Time{}, nil)
			}},
		{name: "UpsertCompletenessStates",
			run: func(ctx context.Context, c *client.Client) error {
				return c.UpsertCompletenessStates(ctx, []workflowv1.CompletenessState{{
					RunID: "run-1", CollectorKind: "jira", Keyspace: "k", PhaseName: "p",
					Status: "complete", ObservedAt: now, UpdatedAt: now,
				}})
			},
			invalid: func(ctx context.Context, c *client.Client) error {
				return c.UpsertCompletenessStates(ctx, nil)
			}},
		{name: "ReconcileWorkflowRuns",
			run: func(ctx context.Context, c *client.Client) error {
				_, err := c.ReconcileWorkflowRuns(ctx, now)
				return err
			},
			invalid: func(ctx context.Context, c *client.Client) error {
				_, err := c.ReconcileWorkflowRuns(ctx, time.Time{})
				return err
			}},
		{name: "ReapExpiredClaims",
			run: func(ctx context.Context, c *client.Client) error {
				_, err := c.ReapExpiredClaims(ctx, now, 10, time.Second)
				return err
			},
			invalid: func(ctx context.Context, c *client.Client) error {
				_, err := c.ReapExpiredClaims(ctx, now, 0, time.Second)
				return err
			}},
		{name: "ReportRunProgress",
			run: func(ctx context.Context, c *client.Client) error {
				return c.ReportRunProgress(ctx, workflowv1.ReportRunProgressRequest{
					RunID: "run-1", ObservedAt: now,
					Progress: []workflowv1.CollectorRunProgress{{CollectorKind: "jira"}},
				})
			},
			invalid: func(ctx context.Context, c *client.Client) error {
				return c.ReportRunProgress(ctx, workflowv1.ReportRunProgressRequest{})
			}},
		{name: "GetRunStatus", run: func(ctx context.Context, c *client.Client) error {
			_, err := c.GetRunStatus(ctx, "run-1")
			return err
		}},
		{name: "ListRuns", run: func(ctx context.Context, c *client.Client) error {
			_, err := c.ListRuns(ctx, workflowv1.ListRunsRequest{})
			return err
		}},
		{name: "GetRunCompleteness", run: func(ctx context.Context, c *client.Client) error {
			_, err := c.GetRunCompleteness(ctx, "run-1")
			return err
		}},
		{name: "ListCollectorInstances", run: func(ctx context.Context, c *client.Client) error {
			_, err := c.ListCollectorInstances(ctx)
			return err
		}},
		{name: "QueueDepthSnapshot", run: func(ctx context.Context, c *client.Client) error {
			_, err := c.QueueDepthSnapshot(ctx)
			return err
		}},
	}
}

func newClientFor(t *testing.T, handler http.HandlerFunc) (*client.Client, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	plane, err := client.New(srv.URL)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	return plane, &hits
}

// TestEveryMethodSurfacesProtocolErrors requires each client method to
// hand back the server's error code and retry hint intact, so callers can
// switch on stale_fence versus not_found.
func TestEveryMethodSurfacesProtocolErrors(t *testing.T) {
	plane, _ := newClientFor(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"code":"stale_fence","message":"superseded","retry_after_seconds":9}`))
	})
	for _, tc := range clientCalls() {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run(t.Context(), plane)
			var proto *workflowv1.ProtocolError
			if !errors.As(err, &proto) {
				t.Fatalf("error = %v (%T), want *ProtocolError", err, err)
			}
			if proto.Code != workflowv1.CodeStaleFence || proto.Message != "superseded" || proto.RetryAfterSeconds != 9 {
				t.Fatalf("protocol error = %+v, want stale_fence/superseded/9", proto)
			}
		})
	}
}

// TestEveryMethodReportsNonProtocolFailures requires an HTML 502 from a
// proxy and a truncated success body to fail with the operation name
// instead of decoding into a zero value.
func TestEveryMethodReportsNonProtocolFailures(t *testing.T) {
	t.Run("non-JSON error body", func(t *testing.T) {
		plane, _ := newClientFor(t, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("<html>bad gateway</html>"))
		})
		for _, tc := range clientCalls() {
			err := tc.run(t.Context(), plane)
			if err == nil || !strings.Contains(err.Error(), "status 502") {
				t.Fatalf("%s: error = %v, want it to carry the status", tc.name, err)
			}
			var proto *workflowv1.ProtocolError
			if errors.As(err, &proto) {
				t.Fatalf("%s: a proxy error was reported as a protocol error: %v", tc.name, err)
			}
		}
	})
	t.Run("truncated success body", func(t *testing.T) {
		plane, _ := newClientFor(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"ok":`))
		})
		decoding := 0
		for _, tc := range clientCalls() {
			err := tc.run(t.Context(), plane)
			if err == nil {
				t.Fatalf("%s: truncated body accepted", tc.name)
			}
			if strings.Contains(err.Error(), "decode") {
				decoding++
			}
		}
		if decoding != len(clientCalls()) {
			t.Fatalf("%d of %d methods named the decode failure", decoding, len(clientCalls()))
		}
	})
}

// TestInvalidArgumentsNeverReachTheNetwork requires local validation to
// reject a bad request before a single byte is sent.
func TestInvalidArgumentsNeverReachTheNetwork(t *testing.T) {
	plane, hits := newClientFor(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	})
	checked := 0
	for _, tc := range clientCalls() {
		if tc.invalid == nil {
			continue
		}
		checked++
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.invalid(t.Context(), plane); err == nil {
				t.Fatal("invalid arguments accepted")
			}
		})
	}
	if checked < 12 {
		t.Fatalf("only %d methods exercised local validation", checked)
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("invalid requests produced %d network calls", got)
	}
}

// TestUnreachableServerNamesTheOperation requires a dead server to fail
// with the route and dial cause in the message, and a canceled context to stop the
// call.
func TestUnreachableServerNamesTheOperation(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	plane, err := client.New(srv.URL)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	srv.Close()
	for _, tc := range clientCalls() {
		err := tc.run(t.Context(), plane)
		if err == nil || !strings.Contains(err.Error(), "/workflow.v1/") ||
			!strings.Contains(err.Error(), "connection refused") {
			t.Fatalf("%s: error = %v, want the route and the dial failure", tc.name, err)
		}
	}

	live, _ := newClientFor(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := live.QueueDepthSnapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v, want context.Canceled", err)
	}
}

// TestSuccessBodiesNormalizeEmptyCollections requires empty server
// answers to decode to non-nil slices so callers can range and marshal
// without nil checks, and counts to come back as the server stated them.
func TestSuccessBodiesNormalizeEmptyCollections(t *testing.T) {
	plane, _ := newClientFor(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"claims":null,"states":null,"instances":null,"inserted_work_items":4,"accepted":4,"transitioned":2}`))
	})
	claims, err := plane.ReapExpiredClaims(t.Context(), time.Now(), 5, 0)
	if err != nil || claims == nil || len(claims) != 0 {
		t.Fatalf("reap = %#v, %v, want an empty non-nil slice", claims, err)
	}
	states, err := plane.GetRunCompleteness(t.Context(), "run-1")
	if err != nil || states == nil || len(states) != 0 {
		t.Fatalf("completeness = %#v, %v, want an empty non-nil slice", states, err)
	}
	instances, err := plane.ListCollectorInstances(t.Context())
	if err != nil || instances == nil || len(instances) != 0 {
		t.Fatalf("instances = %#v, %v, want an empty non-nil slice", instances, err)
	}
	transitioned, err := plane.ReconcileWorkflowRuns(t.Context(), time.Now())
	if err != nil || transitioned != 2 {
		t.Fatalf("transitioned = %d, %v, want 2", transitioned, err)
	}
}
