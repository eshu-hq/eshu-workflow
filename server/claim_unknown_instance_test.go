// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	"github.com/eshu-hq/eshu-workflow/server"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// ghostInstanceID names a collector instance the plane never declared. It
// passes selector validation, so the claim path itself must answer idle.
const ghostInstanceID = "ghost-instance"

// seedGhostStack opens a live server whose database holds one pending item
// for the ghost instance and no instance row. Callers that need the log
// buffer or registry pass non-nil out-parameters.
func seedGhostStack(
	t *testing.T,
	logger *slog.Logger,
	reg *prometheus.Registry,
) (*httptest.Server, time.Time) {
	t.Helper()
	st, _ := liveStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	runID := fmt.Sprintf("ghost-%d", now.UnixNano())
	if err := st.CreateRun(ctx, workflowv1.Run{
		RunID: runID, TriggerKind: workflowv1.TriggerKindSchedule,
		Status: workflowv1.RunStatusCollectionActive, CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{{
		WorkItemID: "ghost-item", RunID: runID,
		CollectorKind: "jira", CollectorInstanceID: ghostInstanceID,
		SourceSystem: "jira", ScopeID: "scope-ghost",
		AcceptanceUnitID: "au-ghost", SourceRunID: "sr-ghost", GenerationID: "gen-ghost",
		Status:    workflowv1.WorkItemStatusPending,
		CreatedAt: now, UpdatedAt: now,
	}}); err != nil {
		t.Fatalf("enqueue ghost item: %v", err)
	}
	srv, err := server.New(st, logger)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	if reg != nil {
		srv.Instruments = telemetry.NewInstruments(reg)
	}
	return httptest.NewServer(srv.Handler()), now
}

// ghostClaimBody marshals a ClaimNext request for the ghost instance.
func ghostClaimBody(t *testing.T, now time.Time, mutate func(*workflowv1.ClaimNextRequest)) string {
	t.Helper()
	body := workflowv1.ClaimNextRequest{
		Selector: workflowv1.ClaimSelector{
			CollectorKind: "jira", CollectorInstanceID: ghostInstanceID,
			OwnerID: "owner-ghost", ClaimID: "claim-ghost",
		},
		ObservedAt: now, LeaseTTL: time.Minute,
	}
	if mutate != nil {
		mutate(&body)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(raw)
}

// TestUnknownInstanceClaimIsNotFoundOverHTTP requires a ClaimNext for a
// valid selector naming an undeclared instance to answer 404 not_found
// with a decodable envelope, even when a stranded item waits for that
// instance. Before the fix the store issued the ghost row, so the handler
// answered 200 instead of the idle 404.
func TestUnknownInstanceClaimIsNotFoundOverHTTP(t *testing.T) {
	httpSrv, now := seedGhostStack(t, nil, nil)
	defer httpSrv.Close()

	resp := post(t, httpSrv, "/workflow.v1/ClaimNext", ghostClaimBody(t, now, nil))
	envelope := decodeEnvelope(t, resp)
	if resp.StatusCode != http.StatusNotFound || envelope.Code != workflowv1.CodeNotFound {
		t.Fatalf("ghost claim: status %d code %q, want 404 not_found",
			resp.StatusCode, envelope.Code)
	}
	if envelope.Message != "no eligible work" {
		t.Fatalf("ghost claim message = %q, want the idle answer", envelope.Message)
	}
}

// counterValue sums one counter family whose labels include every pair in
// want.
func counterValue(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var total float64
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			match := true
			for k, v := range want {
				if labels[k] != v {
					match = false
				}
			}
			if match && metric.GetCounter() != nil {
				total += metric.GetCounter().GetValue()
			}
		}
	}
	return total
}

// TestUnknownInstanceIdleLogsAndCounts requires every unknown-instance idle
// to emit the claim-next-idle INFO log naming the instance id and to
// increment the idle claim metric. Before the fix the ghost claim issued,
// so no idle log fired and the idle counter stayed at zero.
func TestUnknownInstanceIdleLogsAndCounts(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	reg := prometheus.NewRegistry()
	httpSrv, now := seedGhostStack(t, logger, reg)
	defer httpSrv.Close()

	resp := post(t, httpSrv, "/workflow.v1/ClaimNext", ghostClaimBody(t, now, nil))
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("ghost claim status = %d, want 404", resp.StatusCode)
	}
	if got := counterValue(t, reg, "workflow_claim_grants_total",
		map[string]string{"result": "idle"}); got != 1 {
		t.Fatalf("idle claim counter = %v, want 1", got)
	}
	logged := logs.String()
	if !strings.Contains(logged, "claim next idle") ||
		!strings.Contains(logged, ghostInstanceID) {
		t.Fatalf("idle log = %q, want the claim-next-idle line naming %q", logged, ghostInstanceID)
	}
}

// TestClaimNextStoreFailureStaysInternal requires an infrastructure failure
// behind ClaimNext to stay a 500 with a generic wire body, even for an
// unknown instance: a store the plane cannot read is never idle.
func TestClaimNextStoreFailureStaysInternal(t *testing.T) {
	st, _ := liveStore(t)
	st.Close()
	srv, err := server.New(st, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	resp := post(t, httpSrv, "/workflow.v1/ClaimNext",
		ghostClaimBody(t, time.Now().UTC(), nil))
	envelope := decodeEnvelope(t, resp)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("broken-store claim status = %d, want 500", resp.StatusCode)
	}
	if envelope.Code != "internal" || envelope.Message != "internal error" {
		t.Fatalf("broken-store envelope = %+v, want the generic internal body", envelope)
	}
}

// TestClaimNextInvalidSelectorStaysInvalid requires syntactically invalid
// selectors to stay 400 invalid without reaching the store, pinning the
// validation boundary the unknown-instance idle path sits behind.
func TestClaimNextInvalidSelectorStaysInvalid(t *testing.T) {
	st := &fakeStore{idle: true}
	srv := newTestServer(t, st)
	defer srv.Close()

	now := time.Now().UTC()
	cases := map[string]func(*workflowv1.ClaimNextRequest){
		"blank instance": func(r *workflowv1.ClaimNextRequest) { r.Selector.CollectorInstanceID = "" },
		"blank kind":     func(r *workflowv1.ClaimNextRequest) { r.Selector.CollectorKind = "" },
		"blank owner":    func(r *workflowv1.ClaimNextRequest) { r.Selector.OwnerID = "" },
		"blank claim":    func(r *workflowv1.ClaimNextRequest) { r.Selector.ClaimID = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			resp := post(t, srv, "/workflow.v1/ClaimNext", ghostClaimBody(t, now, mutate))
			envelope := decodeEnvelope(t, resp)
			if resp.StatusCode != http.StatusBadRequest || envelope.Code != workflowv1.CodeInvalid {
				t.Fatalf("invalid selector: status %d code %q, want 400 invalid",
					resp.StatusCode, envelope.Code)
			}
		})
	}
	if len(st.calls) != 0 {
		t.Fatalf("invalid selectors reached the store: %v", st.calls)
	}
}
