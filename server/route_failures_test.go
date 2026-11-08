// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server_test

import (
	"context"
	"fmt"
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

// routeCase pairs one protocol route with a body its validator accepts.
type routeCase struct {
	op        workflowv1.Operation
	validBody string
	// invalidBody is rejected by the request validator. Empty means the
	// route has no validator and accepts any well-formed JSON.
	invalidBody string
}

func routeCases() []routeCase {
	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	later := time.Now().UTC().Add(time.Hour).Truncate(time.Second).Format(time.RFC3339Nano)
	mutation := fmt.Sprintf(
		`{"work_item_id":"item-1","claim_id":"c-1","fencing_token":1,"owner_id":"o-1","observed_at":%q,"lease_duration_ns":60000000000}`, now)
	badMutation := `{"claim_id":"c-1"}`
	return []routeCase{
		{workflowv1.OpClaimNext, fmt.Sprintf(
			`{"selector":{"collector_kind":"jira","collector_instance_id":"i-1","owner_id":"o-1","claim_id":"c-1"},"observed_at":%q,"lease_ttl_ns":60000000000}`, now), `{}`},
		{workflowv1.OpCheckClaimPacing, fmt.Sprintf(
			`{"collector_kind":"jira","collector_instance_id":"i-1","observed_at":%q}`, now), `{}`},
		{workflowv1.OpHeartbeatClaim, mutation, badMutation},
		{workflowv1.OpCompleteClaim, mutation, badMutation},
		{workflowv1.OpReleaseClaim, mutation, badMutation},
		{workflowv1.OpFailClaimRetryable, mutation, badMutation},
		{workflowv1.OpFailClaimTerminal, mutation, badMutation},
		{workflowv1.OpValidateAndPinClaimForCommit, fmt.Sprintf(
			`{"claim_id":"c-1","fencing_token":1,"scope_id":"scope-1","source_run_id":"sr-1","generation_id":"gen-1","commit_deadline":%q}`, later), `{}`},
		// The commit route is a proxy: its valid body is forwarded, so
		// the invalid body is an over-cap result the satellite refuses
		// itself with zero forwards and zero store calls.
		{workflowv1.OpCommitClaimFacts, fmt.Sprintf(
			`{"work_item_id":"item-1","claim_id":"c-1","fencing_token":1,"owner_id":"o-1","observed_at":%q,"lease_duration_ns":60000000000,`+
				`"idempotency_key":"idem-1","result_json":{"fact_rows":[]},"scope_id":"scope-1","source_run_id":"sr-1","generation_id":"gen-1"}`, now), fmt.Sprintf(
			`{"work_item_id":"item-1","claim_id":"c-1","fencing_token":1,"owner_id":"o-1","observed_at":%q,"lease_duration_ns":60000000000,`+
				`"idempotency_key":"idem-1","result_json":%q,"scope_id":"scope-1","source_run_id":"sr-1","generation_id":"gen-1"}`,
			now, strings.Repeat("x", workflowv1.MaxCommitResultBytes+1))},
		{workflowv1.OpCreateRun, fmt.Sprintf(
			`{"run":{"run_id":"run-1","trigger_kind":"schedule","status":"collection_active","created_at":%q,"updated_at":%q}}`, now, now), `{"run":{}}`},
		{workflowv1.OpEnqueueWorkItems, fmt.Sprintf(
			`{"items":[{"work_item_id":"item-1","run_id":"run-1","collector_kind":"jira","collector_instance_id":"i-1","source_system":"jira","scope_id":"scope-1","acceptance_unit_id":"au-1","source_run_id":"sr-1","generation_id":"gen-1","status":"pending","created_at":%q,"updated_at":%q}]}`, now, now), `{"items":[]}`},
		{workflowv1.OpReconcileCollectorInstances, fmt.Sprintf(
			`{"observed_at":%q,"instances":[{"instance_id":"i-1","collector_kind":"jira","mode":"manual"}]}`, now), `{}`},
		{workflowv1.OpUpsertCompletenessStates, fmt.Sprintf(
			`{"states":[{"run_id":"run-1","collector_kind":"jira","keyspace":"facts","phase_name":"publish","status":"complete","observed_at":%q,"updated_at":%q}]}`, now, now), `{"states":[]}`},
		{workflowv1.OpReconcileWorkflowRuns, fmt.Sprintf(`{"observed_at":%q}`, now), `{}`},
		{workflowv1.OpReapExpiredClaims, fmt.Sprintf(
			`{"observed_at":%q,"limit":10,"requeue_delay_ns":60000000000}`, now), `{"limit":0}`},
		{workflowv1.OpReportRunProgress, fmt.Sprintf(
			`{"run_id":"run-1","observed_at":%q,"progress":[{"collector_kind":"jira"}]}`, now), `{}`},
		{workflowv1.OpGetRunStatus, `{"run_id":"run-1"}`, `{}`},
		{workflowv1.OpListRuns, `{}`, `{"limit":-1}`},
		{workflowv1.OpGetRunCompleteness, `{"run_id":"run-1"}`, `{}`},
		{workflowv1.OpListCollectorInstances, ``, ``},
		{workflowv1.OpQueueDepthSnapshot, ``, ``},
	}
}

func pathFor(t *testing.T, op workflowv1.Operation) string {
	t.Helper()
	route, ok := workflowv1.RouteFor(op)
	if !ok {
		t.Fatalf("no route for %s", op)
	}
	return route.Path
}

func TestRouteCasesCoverTheWholeRouteTable(t *testing.T) {
	covered := map[workflowv1.Operation]bool{}
	for _, rc := range routeCases() {
		covered[rc.op] = true
	}
	for _, route := range workflowv1.RouteTable {
		if !covered[route.Operation] {
			t.Fatalf("route %s has no failure-path case", route.Operation)
		}
	}
}

// TestEveryRouteRejectsMalformedJSON requires an undecodable body to fail
// 400 invalid before any store call, on all 21 routes.
func TestEveryRouteRejectsMalformedJSON(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(t, st)
	defer srv.Close()

	for _, rc := range routeCases() {
		t.Run(string(rc.op), func(t *testing.T) {
			resp := post(t, srv, pathFor(t, rc.op), `{"not json`)
			envelope := decodeEnvelope(t, resp)
			if resp.StatusCode != http.StatusBadRequest || envelope.Code != workflowv1.CodeInvalid {
				t.Fatalf("status %d code %q, want 400 invalid", resp.StatusCode, envelope.Code)
			}
			if !strings.Contains(envelope.Message, "decode") {
				t.Fatalf("message %q does not name the decode failure", envelope.Message)
			}
		})
	}
	if len(st.calls) != 0 {
		t.Fatalf("malformed bodies reached the store: %v", st.calls)
	}
}

// TestEveryValidatedRouteRejectsInvalidRequests requires a well-formed but
// invalid request to fail 400 invalid without a store call.
func TestEveryValidatedRouteRejectsInvalidRequests(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(t, st)
	defer srv.Close()

	for _, rc := range routeCases() {
		if rc.invalidBody == "" {
			continue
		}
		t.Run(string(rc.op), func(t *testing.T) {
			resp := post(t, srv, pathFor(t, rc.op), rc.invalidBody)
			envelope := decodeEnvelope(t, resp)
			if resp.StatusCode != http.StatusBadRequest || envelope.Code != workflowv1.CodeInvalid {
				t.Fatalf("status %d code %q, want 400 invalid", resp.StatusCode, envelope.Code)
			}
		})
	}
	if len(st.calls) != 0 {
		t.Fatalf("invalid requests reached the store: %v", st.calls)
	}
}

// TestEveryRouteSurfacesStoreFailures requires each route to map a store
// failure to its contract status: protocol errors keep their code, other
// errors become a generic 500 that leaks nothing.
func TestEveryRouteSurfacesStoreFailures(t *testing.T) {
	const secret = "pq: password authentication failed for user claimplane"
	for _, rc := range routeCases() {
		// The commit proxy never calls the store, so there is no store
		// failure to map: its failure modes (unset target, dead core)
		// are covered by the dedicated proxy tests instead.
		if rc.op == workflowv1.OpCommitClaimFacts {
			continue
		}
		t.Run(string(rc.op), func(t *testing.T) {
			internal := &fakeStore{claimErr: fmt.Errorf("%s", secret)}
			srv := newTestServer(t, internal)
			defer srv.Close()
			resp := post(t, srv, pathFor(t, rc.op), rc.validBody)
			envelope := decodeEnvelope(t, resp)
			if resp.StatusCode != http.StatusInternalServerError || envelope.Code != "internal" {
				t.Fatalf("internal failure: status %d code %q, want 500 internal", resp.StatusCode, envelope.Code)
			}
			if strings.Contains(envelope.Message, "password") {
				t.Fatalf("envelope leaks store detail: %q", envelope.Message)
			}
			if len(internal.calls) != 1 {
				t.Fatalf("store calls = %v, want exactly one", internal.calls)
			}

			stale := &fakeStore{claimErr: &workflowv1.ProtocolError{Code: workflowv1.CodeStaleFence, Message: "superseded"}}
			staleSrv := newTestServer(t, stale)
			defer staleSrv.Close()
			resp = post(t, staleSrv, pathFor(t, rc.op), rc.validBody)
			envelope = decodeEnvelope(t, resp)
			if resp.StatusCode != http.StatusConflict || envelope.Code != workflowv1.CodeStaleFence {
				t.Fatalf("protocol failure: status %d code %q, want 409 stale_fence", resp.StatusCode, envelope.Code)
			}
		})
	}
}

// TestOversizedBodyIsRejected requires the body cap to turn an oversized
// request into a 400 instead of buffering it.
func TestOversizedBodyIsRejected(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(t, st)
	defer srv.Close()

	padding := strings.Repeat("a", (1<<20)+1024)
	body := fmt.Sprintf(`{"run_id":%q}`, padding)
	resp := post(t, srv, pathFor(t, workflowv1.OpGetRunStatus), body)
	envelope := decodeEnvelope(t, resp)
	if resp.StatusCode != http.StatusBadRequest || envelope.Code != workflowv1.CodeInvalid {
		t.Fatalf("oversized body: status %d code %q, want 400 invalid", resp.StatusCode, envelope.Code)
	}
	if len(st.calls) != 0 {
		t.Fatalf("oversized body reached the store: %v", st.calls)
	}
}

func TestNewRequiresStore(t *testing.T) {
	if _, err := server.New(nil, nil); err == nil {
		t.Fatal("server.New accepted a nil store")
	}
}

// degradedStore answers ClaimNext with a fail-open reason so the server's
// degraded-mode accounting is observable.
type degradedStore struct {
	*fakeStore
	reason string
}

func (d degradedStore) ClaimNext(
	ctx context.Context,
	sel workflowv1.ClaimSelector,
	now time.Time,
	ttl time.Duration,
) (workflowv1.ClaimNextResponse, error) {
	resp, err := d.fakeStore.ClaimNext(ctx, sel, now, ttl)
	resp.DegradedReason = d.reason
	return resp, err
}

// metricValue sums the samples of one metric family whose labels include
// every pair in want.
func metricValue(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) float64 {
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
			if match {
				total += metric.GetCounter().GetValue()
			}
		}
	}
	return total
}

// TestClaimNextRecordsOutcomeAndDegradedTelemetry requires every ClaimNext
// outcome to land on the claim counter and a fail-open answer to land on
// the degraded counter under its reason.
func TestClaimNextRecordsOutcomeAndDegradedTelemetry(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	claimBody := fmt.Sprintf(
		`{"selector":{"collector_kind":"jira","collector_instance_id":"i-1","owner_id":"o-1","claim_id":"c-1"},"observed_at":%q,"lease_ttl_ns":60000000000}`, now)

	cases := []struct {
		name   string
		store  *fakeStore
		reason string
		result string
		signal string
		status int
	}{
		{"issued", &fakeStore{}, "", "issued", "", http.StatusOK},
		{"issued while degraded", &fakeStore{}, "quota_floor", "issued", "", http.StatusOK},
		{"deferred", &fakeStore{paced: true}, "", "deferred", "domain", http.StatusOK},
		{"idle", &fakeStore{idle: true}, "pacing_depths", "idle", "", http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			srv, err := server.New(degradedStore{fakeStore: tc.store, reason: tc.reason}, nil)
			if err != nil {
				t.Fatalf("server.New: %v", err)
			}
			srv.Instruments = telemetry.NewInstruments(reg)
			httpSrv := httptest.NewServer(srv.Handler())
			defer httpSrv.Close()

			resp := post(t, httpSrv, pathFor(t, workflowv1.OpClaimNext), claimBody)
			resp.Body.Close()
			if resp.StatusCode != tc.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.status)
			}
			got := metricValue(t, reg, "workflow_claim_grants_total",
				map[string]string{"result": tc.result, "signal": tc.signal})
			if got != 1 {
				t.Fatalf("claim counter {%s,%s} = %v, want 1", tc.result, tc.signal, got)
			}
			degraded := metricValue(t, reg, "workflow_claim_degraded_total", nil)
			wantDegraded := 0.0
			if tc.reason != "" {
				wantDegraded = 1
				if byReason := metricValue(t, reg, "workflow_claim_degraded_total",
					map[string]string{"reason": tc.reason}); byReason != 1 {
					t.Fatalf("degraded counter for %q = %v, want 1", tc.reason, byReason)
				}
			}
			if degraded != wantDegraded {
				t.Fatalf("degraded counter total = %v, want %v", degraded, wantDegraded)
			}
		})
	}
}

// TestPinRecordsOutcomeTelemetry requires the commit-pin counter to
// separate a pinned grant from a failed pin by protocol code.
func TestPinRecordsOutcomeTelemetry(t *testing.T) {
	body := routeCases()[7].validBody
	if routeCases()[7].op != workflowv1.OpValidateAndPinClaimForCommit {
		t.Fatal("route case order changed; pin case is no longer index 7")
	}

	pinned := &fakeStore{pinResp: workflowv1.CommitPinResponse{Pinned: true, CommitID: "commit-1"}}
	reg := prometheus.NewRegistry()
	srv, err := server.New(pinned, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.Instruments = telemetry.NewInstruments(reg)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()
	resp := post(t, httpSrv, pathFor(t, workflowv1.OpValidateAndPinClaimForCommit), body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pin status = %d, want 200", resp.StatusCode)
	}
	if got := metricValue(t, reg, "workflow_commit_pins_total", map[string]string{"result": "pinned"}); got != 1 {
		t.Fatalf("pinned counter = %v, want 1", got)
	}

	stale := &fakeStore{claimErr: &workflowv1.ProtocolError{Code: workflowv1.CodeStaleFence, Message: "superseded"}}
	reg2 := prometheus.NewRegistry()
	srv2, err := server.New(stale, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv2.Instruments = telemetry.NewInstruments(reg2)
	httpSrv2 := httptest.NewServer(srv2.Handler())
	defer httpSrv2.Close()
	resp = post(t, httpSrv2, pathFor(t, workflowv1.OpValidateAndPinClaimForCommit), body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale pin status = %d, want 409", resp.StatusCode)
	}
	if got := metricValue(t, reg2, "workflow_commit_pins_total", map[string]string{"result": workflowv1.CodeStaleFence}); got != 1 {
		t.Fatalf("stale pin counter = %v, want 1", got)
	}
	if got := metricValue(t, reg2, "workflow_commit_pins_total", map[string]string{"result": "pinned"}); got != 0 {
		t.Fatalf("stale pin counted as pinned: %v", got)
	}
}
