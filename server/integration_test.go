// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/server"
	claimstore "github.com/eshu-hq/eshu-workflow/store"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// shouldRequireDB reports whether a missing test database must fail the
// end-to-end test instead of skipping it. CI sets CI=true and operators
// can force the same with CLAIMPLANE_REQUIRE_DB=1, so a broken database
// service can never pass vacuously.
func shouldRequireDB() bool {
	return os.Getenv("CI") == "true" || os.Getenv("CLAIMPLANE_REQUIRE_DB") == "1"
}

func TestShouldRequireDB(t *testing.T) {
	t.Setenv("CI", "true")
	t.Setenv("CLAIMPLANE_REQUIRE_DB", "")
	if !shouldRequireDB() {
		t.Fatalf("CI=true must require the test database")
	}
	t.Setenv("CI", "")
	t.Setenv("CLAIMPLANE_REQUIRE_DB", "1")
	if !shouldRequireDB() {
		t.Fatalf("CLAIMPLANE_REQUIRE_DB=1 must require the test database")
	}
	t.Setenv("CI", "")
	t.Setenv("CLAIMPLANE_REQUIRE_DB", "")
	if shouldRequireDB() {
		t.Fatalf("unset CI and CLAIMPLANE_REQUIRE_DB must allow the skip")
	}
}

func liveStore(t *testing.T) (*claimstore.Store, string) {
	t.Helper()
	dsn := os.Getenv("CLAIMPLANE_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://claimplane_test:claimplane_test@localhost:5432/claimplane_test_server?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	st, err := claimstore.Open(ctx, dsn)
	if err != nil {
		if shouldRequireDB() {
			t.Fatalf("test database unavailable and CI requires it: %v", err)
		}
		t.Skipf("no test database (set CLAIMPLANE_REQUIRE_DB=1 to require it): %v", err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := st.TruncateForTest(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return st, dsn
}

func call(t *testing.T, srv *httptest.Server, path string, reqBody, respBody any) int {
	t.Helper()
	var body []byte
	if reqBody != nil {
		var err error
		body, err = json.Marshal(reqBody)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
	}
	httpReq, err := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	httpReq.Header.Set(workflowv1.VersionHeader, "1")
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		t.Fatalf("do %s: %v", path, err)
	}
	defer resp.Body.Close()
	if respBody != nil && resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
	}
	return resp.StatusCode
}

// TestEndToEndClaimLifecycleOverHTTP walks one work item through the live
// server: enqueue, claim, heartbeat, pin, complete, and the read plane.
func TestEndToEndClaimLifecycleOverHTTP(t *testing.T) {
	st, _ := liveStore(t)
	ctx := context.Background()
	srv, err := server.New(st, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	now := time.Now().UTC().Truncate(time.Millisecond)
	runID := fmt.Sprintf("e2e-%d", now.UnixNano())
	if err := st.CreateRun(ctx, workflowv1.Run{
		RunID: runID, TriggerKind: workflowv1.TriggerKindSchedule,
		Status:    workflowv1.RunStatusCollectionActive,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.Enqueue(ctx, []workflowv1.WorkItem{{
		WorkItemID: "e2e-item", RunID: runID,
		CollectorKind: "jira", CollectorInstanceID: "jira-primary",
		SourceSystem: "jira", ScopeID: "scope-e2e",
		TenantID: "tenant-e2e", WorkspaceID: "workspace-e2e",
		SubjectClass: "project", PolicyRevisionHash: "policy-e2e",
		AcceptanceUnitID: "au-e2e", SourceRunID: "sr-e2e", GenerationID: "gen-e2e",
		Status:    workflowv1.WorkItemStatusPending,
		CreatedAt: now, UpdatedAt: now,
	}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var claimed workflowv1.ClaimNextResponse
	if status := call(t, httpSrv, "/workflow.v1/ClaimNext", workflowv1.ClaimNextRequest{
		Selector: workflowv1.ClaimSelector{
			CollectorKind: "jira", CollectorInstanceID: "jira-primary",
			OwnerID: "owner-e2e", ClaimID: "claim-e2e",
		},
		ObservedAt: now, LeaseTTL: time.Minute,
	}, &claimed); status != http.StatusOK || !claimed.Issued {
		t.Fatalf("claim status = %d issued = %v", status, claimed.Issued)
	}
	mutation := workflowv1.ClaimMutation{
		WorkItemID: "e2e-item", ClaimID: "claim-e2e",
		FencingToken: claimed.Grant.Claim.FencingToken,
		OwnerID:      "owner-e2e",
		ObservedAt:   now, LeaseDuration: time.Minute,
	}
	var hbAck map[string]any
	if status := call(t, httpSrv, "/workflow.v1/HeartbeatClaim", mutation, &hbAck); status != http.StatusOK {
		t.Fatalf("heartbeat status = %d", status)
	}

	var pin workflowv1.CommitPinResponse
	if status := call(t, httpSrv, "/workflow.v1/ValidateAndPinClaimForCommit",
		workflowv1.CommitPinRequest{
			ClaimID: "claim-e2e", FencingToken: claimed.Grant.Claim.FencingToken,
			ScopeID: "scope-e2e", SourceRunID: "sr-e2e", GenerationID: "gen-e2e",
			CommitDeadline: now.Add(time.Hour),
		}, &pin); status != http.StatusOK || !pin.Pinned || pin.CommitID == "" {
		t.Fatalf("pin status = %d resp = %+v", status, pin)
	}

	var doneAck map[string]any
	if status := call(t, httpSrv, "/workflow.v1/CompleteClaim", mutation, &doneAck); status != http.StatusOK {
		t.Fatalf("complete status = %d", status)
	}

	progress := workflowv1.ReportRunProgressRequest{
		RunID: runID, ObservedAt: now,
		Progress: []workflowv1.CollectorRunProgress{{
			CollectorKind: "jira", TotalWorkItems: 1, CompletedWorkItems: 1,
			FactlessCompletedItems:   1,
			PublishedPhaseCounts:     map[string]int{},
			TerminalDeadLetterCounts: map[string]int{},
		}},
	}
	var progressAck map[string]any
	if status := call(t, httpSrv, "/workflow.v1/ReportRunProgress", progress, &progressAck); status != http.StatusOK {
		t.Fatalf("report status = %d", status)
	}

	var statusResp workflowv1.GetRunStatusResponse
	if status := call(t, httpSrv, "/workflow.v1/GetRunStatus",
		workflowv1.GetRunStatusRequest{RunID: runID}, &statusResp); status != http.StatusOK {
		t.Fatalf("run status = %d", status)
	}
	if len(statusResp.Progress) != 1 || statusResp.Progress[0].CompletedWorkItems != 1 {
		t.Fatalf("run status progress = %+v", statusResp.Progress)
	}

	var depth workflowv1.QueueDepthSnapshot
	if status := call(t, httpSrv, "/workflow.v1/QueueDepthSnapshot", nil, &depth); status != http.StatusOK {
		t.Fatalf("depth status = %d", status)
	}
	if depth.GlobalPending != 0 || depth.GlobalClaimed != 0 {
		t.Fatalf("depth = %+v, want drained", depth)
	}

	var runs workflowv1.ListRunsResponse
	if status := call(t, httpSrv, "/workflow.v1/ListRuns",
		workflowv1.ListRunsRequest{}, &runs); status != http.StatusOK || len(runs.Runs) == 0 {
		t.Fatalf("list runs status = %d runs = %+v", status, runs)
	}

	var pacing workflowv1.ClaimPacingDecision
	if status := call(t, httpSrv, "/workflow.v1/CheckClaimPacing",
		workflowv1.CheckClaimPacingRequest{
			CollectorKind: "jira", CollectorInstanceID: "jira-primary", ObservedAt: now,
		}, &pacing); status != http.StatusOK || pacing.Paced {
		t.Fatalf("pacing status = %d decision = %+v", status, pacing)
	}
}
