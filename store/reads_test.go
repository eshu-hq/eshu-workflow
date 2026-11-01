// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/store"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

func createRunAt(t *testing.T, st *claimstore.Store, ctx context.Context, id string, status workflowv1.RunStatus, updated time.Time) {
	t.Helper()
	if err := st.CreateRun(ctx, workflowv1.Run{
		RunID: id, TriggerKind: workflowv1.TriggerKindSchedule, Status: status,
		CreatedAt: updated.Add(-time.Hour), UpdatedAt: updated,
	}); err != nil {
		t.Fatalf("create run %s: %v", id, err)
	}
}

// TestListRunsPagesNewestFirstWithStatusFilter requires the cursor to walk
// every run exactly once, newest first, and the status filter to narrow
// the page set.
func TestListRunsPagesNewestFirstWithStatusFilter(t *testing.T) {
	st, ctx := openFreshStore(t)
	base := time.Now().UTC().Truncate(time.Millisecond)
	for i := 0; i < 5; i++ {
		status := workflowv1.RunStatusCollectionActive
		if i%2 == 1 {
			status = workflowv1.RunStatusComplete
		}
		createRunAt(t, st, ctx, fmt.Sprintf("page-run-%d", i), status, base.Add(time.Duration(i)*time.Minute))
	}

	var seen []string
	cursor := ""
	for page := 0; page < 10; page++ {
		resp, err := st.ListRuns(ctx, workflowv1.ListRunsRequest{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatalf("list page %d: %v", page, err)
		}
		for _, run := range resp.Runs {
			seen = append(seen, run.RunID)
		}
		if resp.NextCursor == "" {
			break
		}
		cursor = resp.NextCursor
	}
	want := []string{"page-run-4", "page-run-3", "page-run-2", "page-run-1", "page-run-0"}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("paged runs = %v, want %v", seen, want)
	}

	complete, err := st.ListRuns(ctx, workflowv1.ListRunsRequest{Status: workflowv1.RunStatusComplete})
	if err != nil {
		t.Fatalf("list complete: %v", err)
	}
	if len(complete.Runs) != 2 || complete.NextCursor != "" {
		t.Fatalf("complete runs = %+v, want exactly the two complete runs and no next cursor", complete)
	}
	for _, run := range complete.Runs {
		if run.Status != workflowv1.RunStatusComplete {
			t.Fatalf("status filter leaked %s", run.Status)
		}
	}

	empty, err := st.ListRuns(ctx, workflowv1.ListRunsRequest{Status: workflowv1.RunStatusFailed})
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if empty.Runs == nil || len(empty.Runs) != 0 {
		t.Fatalf("no-match list = %#v, want an empty non-nil slice", empty.Runs)
	}

	if _, err := st.ListRuns(ctx, workflowv1.ListRunsRequest{Limit: -1}); !isCode(err, workflowv1.CodeInvalid) {
		t.Fatalf("negative limit error = %v, want invalid", err)
	}
}

// TestRunRoundTripsOptionalFields requires optional run fields to survive
// a write and read, and an unparsable scope set to be stored as an empty
// set rather than rejecting the run.
func TestRunRoundTripsOptionalFields(t *testing.T) {
	st, ctx := openFreshStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)

	full := workflowv1.Run{
		RunID: "opt-run-1", TriggerKind: workflowv1.TriggerKindWebhook,
		Status:             workflowv1.RunStatusComplete,
		RequestedScopeSet:  `["scope-a","scope-b"]`,
		RequestedCollector: "jira",
		CreatedAt:          now.Add(-time.Hour), UpdatedAt: now, FinishedAt: now,
	}
	if err := st.CreateRun(ctx, full); err != nil {
		t.Fatalf("create full run: %v", err)
	}
	got, err := st.GetRun(ctx, "opt-run-1")
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.RequestedCollector != "jira" || !got.FinishedAt.Equal(now) {
		t.Fatalf("round trip lost optional fields: %+v", got)
	}
	if got.RequestedScopeSet != `["scope-a", "scope-b"]` {
		t.Fatalf("scope set = %q, want the stored JSON array", got.RequestedScopeSet)
	}

	broken := full
	broken.RunID = "opt-run-2"
	broken.RequestedScopeSet = "{not json"
	if err := st.CreateRun(ctx, broken); err != nil {
		t.Fatalf("create run with unparsable scope set: %v", err)
	}
	stored, err := st.GetRun(ctx, "opt-run-2")
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if stored.RequestedScopeSet != "[]" {
		t.Fatalf("unparsable scope set stored as %q, want []", stored.RequestedScopeSet)
	}

	bare := workflowv1.Run{
		RunID: "opt-run-3", TriggerKind: workflowv1.TriggerKindReplay,
		Status: workflowv1.RunStatusCollectionPending, CreatedAt: now, UpdatedAt: now,
	}
	if err := st.CreateRun(ctx, bare); err != nil {
		t.Fatalf("create bare run: %v", err)
	}
	bareGot, err := st.GetRun(ctx, "opt-run-3")
	if err != nil {
		t.Fatalf("get bare run: %v", err)
	}
	if bareGot.RequestedCollector != "" || !bareGot.FinishedAt.IsZero() {
		t.Fatalf("bare run grew optional fields: %+v", bareGot)
	}
	if bareGot.RequestedScopeSet != "[]" {
		t.Fatalf("bare run scope set = %q, want []", bareGot.RequestedScopeSet)
	}

	if _, err := st.GetRun(ctx, "no-such-run"); !isCode(err, workflowv1.CodeNotFound) {
		t.Fatalf("missing run error = %v, want not_found", err)
	}
	if err := st.CreateRun(ctx, workflowv1.Run{}); !isCode(err, workflowv1.CodeInvalid) {
		t.Fatalf("invalid run error = %v, want invalid", err)
	}
}

// TestRunStatusCombinesProgressAndCompleteness requires the read plane to
// return the stored report-plane push and checkpoints, and to reject a
// push for a run that does not exist.
func TestRunStatusCombinesProgressAndCompleteness(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)
	now := time.Now().UTC().Truncate(time.Millisecond)

	push := workflowv1.ReportRunProgressRequest{
		RunID: runID, ObservedAt: now,
		Progress: []workflowv1.CollectorRunProgress{{
			CollectorKind: "jira", TotalWorkItems: 4, PendingWorkItems: 1, ClaimedWorkItems: 1,
			CompletedWorkItems: 2, FactlessCompletedItems: 1,
			PublishedPhaseCounts:     map[string]int{"facts/publish": 2},
			TerminalDeadLetterCounts: map[string]int{"facts/publish": 1},
		}, {
			CollectorKind: "git", TotalWorkItems: 1,
		}},
	}
	if err := st.ReportProgress(ctx, push); err != nil {
		t.Fatalf("report progress: %v", err)
	}
	// A second push for the same family replaces the counts.
	push.Progress[1].TotalWorkItems = 3
	push.Progress[1].PendingWorkItems = 3
	if err := st.ReportProgress(ctx, push); err != nil {
		t.Fatalf("replay report progress: %v", err)
	}
	if err := st.UpsertCompleteness(ctx, []workflowv1.CompletenessState{{
		RunID: runID, CollectorKind: "jira", Keyspace: "facts", PhaseName: "publish",
		Required: true, Status: "complete", ObservedAt: now, UpdatedAt: now,
	}}); err != nil {
		t.Fatalf("upsert completeness: %v", err)
	}

	status, err := st.GetRunStatus(ctx, runID, now)
	if err != nil {
		t.Fatalf("run status: %v", err)
	}
	if err := status.Validate(); err != nil {
		t.Fatalf("run status violates its own contract: %v", err)
	}
	if len(status.Progress) != 2 || status.Progress[0].CollectorKind != "git" || status.Progress[1].CollectorKind != "jira" {
		t.Fatalf("progress = %+v, want git then jira", status.Progress)
	}
	if status.Progress[0].TotalWorkItems != 3 || status.Progress[0].PendingWorkItems != 3 {
		t.Fatalf("replayed push did not replace git counts: %+v", status.Progress[0])
	}
	jira := status.Progress[1]
	if jira.FactlessCompletedItems != 1 || jira.PublishedPhaseCounts["facts/publish"] != 2 ||
		jira.TerminalDeadLetterCounts["facts/publish"] != 1 {
		t.Fatalf("jira progress lost fields: %+v", jira)
	}
	if len(status.Completeness) != 1 || status.Completeness[0].Status != "complete" {
		t.Fatalf("completeness = %+v, want the one stored checkpoint", status.Completeness)
	}

	missing := push
	missing.RunID = "no-such-run"
	if err := st.ReportProgress(ctx, missing); !isCode(err, workflowv1.CodeNotFound) {
		t.Fatalf("push for a missing run error = %v, want not_found", err)
	}
	if err := st.ReportProgress(ctx, workflowv1.ReportRunProgressRequest{}); !isCode(err, workflowv1.CodeInvalid) {
		t.Fatalf("empty push error = %v, want invalid", err)
	}
	if _, err := st.GetRunStatus(ctx, "no-such-run", now); !isCode(err, workflowv1.CodeNotFound) {
		t.Fatalf("status of a missing run error = %v, want not_found", err)
	}

	bare := seedRun(t, ctx, st)
	bareStatus, err := st.GetRunStatus(ctx, bare, now)
	if err != nil {
		t.Fatalf("bare run status: %v", err)
	}
	if bareStatus.Progress == nil || len(bareStatus.Progress) != 0 {
		t.Fatalf("run without a push progress = %#v, want an empty non-nil slice", bareStatus.Progress)
	}
}

// TestQueueDepthCountsPerFamily requires pending, retryable, and expired
// items to count as backlog and claimed items to count separately.
func TestQueueDepthCountsPerFamily(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)

	empty, err := st.QueueDepth(ctx, time.Now())
	if err != nil {
		t.Fatalf("empty depth: %v", err)
	}
	if empty.GlobalPending != 0 || empty.GlobalClaimed != 0 || len(empty.Families) != 0 {
		t.Fatalf("drained depth = %+v, want zeroes", empty)
	}

	items := []workflowv1.WorkItem{
		seedItem(runID, "qd-jira-1", false),
		seedItem(runID, "qd-jira-2", false),
		seedItem(runID, "qd-jira-3", false),
	}
	git := seedItem(runID, "qd-git-1", false)
	git.CollectorKind = "git"
	git.CollectorInstanceID = "git-primary"
	git.FairnessKey = "git:git-primary:bulk"
	items = append(items, git)
	if _, err := st.Enqueue(ctx, items); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimOne(t, ctx, st, "qd-claim-1", "owner-1", time.Minute)

	depth, err := st.QueueDepth(ctx, time.Now())
	if err != nil {
		t.Fatalf("depth: %v", err)
	}
	if err := depth.Validate(); err != nil {
		t.Fatalf("depth violates its own contract: %v", err)
	}
	if depth.GlobalPending != 3 || depth.GlobalClaimed != 1 {
		t.Fatalf("global depth = pending %d claimed %d, want 3 and 1", depth.GlobalPending, depth.GlobalClaimed)
	}
	byKind := map[workflowv1.CollectorKind]workflowv1.FamilyQueueDepth{}
	for _, family := range depth.Families {
		byKind[family.CollectorKind] = family
	}
	if got := byKind["jira"]; got.Pending != 2 || got.Claimed != 1 {
		t.Fatalf("jira depth = %+v, want pending 2 claimed 1", got)
	}
	if got := byKind["git"]; got.Pending != 1 || got.Claimed != 0 {
		t.Fatalf("git depth = %+v, want pending 1 claimed 0", got)
	}
}

// TestEnqueueSpansBatchesIdempotently requires a batch larger than the
// internal insert batch to land fully once and insert nothing on replay.
func TestEnqueueSpansBatchesIdempotently(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)
	items := make([]workflowv1.WorkItem, 0, 520)
	for i := 0; i < 520; i++ {
		items = append(items, seedItem(runID, fmt.Sprintf("bulk-%04d", i), false))
	}
	inserted, err := st.Enqueue(ctx, items)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if inserted != len(items) {
		t.Fatalf("inserted = %d, want %d", inserted, len(items))
	}
	replayed, err := st.Enqueue(ctx, items)
	if err != nil {
		t.Fatalf("replay enqueue: %v", err)
	}
	if replayed != 0 {
		t.Fatalf("replay inserted %d rows, want 0", replayed)
	}
	depth, err := st.QueueDepth(ctx, time.Now())
	if err != nil {
		t.Fatalf("depth: %v", err)
	}
	if depth.GlobalPending != int64(len(items)) {
		t.Fatalf("pending = %d, want %d", depth.GlobalPending, len(items))
	}

	bad := seedItem(runID, "", false)
	if _, err := st.Enqueue(ctx, []workflowv1.WorkItem{bad}); !isCode(err, workflowv1.CodeInvalid) {
		t.Fatalf("invalid item error = %v, want invalid", err)
	}
}
