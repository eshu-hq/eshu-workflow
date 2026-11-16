// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package coordinator_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/coordinator"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestAllRunStatusesPinned ties the guard's walk list to the exhaustive
// status set: adding a RunStatus fails here until it is classified, so a
// future status cannot silently escape the guard and admit duplicates.
func TestAllRunStatusesPinned(t *testing.T) {
	got := workflowv1.AllRunStatuses()
	want := []workflowv1.RunStatus{
		workflowv1.RunStatusCollectionPending,
		workflowv1.RunStatusCollectionActive,
		workflowv1.RunStatusCollectionComplete,
		workflowv1.RunStatusReducerConverging,
		workflowv1.RunStatusComplete,
		workflowv1.RunStatusFailed,
	}
	if len(got) != len(want) {
		t.Fatalf("AllRunStatuses() has %d entries, want %d: classify the new status", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("AllRunStatuses()[%d] = %q, want %q", i, string(got[i]), string(want[i]))
		}
		if err := got[i].Validate(); err != nil {
			t.Fatalf("AllRunStatuses()[%d] fails Validate: %v", i, err)
		}
	}
	for _, status := range got {
		terminal := status == workflowv1.RunStatusComplete || status == workflowv1.RunStatusFailed
		if status.IsTerminal() != terminal {
			t.Fatalf("status %q IsTerminal() = %v, want %v: the guard walks exactly the non-terminal set", string(status), status.IsTerminal(), terminal)
		}
	}
}

// seedTerminalHistory fills the fake plane with n completed runs newer
// than coverAt, so an unfiltered newest-first walk buries anything older
// past the page-cap budget at small fake page sizes.
func seedTerminalHistory(plane *fakePlane, n int, now time.Time) {
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("history-%03d", i)
		ts := now.Add(-time.Duration(i+1) * time.Second)
		plane.runs[id] = workflowv1.Run{
			RunID:       id,
			TriggerKind: workflowv1.TriggerKindSchedule,
			Status:      workflowv1.RunStatusComplete,
			CreatedAt:   ts,
			UpdatedAt:   ts,
		}
	}
}

func guardReconcile(t *testing.T, plane *fakePlane) error {
	t.Helper()

	planners := coordinator.NewRegistry()
	planners.Register("jira", &recordingPlanner{targets: []coordinator.PlanTarget{{ScopeID: "scope-a"}, {ScopeID: "scope-b"}}})
	svc := coordinator.Service{
		Config:   activeConfig(schedulableInstance("jira-primary", "jira")),
		Plane:    plane,
		Planners: planners,
		Logger:   slog.New(slog.DiscardHandler),
		Clock:    func() time.Time { return time.Now().UTC() },
	}
	return svc.Reconcile(context.Background())
}

// TestGuardFindsCoverBuriedPastTerminalHistory reproduces the live
// satellite crashloop shape: terminal history deeper than cap x page
// with the covering open run older than all of it. The unfiltered walk
// trips the page cap before reaching the cover; the status-filtered walk
// must find it. It fails until the guard walks per-status.
func TestGuardFindsCoverBuriedPastTerminalHistory(t *testing.T) {
	plane := newFakePlane()
	plane.pageSize = 2
	now := time.Now().UTC().Truncate(time.Second)
	seedTerminalHistory(plane, 45, now)
	plane.runs["cover-old"] = workflowv1.Run{
		RunID: "cover-old", TriggerKind: workflowv1.TriggerKindSchedule,
		Status: workflowv1.RunStatusCollectionActive, RequestedCollector: "jira-primary",
		RequestedScopeSet: `["scope-a","scope-b"]`,
		CreatedAt:         now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
	}

	if err := guardReconcile(t, plane); err != nil {
		t.Fatalf("reconcile with buried cover: %v (want covered, admit nothing)", err)
	}
	if got := plane.itemCount(); got != 0 {
		t.Fatalf("guarded plan admitted %d work items, want 0 (cover buried past history)", got)
	}
}

// TestGuardAdmitsWhenOnlyTerminalHistory reproduces the posfact live
// shape: terminal history deeper than the walk budget with no open run
// at all. The unfiltered walk trips hunting a cover that cannot exist;
// the status-filtered walk must exhaust and admit. It fails until the
// guard walks per-status.
func TestGuardAdmitsWhenOnlyTerminalHistory(t *testing.T) {
	plane := newFakePlane()
	plane.pageSize = 2
	now := time.Now().UTC().Truncate(time.Second)
	seedTerminalHistory(plane, 45, now)

	if err := guardReconcile(t, plane); err != nil {
		t.Fatalf("reconcile with terminal-only history: %v (want admit)", err)
	}
	if got := plane.itemCount(); got != 2 {
		t.Fatalf("guarded plan admitted %d work items, want 2 (scope-a, scope-b)", got)
	}
}

// TestGuardCapStillTripsOnDeepNonTerminalWalk pins the fail-closed
// bound: more than cap pages of same-status non-terminal runs without a
// cover must still trip, even with per-status walks.
func TestGuardCapStillTripsOnDeepNonTerminalWalk(t *testing.T) {
	plane := newFakePlane()
	plane.pageSize = 2
	now := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 45; i++ {
		id := fmt.Sprintf("active-%03d", i)
		ts := now.Add(-time.Duration(i+1) * time.Second)
		plane.runs[id] = workflowv1.Run{
			RunID:       id,
			TriggerKind: workflowv1.TriggerKindSchedule,
			Status:      workflowv1.RunStatusCollectionActive,
			CreatedAt:   ts,
			UpdatedAt:   ts,
		}
	}

	err := guardReconcile(t, plane)
	if err == nil || !strings.Contains(err.Error(), "pages") {
		t.Fatalf("reconcile error = %v, want the page-cap bound (fail-closed)", err)
	}
}
