// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package coordinator_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// errTestPlaneUnreachable models a claim-plane that is not yet
// listening, as seen on joint rollouts.
var errTestPlaneUnreachable = errors.New("connection refused")

// fakePlane is an in-memory ControlPlane: idempotent run upserts and
// work-item inserts with a mutex, so concurrent reconciles contend the
// same way they do against the real table.
type fakePlane struct {
	mu         sync.Mutex
	runs       map[string]workflowv1.Run
	items      map[string]workflowv1.WorkItem
	instances  map[string]workflowv1.CollectorInstance
	inserted   int
	calls      []string
	pageSize   int
	loopCursor bool
	depthErr   error
	// reconcileFailures counts down forced ReconcileCollectorInstances
	// failures, modeling a claim-plane that is not yet reachable.
	reconcileFailures int
}

func newFakePlane() *fakePlane {
	return &fakePlane{
		runs:      map[string]workflowv1.Run{},
		items:     map[string]workflowv1.WorkItem{},
		instances: map[string]workflowv1.CollectorInstance{},
		pageSize:  2,
	}
}

func (f *fakePlane) note(op string) {
	f.calls = append(f.calls, op)
}

func (f *fakePlane) CreateRun(_ context.Context, run workflowv1.Run) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.note("CreateRun")
	f.runs[run.RunID] = run
	return nil
}

func (f *fakePlane) EnqueueWorkItems(_ context.Context, items []workflowv1.WorkItem) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.note("EnqueueWorkItems")
	took := 0
	for _, item := range items {
		if _, ok := f.items[item.WorkItemID]; !ok {
			f.items[item.WorkItemID] = item
			took++
		}
	}
	f.inserted += took
	return took, nil
}

func (f *fakePlane) ReconcileCollectorInstances(
	_ context.Context,
	now time.Time,
	desired []workflowv1.DesiredCollectorInstance,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.note("ReconcileCollectorInstances")
	if f.reconcileFailures > 0 {
		f.reconcileFailures--
		return errTestPlaneUnreachable
	}
	keep := map[string]bool{}
	for _, d := range desired {
		keep[d.InstanceID] = true
		inst, ok := f.instances[d.InstanceID]
		if !ok {
			inst = workflowv1.CollectorInstance{CreatedAt: now}
		}
		inst.InstanceID = d.InstanceID
		inst.CollectorKind = d.CollectorKind
		inst.Mode = d.Mode
		inst.Enabled = d.Enabled
		inst.Bootstrap = d.Bootstrap
		inst.ClaimsEnabled = d.ClaimsEnabled
		inst.DisplayName = d.DisplayName
		inst.Configuration = d.Configuration
		inst.LastObservedAt = now
		inst.UpdatedAt = now
		if !d.Enabled {
			inst.DeactivatedAt = now
		}
		f.instances[d.InstanceID] = inst
	}
	for id, inst := range f.instances {
		if !keep[id] && (inst.Enabled || inst.DeactivatedAt.IsZero()) {
			inst.Enabled = false
			inst.DeactivatedAt = now
			inst.UpdatedAt = now
			f.instances[id] = inst
		}
	}
	return nil
}

func (f *fakePlane) ListCollectorInstances(_ context.Context) ([]workflowv1.CollectorInstance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.note("ListCollectorInstances")
	out := make([]workflowv1.CollectorInstance, 0, len(f.instances))
	for _, inst := range f.instances {
		out = append(out, inst)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
	return out, nil
}

func (f *fakePlane) ListRuns(
	_ context.Context,
	req workflowv1.ListRunsRequest,
) (workflowv1.ListRunsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.note("ListRuns")
	all := make([]workflowv1.RunSummary, 0, len(f.runs))
	for _, run := range f.runs {
		if req.Status != "" && run.Status != req.Status {
			continue
		}
		all = append(all, workflowv1.RunSummary(run))
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].UpdatedAt.Equal(all[j].UpdatedAt) {
			return all[i].RunID > all[j].RunID
		}
		return all[i].UpdatedAt.After(all[j].UpdatedAt)
	})
	// Honor the cursor the way the store does: pages walk strictly
	// older than (updated_at, run_id), so the guard test genuinely
	// exercises multi-page walks instead of rereading page one.
	if req.Cursor != "" {
		cut := strings.LastIndex(req.Cursor, "|")
		var cursorTime time.Time
		cursorID := ""
		if cut >= 0 {
			cursorTime, _ = time.Parse(time.RFC3339Nano, req.Cursor[:cut])
			cursorID = req.Cursor[cut+1:]
		}
		kept := all[:0]
		for _, summary := range all {
			if summary.UpdatedAt.UTC().Before(cursorTime) ||
				(summary.UpdatedAt.UTC().Equal(cursorTime) && summary.RunID < cursorID) {
				kept = append(kept, summary)
			}
		}
		all = kept
	}
	limit := req.EffectiveLimit()
	if f.pageSize > 0 && limit > f.pageSize {
		limit = f.pageSize
	}
	// Cursor names the last returned row, exactly like the store: the
	// next page walks strictly older than it.
	resp := workflowv1.ListRunsResponse{AsOf: time.Now().UTC(), Runs: []workflowv1.RunSummary{}}
	if len(all) > limit {
		last := all[limit-1]
		resp.NextCursor = last.UpdatedAt.UTC().Format(time.RFC3339Nano) + "|" + last.RunID
		all = all[:limit]
	}
	resp.Runs = append(resp.Runs, all...)
	if f.loopCursor {
		resp.NextCursor = "loop"
	}
	return resp, nil
}

func (f *fakePlane) ReconcileWorkflowRuns(_ context.Context, _ time.Time) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.note("ReconcileWorkflowRuns")
	return 0, nil
}

func (f *fakePlane) ReapExpiredClaims(
	_ context.Context,
	_ time.Time,
	_ int,
	_ time.Duration,
) ([]workflowv1.Claim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.note("ReapExpiredClaims")
	return []workflowv1.Claim{}, nil
}

func (f *fakePlane) GetRunStatus(
	_ context.Context,
	runID string,
) (workflowv1.GetRunStatusResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.note("GetRunStatus")
	run, ok := f.runs[runID]
	if !ok {
		return workflowv1.GetRunStatusResponse{}, &workflowv1.ProtocolError{
			Code:    workflowv1.CodeNotFound,
			Message: fmt.Sprintf("run %q not found", runID),
		}
	}
	now := time.Now().UTC()
	return workflowv1.GetRunStatusResponse{Run: run, AsOf: now}, nil
}

func (f *fakePlane) QueueDepthSnapshot(_ context.Context) (workflowv1.QueueDepthSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.note("QueueDepthSnapshot")
	if f.depthErr != nil {
		return workflowv1.QueueDepthSnapshot{}, f.depthErr
	}
	return workflowv1.QueueDepthSnapshot{AsOf: time.Now().UTC()}, nil
}

func (f *fakePlane) itemCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.items)
}
