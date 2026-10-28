// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// CreateRun upserts one durable workflow run.
func (s *Store) CreateRun(ctx context.Context, run workflowv1.Run) error {
	if err := run.Validate(); err != nil {
		return staleAsInvalid(err)
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO workflow_runs (
    run_id, trigger_kind, status, requested_scope_set, requested_collector,
    created_at, updated_at, finished_at
) VALUES ($1, $2, $3, $4::jsonb, NULLIF($5, ''), $6, $7, NULLIF($8, '')::timestamptz)
ON CONFLICT (run_id) DO UPDATE
SET trigger_kind = EXCLUDED.trigger_kind,
    requested_scope_set = EXCLUDED.requested_scope_set,
    requested_collector = EXCLUDED.requested_collector,
    updated_at = EXCLUDED.updated_at`,
		run.RunID, string(run.TriggerKind), string(run.Status),
		scopeSetJSON(run.RequestedScopeSet), run.RequestedCollector,
		run.CreatedAt.UTC(), run.UpdatedAt.UTC(), rfc3339OrEmpty(run.FinishedAt),
	)
	if err != nil {
		return fmt.Errorf("create run: %w", err)
	}
	return nil
}

func scopeSetJSON(raw string) string {
	trimmed := raw
	if trimmed == "" {
		return "[]"
	}
	if !json.Valid([]byte(trimmed)) {
		return "[]"
	}
	return trimmed
}

func rfc3339OrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// Enqueue inserts work items in batches. Conflict rows are skipped: enqueue
// is idempotent on work_item_id, so an ambiguous-timeout replay of the same
// batch lands nothing twice.
func (s *Store) Enqueue(ctx context.Context, items []workflowv1.WorkItem) error {
	for i := range items {
		if err := items[i].Validate(); err != nil {
			return staleAsInvalid(fmt.Errorf("enqueue item %d: %w", i, err))
		}
	}
	const batch = 250
	for start := 0; start < len(items); start += batch {
		end := start + batch
		if end > len(items) {
			end = len(items)
		}
		if err := s.enqueueBatch(ctx, items[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) enqueueBatch(ctx context.Context, items []workflowv1.WorkItem) error {
	if len(items) == 0 {
		return nil
	}
	cols := []string{
		"work_item_id", "run_id", "collector_kind", "collector_instance_id",
		"source_system", "scope_id", "tenant_id", "workspace_id", "subject_class",
		"policy_revision_hash", "acceptance_unit_id", "source_run_id", "generation_id",
		"fairness_key", "claim_priority", "claim_ecosystem", "status", "attempt_count",
		"current_claim_id", "current_fencing_token", "current_owner_id",
		"lease_expires_at", "visible_at", "last_claimed_at", "last_completed_at",
		"last_failure_class", "last_failure_message", "created_at", "updated_at",
	}
	query := "INSERT INTO workflow_work_items (" + joinCols(cols) + ") VALUES "
	args := make([]any, 0, len(items)*len(cols))
	for i, item := range items {
		row := []any{
			item.WorkItemID, item.RunID, string(item.CollectorKind),
			item.CollectorInstanceID, item.SourceSystem, item.ScopeID, item.TenantID,
			item.WorkspaceID, item.SubjectClass, item.PolicyRevisionHash,
			item.AcceptanceUnitID, item.SourceRunID, item.GenerationID,
			nullableString(item.FairnessKey), item.ClaimPriority, item.ClaimEcosystem,
			string(item.Status), item.AttemptCount,
			nullableString(item.CurrentClaimID), item.CurrentFencingToken,
			nullableString(item.CurrentOwnerID),
			nullableTime(item.LeaseExpiresAt), nullableTime(item.VisibleAt),
			nullableTime(item.LastClaimedAt), nullableTime(item.LastCompletedAt),
			nullableString(item.LastFailureClass), nullableString(item.LastFailureMessage),
			item.CreatedAt.UTC(), item.UpdatedAt.UTC(),
		}
		base := i*len(cols) + 1
		placeholders := make([]string, len(cols))
		for j := range cols {
			placeholders[j] = fmt.Sprintf("$%d", base+j)
		}
		if i > 0 {
			query += ", "
		}
		query += "(" + joinCols(placeholders) + ")"
		args = append(args, row...)
	}
	query += " ON CONFLICT DO NOTHING"
	if _, err := s.pool.Exec(ctx, query, args...); err != nil {
		return fmt.Errorf("enqueue work items: %w", err)
	}
	return nil
}

func joinCols(cols []string) string {
	out := ""
	for i, col := range cols {
		if i > 0 {
			out += ", "
		}
		out += col
	}
	return out
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}

// GetRun reads one run by id.
func (s *Store) GetRun(ctx context.Context, runID string) (workflowv1.Run, error) {
	var run workflowv1.Run
	var finishedAt *time.Time
	var requestedCollector *string
	var scopeSet string
	err := s.pool.QueryRow(ctx, `SELECT run_id, trigger_kind, status,
requested_scope_set::text, requested_collector, created_at, updated_at, finished_at
FROM workflow_runs WHERE run_id = $1`, runID).Scan(
		&run.RunID, &run.TriggerKind, &run.Status, &scopeSet,
		&requestedCollector, &run.CreatedAt, &run.UpdatedAt, &finishedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return run, &workflowv1.ProtocolError{
				Code:    workflowv1.CodeNotFound,
				Message: fmt.Sprintf("run %q not found", runID),
			}
		}
		return run, fmt.Errorf("get run: %w", err)
	}
	run.RequestedScopeSet = scopeSet
	if requestedCollector != nil {
		run.RequestedCollector = *requestedCollector
	}
	if finishedAt != nil {
		run.FinishedAt = *finishedAt
	}
	return run, nil
}

// ListRuns pages runs newest first. The cursor is an RFC3339Nano updated_at
// plus run id tiebreak, encoded as "timestamp|run_id".
func (s *Store) ListRuns(ctx context.Context, req workflowv1.ListRunsRequest) (workflowv1.ListRunsResponse, error) {
	if err := req.Validate(); err != nil {
		return workflowv1.ListRunsResponse{}, staleAsInvalid(err)
	}
	limit := req.EffectiveLimit()
	rows, err := s.pool.Query(ctx, `SELECT run_id, trigger_kind, status,
requested_scope_set::text, requested_collector, created_at, updated_at, finished_at
FROM workflow_runs
WHERE ($1 = '' OR status = $1)
  AND ($2 = '' OR (updated_at, run_id) < ($3::timestamptz, $4))
ORDER BY updated_at DESC, run_id DESC
LIMIT $5`,
		string(req.Status), req.Cursor, cursorTime(req.Cursor), cursorID(req.Cursor), limit+1)
	if err != nil {
		return workflowv1.ListRunsResponse{}, fmt.Errorf("list runs: %w", err)
	}
	defer rows.Close()
	resp := workflowv1.ListRunsResponse{AsOf: time.Now().UTC()}
	for rows.Next() {
		var summary workflowv1.RunSummary
		var finishedAt *time.Time
		var requestedCollector *string
		if err := rows.Scan(&summary.RunID, &summary.TriggerKind, &summary.Status,
			&summary.RequestedScopeSet, &requestedCollector,
			&summary.CreatedAt, &summary.UpdatedAt, &finishedAt); err != nil {
			return resp, fmt.Errorf("list runs scan: %w", err)
		}
		if requestedCollector != nil {
			summary.RequestedCollector = *requestedCollector
		}
		if finishedAt != nil {
			summary.FinishedAt = *finishedAt
		}
		resp.Runs = append(resp.Runs, summary)
	}
	if err := rows.Err(); err != nil {
		return resp, fmt.Errorf("list runs: %w", err)
	}
	if len(resp.Runs) > limit {
		last := resp.Runs[limit-1]
		resp.NextCursor = last.UpdatedAt.UTC().Format(time.RFC3339Nano) + "|" + last.RunID
		resp.Runs = resp.Runs[:limit]
	}
	if resp.Runs == nil {
		resp.Runs = []workflowv1.RunSummary{}
	}
	return resp, nil
}

func cursorTime(cursor string) any {
	for i := len(cursor) - 1; i >= 0; i-- {
		if cursor[i] == '|' {
			if parsed, err := time.Parse(time.RFC3339Nano, cursor[:i]); err == nil {
				return parsed.UTC()
			}
			return nil
		}
	}
	return nil
}

func cursorID(cursor string) string {
	for i := len(cursor) - 1; i >= 0; i-- {
		if cursor[i] == '|' {
			return cursor[i+1:]
		}
	}
	return ""
}

// ReconcileRuns advances non-terminal runs whose queue drained: with no
// pending or claimed items left, the run is complete. Returns the count of
// runs transitioned.
func (s *Store) ReconcileRuns(ctx context.Context, now time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE workflow_runs AS run
SET status = 'complete', finished_at = COALESCE(run.finished_at, $1), updated_at = $1
WHERE run.status NOT IN ('complete', 'failed')
  AND NOT EXISTS (
      SELECT 1 FROM workflow_work_items AS item
      WHERE item.run_id = run.run_id
        AND item.status IN ('pending', 'claimed')
  )`, now.UTC())
	if err != nil {
		return 0, fmt.Errorf("reconcile runs: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
