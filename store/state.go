// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// ReconcileInstances upserts the declared instances and deactivates every
// instance absent from the declaration, exactly like the in-tree
// coordinator-state wiring.
func (s *Store) ReconcileInstances(
	ctx context.Context,
	now time.Time,
	desired []workflowv1.DesiredCollectorInstance,
) error {
	for i := range desired {
		if err := desired[i].Validate(); err != nil {
			return staleAsInvalid(fmt.Errorf("instance %d: %w", i, err))
		}
	}
	ids := make([]string, 0, len(desired))
	for _, inst := range desired {
		ids = append(ids, inst.InstanceID)
		_, err := s.pool.Exec(ctx, `INSERT INTO collector_instances (
    instance_id, collector_kind, mode, enabled, bootstrap, claims_enabled,
    display_name, configuration, last_observed_at, deactivated_at,
    created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8::jsonb, $9::timestamptz,
    CASE WHEN $4 THEN NULL ELSE $9::timestamptz END, $9::timestamptz, $9::timestamptz)
ON CONFLICT (instance_id) DO UPDATE
SET collector_kind = EXCLUDED.collector_kind,
    mode = EXCLUDED.mode,
    enabled = EXCLUDED.enabled,
    bootstrap = EXCLUDED.bootstrap,
    claims_enabled = EXCLUDED.claims_enabled,
    display_name = EXCLUDED.display_name,
    configuration = EXCLUDED.configuration,
    last_observed_at = EXCLUDED.last_observed_at,
    deactivated_at = EXCLUDED.deactivated_at,
    updated_at = EXCLUDED.updated_at`,
			inst.InstanceID, string(inst.CollectorKind), string(inst.Mode),
			inst.Enabled, inst.Bootstrap, inst.ClaimsEnabled,
			inst.DisplayName, configJSON(inst.Configuration), now.UTC(),
		)
		if err != nil {
			return fmt.Errorf("upsert collector instance: %w", err)
		}
	}
	if _, err := s.pool.Exec(ctx, `UPDATE collector_instances
SET enabled = FALSE, claims_enabled = FALSE,
    deactivated_at = $1, updated_at = $1
WHERE NOT (instance_id = ANY($2::text[]))
  AND (enabled = TRUE OR deactivated_at IS NULL)`, now.UTC(), ids); err != nil {
		return fmt.Errorf("deactivate stale collector instances: %w", err)
	}
	return nil
}

func configJSON(raw string) string {
	if raw == "" {
		return "{}"
	}
	return raw
}

// ListInstances returns every durable collector instance, read-only.
func (s *Store) ListInstances(ctx context.Context) ([]workflowv1.CollectorInstance, error) {
	rows, err := s.pool.Query(ctx, `SELECT instance_id, collector_kind, mode,
enabled, bootstrap, claims_enabled, COALESCE(display_name, ''),
configuration::text, last_observed_at, deactivated_at, created_at, updated_at
FROM collector_instances ORDER BY collector_kind ASC, instance_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("list collector instances: %w", err)
	}
	defer rows.Close()
	instances := []workflowv1.CollectorInstance{}
	for rows.Next() {
		var inst workflowv1.CollectorInstance
		var deactivatedAt *time.Time
		var configuration string
		if err := rows.Scan(&inst.InstanceID, &inst.CollectorKind, &inst.Mode,
			&inst.Enabled, &inst.Bootstrap, &inst.ClaimsEnabled,
			&inst.DisplayName, &configuration,
			&inst.LastObservedAt, &deactivatedAt, &inst.CreatedAt, &inst.UpdatedAt); err != nil {
			return nil, fmt.Errorf("list collector instances scan: %w", err)
		}
		inst.Configuration = configuration
		if deactivatedAt != nil {
			inst.DeactivatedAt = *deactivatedAt
		}
		instances = append(instances, inst)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list collector instances: %w", err)
	}
	return instances, nil
}

// UpsertCompleteness records reducer-facing completion checkpoints.
func (s *Store) UpsertCompleteness(ctx context.Context, states []workflowv1.CompletenessState) error {
	for i := range states {
		if err := states[i].Validate(); err != nil {
			return staleAsInvalid(fmt.Errorf("completeness %d: %w", i, err))
		}
		_, err := s.pool.Exec(ctx, `INSERT INTO workflow_run_completeness (
    run_id, collector_kind, keyspace, phase_name, required, status, detail,
    observed_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9)
ON CONFLICT (run_id, collector_kind, keyspace, phase_name) DO UPDATE
SET required = EXCLUDED.required, status = EXCLUDED.status,
    detail = EXCLUDED.detail, observed_at = EXCLUDED.observed_at,
    updated_at = EXCLUDED.updated_at`,
			states[i].RunID, string(states[i].CollectorKind), states[i].Keyspace,
			states[i].PhaseName, states[i].Required, states[i].Status,
			states[i].Detail, states[i].ObservedAt.UTC(), states[i].UpdatedAt.UTC(),
		)
		if err != nil {
			return fmt.Errorf("upsert completeness: %w", err)
		}
	}
	return nil
}

// GetCompleteness reads the checkpoints of one run.
func (s *Store) GetCompleteness(ctx context.Context, runID string) ([]workflowv1.CompletenessState, error) {
	rows, err := s.pool.Query(ctx, `SELECT run_id, collector_kind, keyspace,
phase_name, required, status, detail, observed_at, updated_at
FROM workflow_run_completeness WHERE run_id = $1
ORDER BY collector_kind ASC, keyspace ASC, phase_name ASC`, runID)
	if err != nil {
		return nil, fmt.Errorf("get completeness: %w", err)
	}
	defer rows.Close()
	states := []workflowv1.CompletenessState{}
	for rows.Next() {
		var state workflowv1.CompletenessState
		var detail *string
		if err := rows.Scan(&state.RunID, &state.CollectorKind, &state.Keyspace,
			&state.PhaseName, &state.Required, &state.Status, &detail,
			&state.ObservedAt, &state.UpdatedAt); err != nil {
			return nil, fmt.Errorf("get completeness scan: %w", err)
		}
		if detail != nil {
			state.Detail = *detail
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get completeness: %w", err)
	}
	return states, nil
}

// ReportProgress stores core's report-plane push: published phase counts,
// dead-letter counts, and factless counts per run and family. The service
// may not read fact tables, so the read plane reconciles from these pushes.
func (s *Store) ReportProgress(ctx context.Context, req workflowv1.ReportRunProgressRequest) error {
	if err := req.Validate(); err != nil {
		return staleAsInvalid(err)
	}
	if _, err := s.GetRun(ctx, req.RunID); err != nil {
		return err
	}
	for _, progress := range req.Progress {
		published, err := json.Marshal(progress.PublishedPhaseCounts)
		if err != nil {
			return fmt.Errorf("marshal published phase counts: %w", err)
		}
		deadLetter, err := json.Marshal(progress.TerminalDeadLetterCounts)
		if err != nil {
			return fmt.Errorf("marshal dead-letter counts: %w", err)
		}
		_, err = s.pool.Exec(ctx, `INSERT INTO workflow_run_progress (
    run_id, collector_kind, total_work_items, pending_work_items,
    claimed_work_items, completed_work_items, failed_terminal_items,
    factless_completed_items, published_phase_counts,
    terminal_dead_letter_counts, observed_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11, $11)
ON CONFLICT (run_id, collector_kind) DO UPDATE
SET total_work_items = EXCLUDED.total_work_items,
    pending_work_items = EXCLUDED.pending_work_items,
    claimed_work_items = EXCLUDED.claimed_work_items,
    completed_work_items = EXCLUDED.completed_work_items,
    failed_terminal_items = EXCLUDED.failed_terminal_items,
    factless_completed_items = EXCLUDED.factless_completed_items,
    published_phase_counts = EXCLUDED.published_phase_counts,
    terminal_dead_letter_counts = EXCLUDED.terminal_dead_letter_counts,
    observed_at = EXCLUDED.observed_at,
    updated_at = EXCLUDED.updated_at`,
			req.RunID, string(progress.CollectorKind),
			progress.TotalWorkItems, progress.PendingWorkItems,
			progress.ClaimedWorkItems, progress.CompletedWorkItems,
			progress.FailedTerminalItems, progress.FactlessCompletedItems,
			string(published), string(deadLetter), req.ObservedAt.UTC(),
		)
		if err != nil {
			return fmt.Errorf("report run progress: %w", err)
		}
	}
	return nil
}

// GetProgress reads the stored report-plane pushes of one run.
func (s *Store) GetProgress(ctx context.Context, runID string) ([]workflowv1.CollectorRunProgress, error) {
	rows, err := s.pool.Query(ctx, `SELECT collector_kind, total_work_items,
pending_work_items, claimed_work_items, completed_work_items,
failed_terminal_items, factless_completed_items,
published_phase_counts::text, terminal_dead_letter_counts::text
FROM workflow_run_progress WHERE run_id = $1 ORDER BY collector_kind ASC`, runID)
	if err != nil {
		return nil, fmt.Errorf("get run progress: %w", err)
	}
	defer rows.Close()
	progress := []workflowv1.CollectorRunProgress{}
	for rows.Next() {
		var entry workflowv1.CollectorRunProgress
		var published, deadLetter string
		if err := rows.Scan(&entry.CollectorKind, &entry.TotalWorkItems,
			&entry.PendingWorkItems, &entry.ClaimedWorkItems,
			&entry.CompletedWorkItems, &entry.FailedTerminalItems,
			&entry.FactlessCompletedItems, &published, &deadLetter); err != nil {
			return nil, fmt.Errorf("get run progress scan: %w", err)
		}
		if err := json.Unmarshal([]byte(published), &entry.PublishedPhaseCounts); err != nil {
			return nil, fmt.Errorf("decode published phase counts: %w", err)
		}
		if err := json.Unmarshal([]byte(deadLetter), &entry.TerminalDeadLetterCounts); err != nil {
			return nil, fmt.Errorf("decode dead-letter counts: %w", err)
		}
		progress = append(progress, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("get run progress: %w", err)
	}
	return progress, nil
}

// QueueDepth counts outstanding work items per family plus global totals,
// replacing direct work-item table reads for backpressure status.
func (s *Store) QueueDepth(ctx context.Context, now time.Time) (workflowv1.QueueDepthSnapshot, error) {
	snapshot := workflowv1.QueueDepthSnapshot{AsOf: now.UTC()}
	rows, err := s.pool.Query(ctx, `SELECT collector_kind, status, COUNT(*)::BIGINT
FROM workflow_work_items
WHERE status IN ('pending', 'claimed', 'failed_retryable', 'expired')
GROUP BY collector_kind, status ORDER BY collector_kind, status`)
	if err != nil {
		return snapshot, fmt.Errorf("queue depth: %w", err)
	}
	defer rows.Close()
	byKind := map[string]*workflowv1.FamilyQueueDepth{}
	order := []string{}
	for rows.Next() {
		var kind, status string
		var count int64
		if err := rows.Scan(&kind, &status, &count); err != nil {
			return snapshot, fmt.Errorf("queue depth scan: %w", err)
		}
		entry, ok := byKind[kind]
		if !ok {
			entry = &workflowv1.FamilyQueueDepth{CollectorKind: workflowv1.CollectorKind(kind)}
			byKind[kind] = entry
			order = append(order, kind)
		}
		switch status {
		case "pending", "failed_retryable", "expired":
			entry.Pending += count
			snapshot.GlobalPending += count
		case "claimed":
			entry.Claimed += count
			snapshot.GlobalClaimed += count
		}
	}
	if err := rows.Err(); err != nil {
		return snapshot, fmt.Errorf("queue depth: %w", err)
	}
	for _, kind := range order {
		snapshot.Families = append(snapshot.Families, *byKind[kind])
	}
	return snapshot, nil
}

// GetRunStatus assembles the read-plane run status: the run, stored
// report-plane progress, and completeness checkpoints.
func (s *Store) GetRunStatus(
	ctx context.Context,
	runID string,
	now time.Time,
) (workflowv1.GetRunStatusResponse, error) {
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return workflowv1.GetRunStatusResponse{}, err
	}
	progress, err := s.GetProgress(ctx, runID)
	if err != nil {
		return workflowv1.GetRunStatusResponse{}, err
	}
	completeness, err := s.GetCompleteness(ctx, runID)
	if err != nil {
		return workflowv1.GetRunStatusResponse{}, err
	}
	return workflowv1.GetRunStatusResponse{
		Run:          run,
		Progress:     progress,
		Completeness: completeness,
		AsOf:         now.UTC(),
	}, nil
}
