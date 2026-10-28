-- SPDX-License-Identifier: MIT
-- Copyright (c) 2025-2026 eshu-hq

-- Commit pin and report-plane progress. New in Stage-1b; no eshu-main
-- equivalent.
--
-- The pin replaces the in-transaction heartbeat the in-tree path relied on
-- (CommitClaimedScopeGeneration ran HeartbeatClaim on the same transaction
-- as fact persistence). A cross-service protocol cannot share that
-- transaction, so the pin holds the claim server-side: the reaper must not
-- expire a claim whose commit_pinned_until still covers the reap clock.
-- Core tags the generation with the returned commit id and never activates
-- a generation below the highest activated fencing token.
--
-- workflow_run_progress stores core's report-plane pushes (published phase
-- counts, dead-letter counts, factless counts) per run and family. The
-- service may not read fact_records or fact_work_items, so the read plane
-- reconciles from these pushes.

ALTER TABLE workflow_claims
    ADD COLUMN IF NOT EXISTS commit_id TEXT NULL,
    ADD COLUMN IF NOT EXISTS commit_pinned_until TIMESTAMPTZ NULL;

CREATE TABLE IF NOT EXISTS workflow_run_progress (
    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    collector_kind TEXT NOT NULL,
    total_work_items INTEGER NOT NULL DEFAULT 0,
    pending_work_items INTEGER NOT NULL DEFAULT 0,
    claimed_work_items INTEGER NOT NULL DEFAULT 0,
    completed_work_items INTEGER NOT NULL DEFAULT 0,
    failed_terminal_items INTEGER NOT NULL DEFAULT 0,
    factless_completed_items INTEGER NOT NULL DEFAULT 0,
    published_phase_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    terminal_dead_letter_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    observed_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, collector_kind)
);
