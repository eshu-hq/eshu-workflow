-- SPDX-License-Identifier: MIT
-- Copyright (c) 2025-2026 eshu-hq

-- Claim-plane base schema: fresh-install equivalent of eshu-main
-- migrations 014 (control plane), 184/185 (failure classification),
-- 201 (claim class columns), 202 (fair-claim index), 204 (class backfill).
--
-- Fresh-install simplifications, all behavior-preserving on empty tables:
--   * 203's DROP of the step-1 FIFO index is skipped: migration 200 never
--     runs here, so the index never exists.
--   * 204's backlog backfill is skipped: no legacy rows exist. Rows carry
--     their class at enqueue time.
--   * 202's CONCURRENTLY is a plain build: no concurrent writes exist
--     during a fresh install, and plain builds run inside the migrator's
--     per-file transaction.
--   * Tenant/producer grant tables are deliberately absent: grants stay
--     core-owned. Heartbeat and complete enforce fencing only; grant
--     authority moves to the commit pin (migration 004) plus core admission.

CREATE TABLE IF NOT EXISTS workflow_runs (
    run_id TEXT PRIMARY KEY,
    trigger_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_scope_set JSONB NOT NULL DEFAULT '[]'::jsonb,
    requested_collector TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NULL
);
CREATE INDEX IF NOT EXISTS workflow_runs_status_updated_idx
    ON workflow_runs (status, updated_at DESC);

CREATE TABLE IF NOT EXISTS workflow_work_items (
    work_item_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
    collector_kind TEXT NOT NULL,
    collector_instance_id TEXT NOT NULL,
    source_system TEXT NOT NULL DEFAULT '',
    scope_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL DEFAULT '',
    workspace_id TEXT NOT NULL DEFAULT '',
    subject_class TEXT NOT NULL DEFAULT '',
    policy_revision_hash TEXT NOT NULL DEFAULT '',
    acceptance_unit_id TEXT NOT NULL DEFAULT '',
    source_run_id TEXT NOT NULL DEFAULT '',
    generation_id TEXT NOT NULL,
    fairness_key TEXT NULL,
    claim_priority INTEGER NOT NULL DEFAULT 0,
    claim_ecosystem TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    current_claim_id TEXT NULL,
    current_fencing_token BIGINT NOT NULL DEFAULT 0,
    current_owner_id TEXT NULL,
    lease_expires_at TIMESTAMPTZ NULL,
    visible_at TIMESTAMPTZ NULL,
    last_claimed_at TIMESTAMPTZ NULL,
    last_completed_at TIMESTAMPTZ NULL,
    last_failure_class TEXT NULL,
    last_failure_message TEXT NULL,
    last_failure_operation TEXT NULL,
    last_failure_status_code INTEGER NULL,
    last_failure_terminal BOOLEAN NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS workflow_work_items_claimable_idx
    ON workflow_work_items (
        collector_kind,
        collector_instance_id,
        status,
        visible_at,
        updated_at DESC
    );
CREATE INDEX IF NOT EXISTS workflow_work_items_lease_idx
    ON workflow_work_items (lease_expires_at)
    WHERE lease_expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS workflow_work_items_run_idx
    ON workflow_work_items (run_id, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS workflow_work_items_registry_status_updated_idx
    ON workflow_work_items (collector_kind, status, updated_at DESC)
    WHERE collector_kind IN ('oci_registry', 'package_registry');
CREATE INDEX IF NOT EXISTS workflow_work_items_collector_scope_generation_updated_idx
    ON workflow_work_items (
        collector_kind,
        scope_id,
        generation_id,
        updated_at DESC,
        work_item_id ASC
    );
CREATE UNIQUE INDEX IF NOT EXISTS workflow_work_items_tfstate_candidate_nonterminal_idx
    ON workflow_work_items (
        collector_instance_id,
        scope_id,
        generation_id
    )
    WHERE collector_kind = 'terraform_state'
      AND generation_id LIKE 'terraform_state_candidate:%'
      AND status IN ('pending', 'claimed', 'failed_retryable', 'expired');
CREATE INDEX IF NOT EXISTS workflow_work_items_phase_tuple_idx
    ON workflow_work_items (run_id, scope_id, acceptance_unit_id, source_run_id, generation_id);
CREATE INDEX IF NOT EXISTS workflow_work_items_family_queue_depth_idx
    ON workflow_work_items (collector_kind, source_system, status)
    WHERE status IN ('pending', 'claimed', 'failed_retryable', 'expired');
CREATE INDEX IF NOT EXISTS workflow_work_items_fair_claim_idx
    ON workflow_work_items (
        collector_kind,
        collector_instance_id,
        status,
        claim_priority,
        (COALESCE(fairness_key, '')),
        (COALESCE(visible_at, created_at)),
        created_at,
        work_item_id
    );

CREATE TABLE IF NOT EXISTS workflow_claims (
    claim_id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL REFERENCES workflow_work_items(work_item_id) ON DELETE CASCADE,
    fencing_token BIGINT NOT NULL,
    owner_id TEXT NOT NULL,
    status TEXT NOT NULL,
    claimed_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NULL,
    failure_class TEXT NULL,
    failure_message TEXT NULL,
    failure_operation TEXT NULL,
    failure_status_code INTEGER NULL,
    failure_terminal BOOLEAN NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (work_item_id, fencing_token)
);
CREATE INDEX IF NOT EXISTS workflow_claims_active_expiry_idx
    ON workflow_claims (status, lease_expires_at ASC);
CREATE INDEX IF NOT EXISTS workflow_claims_work_item_idx
    ON workflow_claims (work_item_id, updated_at DESC);
