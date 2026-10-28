// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore

// claimNextQuery is the priority-then-rotation fair claim selector,
// ported from the in-tree statement. Selection, fencing increment, and
// claim-row insert stay one atomic statement: concurrent claimers serialize
// on the picked item row and SKIP LOCKED sheds same-head collisions.
//
// $1 collector_kind, $2 collector_instance_id, $3 now (visibility, lease
// base, stamps), $4 owner_id, $5 claim_id, $6 lease_expires_at, $7 draw
// salt (empty draws server-side randomness per head row).
const claimNextQuery = `
WITH RECURSIVE keys AS (
    (SELECT claim_priority, COALESCE(fairness_key, '') AS fairness_key
    FROM workflow_work_items
    WHERE collector_kind = $1
      AND collector_instance_id = $2
      AND status = 'pending'
      AND source_system <> ''
      AND acceptance_unit_id <> ''
      AND source_run_id <> ''
      AND generation_id <> ''
      AND (visible_at IS NULL OR visible_at <= $3)
    ORDER BY claim_priority, COALESCE(fairness_key, '') LIMIT 1)
    UNION ALL
    (SELECT s.claim_priority, s.fairness_key FROM keys k, LATERAL (
        SELECT i.claim_priority, COALESCE(i.fairness_key, '') AS fairness_key
        FROM workflow_work_items AS i
        WHERE i.collector_kind = $1
          AND i.collector_instance_id = $2
          AND i.status = 'pending'
          AND i.source_system <> ''
          AND i.acceptance_unit_id <> ''
          AND i.source_run_id <> ''
          AND i.generation_id <> ''
          AND (i.visible_at IS NULL OR i.visible_at <= $3)
          AND (i.claim_priority, COALESCE(i.fairness_key, '')) > (k.claim_priority, k.fairness_key)
        ORDER BY i.claim_priority, COALESCE(i.fairness_key, '') LIMIT 1) s)
),
heads AS (
    SELECT k.claim_priority, k.fairness_key,
        (SELECT i.work_item_id
        FROM workflow_work_items AS i
        WHERE i.collector_kind = $1
          AND i.collector_instance_id = $2
          AND i.status = 'pending'
          AND i.source_system <> ''
          AND i.acceptance_unit_id <> ''
          AND i.source_run_id <> ''
          AND i.generation_id <> ''
          AND (i.visible_at IS NULL OR i.visible_at <= $3)
          AND i.claim_priority = k.claim_priority
          AND COALESCE(i.fairness_key, '') = k.fairness_key
        ORDER BY COALESCE(i.visible_at, i.created_at), i.created_at, i.work_item_id LIMIT 1) AS work_item_id
    FROM keys k
),
ordered AS (
    SELECT work_item_id, claim_priority,
        encode(sha256(convert_to(fairness_key || COALESCE(NULLIF($7, ''), random()::text), 'UTF8')), 'hex') AS draw
    FROM heads WHERE work_item_id IS NOT NULL
),
pick AS (
    SELECT work_item_id FROM ordered
    ORDER BY claim_priority, draw
    LIMIT 1
),
candidate AS (
    SELECT item.work_item_id
    FROM workflow_work_items AS item
    WHERE item.work_item_id = (SELECT work_item_id FROM pick)
      AND item.collector_kind = $1
      AND item.collector_instance_id = $2
      AND item.status = 'pending'
      AND item.source_system <> ''
      AND item.acceptance_unit_id <> ''
      AND item.source_run_id <> ''
      AND item.generation_id <> ''
      AND (item.visible_at IS NULL OR item.visible_at <= $3)
    FOR UPDATE OF item SKIP LOCKED
),
claimed_item AS (
    UPDATE workflow_work_items AS item
    SET status = 'claimed',
        attempt_count = item.attempt_count + 1,
        current_claim_id = $5,
        current_fencing_token = item.current_fencing_token + 1,
        current_owner_id = $4,
        lease_expires_at = $6,
        last_claimed_at = $3,
        updated_at = $3
    FROM candidate
    WHERE item.work_item_id = candidate.work_item_id
    RETURNING
        item.work_item_id,
        item.run_id,
        item.collector_kind,
        item.collector_instance_id,
        item.source_system,
        item.scope_id,
        item.tenant_id,
        item.workspace_id,
        item.subject_class,
        item.policy_revision_hash,
        item.acceptance_unit_id,
        item.source_run_id,
        item.generation_id,
        COALESCE(item.fairness_key, '') AS fairness_key,
        item.claim_priority,
        COALESCE(item.claim_ecosystem, '') AS claim_ecosystem,
        item.status,
        item.attempt_count,
        COALESCE(item.current_claim_id, '') AS current_claim_id,
        item.current_fencing_token,
        COALESCE(item.current_owner_id, '') AS current_owner_id,
        item.lease_expires_at,
        item.created_at,
        item.updated_at
),
inserted_claim AS (
    INSERT INTO workflow_claims (
        claim_id,
        work_item_id,
        fencing_token,
        owner_id,
        status,
        claimed_at,
        heartbeat_at,
        lease_expires_at,
        created_at,
        updated_at
    )
    SELECT
        $5,
        work_item_id,
        current_fencing_token,
        $4,
        'active',
        $3,
        $3,
        $6,
        $3,
        $3
    FROM claimed_item
    RETURNING
        work_item_id,
        claim_id,
        fencing_token,
        owner_id,
        status,
        claimed_at,
        heartbeat_at,
        lease_expires_at,
        created_at,
        updated_at
)
SELECT
    claimed_item.work_item_id,
    claimed_item.run_id,
    claimed_item.collector_kind,
    claimed_item.collector_instance_id,
    claimed_item.source_system,
    claimed_item.scope_id,
    claimed_item.tenant_id,
    claimed_item.workspace_id,
    claimed_item.subject_class,
    claimed_item.policy_revision_hash,
    claimed_item.acceptance_unit_id,
    claimed_item.source_run_id,
    claimed_item.generation_id,
    claimed_item.fairness_key,
    claimed_item.claim_priority,
    claimed_item.claim_ecosystem,
    claimed_item.status,
    claimed_item.attempt_count,
    claimed_item.current_claim_id,
    claimed_item.current_fencing_token,
    claimed_item.current_owner_id,
    claimed_item.lease_expires_at,
    claimed_item.created_at,
    claimed_item.updated_at,
    inserted_claim.claim_id,
    inserted_claim.fencing_token,
    inserted_claim.owner_id,
    inserted_claim.status,
    inserted_claim.claimed_at,
    inserted_claim.heartbeat_at,
    inserted_claim.lease_expires_at,
    inserted_claim.created_at,
    inserted_claim.updated_at
FROM claimed_item
JOIN inserted_claim ON inserted_claim.work_item_id = claimed_item.work_item_id
`

// heartbeatClaimQuery renews the lease on the live epoch. The in-tree
// tenant-grant EXISTS block is deliberately absent: this database owns no
// grant tables, and grant authority moves to the commit pin plus core
// admission. Fencing (claim id, token, owner, live status) is the gate.
//
// $1 now, $2 lease_expires_at, $3 fencing_token, $4 owner_id, $5 claim_id,
// $6 work_item_id.
const heartbeatClaimQuery = `
WITH candidate AS (
    SELECT item.work_item_id
    FROM workflow_work_items AS item
    JOIN workflow_claims AS claim
      ON claim.claim_id = $5
     AND claim.work_item_id = item.work_item_id
    WHERE item.work_item_id = $6
      AND item.current_claim_id = $5
      AND item.current_fencing_token = $3
      AND item.current_owner_id = $4
      AND item.status = 'claimed'
      AND claim.fencing_token = $3
      AND claim.owner_id = $4
      AND claim.status = 'active'
    FOR UPDATE OF item, claim
),
updated_claim AS (
    UPDATE workflow_claims AS claim
    SET heartbeat_at = $1,
        lease_expires_at = $2,
        updated_at = $1
    FROM candidate
    WHERE claim.claim_id = $5
      AND claim.work_item_id = candidate.work_item_id
    RETURNING claim.claim_id
)
UPDATE workflow_work_items AS item
SET lease_expires_at = $2,
    updated_at = $1
FROM candidate
WHERE item.work_item_id = candidate.work_item_id
  AND EXISTS (SELECT 1 FROM updated_claim)
`

// completeClaimQuery finishes the live epoch and its work item. Same
// grant-join removal as the heartbeat. $7..$10 carry the optional resolved
// phase identity (blank keeps the planned identity).
const completeClaimQuery = `
WITH candidate AS (
    SELECT item.work_item_id
    FROM workflow_work_items AS item
    JOIN workflow_claims AS claim
      ON claim.claim_id = $5
     AND claim.work_item_id = item.work_item_id
    WHERE item.work_item_id = $6
      AND item.current_claim_id = $5
      AND item.current_fencing_token = $3
      AND item.current_owner_id = $4
      AND item.status = 'claimed'
      AND claim.fencing_token = $3
      AND claim.owner_id = $4
      AND claim.status = 'active'
    FOR UPDATE OF item, claim
),
updated_claim AS (
    UPDATE workflow_claims AS claim
    SET status = 'completed',
        finished_at = $1,
        updated_at = $1
    FROM candidate
    WHERE claim.claim_id = $5
      AND claim.work_item_id = candidate.work_item_id
    RETURNING claim.claim_id
)
UPDATE workflow_work_items AS item
SET status = 'completed',
    scope_id = COALESCE(NULLIF($7, ''), item.scope_id),
    acceptance_unit_id = COALESCE(NULLIF($8, ''), item.acceptance_unit_id),
    source_run_id = COALESCE(NULLIF($9, ''), item.source_run_id),
    generation_id = COALESCE(NULLIF($10, ''), item.generation_id),
    current_claim_id = NULL,
    current_owner_id = NULL,
    lease_expires_at = $2,
    last_completed_at = $1,
    updated_at = $1,
    last_failure_class = NULL,
    last_failure_message = NULL,
    last_failure_operation = NULL,
    last_failure_status_code = NULL,
    last_failure_terminal = NULL
FROM candidate
WHERE item.work_item_id = candidate.work_item_id
  AND EXISTS (SELECT 1 FROM updated_claim)
`
