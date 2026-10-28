// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore

// releaseClaimQuery gives up the live epoch without recording failure.
// $1 now, $2 lease_expires_at (NULL release), $3 fencing, $4 owner,
// $5 claim_id, $6 work_item_id.
const releaseClaimQuery = `
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
    SET status = 'released',
        finished_at = $1,
        updated_at = $1
    FROM candidate
    WHERE claim.claim_id = $5
      AND claim.work_item_id = candidate.work_item_id
    RETURNING claim.claim_id
)
UPDATE workflow_work_items AS item
SET status = 'pending',
    current_claim_id = NULL,
    current_owner_id = NULL,
    lease_expires_at = $2,
    visible_at = $1,
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

// failClaimRetryableQuery ends the epoch retryable and requeues the item.
const failClaimRetryableQuery = `
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
    SET status = 'failed_retryable',
        finished_at = $1,
        failure_class = NULLIF($7, ''),
        failure_message = NULLIF($8, ''),
        failure_operation = NULLIF($9, ''),
        failure_status_code = NULLIF($10, 0),
        failure_terminal = $11,
        updated_at = $1
    FROM candidate
    WHERE claim.claim_id = $5
      AND claim.work_item_id = candidate.work_item_id
    RETURNING claim.claim_id
)
UPDATE workflow_work_items AS item
SET status = 'pending',
    current_claim_id = NULL,
    current_owner_id = NULL,
    lease_expires_at = NULL,
    visible_at = $2,
    updated_at = $1,
    last_failure_class = NULLIF($7, ''),
    last_failure_message = NULLIF($8, ''),
    last_failure_operation = NULLIF($9, ''),
    last_failure_status_code = NULLIF($10, 0),
    last_failure_terminal = $11
FROM candidate
WHERE item.work_item_id = candidate.work_item_id
  AND EXISTS (SELECT 1 FROM updated_claim)
`

// failClaimTerminalQuery ends the epoch terminal without requeueing.
const failClaimTerminalQuery = `
WITH candidate AS (
    SELECT item.work_item_id
    FROM workflow_work_items AS item
    JOIN workflow_claims AS claim
      ON claim.claim_id = $4
     AND claim.work_item_id = item.work_item_id
    WHERE item.work_item_id = $5
      AND item.current_claim_id = $4
      AND item.current_fencing_token = $2
      AND item.current_owner_id = $3
      AND item.status = 'claimed'
      AND claim.fencing_token = $2
      AND claim.owner_id = $3
      AND claim.status = 'active'
    FOR UPDATE OF item, claim
),
updated_claim AS (
    UPDATE workflow_claims AS claim
    SET status = 'failed_terminal',
        finished_at = $1,
        failure_class = NULLIF($6, ''),
        failure_message = NULLIF($7, ''),
        failure_operation = NULLIF($8, ''),
        failure_status_code = NULLIF($9, 0),
        failure_terminal = $10,
        updated_at = $1
    FROM candidate
    WHERE claim.claim_id = $4
      AND claim.work_item_id = candidate.work_item_id
    RETURNING claim.claim_id
)
UPDATE workflow_work_items AS item
SET status = 'failed_terminal',
    current_claim_id = NULL,
    current_owner_id = NULL,
    lease_expires_at = NULL,
    updated_at = $1,
    last_failure_class = NULLIF($6, ''),
    last_failure_message = NULLIF($7, ''),
    last_failure_operation = NULLIF($8, ''),
    last_failure_status_code = NULLIF($9, 0),
    last_failure_terminal = $10
FROM candidate
WHERE item.work_item_id = candidate.work_item_id
  AND EXISTS (SELECT 1 FROM updated_claim)
`

// reapExpiredClaimsQuery expires stale active claims and requeues their
// work. The pin guard is the CommitPin contract: a claim whose
// commit_pinned_until still covers the reap clock is never expired,
// replacing the in-transaction heartbeat the in-tree path relied on.
const reapExpiredClaimsQuery = `
WITH candidate AS (
    SELECT
        claim.claim_id,
        claim.work_item_id,
        claim.fencing_token,
        claim.owner_id,
        claim.claimed_at,
        claim.heartbeat_at,
        claim.lease_expires_at,
        claim.created_at
    FROM workflow_claims AS claim
    JOIN workflow_work_items AS item
      ON item.work_item_id = claim.work_item_id
    WHERE claim.status = 'active'
      AND claim.lease_expires_at < $1
      AND (claim.commit_pinned_until IS NULL OR claim.commit_pinned_until <= $1)
      AND item.current_claim_id = claim.claim_id
      AND item.current_fencing_token = claim.fencing_token
      AND item.current_owner_id = claim.owner_id
      AND item.status = 'claimed'
    ORDER BY claim.lease_expires_at ASC, claim.claim_id ASC
    LIMIT $2
    FOR UPDATE OF claim, item SKIP LOCKED
),
updated_claim AS (
    UPDATE workflow_claims AS claim
    SET status = 'expired',
        finished_at = $1,
        updated_at = $1
    FROM candidate
    WHERE claim.claim_id = candidate.claim_id
    RETURNING
        candidate.claim_id,
        candidate.work_item_id,
        candidate.fencing_token,
        candidate.owner_id,
        'expired' AS status,
        candidate.claimed_at,
        candidate.heartbeat_at,
        candidate.lease_expires_at,
        candidate.created_at,
        $1 AS updated_at
)
,
updated_item AS (
UPDATE workflow_work_items AS item
SET status = 'pending',
    current_claim_id = NULL,
    current_owner_id = NULL,
    lease_expires_at = NULL,
    visible_at = $3,
    updated_at = $1
FROM candidate
WHERE item.work_item_id = candidate.work_item_id
  AND item.current_claim_id = candidate.claim_id
  AND item.current_fencing_token = candidate.fencing_token
  AND item.current_owner_id = candidate.owner_id
  AND item.status = 'claimed'
RETURNING item.work_item_id
)
SELECT
    updated_claim.claim_id,
    updated_claim.work_item_id,
    updated_claim.fencing_token,
    updated_claim.owner_id,
    updated_claim.status,
    updated_claim.claimed_at,
    updated_claim.heartbeat_at,
    updated_claim.lease_expires_at,
    updated_claim.created_at,
    updated_claim.updated_at
FROM updated_claim
JOIN updated_item ON updated_item.work_item_id = updated_claim.work_item_id
`
