-- SPDX-License-Identifier: MIT
-- Copyright (c) 2025-2026 eshu-hq

-- Pacing latch and quota windows: fresh-install equivalent of eshu-main
-- migrations 205 (claim-pacing latch) and 206 (component quota windows).
-- Rows appear only after a first engagement, so no backfill is needed.

CREATE TABLE IF NOT EXISTS workflow_claim_pacing (
    signal TEXT NOT NULL,
    domain TEXT NOT NULL,
    engaged BOOLEAN NOT NULL DEFAULT FALSE,
    engaged_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (signal, domain)
);

CREATE TABLE IF NOT EXISTS component_quota_windows (
    component TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    grants INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (component, scope_id, window_start)
);
