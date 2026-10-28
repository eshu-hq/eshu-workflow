// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

// Package workflowv1 is the versioned workflow protocol shared by the
// eshu-workflow service and its callers in Eshu core.
//
// Protocol "workflow.v1" owns three planes: the claim plane (collectors
// claim, heartbeat, complete, release, or fail work; core pins a claim
// server-side before committing facts), the report plane (core pushes run
// progress the service may no longer read from core tables), and the read
// plane (run status, run lists, completeness, collector instances, and
// queue-depth snapshots that replace direct table reads).
//
// The service holds its own database. Shared physical tables are not part
// of this contract: every cross-service read or write travels through one
// of the operations in [RouteTable].
//
// Closed string enums reject unknown members: adding a member is a major
// protocol change while any reader fails the read. CollectorKind stays an
// open identifier because collector families grow independently.
package workflowv1
