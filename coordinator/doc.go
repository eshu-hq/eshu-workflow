// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

// Package coordinator ports the workflow coordinator service onto the
// merged claim plane: family planners, fairness-ordered scheduling, and
// the reconcile, reap, and run-reconciliation loops, all driven through
// the claim-plane HTTP API. The package never touches a database and
// never imports core internals: tenant and producer grants stay
// core-owned and arrive only as planner inputs, never as reads.
//
// Planning model: each schedulable collector instance declares its target
// list in its configuration document under "targets". Provider-specific
// target discovery stays in core; what crosses the service boundary is
// the declared list, planned into a deterministic run (idempotent by
// run id) and idempotent work items. The admission guard skips a plan
// when a non-terminal run already covers the same collector and scope
// set; whole-plan skips are exact, and partial overlap across different
// scope sets may double-queue (parity gap: the in-tree guard compares
// per-target tuples in SQL, and no item-listing route exists yet).
//
// Family isolation is a deliberate deviation from the in-tree runner: a
// planner that errors fails its family but never stops the pass for
// healthy families, so one sick family cannot starve the rest. The
// reconcile still returns the joined error so the operator sees it.
package coordinator
