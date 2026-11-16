# Concepts

Plain-language terms first. A **collector** gathers facts (typed
records) about a system. A **claim** is one worker taking exclusive
ownership of one work item for a limited time — that limit is its
**lease**. If the worker dies, the lease expires and someone else can
claim the item. A **run** is one planned batch of work items for a collector.

## The story

Eshu runs many collectors on schedules. Something has to plan their
runs, hand out work items one at a time, notice when a worker has gone
quiet, and report how far each run got. That job used to live inside
Eshu core and share core's database tables. This repo moves it behind
a protocol instead: the claim-plane owns the state in its own Postgres
database, the coordinator plans work by calling it over HTTP, and core
talks to both only through that same protocol. Shared physical tables
are explicitly not part of the contract, as
[workflowv1/doc.go](../../workflowv1/doc.go) puts it.

A run becomes work items, a worker leases one item at a time and keeps
the lease alive with heartbeats, and the reaper hands the item back
out if the lease lapses. Before committing, the worker pins the claim
so an expiring lease can't kill it mid-commit. That is the whole
shape. Diagrams: [Diagrams](../diagrams/README.md).

## The protocol

One protocol, `workflow.v1` (major version 1), shared by this service
and its callers in core. Every route is a `POST` with a JSON body,
defined in `RouteTable` in
[workflowv1/http.go](../../workflowv1/http.go) — 21 routes in four groups:

| Group | Routes | What they're for |
| --- | --- | --- |
| Claim (9) | `ClaimNext`, `CheckClaimPacing`, `HeartbeatClaim`, `CompleteClaim`, `ReleaseClaim`, `FailClaimRetryable`, `FailClaimTerminal`, `ValidateAndPinClaimForCommit`, `CommitClaimFacts` | Lease work out and settle it. The pin stops an expiring lease from killing a claim mid-commit. `CommitClaimFacts` is a byte-verbatim pass-through to core, dark unless configured. |
| Control (6) | `CreateRun`, `EnqueueWorkItems`, `ReconcileCollectorInstances`, `UpsertCompletenessStates`, `ReconcileWorkflowRuns`, `ReapExpiredClaims` | The coordinator's seams for creating and maintaining work. |
| Report (1) | `ReportRunProgress` | Core pushes run progress the service may no longer read from core tables. |
| Read (5) | `GetRunStatus`, `ListRuns`, `GetRunCompleteness`, `ListCollectorInstances`, `QueueDepthSnapshot` | Status reads that replace direct table access. |

## Boundaries

- **No shared tables, ever.** Don't read this database from another
  service, and don't import Eshu core internals here. A test enforces it.
- **Closed enums reject unknowns** — adding one is a major protocol
  change. `CollectorKind` stays open but never blank.
- **Grants stay core-owned.** Heartbeat/complete check fencing only;
  permission decisions happen at the commit pin plus core admission.
- **Pre-release.** Both services build, run, and pass their suites, but
  there's no production pairing yet ([version matrix](../version-matrix.md)).
