# eshu-workflow

**Status: pre-release.** Both services build, run, and pass their test suites, but no production Eshu system sends them traffic yet — [docs/version-matrix.md](docs/version-matrix.md) records that there is no production core-and-this-service pair in service. Treat this repo as ready to exercise, not as production-accepted.

Two small services that decide *who may work on what, and when* for Eshu's collectors — kept behind one versioned HTTP protocol so no other service ever touches their database tables.

Key terms, plainly: a *collector* is a program that gathers facts (typed records) about a system. A *claim* is one worker taking exclusive ownership of one work item for a limited time; that time limit is its *lease*. If the worker dies, the lease expires and the item can be claimed again. A *run* is one planned batch of work items for a collector.

## What this is and why it exists

Eshu runs many collectors on schedules. Something must plan their runs, hand out work items one at a time, notice when a worker has gone silent, and report how far each run got. That job used to live inside Eshu core, sharing core's database tables. This repo moves it behind a protocol: the claim service owns the state in its own Postgres database, the coordinator plans work by calling that service over HTTP, and core talks to both only through the same protocol. Shared physical tables are explicitly not part of the contract ([workflowv1/doc.go](workflowv1/doc.go)).

The repo ships two binaries:

- **`claim-plane`** — the stateful service. It serves the protocol routes from its own Postgres database, expires stale claims on a timer, and exposes health and metrics on a separate admin port.
- **`workflow-coordinator`** — the planner. It holds no database handle at all. It reconciles the desired set of collector instances and, only when switched on (see below), plans and admits runs through the claim-plane HTTP API.

## Where it fits

```mermaid
flowchart LR
    COORD["workflow-coordinator<br/>(this repo: plans runs)"] -->|"control routes:<br/>create runs, enqueue items"| CP["claim-plane<br/>(this repo: owns claim state)"]
    CP --> DB[("Its own<br/>Postgres database")]
    W["Collector workers<br/>(via Eshu core)"] -->|"claim · heartbeat · complete"| CP
    CORE["Eshu core"] -->|"report progress,<br/>read run status"| CP
    CP -.->|"optional, off unless configured:<br/>forward fact commits"| CORE
```

In words: the coordinator decides what work should exist; the claim-plane records it and leases it out item by item; workers claim and complete items; core reports progress and reads status back. The dashed arrow is a pass-through that stays off until an operator configures a core URL — with it off, that one route fails closed.

## When to use it, and when not to

Use this repo when you need Eshu's work-planning and claim behavior as a separate service, or when you are building a client against the `workflow.v1` protocol.

Do not read its database from another service, and do not import Eshu core internals here — the boundary is enforced by test ([AGENTS.md](AGENTS.md)). If you just need to know whether this is safe to run in production today: it is pre-release, defaults to doing nothing (dark mode, below), and has no production pairing yet.

## Quickstart

Requires Go 1.26.10 (the `go` line in [go.mod](go.mod)) and a Postgres database the claim-plane can own; it runs its own fresh-install migrations from `store/migrations` at startup.

```sh
# 1. Claim-plane: protocol on :8080, admin on :8081.
CLAIMPLANE_DATABASE_URL='postgres://user:pass@localhost:5432/claimplane?sslmode=disable' \
  go run ./cmd/claim-plane --listen :8080 --admin-listen :8081

# 2. Coordinator, in another shell. This turns planning fully on;
#    omit the mode/claims flags and it starts dark (the default).
go run ./cmd/workflow-coordinator \
  --claim-plane-url http://localhost:8080 \
  --admin-listen :8082 \
  --deployment-mode active --claims-enabled \
  --instances-file instances.json
```

Check either process with `curl localhost:8081/healthz` (or `:8082` for the coordinator). Both binaries accept `--version`, which prints the protocol major (`1`) and exits. Both log JSON to stdout and shut down cleanly on SIGINT/SIGTERM. Exit codes: `2` for bad flags, `1` for a failed run, `0` for clean shutdown.

## How it works

### The protocol (`workflowv1`)

One protocol, `workflow.v1` (major version 1), shared by this service and its callers in Eshu core. All routes are `POST` with a JSON body, defined in one table ([workflowv1/http.go](workflowv1/http.go), `RouteTable` — 21 routes):

| Group | Routes | Purpose |
| --- | --- | --- |
| Claim (9) | `ClaimNext`, `CheckClaimPacing`, `HeartbeatClaim`, `CompleteClaim`, `ReleaseClaim`, `FailClaimRetryable`, `FailClaimTerminal`, `ValidateAndPinClaimForCommit`, `CommitClaimFacts` | Lease work out and settle it. The pin stops an expiring lease from killing a claim mid-commit; `CommitClaimFacts` is a byte-verbatim pass-through to core, dark unless configured. |
| Control (6) | `CreateRun`, `EnqueueWorkItems`, `ReconcileCollectorInstances`, `UpsertCompletenessStates`, `ReconcileWorkflowRuns`, `ReapExpiredClaims` | The coordinator's seams for creating and maintaining work. |
| Report (1) | `ReportRunProgress` | Core pushes run progress the service may no longer read from core tables. |
| Read (5) | `GetRunStatus`, `ListRuns`, `GetRunCompleteness`, `ListCollectorInstances`, `QueueDepthSnapshot` | Status reads that replace direct table access. |

The [client](client/doc.go) package drives every route over HTTP; callers never need the database. Closed string sets in the protocol reject unknown values, so adding one is a major protocol change; `CollectorKind` is the exception — open so collector families can grow, but never blank. A caller speaking a different protocol major is rejected (see [docs/version-matrix.md](docs/version-matrix.md)).

### The claim-plane service

`cmd/claim-plane` serves the routes from its own Postgres store ([store](store/doc.go), via `pgx`). Heartbeat and complete check *fencing* only — proof the caller still owns the claim; permission decisions stay in core, enforced at the commit pin plus core's own admission. A background reaper expires stale claims (skipping pinned ones) and reconciles finished runs on a tick. `ClaimNext` can also *pace* workers: optional per-family and global pending-work limits tell a worker to back off instead of claiming more.

### The coordinator

`cmd/workflow-coordinator` reconciles desired collector instances against the claim-plane. Each instance declares its planned targets in the `targets` list of its configuration document:

```json
[{"instance_id": "jira-primary", "collector_kind": "jira", "mode": "scheduled",
  "enabled": true, "claims_enabled": true,
  "configuration": "{\"targets\":[{\"scope_id\":\"scope-a\"}]}"}]
```

Planning is deterministic (same inputs, same run ID, so replays converge), and a planner error fails only its own collector family, never the whole pass. One known simplification, documented in [coordinator/doc.go](coordinator/doc.go): the duplicate-plan guard compares whole target sets only, so partially overlapping sets can queue some work twice.

## Off by default, on purpose

Two separate switches must both be on before any work is planned, and both default to off:

- **Deployment mode** defaults to `dark`. In dark mode the coordinator still reconciles instance state, but it plans nothing, admits nothing, and does not reap. `--deployment-mode active` turns on reaping and run reconciliation.
- **Claims** default to disabled. Even in active mode, planning and admission happen only with `--claims-enabled`, and per instance only when that instance is itself `enabled` with `claims_enabled: true`.

On the claim-plane side, the `CommitClaimFacts` pass-through stays dark until `--core-commit-url` (or the env var below) names core; while unset, that route fails with a 500 that is never a success acknowledgement. A malformed non-empty URL is a startup error.

## Configuration

### claim-plane

| Flag | Env fallback | Default | Meaning |
| --- | --- | --- | --- |
| `--listen` / `--admin-listen` | — | `:8080` / `:8081` | Protocol listener / `/healthz`, `/readyz`, `/metrics` listener |
| `--database-url` | `CLAIMPLANE_DATABASE_URL` | *required* | Postgres DSN for its own database |
| `--core-commit-url` | `CLAIMPLANE_CORE_COMMIT_URL` | unset (route dark) | Core base URL for the commit pass-through |
| `--reap-interval`, `--reap-limit`, `--requeue-delay` | — | protocol defaults | Stale-claim expiry cadence, per-tick cap, and re-visibility delay |
| `--quotas` | — | none | Per-kind hourly quotas as `kind=n,kind=n` |
| `--pacing-*` (7 flags) | — | `0` (off) | Pending-work watermarks and backoff for pacing |
| `--version` | — | — | Print `claim-plane 1` and exit |

### workflow-coordinator

| Flag | Env fallback | Default | Meaning |
| --- | --- | --- | --- |
| `--claim-plane-url` | `WORKFLOW_COORDINATOR_CLAIM_PLANE_URL` | *required* | Claim-plane base URL |
| `--admin-listen` | — | `:8082` | `/healthz`, `/readyz`, `/metrics` listener |
| `--deployment-mode` | — | `dark` | `dark` or `active` (see above) |
| `--claims-enabled` | — | `false` | Plan and admit work in active mode |
| `--instances-file` | — | none (empty set) | JSON file of desired collector instances, as shown above |
| Timing flags (`--reconcile-interval`, `--run-reconcile-interval`, `--reap-interval`, `--reap-limit`, `--requeue-delay`, `--scan-interval`, `--startup-attempts`, `--startup-interval`) | — | `0` selects built-in defaults | Loop timing and startup retry budget while the claim-plane is unreachable |
| `--version` | — | — | Print `workflow-coordinator 1` and exit |

Health semantics differ per binary: on the claim-plane, `/readyz` checks its database; on the coordinator, `/readyz` checks that the claim-plane answers. `/healthz` on both only proves the process is up. Metrics are Prometheus-format on `/metrics`.

## Deploy / run

One container image ships both binaries ([Dockerfile](Dockerfile): static, no shell, runs as `nonroot`). The default entrypoint is `/claim-plane`; override the entrypoint to `/workflow-coordinator` to run the planner from the same image. Ports 8080, 8081, and 8082 are exposed. Each deployment must supply: a Postgres database and `CLAIMPLANE_DATABASE_URL` for the claim-plane; a claim-plane URL (and usually an instances file) for the coordinator. Version pairing follows one rule — both sides must speak the same protocol major — recorded with dated snapshots in [docs/version-matrix.md](docs/version-matrix.md). Pin deploys by image digest, as that document notes.

## Eshu dependencies

| Direction | Dependency | Notes |
| --- | --- | --- |
| This repo → data | Its own Postgres database | Migrations in `store/migrations`, applied at startup. No other service's tables are read or written. |
| This repo → third party | `jackc/pgx/v5`, `prometheus/client_golang`, OpenTelemetry | Direct requirements in [go.mod](go.mod). |
| This repo → Eshu core | HTTP only, and optional | No core internals are imported (enforced by test). The one outbound call is the opt-in `CommitClaimFacts` pass-through. |
| Eshu core → this repo | The `workflow.v1` protocol | Core-side callers use the claim, report, and read routes instead of shared tables; tenant and producer grants stay core-owned. |

## Development

```sh
make ci   # lint + test + race with coverage floor
```

The test suite needs Postgres: set `CLAIMPLANE_TEST_DATABASE_URL` to override the per-package default test databases (`claimplane_test_store`, `claimplane_test_server`, `claimplane_test_coordinator`). CI provides a `postgres:18` container with those databases; locally you need the same. Lane details are in [CONTRIBUTING.md](CONTRIBUTING.md); repo rules are in [AGENTS.md](AGENTS.md).

## Docs and related repos

- [docs/version-matrix.md](docs/version-matrix.md) — which core and service versions may talk, with dated pairing snapshots.
- [docs/vuln-response.md](docs/vuln-response.md) — how vulnerabilities in this service are found and handled.
- [CONTRIBUTING.md](CONTRIBUTING.md) — local gates and CI lanes.
- Eshu core (`eshu-hq/eshu`) owns the collectors, the grants, and the fact commit path this service plans work for.

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md). Licensed MIT — see [LICENSE](LICENSE). There is no standalone `SECURITY.md`; vulnerability handling is in [docs/vuln-response.md](docs/vuln-response.md).
