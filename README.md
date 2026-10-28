# eshu-workflow

Eshu workflow planning and scheduling. Pre-release: holds no cutover
authority until its reviewed cutover. Refs #8875, part of #6707.

## Protocol

`workflowv1` is the versioned workflow protocol (`workflow.v1`, major 1)
shared by this service and its callers in Eshu core. Three planes:

- Claim plane: `ClaimNext` (with inline pacing verdict),
  `CheckClaimPacing`, `HeartbeatClaim`, `CompleteClaim`, `ReleaseClaim`,
  `FailClaimRetryable`, `FailClaimTerminal`, and
  `ValidateAndPinClaimForCommit` — the server-side commit pin that
  replaces the in-transaction heartbeat, so the reaper cannot expire a
  claim before its fact-commit deadline.
- Report plane: `ReportRunProgress` — core pushes run progress the
  service may no longer read from core tables.
- Read plane: `GetRunStatus`, `ListRuns`, `GetRunCompleteness`,
  `ListCollectorInstances`, `QueueDepthSnapshot` — replacing direct
  table reads.

The service holds its own database. Shared physical tables are not part
of the contract. Closed string enums reject unknown members: adding one
is a major protocol change while any reader fails the read.
`CollectorKind` stays open (families grow without a major) but never
blank.

## Claim-plane server

`cmd/claim-plane` serves the 14 protocol routes over its own Postgres
database (`store`, pgx, fresh-install migrations in
`store/migrations`). Heartbeat and complete enforce fencing only: the
in-tree tenant-grant join is gone because grant tables stay core-owned
and grant authority moves to the commit pin plus core admission. An
internal reaper expires stale claims (skipping pinned ones) and
reconciles drained runs. Admin listener serves `/healthz`, `/readyz`,
and `/metrics`.

```sh
CLAIMPLANE_DATABASE_URL='postgres://user:pass@localhost:5432/claimplane?sslmode=disable' \
  go run ./cmd/claim-plane --listen :8080 --admin-listen :8081
```

Tests need Postgres: `CLAIMPLANE_TEST_DATABASE_URL` overrides the
per-package default databases (`claimplane_test_store`,
`claimplane_test_server`).

## Status

- [x] Stage 1a: protocol contract (`workflowv1`) with tests and CI.
- [x] Stage 1b: claim-plane server with its own database and migrations.
- [ ] Stage 1c: coordinator service port (planners, scheduling, report/read planes).
- [ ] Core client, extracted-path parity, ops-qa deploy, rollback
      rehearsal, production acceptance, in-tree removal.

## Verify

```sh
make ci   # lint + test + race with coverage floor
```
