# Architecture

The claim-plane is the only process that touches the database.
Everyone else — the coordinator included — goes through the HTTP
protocol, and the one outbound call back to core stays off until you
configure it. Overview diagram:
[Diagrams: Where the services sit](../diagrams/README.md#where-the-services-sit).

## Inside the claim-plane

`cmd/claim-plane` serves the routes from its own Postgres store
([store](../../store/doc.go), via `pgx`). Migrations live in
`store/migrations` and apply at startup.

- Heartbeat and complete check fencing only — proof the caller still
  owns the claim. Permission decisions stay in core, enforced at the
  commit pin plus core's own admission.
- A background reaper expires stale claims on a tick (skipping pinned
  ones) and reconciles finished runs.
- `ClaimNext` can pace workers: optional per-family and global
  pending-work limits tell a worker to back off instead of claiming
  more. They default to off; [Reference](../reference/README.md) lists
  the flags.
- Health and metrics ride a separate admin port (`:8081`).

## Inside the coordinator

`cmd/workflow-coordinator` holds no database handle at all. It
reconciles desired collector instances (from the instances file)
against the claim-plane over the control routes.

- Planning is deterministic: same inputs, same run ID, so replays
  converge instead of piling up duplicates.
- A planner error fails only its own collector family, never the whole
  pass, so one sick family can't starve the rest.
- One known simplification, documented in
  [coordinator/doc.go](../../coordinator/doc.go): the duplicate-plan
  guard compares whole target sets only, so partially overlapping sets
  can queue some work twice. Know that before trusting queue-depth
  numbers to the item.

## Dependencies

The claim-plane needs its own Postgres database and nothing else's
tables. Direct third-party requirements in [go.mod](../../go.mod):
`jackc/pgx/v5`, `prometheus/client_golang`, and OpenTelemetry. No core
internals are imported anywhere, and a test enforces that. The only
call out to core is the opt-in `CommitClaimFacts` pass-through. Going
the other way, core uses the claim, report, and read routes instead of
shared tables, while tenant and producer grants stay core-owned.
