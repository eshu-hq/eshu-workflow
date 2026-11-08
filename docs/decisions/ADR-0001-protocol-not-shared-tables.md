# ADR-0001: Protocol, not shared tables

## Status

Accepted. Recorded in [workflowv1/doc.go](../../workflowv1/doc.go)
("Shared physical tables are not part of this contract") and in
[AGENTS.md](../../AGENTS.md) ("No shared tables: every cross-service
read or write travels through a `workflowv1` route"). Cutover issue:
`eshu-hq/eshu#8875` (part of `#6707`).

## Context

Run planning and claim state used to live inside Eshu core and share
core's database tables. Moving the job to its own services only helps
if the boundary is real — shared tables would keep the coupling and
just move the code.

## Decision

The claim-plane owns its state in its own Postgres database. Every
cross-service read or write travels through one versioned HTTP
protocol, `workflow.v1` (`RouteTable` in `workflowv1/http.go`). No
service imports Eshu core internals or reads another service's tables;
a boundary test enforces stdlib-only imports in the protocol package.
Closed enums reject unknowns, so a new member is a major protocol bump.

## Consequences

- Core and this service version independently, paired only by protocol
  major — see [version matrix](../version-matrix.md).
- Report and read routes exist specifically to replace table reads
  core used to do directly.
- Anything not expressible as a route doesn't cross the boundary.
  Grants, for example, stay core-owned (see [Concepts](../concepts/README.md)).
