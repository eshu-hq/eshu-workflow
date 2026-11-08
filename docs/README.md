# eshu-workflow docs

Welcome. This repo ships two small services — `claim-plane` (stateful,
owns claim state in its own Postgres) and `workflow-coordinator`
(stateless planner) — behind one versioned HTTP protocol, `workflow.v1`.
Start with [Getting started](getting-started/README.md) to run both
locally. Status: pre-release, no production pairing yet.

## Start here

| Section | What you'll find |
| --- | --- |
| [Getting started](getting-started/README.md) | Run the claim-plane, then the coordinator, and check health |
| [Guides](guides/README.md) | Operate it: container run, gated switches, probes, troubleshooting |
| [Concepts](concepts/README.md) | Claims, leases, runs, the protocol, and the no-shared-tables boundary |
| [Reference](reference/README.md) | Every flag and env fallback for both binaries, plus the instances file |
| [Architecture](architecture/README.md) | Inside the claim-plane and the coordinator; dependencies |
| [Diagrams](diagrams/README.md) | Every diagram in one place, each with a plain-text version |
| [Decisions](decisions/README.md) | Architecture decision records and how to add one |
| [Contributing](contributing/README.md) | `make ci`, the Postgres test setup, and CI lanes |

## Existing docs (kept where they are)

| Doc | What's in it |
| --- | --- |
| [Version matrix](version-matrix.md) | Which core and service versions may talk, with dated pairing snapshots |
| [Vulnerability response](vuln-response.md) | How vulnerabilities in this service get found, owned, and handled |
| [Contributing](../CONTRIBUTING.md) | Local gates and CI lanes (canonical) |
| [Agent rules](../AGENTS.md) | The non-negotiables: no shared tables, closed enums, TDD, file cap |

Back to the [repo README](../README.md).
