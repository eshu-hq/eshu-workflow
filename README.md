# eshu-workflow

Status: pre-release. Both services build, run, and pass their test
suites, but no production Eshu system sends them traffic yet.
[docs/version-matrix.md](docs/version-matrix.md) records that there's no
production pairing of core and this service in service. Treat this repo
as ready to exercise, not production-accepted.

These are two small services that decide who may work on what, and when,
for Eshu's collectors. They sit behind one versioned HTTP protocol so no
other service ever touches their database tables.

A few terms in plain language. A collector is a program that gathers
facts (typed records) about a system. A claim is one worker taking
exclusive ownership of one work item for a limited time, and that time
limit is its lease. If the worker dies, the lease expires and someone
else can claim the item. A run is one planned batch of work items for a
collector.

The repo ships two binaries. `claim-plane` is the stateful one: it
serves the protocol from its own Postgres database, expires stale claims
on a timer, and exposes health and metrics on a separate admin port.
`workflow-coordinator` is the planner: it holds no database handle at
all, reconciles the desired collector instances, and only plans work
when you switch it on.

```mermaid
flowchart LR
    COORD["workflow-coordinator<br/>(this repo: plans runs)"] -->|"control routes:<br/>create runs, enqueue items"| CP["claim-plane<br/>(this repo: owns claim state)"]
    CP --> DB[("Its own<br/>Postgres database")]
    W["Collector workers<br/>(via Eshu core)"] -->|"claim · heartbeat · complete"| CP
    CORE["Eshu core"] -->|"report progress,<br/>read run status"| CP
    CP -.->|"optional, off unless configured:<br/>forward fact commits"| CORE
```

The coordinator decides what work should exist, the claim-plane records
it and leases it out item by item, and core reports progress and reads
status back. The dashed arrow stays off until you configure a core URL,
and while it's off that one route fails closed.

## When it fits, and when it doesn't

Use this repo when you need Eshu's work-planning and claim behaviour as
a separate service, or when you're building a client against the
`workflow.v1` protocol.

Don't read its database from another service, and don't import Eshu core
internals here. That boundary is enforced by test (see
[AGENTS.md](AGENTS.md)). And if you're wondering whether it's safe in
production today: it's pre-release, it defaults to doing nothing, and it
has no production pairing yet.

## Shortest path in

You'll need Go 1.26.10 and a Postgres database the claim-plane can own.
It runs its own fresh-install migrations from `store/migrations` at
startup.

```sh
# 1. Claim-plane: protocol on :8080, admin on :8081.
CLAIMPLANE_DATABASE_URL='postgres://user:pass@localhost:5432/claimplane?sslmode=disable' \
  go run ./cmd/claim-plane

# 2. Coordinator, in another shell. This turns planning fully on;
#    leave the mode and claims flags off and it starts dark instead.
go run ./cmd/workflow-coordinator \
  --claim-plane-url http://localhost:8080 \
  --deployment-mode active --claims-enabled \
  --instances-file instances.json
```

Check either process with `curl localhost:8081/healthz` (or `:8082` for
the coordinator). [Reference](docs/reference/README.md) lists every
flag, and [Operate it](docs/guides/operate-it.md) covers the container
run, health checks, and the off-by-default switches.

## Documentation

| Doc | What's in it |
| --- | --- |
| [Docs index](docs/README.md) | Every docs section, grouped |
| [Getting started](docs/getting-started/README.md) | Run both binaries locally and check health |
| [Guides](docs/guides/README.md) | Operate it (probes, gated defaults, troubleshooting); build a client |
| [Concepts](docs/concepts/README.md) | The protocol, the claim lifecycle, and the boundaries |
| [Reference](docs/reference/README.md) | Every flag and environment fallback for both binaries, plus the instances file |
| [Diagrams](docs/diagrams/README.md) | Every diagram, each with a plain-text version |
| [Version matrix](docs/version-matrix.md) | Which core and service versions may talk, with dated pairing snapshots |
| [Vulnerability response](docs/vuln-response.md) | How vulnerabilities in this service get found, owned, and handled |
| [Contributing](CONTRIBUTING.md) | Local gates and CI lanes |

## Related repos

Eshu core (`eshu-hq/eshu`) owns the collectors, the grants, and the fact
commit path this service plans work for. Core talks to this repo only
through the `workflow.v1` protocol, never through shared tables.

## Development

Run `make ci` before you push. It covers lint, tests, and the race suite
with its coverage floor, and the tests need a Postgres you can point
them at. [Contributing](docs/contributing/README.md) and
[CONTRIBUTING.md](CONTRIBUTING.md) have the details.

## Contributing, security, and licence

See [CONTRIBUTING.md](CONTRIBUTING.md). The licence is MIT, in
[LICENSE](LICENSE). There's no standalone `SECURITY.md`; vulnerability
handling lives in [docs/vuln-response.md](docs/vuln-response.md).
