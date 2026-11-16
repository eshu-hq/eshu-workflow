# Diagrams

Every diagram for eshu-workflow lives on this page. Other pages link
here instead of embedding their own copies. The repo
[README](../../README.md) keeps the first diagram only.

## Where the services sit

The coordinator decides what work should exist, the claim-plane
records it and leases it out item by item, and core reports progress
and reads status back. The dashed arrow stays off until you configure
a core URL, and while it's off that one route fails closed.

```mermaid
flowchart LR
    COORD["workflow-coordinator<br/>(this repo: plans runs)"] -->|"control routes:<br/>create runs, enqueue items"| CP["claim-plane<br/>(this repo: owns claim state)"]
    CP --> DB[("Its own<br/>Postgres database")]
    W["Collector workers<br/>(via Eshu core)"] -->|"claim · heartbeat · complete"| CP
    CORE["Eshu core"] -->|"report progress,<br/>read run status"| CP
    CP -.->|"optional, off unless configured:<br/>forward fact commits"| CORE
```

Text version: The workflow-coordinator calls the claim-plane's control
routes to create runs and enqueue items. The claim-plane stores state
in its own Postgres database. Collector workers (via core) claim,
heartbeat, and complete work against the claim-plane. Core reports
progress and reads run status. Optionally — off unless configured —
the claim-plane forwards fact commits back to core.

## Inside the repo

The claim-plane is the only process that touches the database,
including the background reaper.

```mermaid
flowchart TB
    subgraph repo["eshu-workflow"]
        COORD["workflow-coordinator<br/>plans runs, no database"]
        CP["claim-plane<br/>serves workflow.v1, owns claim state"]
        REAP["background reaper<br/>expires stale claims, reconciles runs"]
    end
    DB[("claim-plane's own<br/>Postgres database")]
    CORE["Eshu core<br/>workers, progress, status reads"]
    COORD -->|"control routes over HTTP"| CP
    CP --> DB
    REAP --> DB
    CORE -->|"claim, report, and read routes"| CP
    CP -.->|"CommitClaimFacts pass-through,<br/>off unless configured"| CORE
```

Text version: Inside eshu-workflow, the coordinator (no database)
calls the claim-plane's control routes over HTTP. The claim-plane and
its background reaper (expires stale claims, reconciles runs) use the
claim-plane's own Postgres. Core uses the claim, report, and read
routes. The `CommitClaimFacts` pass-through back to core is off unless
configured.

## One claim, start to finish

A worker leases one item at a time and keeps the lease alive with
heartbeats. Before committing, it pins the claim so an expiring lease
can't kill it mid-commit.

```mermaid
sequenceDiagram
    participant C as workflow-coordinator
    participant P as claim-plane
    participant W as Collector worker (via core)
    participant E as Eshu core
    C->>P: CreateRun, then EnqueueWorkItems
    W->>P: ClaimNext
    P-->>W: work item + lease (or a pacing backoff)
    W->>P: HeartbeatClaim while it works
    alt worker stalls
        P->>P: lease expires, reaper requeues the item
    else worker finishes
        W->>P: ValidateAndPinClaimForCommit
        W->>P: CommitClaimFacts (forwarded to core if configured)
        P-->>E: fact bytes, verbatim
        W->>P: CompleteClaim
    end
    E->>P: ReportRunProgress
    E->>P: GetRunStatus
```

Text version: (1) Coordinator calls `CreateRun`, then
`EnqueueWorkItems`. (2) Worker `ClaimNext` → work item + lease (or a
pacing backoff). (3) Worker heartbeats while working. (4a) If the
worker stalls, the lease expires and the reaper requeues the item.
(4b) If it finishes: `ValidateAndPinClaimForCommit`, then
`CommitClaimFacts` (forwarded verbatim to core if configured), then
`CompleteClaim`. (5) Core calls `ReportRunProgress` and `GetRunStatus`.
