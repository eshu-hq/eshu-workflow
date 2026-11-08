# Reference

Both binaries take flags. A couple of flags also read an environment
variable as their default, so an env var only counts when you don't
pass the flag. Everything below comes from `parseFlags` in
[cmd/claim-plane/main.go](../../cmd/claim-plane/main.go) and
[cmd/workflow-coordinator/main.go](../../cmd/workflow-coordinator/main.go).
If this page and the code ever disagree, believe the code.

## claim-plane

| Flag | Env fallback | Default | What it does |
| --- | --- | --- | --- |
| `--listen` | none | `:8080` | Protocol listener address |
| `--admin-listen` | none | `:8081` | Admin listener for `/healthz`, `/readyz`, and `/metrics` |
| `--database-url` | `CLAIMPLANE_DATABASE_URL` | required | Postgres DSN for its own database. Empty is a startup error |
| `--core-commit-url` | `CLAIMPLANE_CORE_COMMIT_URL` | unset | Core base URL for the `CommitClaimFacts` pass-through. Unset keeps that route dark |
| `--reap-interval` | none | protocol default | How often the reaper ticks. Must be positive |
| `--reap-limit` | none | protocol default | Claims reaped per tick. Must be positive |
| `--requeue-delay` | none | protocol default | How long a reaped item stays hidden before it can be claimed again |
| `--quotas` | none | empty | Per-kind hourly quotas, written as `kind=n,kind=n` |
| `--pacing-domain-high`, `--pacing-domain-low` | none | `0` | Per-family pending-work watermarks for pacing. `0` leaves pacing off |
| `--pacing-global-high`, `--pacing-global-low` | none | `0` | Global pending-work watermarks |
| `--pacing-grant-delay` | none | `0` | Backoff carried on paced grants |
| `--pacing-max-engagement` | none | `0` | Engagement bound before pacing reports a worker as stuck |
| `--pacing-depth-cache-ttl` | none | `0` | How long a queue-depth observation gets reused |
| `--version` | none | off | Print `claim-plane 1` and exit |

One sharp edge: a malformed non-empty `--core-commit-url` is a startup
error, by design. The service refuses to boot half-dark on a typo,
while an empty value simply keeps the route dark.

## workflow-coordinator

| Flag | Env fallback | Default | What it does |
| --- | --- | --- | --- |
| `--claim-plane-url` | `WORKFLOW_COORDINATOR_CLAIM_PLANE_URL` | required | Claim-plane base URL. Empty is a startup error |
| `--admin-listen` | none | `:8082` | Admin listener for `/healthz`, `/readyz`, and `/metrics` |
| `--deployment-mode` | none | `dark` | `dark` or `active`; see [Operate it](../guides/operate-it.md) |
| `--claims-enabled` | none | `false` | Plan and admit work in active mode |
| `--instances-file` | none | none | JSON file of desired collector instances. No file means an empty set |
| `--reconcile-interval` | none | `0` picks the built-in default | Instance reconciliation interval |
| `--startup-attempts`, `--startup-interval` | none | `0` picks the built-in default | Retries for the first pass while the claim-plane is unreachable, and their spacing |
| `--run-reconcile-interval` | none | `0` picks the built-in default | Drained-run reconciliation interval |
| `--reap-interval` | none | `0` picks the built-in default | Expired-claim reap interval |
| `--scan-interval` | none | `0` picks the built-in default | Deterministic plan-key window |
| `--reap-limit` | none | `0` picks the built-in default | Claims reaped per pass |
| `--requeue-delay` | none | `0` | How long a reaped item stays hidden before it can be claimed again |
| `--version` | none | off | Print `workflow-coordinator 1` and exit |

## The instances file

The coordinator reads its desired collector instances from one JSON
file. Each instance declares its planned targets inside its
`configuration` document, under `targets`. Provider-specific target
discovery stays in core; only the declared list crosses the boundary.

```json
[{"instance_id": "jira-primary", "collector_kind": "jira", "mode": "scheduled",
  "enabled": true, "claims_enabled": true,
  "configuration": "{\"targets\":[{\"scope_id\":\"scope-a\"}]}"}]
```

Every instance is validated as the file loads; one bad entry fails
startup with the entry's index in the error. Note the two per-instance
switches, `enabled` and `claims_enabled` — both must be true, on top of
the coordinator's own flags, before that instance gets work planned.
