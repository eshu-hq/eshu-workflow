# Operate it

## Running it

[Getting started](../getting-started/README.md) has the shortest local
run. Both binaries accept `--version` (prints the protocol major `1`,
exits), log JSON to stdout, and shut down cleanly on SIGINT or SIGTERM
with a 10-second window for in-flight requests. Exit codes match on
both: `2` for bad flags, `1` for a failed run, `0` for a clean
shutdown or `--version`.

One container image ships both binaries. The
[Dockerfile](../../Dockerfile) builds static binaries into a distroless
image with no shell, running as `nonroot`. The default entrypoint is
`/claim-plane`; override it to `/workflow-coordinator` to run the
planner from the same image. Ports 8080, 8081, and 8082 are exposed.

A deployment supplies two things: a Postgres database plus
`CLAIMPLANE_DATABASE_URL` for the claim-plane, and a claim-plane URL
(plus usually an instances file) for the coordinator. Pin deploys by
image digest. [Version matrix](../version-matrix.md) notes why, plus
the one pairing rule that matters: both sides must speak the same
protocol major.

## Off by default, on purpose

Two switches must both be on before the coordinator plans any work,
and both default to off. Check here first when a fresh deployment
looks idle.

- **Deployment mode** defaults to `dark`: the coordinator reconciles
  instance state but plans nothing, admits nothing, and doesn't reap.
  `--deployment-mode active` turns on run reconciliation and the reap loop.
- **Claims** default to disabled on top of that. Even in active mode,
  planning and admission need `--claims-enabled`, and then only for an
  instance that is itself `enabled` with `claims_enabled: true`.
- **`CommitClaimFacts`** on the claim-plane stays dark until
  `--core-commit-url` (or `CLAIMPLANE_CORE_COMMIT_URL`) names core.
  While unset, that route fails with a 500 that is never a success
  acknowledgement. A malformed non-empty URL is a startup error instead.

## Health, readiness, and metrics

Each binary serves `/healthz`, `/readyz`, and `/metrics`
(Prometheus-format) on its admin listener — `:8081` claim-plane,
`:8082` coordinator. The semantics differ, and it matters for probes:

- `/healthz` on both only proves the process is up. Liveness only.
- `/readyz` on the claim-plane checks its database.
- `/readyz` on the coordinator checks that the claim-plane answers (a
  queue-depth snapshot). Plane down → 503 while `/healthz` stays 200;
  it recovers on its own once the plane answers.

## Troubleshooting

- **Nothing gets planned:** work the gates in order —
  `--deployment-mode active`? `--claims-enabled`? Instance `enabled`
  with `claims_enabled: true`? Non-empty `targets` list?
- **Claim-plane won't start:** missing `--database-url`, non-positive
  `--reap-interval`/`--reap-limit`, and malformed `--core-commit-url`
  all fail at startup with exit `2` and a JSON log line naming the
  problem. Past that, confirm the database exists and is reachable.
- **Coordinator won't start:** missing `--claim-plane-url` fails flag
  validation; bad instance entries fail with the entry's index.
- **Coordinator not-ready:** it's telling you the claim-plane isn't
  answering. Check the claim-plane and its `/readyz` first.
- **`CommitClaimFacts` returns 500:** the route is dark, not a core
  outage. Set `--core-commit-url` if you want the pass-through.
- **Work looks double-queued:** that can be real today. The
  duplicate-plan guard compares whole target sets only, so partially
  overlapping sets can queue items twice — documented in
  [Architecture](../architecture/README.md).
