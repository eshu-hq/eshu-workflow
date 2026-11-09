# Getting started with eshu-workflow

You'll need Go 1.26.10 and a Postgres database the claim-plane can
own. It runs its own fresh-install migrations from `store/migrations`
at startup, so you don't run migrations by hand.

## 1. Start the claim-plane

Protocol on `:8080`, admin (`/healthz`, `/readyz`, `/metrics`) on `:8081`:

```sh
CLAIMPLANE_DATABASE_URL='postgres://user:pass@localhost:5432/claimplane?sslmode=disable' \
  go run ./cmd/claim-plane
```

## 2. Start the coordinator

In another shell. This turns planning fully on; leave the mode and
claims flags off and it starts dark instead (see
[Operate it](../guides/operate-it.md) — off by default is on purpose).

```sh
go run ./cmd/workflow-coordinator \
  --claim-plane-url http://localhost:8080 \
  --deployment-mode active --claims-enabled \
  --instances-file instances.json
```

An example `instances.json` entry is in
[Reference](../reference/README.md#the-instances-file). No file at all
is fine too: the coordinator reconciles an empty set.

## 3. Check they're up

```sh
curl localhost:8081/healthz   # claim-plane admin
curl localhost:8082/healthz   # coordinator admin
```

`/healthz` only proves the process is up. `/readyz` is the meaningful
one — semantics per binary are in [Operate it](../guides/operate-it.md).

Handy: both binaries accept `--version`, which prints the protocol
major (`1`) and exits. Every flag is listed in
[Reference](../reference/README.md).
