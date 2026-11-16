# Contributing to eshu-workflow

The canonical guide is [CONTRIBUTING.md](../../CONTRIBUTING.md) — this
page is the quick version, and CONTRIBUTING.md wins on any disagreement.
Repo rules are in [AGENTS.md](../../AGENTS.md).

## Setup

- Go version from [go.mod](../../go.mod), plus golangci-lint v2.12.2.
- Postgres for the tests: set `CLAIMPLANE_TEST_DATABASE_URL` to
  override the per-package default test databases
  (`claimplane_test_store`, `claimplane_test_server`,
  `claimplane_test_coordinator`). CI provides a `postgres:18`
  container with those databases; locally you need the same setup. The
  suite fails rather than skips when the database is missing under
  `CI=true`.

## Before you push

```sh
make ci
```

That runs `go vet`, `golangci-lint` plus the format check, `go mod
tidy -diff`, the 500-line `.go` file cap, `go build`, the full test
suite, and the race suite with the coverage floor.

Changed a lane, the Makefile, or `ci.yml`? Also run
`bash scripts/workflow-seed-matrix.sh` (needs Postgres, like the race suite).

## Ground rules

- TDD for behaviour. Keep every `.go` file under 500 lines with the
  MIT SPDX header.
- No shared tables; never import Eshu core internals. The boundary
  test enforces it.
- Closed enums reject unknowns — a new member is a major protocol bump.
- The coverage floor may only rise.
- Never push to `main`. No AI attribution in commits, PRs, or docs.
- Docs-only PRs run no lane and stay mergeable through `ci-complete`.
