# Contributing to eshu-workflow

## Local gates

`make ci` runs the same lint, test, and race commands CI runs: `go vet`,
`golangci-lint run` plus the gofumpt format check, `go mod tidy -diff`,
the 500-line `.go` file cap, `go build`, the full test suite, and the
race suite with the coverage floor. Run it before every push. It needs
the Go version in `go.mod` and golangci-lint v2.12.2 (the version Eshu
uses and CI installs).

The test suite needs Postgres: CI provides a `postgres:18` service with
the `claimplane_test` role and the `claimplane_test_store`,
`claimplane_test_server`, and `claimplane_test_coordinator` databases
(see the `test`/`race` jobs in `.github/workflows/ci.yml`). The suite
fails rather than skips when the database is missing under `CI=true`.

## CI lanes

Job names are frozen because branch protection requires `ci-complete`:

| Lane | Runs when |
| --- | --- |
| `lint` | Go sources, module files, workflow changes, or unclassified files |
| `test` | Above, plus fixtures and config the tests read |
| `race` | Above (whole module, `-race`, coverprofile artifact, floor check) |
| `shell` | Shell script changes |
| `boundary` | Go, unclassified, or workflow changes (stdlib-only import gate) |
| `image` | `Dockerfile` / image-input changes (build only, no push) |
| `govulncheck` | Dependency changes, pushes to `main`, weekly schedule |
| `gosec` | Advisory SARIF; findings triaged in the open |
| `actionlint` | Workflow changes and pushes to `main` |
| `ci-complete` | Always; fails on any needed lane failure, passes on legitimate skips |

Docs-only PRs run no lane and stay mergeable through `ci-complete`.

## Coverage floor

The `race` job fails below `COVERAGE_FLOOR` in `ci.yml` (parity in
`Makefile`). The floor sits at the latest measured postgres-backed
baseline and may only rise toward the 90% target as tests land
(#12). Never lower it, exclude packages silently, or delete assertions
to move the number.

## Fixture-only CI

PR lanes never run live calls. Fork PRs run the required lanes with a
read-only token and no secrets.
