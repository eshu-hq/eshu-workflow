# Contributing to eshu-workflow

## Local gates

`make ci` runs the same lint, test, and race commands the CI lanes run: `go vet`,
`golangci-lint run` plus the gofumpt format check, `go mod tidy -diff`,
the 500-line `.go` file cap, `go build`, the full test suite, and the
race suite with the coverage floor. Run it before every push. It needs
the Go version in `go.mod` and golangci-lint v2.12.2 (the version Eshu
uses and CI installs).

The test suite needs Postgres: the CI `test` and `race` lanes bootstrap a
`postgres:18` container with the `claimplane_test` role and the
`claimplane_test_store`, `claimplane_test_server`, and
`claimplane_test_coordinator` databases from the `postgres-*` inputs of the
`lanes` job in `.github/workflows/ci.yml`. The suite fails rather than
skips when the database is missing under `CI=true`.

`scripts/workflow-seed-matrix.sh` is a local seeded RED/GREEN check of
those gates, not a CI gate. For vet, lint, format, race, tidy (an unused
require and a go.mod-only reorder), the 500-line cap, and the coverage
floor, it plants a defect, asserts the lane command (and the matching
`make lint` / `make race` recipe) fails and names the seed, removes the
seed, asserts the lane is green again, and leaves the tree as found. Run
it with `bash scripts/workflow-seed-matrix.sh` after changing a lane, the
Makefile, or `ci.yml`; it needs Postgres like the race suite.

## CI lanes

`.github/workflows/ci.yml` is a thin caller: a `changes` job classifies
paths, a `lanes` job runs the shared lane bodies from the pinned
`eshu-hq/.github` reusable `go-collector-ci.yml` (commit SHA, never a
mutable tag), and `ci-complete` aggregates. Lane bodies are not edited here:
change the inputs under `lanes:` or bump the pin (and
`ci_workflow_test.go`, which pins the SHA, the gates, the tool versions, and
the Postgres inputs). Inner jobs show as `lanes / <lane>`. Branch
protection requires only `ci-complete`, so those names may change freely:

| Lane | Runs when |
| --- | --- |
| `lint` | Go sources, module files, workflow changes, or unclassified files |
| `test` | Above, plus fixtures and config the tests read |
| `race` | Above (whole module, `-race`, coverprofile artifact, floor check) |
| `shell` | Shell script changes |
| `boundary` | Go, unclassified, or workflow changes (stdlib-only import gate) |
| `image` | `Dockerfile` / image-input changes (build only, no push) |
| `govulncheck` | Dependency changes, pushes to `main`, weekly schedule |
| `gosec` | Go or workflow changes, pushes to `main`, weekly schedule; advisory SARIF artifact, findings triaged in the open |
| `actionlint` | Workflow changes and pushes to `main` |
| `ci-complete` | Always; fails on any needed lane failure, passes on legitimate skips |

Pushes to `main` run every lane. The weekly schedule runs only
`govulncheck` and `gosec`.

Docs-only PRs run no lane and stay mergeable through `ci-complete`.

## Coverage floor

The `race` lane fails below the `coverage-floor` input in `ci.yml`
(parity with `COVERAGE_FLOOR` in `Makefile`, enforced by
`ci_workflow_test.go`). The floor sits at the latest measured postgres-backed
baseline and may only rise toward the 90% target as tests land
(#12). Never lower it, exclude packages silently, or delete assertions
to move the number.

## Fixture-only CI

PR lanes never run live calls. Fork PRs run the required lanes with a
read-only token and no secrets.
