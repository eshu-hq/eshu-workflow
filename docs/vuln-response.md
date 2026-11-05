# Satellite vulnerability response (Refs `eshu-hq/eshu#8875`, criterion C9)

How a vulnerability in `eshu-workflow` is found, who owns it, and what
blocks what. Every mechanism below exists in CI today; where no process
exists, the doc says so instead of inventing one.

## Detection surfaces

| Surface | Mode | Cadence |
| --- | --- | --- |
| `govulncheck` lane (v1.9.0, `-scan package`) | Blocking | Every push to `main`; every PR touching `go.mod`/`go.sum` or workflows; weekly Monday 06:00 UTC schedule |
| `gosec` lane (v2.30.0, `gosec-fail-on-findings: false`) | Advisory, never blocking | Every push to `main`; every PR touching Go sources or workflows; weekly Monday 06:00 UTC schedule |
| Dependabot (`gomod` + `github-actions`, weekly, grouped) | Proposes PRs | Weekly; `go-deps` and `actions` groups (`.github/dependabot.yml`) |

Lane bodies live in the pinned `eshu-hq/.github` reusable
`go-collector-ci.yml`; the gates above are the `run-*` inputs in
`.github/workflows/ci.yml`, pinned by `ci_workflow_test.go`. Pushes to
`main` run every lane; the weekly schedule runs `govulncheck` and
`gosec` only (`CONTRIBUTING.md`).

## What blocking and advisory mean here

- A `govulncheck` finding fails its lane, and `ci-complete` — the only
  required check — fails on any needed lane failure. A red
  `govulncheck` must go green before the PR merges. There is no
  allowlist or suppression file; the fix is a version bump (usually via
  the Dependabot PR) or a code change, proven by a green re-run.
- A `gosec` finding never fails a lane on PR, push, or schedule: the
  reusable swallows its exit status and findings ride the 14-day gosec
  artifact. A run that produces no SARIF fails at the count step, so a
  silent scanner is itself red. Findings are triaged in the open. The
  flip to blocking (`gosec-fail-on-findings: true` on scheduled runs)
  waits until the false-positive rate here is measured; that condition
  is recorded in `ci.yml`, not decided here.
- Dependabot PRs go through the same lanes as any other PR, so a bump
  that introduces a finding is caught by the two lanes above.

## What CI does not do

- No findings issue is filed automatically: the retired
  `issues: write` grant and its filing step are gone with the move to
  the reusable. A scheduled-run failure is visible as a red run, not
  an issue.
- No SARIF uploads to the Security tab: the reusable runs on a
  read-only token for fork safety. Triage happens from the lane log
  and the artifact.
- No calendar SLA is declared anywhere for this repo. Response is
  driven by the gates above (blocking `govulncheck` on the merge path,
  weekly scans otherwise), not by a clock. If C9 is read as requiring
  a stated timeline, that timeline is an open owner decision, not
  something this runbook can set.

## Reporting and ownership

- Accountable owner: `@linuxdynasty` (`CODEOWNERS`). The owner
  triages scan failures and Dependabot PRs and decides the fix-or-
  accept for advisory `gosec` findings.
- Do not open a public issue for a security problem. Report privately
  (GitHub private vulnerability reporting where enabled, otherwise
  contact the maintainers directly) with impact, reproduction, and
  affected components, per the core security policy
  (`eshu-hq/eshu` `SECURITY.md`: fixes target the latest release
  only; coordinate disclosure after a fix).
- Credential handling is out of scope for this surface: the service
  holds no credentials beyond its database URL and the CI lanes use
  throwaway Postgres credentials (`CONTRIBUTING.md`).
