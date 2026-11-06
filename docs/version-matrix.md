# Core-satellite version-pair support matrix (Refs `eshu-hq/eshu#8875`, criterion C9)

Which core and satellite versions may talk to each other. The protocol
rule is pinned by test; the pairs below are dated snapshots, honestly
labeled, not support guarantees beyond that rule.

## Protocol support

Oldest supported equals newest supported: **`workflow.v1`, major 1**.
There is exactly one protocol generation, pinned by
`workflowv1/version_test.go` (`TestProtocolIdentity` pins the name and
major; `TestSupportedAcceptsCurrentMajorOnly` rejects every other
major). Cross-major calls fail closed: the server validates the
`Workflow-Protocol-Version` header on every request
(`server/server.go`, via `workflowv1.CheckVersion`), and the client
sends it (`client/client.go`). A new closed-enum member is a major
protocol bump while any reader rejects unknowns (`workflowv1/doc.go`);
`CollectorKind` stays open but never blank.

Support rule: a core and a satellite build are a supported pair if and
only if both speak the same protocol major. Today that means v1 with
v1. No v2 exists; the day one lands, this matrix needs a v1/v2 row and
a migration note.

## Satellite releases

| Tag | Commit | Date | Protocol |
| --- | --- | --- | --- |
| `v0.1.0` | `09e23b93` | 2026-10-28 | v1 (verified at tag) |
| `v0.1.1` | `8311ed69` | 2026-11-01 | v1 (verified at tag) |

`image-publish.yml` publishes `ghcr.io/eshu-hq/eshu-workflow`: pushes
to `main` publish the moving `:main`/`:latest` plus immutable
`:sha-<long-sha>` tags; a version-tag push publishes exactly that tag.
No GitHub Releases have been cut for these tags (observed 2026-11-05);
deploy pins use digests, never hand-typed tags.

## Dated build-pair snapshots

Snapshots of what was observed where, on the date given. A snapshot is
evidence for the cutover record, not a support promise: only the
protocol rule above confers support.

| Observed | Satellite build | Core counterpart | Protocol | Status |
| --- | --- | --- | --- | --- |
| 2026-11-04 | `ghcr.io/eshu-hq/eshu-workflow@sha256:e1b564af…e47a4` (tag `v0.1.1`, SLSA-attested, both satellite deployments 1/1) | In-tree pre-cutover path; claim-plane URL unset everywhere | v1 / n/a | Dark: no satellite traffic (per #8875 E1.4 digest rebind) |

There is no production core-satellite pair in service: cutover has not
occurred (`#9376` open, flip variable unset, removal held). The first
serving pair will be recorded here with its date, digests, and the
sensor comparison that accepted it.

## Credential and provenance notes (C9 remainder)

- Artifact provenance: main and tag images carry provenance, cosign,
  SBOM, and attestation hardening (`image-publish.yml`); the live
  digest above was verified back to its builder run and tag via its
  attestation (see the #8875 E1.4 comment for the full hex and run
  link).
- Vulnerability response: `docs/vuln-response.md`.
- Credentials: the service holds no credentials beyond its database
  URL; CI uses throwaway Postgres credentials (`CONTRIBUTING.md`).
