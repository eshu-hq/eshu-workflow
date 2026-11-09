# Decisions

Architecture decision records (ADRs) for eshu-workflow. An ADR captures
one decision, why we made it, and what it costs us.

## Format

One file per decision: `ADR-NNNN-short-title.md`, numbered in order.
Each ADR uses these sections:

- **Status** — proposed, accepted, superseded (link the replacement).
- **Context** — the forces at play, in plain language.
- **Decision** — what we chose.
- **Consequences** — what gets easier, what gets harder.

Keep it short. Link to code and docs instead of restating them.

## Index

| ADR | Decision | Status |
| --- | --- | --- |
| [ADR-0001](ADR-0001-protocol-not-shared-tables.md) | All cross-service traffic goes through `workflow.v1`; no shared tables | accepted |
