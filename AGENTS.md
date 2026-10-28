# eshu-workflow Agent Rules

This repo owns workflow planning, claim coordination, scheduling, and
workflow-owned state behind the versioned `workflow.v1` protocol. The
cutover issue is eshu-hq/eshu#8875 (part of #6707).

## Non-negotiables

- No shared tables: every cross-service read or write travels through a
  `workflowv1` route. Never import Eshu core internals or another
  service repository; the boundary test enforces stdlib-only imports.
- Closed enums reject unknowns: a new member is a major protocol bump.
  `CollectorKind` stays open but never blank.
- TDD for behavior; `make ci` (lint, test, race with coverage floor)
  before every push. Floor may only rise.
- Keep every `.go` file under 500 lines with the MIT SPDX header.
- Never push to `main`. No AI attribution in commits, PRs, or docs.
