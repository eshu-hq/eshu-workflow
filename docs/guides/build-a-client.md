# Build a client

Callers never need the database. Everything goes through the
`workflow.v1` HTTP protocol — every route is a `POST` with a JSON body,
defined in `RouteTable` in
[workflowv1/http.go](../../workflowv1/http.go): 21 routes in four
groups (claim, control, report, read — see
[Concepts](../concepts/README.md#the-protocol)).

## Use the client package

The [client](../../client/doc.go) package drives every route over
HTTP. Start there instead of hand-rolling requests:

1. Point the client at the claim-plane base URL.
2. Speak protocol major 1. A caller speaking a different major gets
   rejected; [version matrix](../version-matrix.md) records the
   pairing rule and dated snapshots.
3. Follow the claim lifecycle: `ClaimNext` → `HeartbeatClaim` while
   working → `ValidateAndPinClaimForCommit` → `CommitClaimFacts` →
   `CompleteClaim` (or release/fail). The sequence diagram is in
   [Diagrams](../diagrams/README.md#one-claim-start-to-finish).

## Watch for

- **Closed enums reject unknowns.** Adding a member is a major
  protocol change. `CollectorKind` is the exception: open, but never blank.
- **Pacing backoffs are normal.** `ClaimNext` may tell you to back off
  instead of granting work. Honour it.
- **`CommitClaimFacts` can be dark.** A 500 there means the
  pass-through isn't configured — never read it as an acknowledgement.
- **Grants stay in core.** Heartbeat and complete check fencing only
  (proof you still own the claim). Permission decisions happen at the
  commit pin plus core's own admission.
