// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

// Operation names one protocol method. The set is fixed at major 1: adding
// an operation is minor, removing or retyping one is major.
type Operation string

const (
	// Claim plane.
	OpClaimNext                    Operation = "ClaimNext"
	OpCheckClaimPacing             Operation = "CheckClaimPacing"
	OpHeartbeatClaim               Operation = "HeartbeatClaim"
	OpCompleteClaim                Operation = "CompleteClaim"
	OpReleaseClaim                 Operation = "ReleaseClaim"
	OpFailClaimRetryable           Operation = "FailClaimRetryable"
	OpFailClaimTerminal            Operation = "FailClaimTerminal"
	OpValidateAndPinClaimForCommit Operation = "ValidateAndPinClaimForCommit"
	// Control plane: the coordinator seams. Each resolves a method the
	// claim-plane client used to reject with ErrClaimPlaneUnsupported to
	// a real route served from the service's own database.
	OpCreateRun                   Operation = "CreateRun"
	OpEnqueueWorkItems            Operation = "EnqueueWorkItems"
	OpReconcileCollectorInstances Operation = "ReconcileCollectorInstances"
	OpUpsertCompletenessStates    Operation = "UpsertCompletenessStates"
	OpReconcileWorkflowRuns       Operation = "ReconcileWorkflowRuns"
	OpReapExpiredClaims           Operation = "ReapExpiredClaims"
	// Report plane.
	OpReportRunProgress Operation = "ReportRunProgress"
	// Read plane.
	OpGetRunStatus           Operation = "GetRunStatus"
	OpListRuns               Operation = "ListRuns"
	OpGetRunCompleteness     Operation = "GetRunCompleteness"
	OpListCollectorInstances Operation = "ListCollectorInstances"
	OpQueueDepthSnapshot     Operation = "QueueDepthSnapshot"
)

// Route binds one operation to its HTTP method and path. Every route is a
// POST with a JSON body so proxies and audit logs treat the protocol
// uniformly; reads stay POST because their filters are bodies.
type Route struct {
	Operation Operation
	Method    string
	Path      string
}

// RouteTable lists every operation of protocol major 1 in route order.
var RouteTable = []Route{
	{OpClaimNext, "POST", "/workflow.v1/ClaimNext"},
	{OpCheckClaimPacing, "POST", "/workflow.v1/CheckClaimPacing"},
	{OpHeartbeatClaim, "POST", "/workflow.v1/HeartbeatClaim"},
	{OpCompleteClaim, "POST", "/workflow.v1/CompleteClaim"},
	{OpReleaseClaim, "POST", "/workflow.v1/ReleaseClaim"},
	{OpFailClaimRetryable, "POST", "/workflow.v1/FailClaimRetryable"},
	{OpFailClaimTerminal, "POST", "/workflow.v1/FailClaimTerminal"},
	{OpValidateAndPinClaimForCommit, "POST", "/workflow.v1/ValidateAndPinClaimForCommit"},
	{OpCreateRun, "POST", "/workflow.v1/CreateRun"},
	{OpEnqueueWorkItems, "POST", "/workflow.v1/EnqueueWorkItems"},
	{OpReconcileCollectorInstances, "POST", "/workflow.v1/ReconcileCollectorInstances"},
	{OpUpsertCompletenessStates, "POST", "/workflow.v1/UpsertCompletenessStates"},
	{OpReconcileWorkflowRuns, "POST", "/workflow.v1/ReconcileWorkflowRuns"},
	{OpReapExpiredClaims, "POST", "/workflow.v1/ReapExpiredClaims"},
	{OpReportRunProgress, "POST", "/workflow.v1/ReportRunProgress"},
	{OpGetRunStatus, "POST", "/workflow.v1/GetRunStatus"},
	{OpListRuns, "POST", "/workflow.v1/ListRuns"},
	{OpGetRunCompleteness, "POST", "/workflow.v1/GetRunCompleteness"},
	{OpListCollectorInstances, "POST", "/workflow.v1/ListCollectorInstances"},
	{OpQueueDepthSnapshot, "POST", "/workflow.v1/QueueDepthSnapshot"},
}

// RouteFor returns the route for one operation.
func RouteFor(op Operation) (Route, bool) {
	for _, route := range RouteTable {
		if route.Operation == op {
			return route, true
		}
	}
	return Route{}, false
}

// ErrorEnvelope is the JSON body for every protocol error.
type ErrorEnvelope struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	// RetryAfterSeconds carries the grant delay on paced (429) errors so
	// runners back off without parsing message text.
	RetryAfterSeconds int64 `json:"retry_after_seconds,omitempty"`
}
