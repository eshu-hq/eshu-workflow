// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// maxBodyBytes bounds one protocol request body.
const maxBodyBytes = 1 << 20

// ClaimStore is the storage surface the HTTP plane serves. The pgx store
// implements it; tests substitute a fake.
type ClaimStore interface {
	ClaimNext(ctx context.Context, selector workflowv1.ClaimSelector, now time.Time, leaseTTL time.Duration) (workflowv1.ClaimNextResponse, error)
	Heartbeat(ctx context.Context, mutation workflowv1.ClaimMutation) error
	Complete(ctx context.Context, mutation workflowv1.ClaimMutation) error
	Release(ctx context.Context, mutation workflowv1.ClaimMutation) error
	FailRetryable(ctx context.Context, mutation workflowv1.ClaimMutation) error
	FailTerminal(ctx context.Context, mutation workflowv1.ClaimMutation) error
	PinForCommit(ctx context.Context, req workflowv1.CommitPinRequest) (workflowv1.CommitPinResponse, error)
	// Control plane: the coordinator seams, served from the service's
	// own database. Handler names differ from store method names
	// (CreateRun served by CreateRun, ReconcileCollectorInstances by
	// ReconcileInstances), so the routes-served test pins identity.
	CreateRun(ctx context.Context, run workflowv1.Run) error
	Enqueue(ctx context.Context, items []workflowv1.WorkItem) (int, error)
	ReconcileInstances(ctx context.Context, now time.Time, desired []workflowv1.DesiredCollectorInstance) error
	UpsertCompleteness(ctx context.Context, states []workflowv1.CompletenessState) error
	ReconcileRuns(ctx context.Context, now time.Time) (int, error)
	Reap(ctx context.Context, asOf time.Time, limit int, requeueDelay time.Duration) ([]workflowv1.Claim, error)
	CheckPacing(ctx context.Context, kind, instance string, now time.Time) (workflowv1.ClaimPacingDecision, error)
	ReportProgress(ctx context.Context, req workflowv1.ReportRunProgressRequest) error
	GetRunStatus(ctx context.Context, runID string, now time.Time) (workflowv1.GetRunStatusResponse, error)
	ListRuns(ctx context.Context, req workflowv1.ListRunsRequest) (workflowv1.ListRunsResponse, error)
	GetCompleteness(ctx context.Context, runID string) ([]workflowv1.CompletenessState, error)
	ListInstances(ctx context.Context) ([]workflowv1.CollectorInstance, error)
	QueueDepth(ctx context.Context, now time.Time) (workflowv1.QueueDepthSnapshot, error)
}

// Server serves the 20 merged workflowv1 routes: the claim plane, the six
// control-plane coordinator seams, the report plane, and the read plane.
type Server struct {
	store       ClaimStore
	logger      *slog.Logger
	Instruments *telemetry.Instruments
	mux         *http.ServeMux
}

// New builds a server over store. A nil logger discards; instruments stay
// nil until the command assigns a registry-backed handle.
func New(store ClaimStore, logger *slog.Logger) (*Server, error) {
	if store == nil {
		return nil, fmt.Errorf("claim store is required")
	}
	srv := &Server{store: store, logger: telemetry.Logger(logger), mux: http.NewServeMux()}
	srv.mux.HandleFunc("POST /workflow.v1/ClaimNext", srv.serveClaimNext)
	srv.mux.HandleFunc("POST /workflow.v1/CheckClaimPacing", srv.serveCheckClaimPacing)
	srv.mux.HandleFunc("POST /workflow.v1/HeartbeatClaim", srv.serveHeartbeat)
	srv.mux.HandleFunc("POST /workflow.v1/CompleteClaim", srv.serveComplete)
	srv.mux.HandleFunc("POST /workflow.v1/ReleaseClaim", srv.serveRelease)
	srv.mux.HandleFunc("POST /workflow.v1/FailClaimRetryable", srv.serveFailRetryable)
	srv.mux.HandleFunc("POST /workflow.v1/FailClaimTerminal", srv.serveFailTerminal)
	srv.mux.HandleFunc("POST /workflow.v1/ValidateAndPinClaimForCommit", srv.servePin)
	srv.mux.HandleFunc("POST /workflow.v1/CreateRun", srv.serveCreateRun)
	srv.mux.HandleFunc("POST /workflow.v1/EnqueueWorkItems", srv.serveEnqueueWorkItems)
	srv.mux.HandleFunc("POST /workflow.v1/ReconcileCollectorInstances", srv.serveReconcileCollectorInstances)
	srv.mux.HandleFunc("POST /workflow.v1/UpsertCompletenessStates", srv.serveUpsertCompletenessStates)
	srv.mux.HandleFunc("POST /workflow.v1/ReconcileWorkflowRuns", srv.serveReconcileWorkflowRuns)
	srv.mux.HandleFunc("POST /workflow.v1/ReapExpiredClaims", srv.serveReapExpiredClaims)
	srv.mux.HandleFunc("POST /workflow.v1/ReportRunProgress", srv.serveReportProgress)
	srv.mux.HandleFunc("POST /workflow.v1/GetRunStatus", srv.serveGetRunStatus)
	srv.mux.HandleFunc("POST /workflow.v1/ListRuns", srv.serveListRuns)
	srv.mux.HandleFunc("POST /workflow.v1/GetRunCompleteness", srv.serveGetRunCompleteness)
	srv.mux.HandleFunc("POST /workflow.v1/ListCollectorInstances", srv.serveListInstances)
	srv.mux.HandleFunc("POST /workflow.v1/QueueDepthSnapshot", srv.serveQueueDepth)
	return srv, nil
}

// Handler returns the protocol mux.
func (srv *Server) Handler() http.Handler {
	return srv.mux
}

// request envelope for a handler invocation.
type invocation struct {
	writer  http.ResponseWriter
	request *http.Request
	start   time.Time
}

// begin enforces the version header before any store call. It reports
// whether the handler may proceed.
func (srv *Server) begin(writer http.ResponseWriter, request *http.Request, operation string) (invocation, bool) {
	inv := invocation{writer: writer, request: request, start: time.Now()}
	if err := workflowv1.CheckVersion(request.Header.Get(workflowv1.VersionHeader)); err != nil {
		srv.fail(inv, operation, err)
		return inv, false
	}
	return inv, true
}

// decode reads a required JSON body into value.
func (srv *Server) decode(inv invocation, operation string, value any) bool {
	inv.request.Body = http.MaxBytesReader(inv.writer, inv.request.Body, maxBodyBytes)
	decoder := json.NewDecoder(inv.request.Body)
	if err := decoder.Decode(value); err != nil {
		srv.fail(inv, operation, &workflowv1.ProtocolError{
			Code:    workflowv1.CodeInvalid,
			Message: fmt.Sprintf("decode %s request: %v", operation, err),
		})
		return false
	}
	return true
}

// decodeOptional reads a body that may be absent (bodyless read routes).
func (srv *Server) decodeOptional(inv invocation, operation string, value any) bool {
	if inv.request.ContentLength == 0 {
		return true
	}
	return srv.decode(inv, operation, value)
}

// answer writes a 200 JSON body.
func (srv *Server) answer(inv invocation, operation string, value any) {
	inv.writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(inv.writer).Encode(value); err != nil {
		srv.logger.Error("encode response",
			slog.String("operation", operation), slog.String("error", err.Error()))
	}
}

// fail maps an error to its contract status and envelope. Protocol errors
// keep their code; everything else is a 500 whose detail stays in the
// server log, never on the wire.
func (srv *Server) fail(inv invocation, operation string, err error) {
	var proto *workflowv1.ProtocolError
	if !errors.As(err, &proto) {
		srv.logger.Error("protocol handler error",
			slog.String("operation", operation), slog.String("error", err.Error()))
		proto = &workflowv1.ProtocolError{Code: "internal", Message: "internal error"}
	}
	srv.Instruments.ObserveMutation(operation, proto.Code)
	inv.writer.Header().Set("Content-Type", "application/json")
	inv.writer.WriteHeader(proto.HTTPStatus())
	_ = json.NewEncoder(inv.writer).Encode(proto.Envelope())
}

// ack is the mutation acknowledgement body.
type ack struct {
	OK bool `json:"ok"`
}
