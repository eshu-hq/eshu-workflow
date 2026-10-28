// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

func (srv *Server) serveClaimNext(writer http.ResponseWriter, request *http.Request) {
	const operation = "ClaimNext"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.ClaimNextRequest
	if !srv.decode(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	resp, err := srv.store.ClaimNext(ctx, body.Selector, body.ObservedAt, body.LeaseTTL)
	if err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.observeDegraded(resp.DegradedReason)
	if !resp.Issued && resp.Deferral.Signal == "" {
		// Idle queue: the contract forbids an unissued response without
		// a deferral, so idleness is a 404, never an empty 200.
		srv.Instruments.ObserveClaim("idle", "")
		srv.logger.Info("claim next idle",
			slog.String("collector_kind", string(body.Selector.CollectorKind)),
			slog.String("collector_instance_id", body.Selector.CollectorInstanceID),
			slog.String("degraded_reason", resp.DegradedReason))
		srv.fail(inv, operation, &workflowv1.ProtocolError{
			Code:    workflowv1.CodeNotFound,
			Message: "no eligible work",
		})
		return
	}
	if resp.Issued {
		srv.Instruments.ObserveClaim("issued", "")
	} else {
		srv.Instruments.ObserveClaim("deferred", string(resp.Deferral.Signal))
	}
	srv.logger.Info("claim next",
		slog.String("collector_kind", string(body.Selector.CollectorKind)),
		slog.String("claim_id", body.Selector.ClaimID),
		slog.Bool("issued", resp.Issued),
		slog.String("degraded_reason", resp.DegradedReason))
	srv.answer(inv, operation, resp)
}

// observeDegraded counts one degraded-mode fail-open answer by reason.
// Empty reasons are healthy answers and record nothing.
func (srv *Server) observeDegraded(reason string) {
	if reason == "" {
		return
	}
	srv.Instruments.ObserveDegraded(reason)
}

func (srv *Server) serveCheckClaimPacing(writer http.ResponseWriter, request *http.Request) {
	const operation = "CheckClaimPacing"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.CheckClaimPacingRequest
	if !srv.decode(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	decision, err := srv.store.CheckPacing(ctx, string(body.CollectorKind), body.CollectorInstanceID, body.ObservedAt)
	if err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.answer(inv, operation, decision)
}

func (srv *Server) serveMutation(
	writer http.ResponseWriter,
	request *http.Request,
	operation string,
	call func(ctx context.Context, mutation workflowv1.ClaimMutation) error,
) {
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.ClaimMutation
	if !srv.decode(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	if err := call(ctx, body); err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.logger.Info("claim mutation",
		slog.String("operation", operation),
		slog.String("claim_id", body.ClaimID),
		slog.Int64("fencing_token", body.FencingToken))
	srv.answer(inv, operation, ack{OK: true})
}

func (srv *Server) serveHeartbeat(writer http.ResponseWriter, request *http.Request) {
	srv.serveMutation(writer, request, "HeartbeatClaim", srv.store.Heartbeat)
}

func (srv *Server) serveComplete(writer http.ResponseWriter, request *http.Request) {
	srv.serveMutation(writer, request, "CompleteClaim", srv.store.Complete)
}

func (srv *Server) serveRelease(writer http.ResponseWriter, request *http.Request) {
	srv.serveMutation(writer, request, "ReleaseClaim", srv.store.Release)
}

func (srv *Server) serveFailRetryable(writer http.ResponseWriter, request *http.Request) {
	srv.serveMutation(writer, request, "FailClaimRetryable", srv.store.FailRetryable)
}

func (srv *Server) serveFailTerminal(writer http.ResponseWriter, request *http.Request) {
	srv.serveMutation(writer, request, "FailClaimTerminal", srv.store.FailTerminal)
}

func (srv *Server) servePin(writer http.ResponseWriter, request *http.Request) {
	const operation = "ValidateAndPinClaimForCommit"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.CommitPinRequest
	if !srv.decode(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	resp, err := srv.store.PinForCommit(ctx, body)
	if err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.Instruments.ObservePin(errorCode(err))
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObservePin("pinned")
	srv.logger.Info("commit pin",
		slog.String("claim_id", body.ClaimID),
		slog.String("commit_id", resp.CommitID))
	srv.answer(inv, operation, resp)
}
