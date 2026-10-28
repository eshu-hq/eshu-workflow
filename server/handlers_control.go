// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server

import (
	"net/http"

	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// Control-plane handlers. These six routes are the coordinator seams: run
// admission, work enqueue, instance reconciliation, completeness checkpoints,
// run reconciliation, and expired-claim reaping, all served from the
// service's own database. Tenant and producer grants stay core-owned and
// never cross these routes.

func (srv *Server) serveCreateRun(writer http.ResponseWriter, request *http.Request) {
	const operation = "CreateRun"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.CreateRunRequest
	if !srv.decode(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	if err := srv.store.CreateRun(ctx, body.Run); err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.answer(inv, operation, workflowv1.CreateRunResponse{RunID: body.Run.RunID})
}

func (srv *Server) serveEnqueueWorkItems(writer http.ResponseWriter, request *http.Request) {
	const operation = "EnqueueWorkItems"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.EnqueueWorkItemsRequest
	if !srv.decode(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	inserted, err := srv.store.Enqueue(ctx, body.Items)
	if err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.answer(inv, operation, workflowv1.EnqueueWorkItemsResponse{
		Accepted: len(body.Items), InsertedWorkItems: inserted,
	})
}

func (srv *Server) serveReconcileCollectorInstances(writer http.ResponseWriter, request *http.Request) {
	const operation = "ReconcileCollectorInstances"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.ReconcileCollectorInstancesRequest
	if !srv.decode(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	if err := srv.store.ReconcileInstances(ctx, body.ObservedAt, body.Instances); err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.answer(inv, operation, ack{OK: true})
}

func (srv *Server) serveUpsertCompletenessStates(writer http.ResponseWriter, request *http.Request) {
	const operation = "UpsertCompletenessStates"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.UpsertCompletenessStatesRequest
	if !srv.decode(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	if err := srv.store.UpsertCompleteness(ctx, body.States); err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.answer(inv, operation, ack{OK: true})
}

func (srv *Server) serveReconcileWorkflowRuns(writer http.ResponseWriter, request *http.Request) {
	const operation = "ReconcileWorkflowRuns"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.ReconcileWorkflowRunsRequest
	if !srv.decode(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	transitioned, err := srv.store.ReconcileRuns(ctx, body.ObservedAt)
	if err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.answer(inv, operation, workflowv1.ReconcileWorkflowRunsResponse{Transitioned: transitioned})
}

func (srv *Server) serveReapExpiredClaims(writer http.ResponseWriter, request *http.Request) {
	const operation = "ReapExpiredClaims"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.ReapExpiredClaimsRequest
	if !srv.decode(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	claims, err := srv.store.Reap(ctx, body.ObservedAt, body.Limit, body.RequeueDelay)
	if err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	if claims == nil {
		claims = []workflowv1.Claim{}
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.Instruments.ObserveReap(len(claims))
	srv.answer(inv, operation, workflowv1.ReapExpiredClaimsResponse{Claims: claims})
}
