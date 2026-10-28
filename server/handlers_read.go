// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server

import (
	"errors"
	"net/http"
	"time"

	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// errorCode extracts the protocol code for spans and metrics. Non-protocol
// errors report "internal": the wire detail stays generic while the log
// keeps the cause.
func errorCode(err error) string {
	var proto *workflowv1.ProtocolError
	if errors.As(err, &proto) {
		return proto.Code
	}
	return "internal"
}

func (srv *Server) serveReportProgress(writer http.ResponseWriter, request *http.Request) {
	const operation = "ReportRunProgress"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.ReportRunProgressRequest
	if !srv.decode(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	if err := srv.store.ReportProgress(ctx, body); err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.answer(inv, operation, ack{OK: true})
}

func (srv *Server) serveGetRunStatus(writer http.ResponseWriter, request *http.Request) {
	const operation = "GetRunStatus"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.GetRunStatusRequest
	if !srv.decode(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	resp, err := srv.store.GetRunStatus(ctx, body.RunID, time.Now().UTC())
	if err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.answer(inv, operation, resp)
}

func (srv *Server) serveListRuns(writer http.ResponseWriter, request *http.Request) {
	const operation = "ListRuns"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.ListRunsRequest
	if !srv.decodeOptional(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	resp, err := srv.store.ListRuns(ctx, body)
	if err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.answer(inv, operation, resp)
}

func (srv *Server) serveGetRunCompleteness(writer http.ResponseWriter, request *http.Request) {
	const operation = "GetRunCompleteness"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body workflowv1.GetRunCompletenessRequest
	if !srv.decode(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	if err := body.Validate(); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{Code: workflowv1.CodeInvalid, Message: err.Error()})
		return
	}
	states, err := srv.store.GetCompleteness(ctx, body.RunID)
	if err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.answer(inv, operation, workflowv1.GetRunCompletenessResponse{
		RunID:  body.RunID,
		States: states,
		AsOf:   time.Now().UTC(),
	})
}

func (srv *Server) serveListInstances(writer http.ResponseWriter, request *http.Request) {
	const operation = "ListCollectorInstances"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body struct{}
	if !srv.decodeOptional(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	instances, err := srv.store.ListInstances(ctx)
	if err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.answer(inv, operation, workflowv1.ListCollectorInstancesResponse{
		Instances: instances,
		AsOf:      time.Now().UTC(),
	})
}

func (srv *Server) serveQueueDepth(writer http.ResponseWriter, request *http.Request) {
	const operation = "QueueDepthSnapshot"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	var body struct{}
	if !srv.decodeOptional(inv, operation, &body) {
		telemetry.Fail(span, workflowv1.CodeInvalid, "decode")
		return
	}
	snapshot, err := srv.store.QueueDepth(ctx, time.Now().UTC())
	if err != nil {
		telemetry.Fail(span, errorCode(err), err.Error())
		srv.fail(inv, operation, err)
		return
	}
	srv.Instruments.ObserveMutation(operation, "ok")
	srv.answer(inv, operation, snapshot)
}
