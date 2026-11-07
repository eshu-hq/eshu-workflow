// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
	"go.opentelemetry.io/otel/trace"
)

// serveCommitClaimFacts proxies one commit envelope to core byte-for-byte.
// The handler performs the version check and nothing else before the
// forward: no result decode, no ClaimStore call, no lock or transaction.
// Core's status and envelope cross back verbatim, so fence and lease
// refusals keep their 409 shape end to end. Any failure before core
// answers — no target configured, over-cap body, dead transport — is a
// local 4xx/500, never an ack.
func (srv *Server) serveCommitClaimFacts(writer http.ResponseWriter, request *http.Request) {
	const operation = "CommitClaimFacts"
	inv, ok := srv.begin(writer, request, operation)
	if !ok {
		return
	}
	ctx, span := telemetry.Start(request.Context(), operation)
	defer span.End()
	// The body is refused before configuration is consulted, so every
	// route answers an undecodable body with 400 invalid and a dark
	// server never forwards: refusal precedes the forward, never the
	// other way around.
	raw, ok := srv.readCommitBody(inv, operation, span)
	if !ok {
		return
	}
	if srv.commitClient == nil || srv.commitCoreURL == "" {
		// Dark default: no core target configured. Fail closed with a
		// 500 that is never an ack; the caller must retry, not complete.
		telemetry.Fail(span, "internal", "core commit target is not configured")
		srv.fail(inv, operation, fmt.Errorf("core commit target is not configured"))
		return
	}
	coreReq, err := http.NewRequestWithContext(
		ctx, http.MethodPost,
		srv.commitCoreURL+"/workflow.v1/CommitClaimFacts", bytes.NewReader(raw))
	if err != nil {
		telemetry.Fail(span, "internal", err.Error())
		srv.fail(inv, operation, err)
		return
	}
	coreReq.Header.Set(workflowv1.VersionHeader, request.Header.Get(workflowv1.VersionHeader))
	coreReq.Header.Set("Content-Type", "application/json")
	coreResp, err := srv.commitClient.Do(coreReq)
	if err != nil {
		// Transport failure: core may or may not have committed, so
		// the answer must never read as an ack.
		telemetry.Fail(span, "internal", err.Error())
		srv.fail(inv, operation, err)
		return
	}
	defer coreResp.Body.Close()
	if coreResp.StatusCode == http.StatusOK {
		srv.Instruments.ObserveMutation(operation, "ok")
	} else {
		srv.Instruments.ObserveMutation(operation, fmt.Sprintf("core_%d", coreResp.StatusCode))
	}
	srv.logger.Info("commit forwarded",
		slog.String("claim_id", commitClaimID(raw)),
		slog.Int("body_bytes", len(raw)),
		slog.Int("core_status", coreResp.StatusCode))
	if contentType := coreResp.Header.Get("Content-Type"); contentType != "" {
		writer.Header().Set("Content-Type", contentType)
	} else {
		writer.Header().Set("Content-Type", "application/json")
	}
	writer.WriteHeader(coreResp.StatusCode)
	if _, err := io.Copy(writer, coreResp.Body); err != nil {
		srv.logger.Error("copy core commit response",
			slog.String("operation", operation), slog.String("error", err.Error()))
	}
}

// readCommitBody reads the raw envelope bounded by the shared body cap,
// then measures — never interprets — the result it carries. An over-cap
// body or an over-cap result is invalid here with zero forwards; core
// re-checks the same cap and strict-decodes the envelope itself.
func (srv *Server) readCommitBody(inv invocation, operation string, span trace.Span) ([]byte, bool) {
	inv.request.Body = http.MaxBytesReader(inv.writer, inv.request.Body, workflowv1.MaxCommitBodyBytes)
	raw, err := io.ReadAll(inv.request.Body)
	if err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{
			Code:    workflowv1.CodeInvalid,
			Message: fmt.Sprintf("decode %s request: %v", operation, err),
		})
		return nil, false
	}
	var envelope workflowv1.CommitClaimFactsRequest
	if err := json.Unmarshal(raw, &envelope); err != nil {
		telemetry.Fail(span, workflowv1.CodeInvalid, err.Error())
		srv.fail(inv, operation, &workflowv1.ProtocolError{
			Code:    workflowv1.CodeInvalid,
			Message: fmt.Sprintf("decode %s request: %v", operation, err),
		})
		return nil, false
	}
	if len(envelope.ResultJSON) > workflowv1.MaxCommitResultBytes {
		telemetry.Fail(span, workflowv1.CodeInvalid, "result over cap")
		srv.fail(inv, operation, &workflowv1.ProtocolError{
			Code: workflowv1.CodeInvalid,
			Message: fmt.Sprintf("result_json of %d bytes exceeds the %d-byte cap",
				len(envelope.ResultJSON), workflowv1.MaxCommitResultBytes),
		})
		return nil, false
	}
	return raw, true
}

// commitClaimID recovers the claim ID for the forward log without
// validating the envelope: an empty string means malformed, logged so.
func commitClaimID(raw []byte) string {
	var envelope workflowv1.CommitClaimFactsRequest
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return ""
	}
	return envelope.ClaimID
}
