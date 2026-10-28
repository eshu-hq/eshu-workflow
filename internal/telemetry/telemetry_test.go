// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package telemetry_test

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
)

func TestNilInstrumentsAreNoOps(t *testing.T) {
	var ins *telemetry.Instruments
	ins.ObserveClaim("issued", "")
	ins.ObserveMutation("HeartbeatClaim", "ok")
	ins.ObservePin("pinned")
	ins.ObserveReap(3)
	ins.ObserveDepth("jira", "pending", 7)
	if ins.Registry() != nil {
		t.Fatalf("nil instruments registry is not nil")
	}
}

func TestCountersIncrement(t *testing.T) {
	ins := telemetry.NewInstruments(nil)
	if ins.Registry() == nil {
		t.Fatalf("registry is nil")
	}
	ins.ObserveClaim("issued", "")
	ins.ObserveClaim("deferred", "domain")
	ins.ObserveMutation("CompleteClaim", "ok")
	ins.ObservePin("pinned")
	ins.ObserveReap(2)
	ins.ObserveDepth("jira", "pending", 7)
	expected := `
# HELP workflow_claim_grants_total ClaimNext outcomes by result: issued, deferred, idle, or error.
# TYPE workflow_claim_grants_total counter
workflow_claim_grants_total{result="issued",signal=""} 1
workflow_claim_grants_total{result="deferred",signal="domain"} 1
`
	if err := testutil.GatherAndCompare(ins.Registry(), strings.NewReader(expected),
		"workflow_claim_grants_total"); err != nil {
		t.Fatalf("grants mismatch: %v", err)
	}
}

func TestSpansRoundTrip(t *testing.T) {
	ctx, span := telemetry.Start(context.Background(), "TestOp")
	telemetry.Fail(span, "stale_fence", "old epoch")
	span.End()
	_ = ctx
}

func TestLogExporterInstallAndShutdown(t *testing.T) {
	shutdown := telemetry.InstallLogExporter(nil)
	ctx, span := telemetry.Start(context.Background(), "ExportOp")
	span.End()
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	_ = ctx
}
