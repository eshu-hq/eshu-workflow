// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package telemetry_test

import (
	"context"
	"io"
	"log/slog"
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
	ins.ObservePinLatency("ValidateAndPinClaimForCommit", "ok", 0.002)
	ins.ObserveReap(3)
	ins.ObserveDegraded("quota_floor")
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
	ins.ObserveDegraded("quota_floor")
	ins.ObserveDepth("jira", "pending", 7)
	expected := `
# HELP workflow_claim_grants_total ClaimNext outcomes by result: issued, deferred, idle, or error.
# TYPE workflow_claim_grants_total counter
workflow_claim_grants_total{result="issued",signal=""} 1
workflow_claim_grants_total{result="deferred",signal="domain"} 1
# HELP workflow_claim_degraded_total Degraded-mode fail-open decisions by reason.
# TYPE workflow_claim_degraded_total counter
workflow_claim_degraded_total{reason="quota_floor"} 1
`
	if err := testutil.GatherAndCompare(ins.Registry(), strings.NewReader(expected),
		"workflow_claim_grants_total", "workflow_claim_degraded_total"); err != nil {
		t.Fatalf("grants mismatch: %v", err)
	}
}

// TestPinLatencyHistogram requires one observation per pin attempt to
// land in the commit-pin histogram under the operation/code labels.
func TestPinLatencyHistogram(t *testing.T) {
	ins := telemetry.NewInstruments(nil)
	if ins.Registry() == nil {
		t.Fatalf("registry is nil")
	}
	ins.ObservePinLatency("ValidateAndPinClaimForCommit", "ok", 0.002)
	ins.ObservePinLatency("ValidateAndPinClaimForCommit", "stale_fence", 0.004)
	expected := `
# HELP workflow_commit_pin_duration_seconds ValidateAndPinClaimForCommit server-side latency in seconds by operation and protocol result code.
# TYPE workflow_commit_pin_duration_seconds histogram
workflow_commit_pin_duration_seconds_bucket{code="ok",operation="ValidateAndPinClaimForCommit",le="0.005"} 1
workflow_commit_pin_duration_seconds_bucket{code="ok",operation="ValidateAndPinClaimForCommit",le="0.01"} 1
workflow_commit_pin_duration_seconds_bucket{code="ok",operation="ValidateAndPinClaimForCommit",le="0.025"} 1
workflow_commit_pin_duration_seconds_bucket{code="ok",operation="ValidateAndPinClaimForCommit",le="0.05"} 1
workflow_commit_pin_duration_seconds_bucket{code="ok",operation="ValidateAndPinClaimForCommit",le="0.1"} 1
workflow_commit_pin_duration_seconds_bucket{code="ok",operation="ValidateAndPinClaimForCommit",le="0.25"} 1
workflow_commit_pin_duration_seconds_bucket{code="ok",operation="ValidateAndPinClaimForCommit",le="0.5"} 1
workflow_commit_pin_duration_seconds_bucket{code="ok",operation="ValidateAndPinClaimForCommit",le="1"} 1
workflow_commit_pin_duration_seconds_bucket{code="ok",operation="ValidateAndPinClaimForCommit",le="2.5"} 1
workflow_commit_pin_duration_seconds_bucket{code="ok",operation="ValidateAndPinClaimForCommit",le="5"} 1
workflow_commit_pin_duration_seconds_bucket{code="ok",operation="ValidateAndPinClaimForCommit",le="10"} 1
workflow_commit_pin_duration_seconds_bucket{code="ok",operation="ValidateAndPinClaimForCommit",le="+Inf"} 1
workflow_commit_pin_duration_seconds_sum{code="ok",operation="ValidateAndPinClaimForCommit"} 0.002
workflow_commit_pin_duration_seconds_count{code="ok",operation="ValidateAndPinClaimForCommit"} 1
workflow_commit_pin_duration_seconds_bucket{code="stale_fence",operation="ValidateAndPinClaimForCommit",le="0.005"} 1
workflow_commit_pin_duration_seconds_bucket{code="stale_fence",operation="ValidateAndPinClaimForCommit",le="0.01"} 1
workflow_commit_pin_duration_seconds_bucket{code="stale_fence",operation="ValidateAndPinClaimForCommit",le="0.025"} 1
workflow_commit_pin_duration_seconds_bucket{code="stale_fence",operation="ValidateAndPinClaimForCommit",le="0.05"} 1
workflow_commit_pin_duration_seconds_bucket{code="stale_fence",operation="ValidateAndPinClaimForCommit",le="0.1"} 1
workflow_commit_pin_duration_seconds_bucket{code="stale_fence",operation="ValidateAndPinClaimForCommit",le="0.25"} 1
workflow_commit_pin_duration_seconds_bucket{code="stale_fence",operation="ValidateAndPinClaimForCommit",le="0.5"} 1
workflow_commit_pin_duration_seconds_bucket{code="stale_fence",operation="ValidateAndPinClaimForCommit",le="1"} 1
workflow_commit_pin_duration_seconds_bucket{code="stale_fence",operation="ValidateAndPinClaimForCommit",le="2.5"} 1
workflow_commit_pin_duration_seconds_bucket{code="stale_fence",operation="ValidateAndPinClaimForCommit",le="5"} 1
workflow_commit_pin_duration_seconds_bucket{code="stale_fence",operation="ValidateAndPinClaimForCommit",le="10"} 1
workflow_commit_pin_duration_seconds_bucket{code="stale_fence",operation="ValidateAndPinClaimForCommit",le="+Inf"} 1
workflow_commit_pin_duration_seconds_sum{code="stale_fence",operation="ValidateAndPinClaimForCommit"} 0.004
workflow_commit_pin_duration_seconds_count{code="stale_fence",operation="ValidateAndPinClaimForCommit"} 1
`
	if err := testutil.GatherAndCompare(ins.Registry(), strings.NewReader(expected),
		"workflow_commit_pin_duration_seconds"); err != nil {
		t.Fatalf("pin latency mismatch: %v", err)
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

func TestLoggerDefaultsToDiscardAndKeepsExplicitLogger(t *testing.T) {
	discard := telemetry.Logger(nil)
	if discard == nil {
		t.Fatal("nil logger input returned a nil logger")
	}
	// Writing through the default must be safe and silent.
	discard.Info("nothing listens", "k", "v")

	explicit := slog.New(slog.NewTextHandler(io.Discard, nil))
	if got := telemetry.Logger(explicit); got != explicit {
		t.Fatal("an explicit logger was replaced")
	}
}
