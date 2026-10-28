// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

// Package telemetry wires operator signals for the claim-plane server:
// OpenTelemetry spans per protocol operation, Prometheus counters per
// outcome, and structured slog fields. The server records through the
// package-level helpers and the Instruments handle; both are nil-safe so
// unit tests serve handlers without a provider.
package telemetry

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Scope names the instrumentation scope for every claim-plane span.
const Scope = "eshu-workflow/claim-plane"

// Tracer returns the claim-plane tracer on the global provider. The
// command wires a real SDK provider; without one spans are valid no-ops.
func Tracer() trace.Tracer {
	return otel.Tracer(Scope)
}

// Start begins one protocol-operation span carrying the operation name.
func Start(ctx context.Context, operation string) (context.Context, trace.Span) {
	return Tracer().Start(ctx, operation,
		trace.WithAttributes(attribute.String("workflow.operation", operation)))
}

// Fail marks the span with the protocol error code and message.
func Fail(span trace.Span, code, message string) {
	span.SetAttributes(attribute.String("workflow.error_code", code))
	span.SetStatus(codes.Error, message)
}

// Instruments holds the Prometheus counters for the claim plane. A nil
// *Instruments is a no-op: every method guards its receiver so handler
// tests need no registry.
type Instruments struct {
	claims   *prometheus.CounterVec
	mutates  *prometheus.CounterVec
	pins     *prometheus.CounterVec
	reaps    prometheus.Counter
	degraded *prometheus.CounterVec
	depths   *prometheus.GaugeVec
	registry *prometheus.Registry
}

// NewInstruments registers claim-plane counters on reg, or a private
// registry when reg is nil.
func NewInstruments(reg *prometheus.Registry) *Instruments {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	ins := &Instruments{
		claims: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "workflow_claim_grants_total",
			Help: "ClaimNext outcomes by result: issued, deferred, idle, or error.",
		}, []string{"result", "signal"}),
		mutates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "workflow_claim_mutations_total",
			Help: "Claim mutations by operation and protocol result code.",
		}, []string{"operation", "code"}),
		pins: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "workflow_commit_pins_total",
			Help: "Commit-pin outcomes by result: pinned, stale, or error.",
		}, []string{"result"}),
		reaps: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "workflow_claim_reaps_total",
			Help: "Claims expired by the reaper.",
		}),
		degraded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "workflow_claim_degraded_total",
			Help: "Degraded-mode fail-open decisions by reason.",
		}, []string{"reason"}),
		depths: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "workflow_queue_depth",
			Help: "Outstanding work items by family and state.",
		}, []string{"collector_kind", "state"}),
		registry: reg,
	}
	reg.MustRegister(ins.claims, ins.mutates, ins.pins, ins.reaps, ins.degraded, ins.depths)
	return ins
}

// Registry returns the underlying registry for the admin /metrics handler.
func (ins *Instruments) Registry() *prometheus.Registry {
	if ins == nil {
		return nil
	}
	return ins.registry
}

// ObserveClaim records one ClaimNext outcome.
func (ins *Instruments) ObserveClaim(result, signal string) {
	if ins == nil {
		return
	}
	ins.claims.WithLabelValues(result, signal).Inc()
}

// ObserveMutation records one claim mutation by operation and code
// ("ok" on success).
func (ins *Instruments) ObserveMutation(operation, code string) {
	if ins == nil {
		return
	}
	ins.mutates.WithLabelValues(operation, code).Inc()
}

// ObservePin records one commit-pin outcome.
func (ins *Instruments) ObservePin(result string) {
	if ins == nil {
		return
	}
	ins.pins.WithLabelValues(result).Inc()
}

// ObserveReap records reaped claims.
func (ins *Instruments) ObserveReap(count int) {
	if ins == nil {
		return
	}
	ins.reaps.Add(float64(count))
}

// ObserveDegraded records one degraded-mode fail-open decision by reason
// (pacing_depths, pacing_latch_read, pacing_latch_advance, quota_floor).
func (ins *Instruments) ObserveDegraded(reason string) {
	if ins == nil {
		return
	}
	ins.degraded.WithLabelValues(reason).Inc()
}

// ObserveDepth sets one family depth gauge.
func (ins *Instruments) ObserveDepth(kind, state string, count int64) {
	if ins == nil {
		return
	}
	ins.depths.WithLabelValues(kind, state).Set(float64(count))
}

// Logger returns logger when non-nil, otherwise a discard logger, so
// handlers never nil-check before logging.
func Logger(logger *slog.Logger) *slog.Logger {
	if logger != nil {
		return logger
	}
	return slog.New(slog.DiscardHandler)
}
