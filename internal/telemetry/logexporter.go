// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
)

// logExporter is a dependency-free span sink: finished spans become one
// debug log line each. It keeps Stage-1b spans observable without an OTLP
// collector; swapping in an OTLP exporter later changes one call site.
type logExporter struct {
	logger *slog.Logger
}

// InstallLogExporter sets the global tracer provider to an SDK provider
// that logs finished spans. It returns the provider shutdown function the
// command defers.
func InstallLogExporter(logger *slog.Logger) func(context.Context) error {
	resolved := Logger(logger)
	provider := trace.NewTracerProvider(trace.WithBatcher(&logExporter{logger: resolved}))
	otel.SetTracerProvider(provider)
	return provider.Shutdown
}

// ExportSpans logs one line per finished span.
func (e *logExporter) ExportSpans(_ context.Context, spans []trace.ReadOnlySpan) error {
	for _, span := range spans {
		attrs := make([]any, 0, 2*len(span.Attributes())+4)
		attrs = append(attrs,
			slog.String("span", span.Name()),
			slog.Duration("duration", span.EndTime().Sub(span.StartTime())),
			slog.String("status", span.Status().Code.String()),
		)
		for _, attr := range span.Attributes() {
			attrs = append(attrs, slog.String(string(attr.Key), attr.Value.String()))
		}
		e.logger.Debug("span finished", attrs...)
	}
	return nil
}

// Shutdown needs no flush channel: the batcher drains on provider shutdown.
func (e *logExporter) Shutdown(context.Context) error { return nil }
