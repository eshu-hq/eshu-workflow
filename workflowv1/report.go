// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"fmt"
	"time"
)

// PhasePublicationKey names one reducer checkpoint the service tracks for
// run reconciliation. Keyspace and phase travel as strings so neither side
// imports the other's phase packages.
type PhasePublicationKey struct {
	Keyspace  string `json:"keyspace"`
	PhaseName string `json:"phase_name"`
}

// Validate checks the publication key identity.
func (k PhasePublicationKey) Validate() error {
	if err := validateIdentifier("keyspace", k.Keyspace); err != nil {
		return err
	}
	return validateIdentifier("phase_name", k.PhaseName)
}

// CollectorRunProgress is core's published view of one family's share of a
// run. The service may no longer read fact_records or fact_work_items, so
// core pushes these counts and the service reconciles from them.
type CollectorRunProgress struct {
	CollectorKind       CollectorKind `json:"collector_kind"`
	TotalWorkItems      int           `json:"total_work_items"`
	PendingWorkItems    int           `json:"pending_work_items"`
	ClaimedWorkItems    int           `json:"claimed_work_items"`
	CompletedWorkItems  int           `json:"completed_work_items"`
	FailedTerminalItems int           `json:"failed_terminal_items"`
	// FactlessCompletedItems counts completed work items whose generation
	// committed nothing to project. A completed claim whose result
	// carries no facts skips ingestion entirely, so the reducer never
	// sees the generation and the publication count can never reach the
	// total. Counting those items as vacuously published keeps the run
	// from wedging in reducer_converging forever. The count must never
	// exceed CompletedWorkItems: it only ever describes items the
	// collector already finished, after their commit already landed.
	FactlessCompletedItems int `json:"factless_completed_items"`
	// PublishedPhaseCounts carries published phase counts keyed by
	// "keyspace/phase".
	PublishedPhaseCounts map[string]int `json:"published_phase_counts,omitempty"`
	// TerminalDeadLetterCounts counts, per required phase keyed by
	// "keyspace/phase", how many same-scope-generation reducer queue rows
	// have permanently dead-lettered (genuinely terminal, non-retryable
	// reducer failure).
	TerminalDeadLetterCounts map[string]int `json:"terminal_dead_letter_counts,omitempty"`
}

// Validate checks the progress counts are sane.
func (p CollectorRunProgress) Validate() error {
	if err := p.CollectorKind.Validate(); err != nil {
		return err
	}
	for name, value := range map[string]int{
		"total_work_items":         p.TotalWorkItems,
		"pending_work_items":       p.PendingWorkItems,
		"claimed_work_items":       p.ClaimedWorkItems,
		"completed_work_items":     p.CompletedWorkItems,
		"failed_terminal_items":    p.FailedTerminalItems,
		"factless_completed_items": p.FactlessCompletedItems,
	} {
		if value < 0 {
			return fmt.Errorf("%s must not be negative", name)
		}
	}
	if p.FactlessCompletedItems > p.CompletedWorkItems {
		return fmt.Errorf("factless_completed_items must not exceed completed_work_items")
	}
	for key, value := range p.PublishedPhaseCounts {
		if value < 0 {
			return fmt.Errorf("published_phase_counts[%q] must not be negative", key)
		}
	}
	for key, value := range p.TerminalDeadLetterCounts {
		if value < 0 {
			return fmt.Errorf("terminal_dead_letter_counts[%q] must not be negative", key)
		}
	}
	return nil
}

// ReportRunProgressRequest is the report-plane push from core to the
// service for one run observation.
type ReportRunProgressRequest struct {
	RunID      string                 `json:"run_id"`
	ObservedAt time.Time              `json:"observed_at"`
	Progress   []CollectorRunProgress `json:"progress"`
}

// Validate checks the report-plane push.
func (r ReportRunProgressRequest) Validate() error {
	if err := validateIdentifier("run_id", r.RunID); err != nil {
		return err
	}
	if err := validateTime("observed_at", r.ObservedAt); err != nil {
		return err
	}
	if len(r.Progress) == 0 {
		return fmt.Errorf("progress must not be empty")
	}
	for i := range r.Progress {
		if err := r.Progress[i].Validate(); err != nil {
			return fmt.Errorf("progress[%d]: %w", i, err)
		}
	}
	return nil
}
