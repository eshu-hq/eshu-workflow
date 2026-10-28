// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"fmt"
	"strings"
)

// CollectorKind names a collector family. It stays open: families grow
// without a protocol major, so readers must accept kinds they never saw.
// Blank is never valid.
type CollectorKind string

// Validate rejects a blank collector kind.
func (k CollectorKind) Validate() error {
	if strings.TrimSpace(string(k)) == "" {
		return fmt.Errorf("collector_kind must not be blank")
	}
	return nil
}

// TriggerKind identifies why a workflow run was created. Closed: a new
// member is a major protocol change.
type TriggerKind string

const (
	TriggerKindBootstrap        TriggerKind = "bootstrap"
	TriggerKindSchedule         TriggerKind = "schedule"
	TriggerKindWebhook          TriggerKind = "webhook"
	TriggerKindReplay           TriggerKind = "replay"
	TriggerKindOperatorRecovery TriggerKind = "operator_recovery"
)

// Validate checks that the trigger kind is known.
func (k TriggerKind) Validate() error {
	switch k {
	case TriggerKindBootstrap, TriggerKindSchedule, TriggerKindWebhook, TriggerKindReplay, TriggerKindOperatorRecovery:
		return nil
	default:
		return fmt.Errorf("unknown workflow trigger kind %q", string(k))
	}
}

// RunStatus describes layered workflow completion. Closed.
type RunStatus string

const (
	RunStatusCollectionPending  RunStatus = "collection_pending"
	RunStatusCollectionActive   RunStatus = "collection_active"
	RunStatusCollectionComplete RunStatus = "collection_complete"
	RunStatusReducerConverging  RunStatus = "reducer_converging"
	RunStatusComplete           RunStatus = "complete"
	RunStatusFailed             RunStatus = "failed"
)

// Validate checks that the run status is known.
func (s RunStatus) Validate() error {
	switch s {
	case RunStatusCollectionPending, RunStatusCollectionActive, RunStatusCollectionComplete, RunStatusReducerConverging, RunStatusComplete, RunStatusFailed:
		return nil
	default:
		return fmt.Errorf("unknown workflow run status %q", string(s))
	}
}

// WorkItemStatus describes the durable lifecycle of one bounded collector
// slice. Closed.
type WorkItemStatus string

const (
	WorkItemStatusPending         WorkItemStatus = "pending"
	WorkItemStatusClaimed         WorkItemStatus = "claimed"
	WorkItemStatusCompleted       WorkItemStatus = "completed"
	WorkItemStatusFailedRetryable WorkItemStatus = "failed_retryable"
	WorkItemStatusFailedTerminal  WorkItemStatus = "failed_terminal"
	WorkItemStatusExpired         WorkItemStatus = "expired"
)

// Validate checks that the work-item status is known.
func (s WorkItemStatus) Validate() error {
	switch s {
	case WorkItemStatusPending, WorkItemStatusClaimed, WorkItemStatusCompleted, WorkItemStatusFailedRetryable, WorkItemStatusFailedTerminal, WorkItemStatusExpired:
		return nil
	default:
		return fmt.Errorf("unknown workflow work-item status %q", string(s))
	}
}

// ClaimStatus describes the lifecycle of one ownership epoch. Closed.
type ClaimStatus string

const (
	ClaimStatusActive          ClaimStatus = "active"
	ClaimStatusCompleted       ClaimStatus = "completed"
	ClaimStatusFailedRetryable ClaimStatus = "failed_retryable"
	ClaimStatusFailedTerminal  ClaimStatus = "failed_terminal"
	ClaimStatusExpired         ClaimStatus = "expired"
	ClaimStatusReleased        ClaimStatus = "released"
)

// Validate checks that the claim status is known.
func (s ClaimStatus) Validate() error {
	switch s {
	case ClaimStatusActive, ClaimStatusCompleted, ClaimStatusFailedRetryable, ClaimStatusFailedTerminal, ClaimStatusExpired, ClaimStatusReleased:
		return nil
	default:
		return fmt.Errorf("unknown workflow claim status %q", string(s))
	}
}

// CollectorMode describes how one collector instance is expected to run.
// Closed.
type CollectorMode string

const (
	CollectorModeContinuous CollectorMode = "continuous"
	CollectorModeScheduled  CollectorMode = "scheduled"
	CollectorModeManual     CollectorMode = "manual"
)

// Validate checks that the collector mode is known.
func (m CollectorMode) Validate() error {
	switch m {
	case CollectorModeContinuous, CollectorModeScheduled, CollectorModeManual:
		return nil
	default:
		return fmt.Errorf("unknown collector mode %q", string(m))
	}
}
