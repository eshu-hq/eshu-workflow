// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"testing"
	"time"
)

func TestReportRequiresProgress(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	req := ReportRunProgressRequest{
		RunID:      "run-1",
		ObservedAt: now,
		Progress: []CollectorRunProgress{{
			CollectorKind:          "jira",
			TotalWorkItems:         10,
			CompletedWorkItems:     4,
			FactlessCompletedItems: 4,
			PublishedPhaseCounts:   map[string]int{"default/collect": 4},
		}},
	}
	if err := req.Validate(); err != nil {
		t.Fatal(err)
	}
	empty := req
	empty.Progress = nil
	if err := empty.Validate(); err == nil {
		t.Fatal("empty progress report validated")
	}
}

func TestFactlessNeverExceedsCompleted(t *testing.T) {
	bad := CollectorRunProgress{
		CollectorKind:          "jira",
		CompletedWorkItems:     2,
		FactlessCompletedItems: 3,
	}
	if err := bad.Validate(); err == nil {
		t.Fatal("factless exceeding completed validated")
	}
}

func TestProgressCountsNeverNegative(t *testing.T) {
	bad := CollectorRunProgress{CollectorKind: "jira", PendingWorkItems: -1}
	if err := bad.Validate(); err == nil {
		t.Fatal("negative pending count validated")
	}
	bad = CollectorRunProgress{
		CollectorKind:            "jira",
		TerminalDeadLetterCounts: map[string]int{"default/collect": -1},
	}
	if err := bad.Validate(); err == nil {
		t.Fatal("negative dead-letter count validated")
	}
}

func TestPhaseKeyIdentity(t *testing.T) {
	if err := (PhasePublicationKey{Keyspace: "default", PhaseName: "collect"}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (PhasePublicationKey{PhaseName: "collect"}).Validate(); err == nil {
		t.Fatal("blank keyspace validated")
	}
}
