// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"testing"
	"time"
)

func TestCollectorRunProgressValidateRejections(t *testing.T) {
	valid := CollectorRunProgress{
		CollectorKind:        "jira",
		TotalWorkItems:       5,
		CompletedWorkItems:   3,
		PublishedPhaseCounts: map[string]int{"default/collect": 2},
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	blank := valid
	blank.CollectorKind = ""
	wantErrContaining(t, blank.Validate(), "collector_kind")

	negPublished := valid
	negPublished.PublishedPhaseCounts = map[string]int{"default/collect": -1}
	wantErrContaining(t, negPublished.Validate(), "published_phase_counts")

	negDead := valid
	negDead.TerminalDeadLetterCounts = map[string]int{"default/collect": -2}
	wantErrContaining(t, negDead.Validate(), "terminal_dead_letter_counts")

	okDead := valid
	okDead.TerminalDeadLetterCounts = map[string]int{"default/collect": 2}
	if err := okDead.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestReportRunProgressRequestValidateRejections(t *testing.T) {
	progress := []CollectorRunProgress{{CollectorKind: "jira", TotalWorkItems: 1}}
	valid := ReportRunProgressRequest{RunID: "run-1", ObservedAt: time.Now(), Progress: progress}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	noRun := valid
	noRun.RunID = ""
	wantErrContaining(t, noRun.Validate(), "run_id")
	noTime := valid
	noTime.ObservedAt = time.Time{}
	wantErrContaining(t, noTime.Validate(), "observed_at")
	empty := valid
	empty.Progress = nil
	wantErrContaining(t, empty.Validate(), "progress must not be empty")
	bad := valid
	bad.Progress = []CollectorRunProgress{{CollectorKind: "jira"}, {}}
	wantErrContaining(t, bad.Validate(), "progress[1]")
}
