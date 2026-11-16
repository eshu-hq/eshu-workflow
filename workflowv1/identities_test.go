// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"strings"
	"testing"
)

// TestRunStatusTerminalClassification pins the closed status set the
// admission guard walks: every known status classifies terminal or open,
// and the open set is exactly what the guard pages. Adding a RunStatus
// fails here until it is classified.
func TestRunStatusTerminalClassification(t *testing.T) {
	t.Parallel()

	all := AllRunStatuses()
	if len(all) != 6 {
		t.Fatalf("AllRunStatuses() has %d entries, want 6: classify the new status", len(all))
	}
	terminal := map[RunStatus]bool{}
	open := map[RunStatus]bool{}
	for _, status := range all {
		if err := status.Validate(); err != nil {
			t.Fatalf("AllRunStatuses() entry %q fails Validate: %v", string(status), err)
		}
		if status.IsTerminal() {
			terminal[status] = true
		} else {
			open[status] = true
		}
	}
	for _, want := range []RunStatus{RunStatusComplete, RunStatusFailed} {
		if !terminal[want] {
			t.Fatalf("status %q must classify terminal", string(want))
		}
	}
	for _, want := range []RunStatus{
		RunStatusCollectionPending,
		RunStatusCollectionActive,
		RunStatusCollectionComplete,
		RunStatusReducerConverging,
	} {
		if !open[want] {
			t.Fatalf("status %q must classify open (guard walks it)", string(want))
		}
	}
}

func TestClosedEnumsAcceptKnownMembers(t *testing.T) {
	if err := TriggerKindBootstrap.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := RunStatusComplete.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := WorkItemStatusPending.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := ClaimStatusActive.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := CollectorModeScheduled.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := ClaimPacingSignalDomain.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestClosedEnumsRejectUnknownMembers(t *testing.T) {
	validators := map[string]func() error{
		"trigger":   func() error { return TriggerKind("someday").Validate() },
		"run":       func() error { return RunStatus("done-ish").Validate() },
		"work item": func() error { return WorkItemStatus("almost").Validate() },
		"claim":     func() error { return ClaimStatus("held").Validate() },
		"mode":      func() error { return CollectorMode("sometimes").Validate() },
		"pacing":    func() error { return ClaimPacingSignal("regional").Validate() },
	}
	for name, validate := range validators {
		if err := validate(); err == nil {
			t.Fatalf("%s enum accepted an unknown member", name)
		}
	}
}

func TestCollectorKindIsOpenButNeverBlank(t *testing.T) {
	if err := CollectorKind("jira").Validate(); err != nil {
		t.Fatal(err)
	}
	// A family the reader never heard of still validates: families grow
	// without a protocol major.
	if err := CollectorKind("brand-new-family").Validate(); err != nil {
		t.Fatal(err)
	}
	for _, blank := range []string{"", "   "} {
		if err := CollectorKind(blank).Validate(); err == nil {
			t.Fatalf("CollectorKind(%q) validated, want an error", blank)
		}
	}
}

func TestUnknownEnumErrorNamesTheValue(t *testing.T) {
	err := ClaimStatus("held").Validate()
	if err == nil || !strings.Contains(err.Error(), "held") {
		t.Fatalf("error = %v, want it to name the value", err)
	}
}
