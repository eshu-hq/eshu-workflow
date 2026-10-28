// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"strings"
	"testing"
)

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
