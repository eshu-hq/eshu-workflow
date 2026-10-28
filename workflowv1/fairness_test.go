// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"testing"
	"time"
)

func TestSchedulerRotatesWithinFamily(t *testing.T) {
	scheduler, err := NewFamilyFairnessScheduler([]FairnessCandidate{
		{CollectorKind: "jira", CollectorInstanceID: "b"},
		{CollectorKind: "jira", CollectorInstanceID: "a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	first, ok := scheduler.Next()
	if !ok || first.CollectorInstanceID != "a" {
		t.Fatalf("first = %+v, want instance a", first)
	}
	second, ok := scheduler.Next()
	if !ok || second.CollectorInstanceID != "b" {
		t.Fatalf("second = %+v, want instance b", second)
	}
}

func TestSchedulerWeightsFamilies(t *testing.T) {
	scheduler, err := NewFamilyFairnessScheduler([]FairnessCandidate{
		{CollectorKind: "heavy", CollectorInstanceID: "h1", Weight: 3},
		{CollectorKind: "light", CollectorInstanceID: "l1", Weight: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for range 8 {
		target, ok := scheduler.Next()
		if !ok {
			t.Fatal("scheduler went dry")
		}
		counts[string(target.CollectorKind)]++
	}
	if counts["heavy"] != 6 || counts["light"] != 2 {
		t.Fatalf("counts = %v, want heavy=6 light=2", counts)
	}
}

func TestSchedulerRejectsBadCandidates(t *testing.T) {
	if _, err := NewFamilyFairnessScheduler([]FairnessCandidate{{CollectorInstanceID: "a"}}); err == nil {
		t.Fatal("blank kind built a scheduler")
	}
	if _, err := NewFamilyFairnessScheduler([]FairnessCandidate{{CollectorKind: "jira"}}); err == nil {
		t.Fatal("blank instance built a scheduler")
	}
	if _, err := NewFamilyFairnessScheduler([]FairnessCandidate{{CollectorKind: "jira", CollectorInstanceID: "a", Weight: -2}}); err == nil {
		t.Fatal("negative weight built a scheduler")
	}
	empty, err := NewFamilyFairnessScheduler(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := empty.Next(); ok {
		t.Fatal("empty scheduler issued a target")
	}
}

func TestCandidatesFromInstancesSkipsNonClaiming(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	candidates, err := FairnessCandidatesFromCollectorInstances([]CollectorInstance{
		{InstanceID: "on", CollectorKind: "jira", Mode: CollectorModeScheduled, Enabled: true, ClaimsEnabled: true, CreatedAt: now, UpdatedAt: now},
		{InstanceID: "off", CollectorKind: "jira", Mode: CollectorModeScheduled, Enabled: true, ClaimsEnabled: false, CreatedAt: now, UpdatedAt: now},
		{InstanceID: "weighted", CollectorKind: "loki", Mode: CollectorModeContinuous, Enabled: true, ClaimsEnabled: true, Configuration: `{"fairness_weight": 4}`, CreatedAt: now, UpdatedAt: now},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 {
		t.Fatalf("candidates = %+v, want the 2 claiming instances", candidates)
	}
	for _, c := range candidates {
		if c.CollectorInstanceID == "weighted" && c.Weight != 4 {
			t.Fatalf("weighted candidate = %+v, want weight 4", c)
		}
	}
	if _, err := FairnessCandidatesFromCollectorInstances([]CollectorInstance{
		{InstanceID: "bad", CollectorKind: "jira", Enabled: true, ClaimsEnabled: true, Configuration: `{"fairness_weight": 0}`},
	}); err == nil {
		t.Fatal("zero fairness_weight passed")
	}
}
