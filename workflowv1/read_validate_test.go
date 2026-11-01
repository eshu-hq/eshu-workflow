// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"testing"
	"time"
)

func validCollectorInstance() CollectorInstance {
	now := time.Now().UTC().Truncate(time.Second)
	return CollectorInstance{
		InstanceID: "jira-primary", CollectorKind: "jira", Mode: CollectorModeManual,
		CreatedAt: now, UpdatedAt: now,
	}
}

func validCompleteness() CompletenessState {
	now := time.Now().UTC().Truncate(time.Second)
	return CompletenessState{
		RunID: "run-1", CollectorKind: "jira", Keyspace: "default",
		PhaseName: "collect", Status: "complete", ObservedAt: now, UpdatedAt: now,
	}
}

func TestRunSummaryValidateDelegatesToRun(t *testing.T) {
	summary := RunSummary(validRun())
	if err := summary.Validate(); err != nil {
		t.Fatal(err)
	}
	summary.Status = "paused"
	wantErrContaining(t, summary.Validate(), "run status")
}

func TestGetRunStatusResponseValidate(t *testing.T) {
	now := time.Now().UTC()
	valid := GetRunStatusResponse{
		Run:          validRun(),
		Progress:     []CollectorRunProgress{{CollectorKind: "jira", TotalWorkItems: 2}},
		Completeness: []CompletenessState{validCompleteness()},
		AsOf:         now,
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}

	badRun := valid
	badRun.Run.RunID = ""
	wantErrContaining(t, badRun.Validate(), "run_id")

	badProgress := valid
	badProgress.Progress = []CollectorRunProgress{{CollectorKind: "jira", TotalWorkItems: -1}}
	wantErrContaining(t, badProgress.Validate(), "progress[0]")

	badState := valid
	badState.Completeness = []CompletenessState{{}}
	wantErrContaining(t, badState.Validate(), "completeness[0]")

	noAsOf := valid
	noAsOf.AsOf = time.Time{}
	wantErrContaining(t, noAsOf.Validate(), "as_of")
}

func TestListRunsRequestValidateAndEffectiveLimit(t *testing.T) {
	if err := (ListRunsRequest{Status: RunStatusComplete, Limit: 10}).Validate(); err != nil {
		t.Fatal(err)
	}
	wantErrContaining(t, (ListRunsRequest{Status: "paused"}).Validate(), "run status")
	wantErrContaining(t, (ListRunsRequest{Limit: -1}).Validate(), "limit")
	wantErrContaining(t, (ListRunsRequest{Limit: maxListRunsLimit + 1}).Validate(), "limit")

	tests := []struct {
		limit int
		want  int
	}{
		{0, defaultListRunsLimit},
		{-5, defaultListRunsLimit},
		{1, 1},
		{maxListRunsLimit, maxListRunsLimit},
	}
	for _, tc := range tests {
		if got := (ListRunsRequest{Limit: tc.limit}).EffectiveLimit(); got != tc.want {
			t.Fatalf("EffectiveLimit(%d) = %d, want %d", tc.limit, got, tc.want)
		}
	}
}

func TestListRunsResponseValidate(t *testing.T) {
	valid := ListRunsResponse{Runs: []RunSummary{RunSummary(validRun())}, AsOf: time.Now()}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := valid
	bad.Runs = []RunSummary{{}}
	wantErrContaining(t, bad.Validate(), "runs[0]")
	noAsOf := valid
	noAsOf.AsOf = time.Time{}
	wantErrContaining(t, noAsOf.Validate(), "as_of")
}

func TestRunIDRequestsRequireIdentifier(t *testing.T) {
	if err := (GetRunStatusRequest{RunID: "run-1"}).Validate(); err != nil {
		t.Fatal(err)
	}
	wantErrContaining(t, (GetRunStatusRequest{}).Validate(), "run_id")
	if err := (GetRunCompletenessRequest{RunID: "run-1"}).Validate(); err != nil {
		t.Fatal(err)
	}
	wantErrContaining(t, (GetRunCompletenessRequest{}).Validate(), "run_id")
}

func TestGetRunCompletenessResponseValidate(t *testing.T) {
	valid := GetRunCompletenessResponse{RunID: "run-1", States: []CompletenessState{validCompleteness()}, AsOf: time.Now()}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	noRun := valid
	noRun.RunID = ""
	wantErrContaining(t, noRun.Validate(), "run_id")
	badState := valid
	badState.States = []CompletenessState{{}}
	wantErrContaining(t, badState.Validate(), "states[0]")
	noAsOf := valid
	noAsOf.AsOf = time.Time{}
	wantErrContaining(t, noAsOf.Validate(), "as_of")
}

func TestListCollectorInstancesResponseValidate(t *testing.T) {
	valid := ListCollectorInstancesResponse{Instances: []CollectorInstance{validCollectorInstance()}, AsOf: time.Now()}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := valid
	bad.Instances = []CollectorInstance{{}}
	wantErrContaining(t, bad.Validate(), "instances[0]")
	noAsOf := valid
	noAsOf.AsOf = time.Time{}
	wantErrContaining(t, noAsOf.Validate(), "as_of")
}

func TestFamilyQueueDepthValidate(t *testing.T) {
	if err := (FamilyQueueDepth{CollectorKind: "jira", Pending: 1, Claimed: 2}).Validate(); err != nil {
		t.Fatal(err)
	}
	wantErrContaining(t, (FamilyQueueDepth{}).Validate(), "collector_kind")
	wantErrContaining(t, (FamilyQueueDepth{CollectorKind: "jira", Pending: -1}).Validate(), "pending")
	wantErrContaining(t, (FamilyQueueDepth{CollectorKind: "jira", Claimed: -1}).Validate(), "claimed")
}

func TestQueueDepthSnapshotValidate(t *testing.T) {
	valid := QueueDepthSnapshot{
		Families:      []FamilyQueueDepth{{CollectorKind: "jira", Pending: 1}},
		GlobalPending: 1,
		GlobalClaimed: 0,
		AsOf:          time.Now(),
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	badFamily := valid
	badFamily.Families = []FamilyQueueDepth{{}}
	wantErrContaining(t, badFamily.Validate(), "families[0]")
	negPending := valid
	negPending.GlobalPending = -1
	wantErrContaining(t, negPending.Validate(), "global_pending")
	negClaimed := valid
	negClaimed.GlobalClaimed = -1
	wantErrContaining(t, negClaimed.Validate(), "global_claimed")
	noAsOf := valid
	noAsOf.AsOf = time.Time{}
	wantErrContaining(t, noAsOf.Validate(), "as_of")
}
