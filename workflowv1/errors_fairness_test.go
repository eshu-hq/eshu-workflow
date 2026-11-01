// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"net/http"
	"testing"
)

func TestEveryProtocolCodeMapsToItsStatus(t *testing.T) {
	tests := []struct {
		code string
		want int
	}{
		{CodeInvalid, http.StatusBadRequest},
		{CodeVersionMismatch, http.StatusBadRequest},
		{CodeNotFound, http.StatusNotFound},
		{CodeClaimConflict, http.StatusConflict},
		{CodeStaleFence, http.StatusConflict},
		{CodeLeaseExpired, http.StatusConflict},
		{CodePaced, http.StatusTooManyRequests},
		{"internal", http.StatusInternalServerError},
		{"a code from the future", http.StatusInternalServerError},
	}
	for _, tc := range tests {
		err := &ProtocolError{Code: tc.code, Message: "m"}
		if got := err.HTTPStatus(); got != tc.want {
			t.Fatalf("HTTPStatus(%q) = %d, want %d", tc.code, got, tc.want)
		}
	}
}

func TestFairnessCandidateValidateRejectsBlankIdentity(t *testing.T) {
	wantErrContaining(t, (FairnessCandidate{CollectorInstanceID: "a"}).Validate(), "collector_kind")
	wantErrContaining(t, (FairnessCandidate{CollectorKind: "jira"}).Validate(), "collector_instance_id")
	wantErrContaining(t, (FairnessCandidate{CollectorKind: "jira", CollectorInstanceID: "a", Weight: -2}).Validate(), `"jira"`)
}

func TestFairnessWeightFromConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    int
		wantErr string
	}{
		{"blank uses default", "", defaultFairnessWeight, ""},
		{"omitted field uses default", `{"other":1}`, defaultFairnessWeight, ""},
		{"explicit weight", `{"fairness_weight":4}`, 4, ""},
		{"zero is invalid", `{"fairness_weight":0}`, 0, "must be positive"},
		{"negative is invalid", `{"fairness_weight":-3}`, 0, "must be positive"},
		{"not json", `{nope`, 0, "parse fairness configuration"},
		{"wrong type", `{"fairness_weight":"heavy"}`, 0, "parse fairness configuration"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := fairnessWeightFromConfiguration(tc.raw)
			if tc.wantErr != "" {
				wantErrContaining(t, err, tc.wantErr)
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("weight = %d, %v, want %d", got, err, tc.want)
			}
		})
	}
}

func TestFairnessCandidatesRejectBadInstanceWeights(t *testing.T) {
	instances := []CollectorInstance{{
		InstanceID: "jira-a", CollectorKind: "jira", Enabled: true, ClaimsEnabled: true,
		Configuration: `{"fairness_weight":0}`,
	}}
	if _, err := FairnessCandidatesFromCollectorInstances(instances); err == nil {
		t.Fatal("zero fairness weight produced candidates")
	}
}
