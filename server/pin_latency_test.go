// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	"github.com/eshu-hq/eshu-workflow/server"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestPinRecordsCommitLatency requires the commit-pin path to observe one
// server-side latency sample per pin attempt on
// workflow_commit_pin_duration_seconds, labeled by operation and protocol
// result code, so a future server-latency SLO has a signal to read.
func TestPinRecordsCommitLatency(t *testing.T) {
	body := routeCases()[7].validBody
	if routeCases()[7].op != workflowv1.OpValidateAndPinClaimForCommit {
		t.Fatal("route case order changed; pin case is no longer index 7")
	}
	path := pathFor(t, workflowv1.OpValidateAndPinClaimForCommit)

	pinned := &fakeStore{pinResp: workflowv1.CommitPinResponse{Pinned: true, CommitID: "commit-1"}}
	reg := prometheus.NewRegistry()
	srv, err := server.New(pinned, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.Instruments = telemetry.NewInstruments(reg)
	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()
	resp := post(t, httpSrv, path, body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pin status = %d, want 200", resp.StatusCode)
	}
	if got := histogramSamples(t, reg, "workflow_commit_pin_duration_seconds",
		map[string]string{"operation": "ValidateAndPinClaimForCommit", "code": "ok"}); got != 1 {
		t.Fatalf("pin latency samples {ok} = %d, want 1", got)
	}

	stale := &fakeStore{claimErr: &workflowv1.ProtocolError{Code: workflowv1.CodeStaleFence, Message: "superseded"}}
	reg2 := prometheus.NewRegistry()
	srv2, err := server.New(stale, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv2.Instruments = telemetry.NewInstruments(reg2)
	httpSrv2 := httptest.NewServer(srv2.Handler())
	defer httpSrv2.Close()
	resp = post(t, httpSrv2, path, body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("stale pin status = %d, want 409", resp.StatusCode)
	}
	if got := histogramSamples(t, reg2, "workflow_commit_pin_duration_seconds",
		map[string]string{"operation": "ValidateAndPinClaimForCommit", "code": workflowv1.CodeStaleFence}); got != 1 {
		t.Fatalf("pin latency samples {stale_fence} = %d, want 1", got)
	}
}

// histogramSamples sums the sample counts of one histogram family whose
// labels include every pair in want.
func histogramSamples(t *testing.T, reg *prometheus.Registry, name string, want map[string]string) uint64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var total uint64
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			match := true
			for k, v := range want {
				if labels[k] != v {
					match = false
				}
			}
			if match && metric.GetHistogram() != nil {
				total += metric.GetHistogram().GetSampleCount()
			}
		}
	}
	return total
}
