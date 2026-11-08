// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestCommitCapsShareOneArithmeticSource pins the size contract the
// satellite proxy and core both enforce: a 4 MiB result plus a 64 KiB
// framing budget, from one constant so the two sides cannot drift.
func TestCommitCapsShareOneArithmeticSource(t *testing.T) {
	if MaxCommitResultBytes != 4<<20 {
		t.Fatalf("MaxCommitResultBytes = %d, want %d", MaxCommitResultBytes, 4<<20)
	}
	if MaxCommitBodyBytes != MaxCommitResultBytes+64<<10 {
		t.Fatalf("MaxCommitBodyBytes = %d, want result+64KiB = %d",
			MaxCommitBodyBytes, MaxCommitResultBytes+64<<10)
	}
	if MaxCommitBodyBytes != 4259840 {
		t.Fatalf("MaxCommitBodyBytes = %d, want 4259840", MaxCommitBodyBytes)
	}
}

func validCommitMutation() ClaimMutation {
	return ClaimMutation{
		WorkItemID: "item-1", ClaimID: "c-1", FencingToken: 7,
		OwnerID: "o-1", ObservedAt: time.Now().UTC(),
		LeaseDuration: time.Minute,
	}
}

func validCommitRequest() CommitClaimFactsRequest {
	return CommitClaimFactsRequest{
		ClaimMutation:  validCommitMutation(),
		IdempotencyKey: "idem-1",
		ResultJSON:     json.RawMessage(`{"fact_rows":[]}`),
		ScopeID:        "scope-1",
		SourceRunID:    "sr-1",
		GenerationID:   "gen-1",
	}
}

// TestCommitClaimFactsRequestValidate requires the client-side contract:
// a fenced mutation, a bounded result, and the planned IDs core matches
// the pin against.
func TestCommitClaimFactsRequestValidate(t *testing.T) {
	if err := validCommitRequest().Validate(); err != nil {
		t.Fatalf("valid request: %v", err)
	}
	cases := map[string]func(*CommitClaimFactsRequest){
		"bad mutation":   func(r *CommitClaimFactsRequest) { r.FencingToken = 0 },
		"empty idem key": func(r *CommitClaimFactsRequest) { r.IdempotencyKey = "" },
		"empty result":   func(r *CommitClaimFactsRequest) { r.ResultJSON = nil },
		"blank result":   func(r *CommitClaimFactsRequest) { r.ResultJSON = json.RawMessage(`   `) },
		"oversize result": func(r *CommitClaimFactsRequest) {
			r.ResultJSON = json.RawMessage(strings.Repeat("x", MaxCommitResultBytes+1))
		},
		"empty scope":      func(r *CommitClaimFactsRequest) { r.ScopeID = "" },
		"empty source run": func(r *CommitClaimFactsRequest) { r.SourceRunID = "" },
		"empty generation": func(r *CommitClaimFactsRequest) { r.GenerationID = "" },
	}
	for name, breakReq := range cases {
		req := validCommitRequest()
		breakReq(&req)
		if err := req.Validate(); err == nil {
			t.Fatalf("%s validated without error", name)
		}
	}
	// A result at exactly the cap passes: producers may legally emit it.
	atCap := validCommitRequest()
	atCap.ResultJSON = json.RawMessage(strings.Repeat("x", MaxCommitResultBytes))
	if err := atCap.Validate(); err != nil {
		t.Fatalf("result at cap: %v", err)
	}
}

// TestCommitRequestStaysFlatOnTheWire requires the embedded mutation to
// serialize at the top level beside the commit fields, and the result to
// survive a round trip byte-for-byte without interpretation.
func TestCommitRequestStaysFlatOnTheWire(t *testing.T) {
	rawResult := `{"fact_rows":[{"id":"f-1"}],"unknown_future_field":[1,2,{}]}`
	req := validCommitRequest()
	req.ResultJSON = json.RawMessage(rawResult)
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, field := range []string{
		"work_item_id", "claim_id", "fencing_token", "idempotency_key",
		"result_json", "scope_id", "source_run_id", "generation_id",
	} {
		if _, ok := decoded[field]; !ok {
			t.Fatalf("wire body misses top-level field %q: %s", field, raw)
		}
	}
	if string(decoded["result_json"]) != rawResult {
		t.Fatalf("result_json round trip = %s, want %s", decoded["result_json"], rawResult)
	}
}
