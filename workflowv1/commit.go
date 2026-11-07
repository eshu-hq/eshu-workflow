// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	// MaxCommitResultBytes caps one commit's result payload. It pairs
	// with the producer stdout bound: results the producer legally
	// emits must pass, so the cap never shrinks below what the
	// adapter may write.
	MaxCommitResultBytes = 4 << 20
	// MaxCommitBodyBytes caps the whole CommitClaimFacts body on both
	// the satellite and core: the result plus a 64 KiB budget for the
	// mutation, idempotency key, planned IDs, and JSON framing. The
	// body cap itself is the bound on unbounded identifier fields:
	// validateIdentifier sets no length limit, so an envelope past
	// this size fails closed before any forward.
	MaxCommitBodyBytes = MaxCommitResultBytes + 64<<10
)

// CommitClaimFactsRequest asks core to persist one claim's fact result
// through the claimed-commit fence. The mutation stays embedded and
// flat on the wire; the result rides as raw JSON so neither the client
// nor the satellite proxy interprets it — only core strict-decodes it.
type CommitClaimFactsRequest struct {
	ClaimMutation
	// IdempotencyKey lets a retried commit after an ambiguous ack land
	// exactly once: core matches it before writing.
	IdempotencyKey string `json:"idempotency_key"`
	// ResultJSON is the opaque fact result. It is never decoded outside
	// core; the satellite measures only its length.
	ResultJSON json.RawMessage `json:"result_json"`
	// ScopeID, SourceRunID, and GenerationID name the planned identity
	// core matches the commit pin against. Core never scrapes its
	// target out of the result echo: the echo is checked against these
	// fields, and the satellite pin matches them against the plane row.
	ScopeID      string `json:"scope_id"`
	SourceRunID  string `json:"source_run_id"`
	GenerationID string `json:"generation_id"`
}

// Validate checks the client-side commit contract: the fenced mutation,
// a present and bounded result, and the planned IDs.
func (r CommitClaimFactsRequest) Validate() error {
	if err := r.ClaimMutation.Validate(); err != nil {
		return err
	}
	if err := validateIdentifier("idempotency_key", r.IdempotencyKey); err != nil {
		return err
	}
	if len(r.ResultJSON) == 0 || len(strings.TrimSpace(string(r.ResultJSON))) == 0 {
		return fmt.Errorf("result_json must be present")
	}
	if len(r.ResultJSON) > MaxCommitResultBytes {
		return fmt.Errorf("result_json of %d bytes exceeds the %d-byte cap",
			len(r.ResultJSON), MaxCommitResultBytes)
	}
	if err := validateIdentifier("scope_id", r.ScopeID); err != nil {
		return err
	}
	if err := validateIdentifier("source_run_id", r.SourceRunID); err != nil {
		return err
	}
	if err := validateIdentifier("generation_id", r.GenerationID); err != nil {
		return err
	}
	return nil
}
