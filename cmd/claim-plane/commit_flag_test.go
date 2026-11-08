// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package main

import (
	"context"
	"strings"
	"testing"
)

// TestParseFlagsAcceptsCoreCommitURL requires the proxy target to be
// configurable by flag or environment, defaulting to unset (dark: the
// route fails closed until an operator configures core).
func TestParseFlagsAcceptsCoreCommitURL(t *testing.T) {
	opts, err := parseFlags([]string{
		"--database-url", "postgres://localhost:5432/x?sslmode=disable",
		"--core-commit-url", "http://core:8080",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if opts.coreCommitURL != "http://core:8080" {
		t.Fatalf("coreCommitURL = %q, want the flag value", opts.coreCommitURL)
	}

	// A malformed non-empty URL is a startup error, never a silent dark
	// boot: run rejects it before dialing the database.
	if err := run(context.Background(), testLogger(), options{
		databaseURL:   "postgres://localhost:5432/x?sslmode=disable",
		coreCommitURL: "gopher://core",
	}); err == nil {
		t.Fatal("malformed core commit URL accepted")
	}
	if err := run(context.Background(), testLogger(), options{
		databaseURL:   "postgres://localhost:5432/x?sslmode=disable",
		coreCommitURL: "http://core:8080",
	}); err == nil || strings.Contains(err.Error(), "core commit") {
		t.Fatalf("valid core URL must proceed past validation to dialing, got %v", err)
	}

	t.Setenv("CLAIMPLANE_CORE_COMMIT_URL", "http://core-from-env:8080")
	opts, err = parseFlags([]string{
		"--database-url", "postgres://localhost:5432/x?sslmode=disable",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if opts.coreCommitURL != "http://core-from-env:8080" {
		t.Fatalf("coreCommitURL = %q, want the env value", opts.coreCommitURL)
	}
}

// TestRunRejectsUnparseableCoreCommitURL requires startup to fail fast on
// a URL no parser accepts: the proxy never boots with a target it cannot
// even parse, instead of failing later at the first forward.
func TestRunRejectsUnparseableCoreCommitURL(t *testing.T) {
	err := run(context.Background(), testLogger(), options{
		databaseURL:   "postgres://localhost:5432/x?sslmode=disable",
		coreCommitURL: "http://[::1",
	})
	if err == nil || !strings.Contains(err.Error(), "core commit") {
		t.Fatalf("unparseable core commit URL: err = %v, want invalid core commit URL", err)
	}
}
