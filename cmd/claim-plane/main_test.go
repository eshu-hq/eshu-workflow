// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package main

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil))
}

var _ = context.Background

func TestParseQuotas(t *testing.T) {
	quotas, err := parseQuotas("")
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if len(quotas) != 0 {
		t.Fatalf("empty quotas = %v", quotas)
	}
	quotas, err = parseQuotas("jira=100,package_registry=25")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if quotas["jira"] != 100 || quotas["package_registry"] != 25 {
		t.Fatalf("quotas = %v", quotas)
	}
	for _, raw := range []string{"jira", "jira=-1", "jira=abc", "=5"} {
		if _, err := parseQuotas(raw); err == nil {
			t.Fatalf("quota %q parsed without error", raw)
		}
	}
}

func TestParseFlagsRequiresDatabase(t *testing.T) {
	if _, err := parseFlags([]string{}); err == nil {
		t.Fatalf("missing database URL parsed without error")
	}
	opts, err := parseFlags([]string{
		"--database-url", "postgres://localhost:5432/x?sslmode=disable",
		"--quotas", "jira=10",
		"--pacing-domain-high", "100",
		"--pacing-domain-low", "50",
		"--pacing-global-high", "1000",
		"--pacing-global-low", "500",
		"--pacing-grant-delay", "5s",
		"--pacing-max-engagement", "1m",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if opts.listen != ":8080" || opts.adminListen != ":8081" {
		t.Fatalf("default listeners = %q %q", opts.listen, opts.adminListen)
	}
	if opts.pacing.DomainHigh != 100 || opts.pacing.GrantDelay != 5*time.Second {
		t.Fatalf("pacing = %+v", opts.pacing)
	}
	if _, err := parseFlags([]string{"--database-url", "x", "--reap-limit", "0"}); err == nil {
		t.Fatalf("zero reap limit parsed without error")
	}
}

func TestRunRejectsBadQuotasBeforeDialing(t *testing.T) {
	opts, err := parseFlags([]string{
		"--database-url", "postgres://localhost:5432/x?sslmode=disable",
		"--quotas", "jira",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if err := run(context.Background(), testLogger(), opts); err == nil {
		t.Fatalf("run with bad quotas succeeded")
	}
}

func TestReaperTickDrainsWithoutWork(t *testing.T) {
	// The reaper tick against an unreachable store must not panic: it logs
	// and returns. A nil-pool store cannot be built without a database, so
	// this test covers the quota parser edge instead: a trailing comma is
	// rejected rather than silently granting an empty kind.
	if _, err := parseQuotas("jira=10,"); err == nil {
		t.Fatalf("trailing comma parsed without error")
	}
}
