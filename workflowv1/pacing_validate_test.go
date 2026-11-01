// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"strings"
	"testing"
	"time"
)

func TestClaimPacingConfigValidateRejections(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ClaimPacingConfig)
		want   string
	}{
		{"global low at high", func(c *ClaimPacingConfig) { c.GlobalLow = c.GlobalHigh }, "global low"},
		{"global low above high", func(c *ClaimPacingConfig) { c.GlobalLow = c.GlobalHigh + 1 }, "global low"},
		{"negative domain low", func(c *ClaimPacingConfig) { c.DomainLow = -1 }, "domain low"},
		{"negative global low", func(c *ClaimPacingConfig) { c.GlobalLow = -1 }, "global low"},
		{"negative cache ttl", func(c *ClaimPacingConfig) { c.DepthCacheTTL = -time.Second }, "depth cache TTL"},
		{"missing grant delay", func(c *ClaimPacingConfig) { c.GrantDelay = 0 }, "partially configured"},
		{"missing domain high", func(c *ClaimPacingConfig) { c.DomainHigh = 0 }, "partially configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := pacingConfig()
			tc.mutate(&cfg)
			wantErrContaining(t, cfg.Validate(), tc.want)
		})
	}
	cached := pacingConfig()
	cached.DepthCacheTTL = time.Second
	if err := cached.Validate(); err != nil {
		t.Fatalf("positive depth cache TTL rejected: %v", err)
	}
}

func TestClaimPacingSignalValidate(t *testing.T) {
	for _, s := range []ClaimPacingSignal{ClaimPacingSignalDomain, ClaimPacingSignalGlobal} {
		if err := s.Validate(); err != nil {
			t.Fatalf("%q rejected: %v", s, err)
		}
	}
	wantErrContaining(t, ClaimPacingSignal("").Validate(), "unknown claim pacing signal")
	wantErrContaining(t, ClaimPacingSignal("regional").Validate(), `"regional"`)
}

func TestClaimPacingDepthsValidate(t *testing.T) {
	if err := (ClaimPacingDepths{Domain: 1, Global: 2}).Validate(); err != nil {
		t.Fatal(err)
	}
	wantErrContaining(t, (ClaimPacingDepths{Domain: -1}).Validate(), "domain depth")
	wantErrContaining(t, (ClaimPacingDepths{Global: -1}).Validate(), "global depth")
}

func TestClaimPacingDeferralValidateRejectsUnknownSignal(t *testing.T) {
	wantErrContaining(t, (ClaimPacingDeferral{RetryAfter: time.Second}).Validate(), "pacing signal")
}

func TestClaimPacingDeferralErrorDescribesPacing(t *testing.T) {
	plain := ClaimPacingDeferral{Signal: ClaimPacingSignalDomain, Domain: "jira/primary", RetryAfter: 5 * time.Second}
	msg := plain.Error()
	for _, want := range []string{"domain", `"jira/primary"`, "5s"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("Error() = %q, missing %q", msg, want)
		}
	}
	if strings.Contains(msg, "stuck") {
		t.Fatalf("healthy deferral reported stuck: %q", msg)
	}
	stuck := plain
	stuck.Stuck = true
	if !strings.Contains(stuck.Error(), "stuck") {
		t.Fatalf("stuck deferral Error() = %q, want it to name the stuck engagement", stuck.Error())
	}
}

func TestDecidePacingEngagesOnGlobalCeilingWhenUnlatched(t *testing.T) {
	cfg := pacingConfig()
	got := DecideClaimPacing(cfg, ClaimPacingDepths{Domain: 1, Global: cfg.GlobalHigh + 1}, ClaimPacingLatch{}, 0)
	if !got.Paced || got.Signal != ClaimPacingSignalGlobal {
		t.Fatalf("decision = %+v, want global-paced", got)
	}
}

func TestDecidePacingGlobalLatchReleasesOrSwitches(t *testing.T) {
	cfg := pacingConfig()
	latch := ClaimPacingLatch{Engaged: true, Signal: ClaimPacingSignalGlobal}

	held := DecideClaimPacing(cfg, ClaimPacingDepths{Global: cfg.GlobalLow + 1}, latch, 0)
	if !held.Paced || held.Signal != ClaimPacingSignalGlobal {
		t.Fatalf("decision between watermarks = %+v, want global latch held", held)
	}
	released := DecideClaimPacing(cfg, ClaimPacingDepths{Global: cfg.GlobalLow}, latch, 0)
	if released.Paced {
		t.Fatalf("decision at the low watermark = %+v, want released", released)
	}
	// Global cooled but the family went hot: switch rather than release.
	switched := DecideClaimPacing(cfg, ClaimPacingDepths{Domain: cfg.DomainHigh + 1, Global: 0}, latch, 0)
	if !switched.Paced || switched.Signal != ClaimPacingSignalDomain {
		t.Fatalf("decision = %+v, want domain-paced", switched)
	}
}

func TestDecidePacingDomainLatchSwitchesToGlobal(t *testing.T) {
	cfg := pacingConfig()
	latch := ClaimPacingLatch{Engaged: true, Signal: ClaimPacingSignalDomain}
	// Covered on the domain side by TestPacingReengagesOnNewlyHotSignal;
	// here the domain is exactly at its low watermark and global is hot.
	got := DecideClaimPacing(cfg, ClaimPacingDepths{Domain: cfg.DomainLow, Global: cfg.GlobalHigh + 1}, latch, 0)
	if !got.Paced || got.Signal != ClaimPacingSignalGlobal {
		t.Fatalf("decision = %+v, want global-paced", got)
	}
}

func TestDecidePacingUnknownLatchSignalFailsSafe(t *testing.T) {
	cfg := pacingConfig()
	// A latch carrying a signal this build does not know is treated as
	// disengaged and re-evaluated from depths, never trusted blindly.
	latch := ClaimPacingLatch{Engaged: true, Signal: "regional"}
	cool := DecideClaimPacing(cfg, ClaimPacingDepths{}, latch, time.Hour)
	if cool.Paced {
		t.Fatalf("unknown latch signal with cool depths paced: %+v", cool)
	}
	hot := DecideClaimPacing(cfg, ClaimPacingDepths{Domain: cfg.DomainHigh + 1}, latch, 0)
	if !hot.Paced || hot.Signal != ClaimPacingSignalDomain {
		t.Fatalf("decision = %+v, want domain-paced from fresh depths", hot)
	}
}

func TestQuotaAssignmentValidate(t *testing.T) {
	if err := (QuotaAssignment{CollectorKind: "jira", Quota: ClaimQuota{MaxGenerationsPerHour: 5}}).Validate(); err != nil {
		t.Fatal(err)
	}
	wantErrContaining(t, (QuotaAssignment{Quota: ClaimQuota{}}).Validate(), "collector_kind")
	wantErrContaining(t, (QuotaAssignment{CollectorKind: "jira", Quota: ClaimQuota{MaxGenerationsPerHour: -1}}).Validate(), "max_generations_per_hour")
}
