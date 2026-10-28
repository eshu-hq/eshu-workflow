// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"testing"
	"time"
)

func pacingConfig() ClaimPacingConfig {
	return ClaimPacingConfig{
		DomainHigh:    100,
		DomainLow:     60,
		GlobalHigh:    1000,
		GlobalLow:     600,
		GrantDelay:    5 * time.Second,
		MaxEngagement: time.Minute,
	}
}

func TestPacingDisabledByDefault(t *testing.T) {
	decision := DecideClaimPacing(ClaimPacingConfig{}, ClaimPacingDepths{Domain: 1 << 30, Global: 1 << 30}, ClaimPacingLatch{}, 0)
	if decision.Paced {
		t.Fatal("zero config paced an astronomical depth")
	}
}

func TestPacingEngagesStrictlyAboveHigh(t *testing.T) {
	cfg := pacingConfig()
	atHigh := DecideClaimPacing(cfg, ClaimPacingDepths{Domain: 100, Global: 10}, ClaimPacingLatch{}, 0)
	if atHigh.Paced {
		t.Fatal("depth exactly at the high watermark paced")
	}
	above := DecideClaimPacing(cfg, ClaimPacingDepths{Domain: 101, Global: 10}, ClaimPacingLatch{}, 0)
	if !above.Paced || above.Signal != ClaimPacingSignalDomain {
		t.Fatalf("decision = %+v, want domain-paced", above)
	}
	if above.RetryAfter != cfg.GrantDelay {
		t.Fatalf("RetryAfter = %s, want grant delay %s", above.RetryAfter, cfg.GrantDelay)
	}
}

func TestPacingReleasesAtOrBelowLow(t *testing.T) {
	cfg := pacingConfig()
	latch := ClaimPacingLatch{Engaged: true, Signal: ClaimPacingSignalDomain}
	held := DecideClaimPacing(cfg, ClaimPacingDepths{Domain: 80, Global: 10}, latch, 0)
	if !held.Paced {
		t.Fatal("depth between watermarks released the latch")
	}
	released := DecideClaimPacing(cfg, ClaimPacingDepths{Domain: 60, Global: 10}, latch, 0)
	if released.Paced {
		t.Fatal("depth at the low watermark stayed paced")
	}
}

func TestPacingReengagesOnNewlyHotSignal(t *testing.T) {
	cfg := pacingConfig()
	latch := ClaimPacingLatch{Engaged: true, Signal: ClaimPacingSignalDomain}
	// Domain cooled but the global ceiling tripped: switch, don't release.
	decision := DecideClaimPacing(cfg, ClaimPacingDepths{Domain: 10, Global: 2000}, latch, 0)
	if !decision.Paced || decision.Signal != ClaimPacingSignalGlobal {
		t.Fatalf("decision = %+v, want global-paced", decision)
	}
}

func TestPacingStuckPastMaxEngagement(t *testing.T) {
	cfg := pacingConfig()
	latch := ClaimPacingLatch{Engaged: true, Signal: ClaimPacingSignalGlobal}
	fresh := DecideClaimPacing(cfg, ClaimPacingDepths{Domain: 10, Global: 2000}, latch, time.Second)
	if fresh.Stuck {
		t.Fatal("fresh engagement reported stuck")
	}
	stale := DecideClaimPacing(cfg, ClaimPacingDepths{Domain: 10, Global: 2000}, latch, 2*time.Minute)
	if !stale.Paced || !stale.Stuck {
		t.Fatalf("decision = %+v, want paced+stuck", stale)
	}
}

func TestPacingConfigRejectsHalfConfiguration(t *testing.T) {
	if err := (ClaimPacingConfig{}).Validate(); err != nil {
		t.Fatal(err)
	}
	half := pacingConfig()
	half.MaxEngagement = 0
	if err := half.Validate(); err == nil {
		t.Fatal("half-configured pacing validated")
	}
	bad := pacingConfig()
	bad.DomainLow = bad.DomainHigh
	if err := bad.Validate(); err == nil {
		t.Fatal("low watermark at high watermark validated")
	}
}

func TestDeferralCarriesPositiveBackoff(t *testing.T) {
	deferral := ClaimPacingDeferral{Signal: ClaimPacingSignalDomain, Domain: "jira/primary", RetryAfter: 5 * time.Second}
	if err := deferral.Validate(); err != nil {
		t.Fatal(err)
	}
	var _ error = deferral
	if got := deferral.RetryAfterDelay(); got != 5*time.Second {
		t.Fatalf("RetryAfterDelay = %s, want 5s", got)
	}
	zero := deferral
	zero.RetryAfter = 0
	if err := zero.Validate(); err == nil {
		t.Fatal("zero backoff deferral validated")
	}
}
