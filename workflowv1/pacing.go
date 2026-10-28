// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"fmt"
	"time"
)

// ClaimPacingSignal names which depth signal engaged the claim-pacing
// latch: per-domain pending depth or the global pending ceiling. Closed.
type ClaimPacingSignal string

const (
	// ClaimPacingSignalDomain paces only claims into the saturated
	// collector family (collector_kind, collector_instance_id).
	ClaimPacingSignalDomain ClaimPacingSignal = "domain"
	// ClaimPacingSignalGlobal paces every claim to protect the database
	// itself: table growth, claim-query cost, and retention lag.
	ClaimPacingSignalGlobal ClaimPacingSignal = "global"
)

// Validate rejects an unknown pacing signal. Unknown signals fail safe at
// the decision layer, which re-evaluates them as disengaged.
func (s ClaimPacingSignal) Validate() error {
	switch s {
	case ClaimPacingSignalDomain, ClaimPacingSignalGlobal:
		return nil
	default:
		return fmt.Errorf("unknown claim pacing signal %q", string(s))
	}
}

// ClaimPacingConfig carries the config-named depth thresholds and delays
// for claim-issuance pacing. The zero value disables pacing: depth is never
// read and grants are never delayed unless an operator configures every
// watermark and delay.
//
// Per-domain depth is pending work items in the claiming collector family;
// global depth is pending work items system-wide. Each signal engages
// strictly above its high watermark and releases at or below its low
// watermark, so the gap between the two is what stops oscillation.
type ClaimPacingConfig struct {
	DomainHigh int64 `json:"domain_high"`
	DomainLow  int64 `json:"domain_low"`
	GlobalHigh int64 `json:"global_high"`
	GlobalLow  int64 `json:"global_low"`
	// GrantDelay is the backoff a paced grant carries on the existing
	// retry-after path: the runner waits it out instead of treating the
	// paced grant as a failure.
	GrantDelay time.Duration `json:"grant_delay_ns"`
	// MaxEngagement bounds one latch engagement: past it with depth still
	// above the release watermark, the verdict stays paced and raises
	// Stuck instead of releasing blindly into a saturated system.
	MaxEngagement time.Duration `json:"max_engagement_ns"`
	// DepthCacheTTL bounds how long one server reuses a depth
	// observation. Zero reads depth on every check (deterministic).
	// Positive values amortize the pending-count scan. The latch row
	// stays authoritative in storage and is always read fresh; only the
	// depth numbers may lag by the TTL.
	DepthCacheTTL time.Duration `json:"depth_cache_ttl_ns,omitempty"`
}

// Enabled reports whether pacing is configured. Every watermark and delay
// must be set: a half-configured throttle is a misconfiguration, not a
// conservative default.
func (c ClaimPacingConfig) Enabled() bool {
	return c.DomainHigh > 0 && c.GlobalHigh > 0 &&
		c.GrantDelay > 0 && c.MaxEngagement > 0
}

// Validate rejects an enabled config whose watermarks cannot latch: each
// low watermark must sit strictly below its high watermark, delays must be
// positive, and the depth cache TTL must not be negative. The disabled zero
// value is valid.
func (c ClaimPacingConfig) Validate() error {
	if !c.Enabled() {
		if c == (ClaimPacingConfig{}) {
			return nil
		}
		return fmt.Errorf("claim pacing is partially configured: set every watermark and delay, or none")
	}
	if c.DomainLow < 0 || c.DomainLow >= c.DomainHigh {
		return fmt.Errorf("claim pacing domain low %d must be below domain high %d", c.DomainLow, c.DomainHigh)
	}
	if c.GlobalLow < 0 || c.GlobalLow >= c.GlobalHigh {
		return fmt.Errorf("claim pacing global low %d must be below global high %d", c.GlobalLow, c.GlobalHigh)
	}
	if c.DepthCacheTTL < 0 {
		return fmt.Errorf("claim pacing depth cache TTL %s must not be negative", c.DepthCacheTTL)
	}
	return nil
}

// ClaimPacingDepths carries one depth observation for a pacing verdict:
// pending work items in the claiming family and pending work items
// system-wide.
type ClaimPacingDepths struct {
	Domain int64 `json:"domain"`
	Global int64 `json:"global"`
}

// Validate rejects negative depth observations.
func (d ClaimPacingDepths) Validate() error {
	if d.Domain < 0 {
		return fmt.Errorf("domain depth must not be negative")
	}
	if d.Global < 0 {
		return fmt.Errorf("global depth must not be negative")
	}
	return nil
}

// ClaimPacingLatch is the shared hysteresis bit: whether pacing is engaged
// and on which signal. It lives in server storage alongside the claim path
// so coordinators cannot disagree about whether pacing is on.
type ClaimPacingLatch struct {
	Engaged bool              `json:"engaged"`
	Signal  ClaimPacingSignal `json:"signal,omitempty"`
}

// ClaimPacingDecision is one pacing verdict for one claim attempt.
type ClaimPacingDecision struct {
	Paced bool `json:"paced"`
	// Signal names the engaging depth signal and stays set while the
	// latch holds, so the operator can answer "is pacing on, and why".
	Signal ClaimPacingSignal `json:"signal,omitempty"`
	// Domain carries the saturated family as "kind/instance" for the
	// domain signal and stays empty for the global ceiling.
	Domain string `json:"domain,omitempty"`
	// RetryAfter is the grant delay, set exactly when Paced.
	RetryAfter time.Duration `json:"retry_after_ns,omitempty"`
	// Stuck reports an engagement older than MaxEngagement with depth
	// still above the release watermark: still paced, and loud.
	Stuck bool `json:"stuck,omitempty"`
}

// DecideClaimPacing is the pure pacing verdict over one depth observation
// and the shared latch. It never touches storage: the server reads depths
// and the latch, calls this, and advances the latch on transitions.
//
// Per-domain depth is consulted first with the global depth as the ceiling.
// Engagement is strictly above the high watermark; release is at or below
// the low watermark, so depths between the two follow the latch. A latch
// engaged on one signal while the other signal trips re-engages on the
// newly hot signal rather than releasing into a saturated system.
func DecideClaimPacing(
	cfg ClaimPacingConfig,
	depths ClaimPacingDepths,
	latch ClaimPacingLatch,
	engagedFor time.Duration,
) ClaimPacingDecision {
	if !cfg.Enabled() {
		return ClaimPacingDecision{}
	}
	domainHot := depths.Domain > cfg.DomainHigh
	globalHot := depths.Global > cfg.GlobalHigh
	if !latch.Engaged || latch.Signal.Validate() != nil {
		switch {
		case domainHot:
			return paced(cfg, ClaimPacingSignalDomain, 0)
		case globalHot:
			return paced(cfg, ClaimPacingSignalGlobal, 0)
		default:
			return ClaimPacingDecision{}
		}
	}
	switch latch.Signal {
	case ClaimPacingSignalDomain:
		if depths.Domain <= cfg.DomainLow {
			if globalHot {
				return paced(cfg, ClaimPacingSignalGlobal, 0)
			}
			return ClaimPacingDecision{}
		}
	case ClaimPacingSignalGlobal:
		if depths.Global <= cfg.GlobalLow {
			if domainHot {
				return paced(cfg, ClaimPacingSignalDomain, 0)
			}
			return ClaimPacingDecision{}
		}
	}
	return paced(cfg, latch.Signal, engagedFor)
}

// paced builds an engaged verdict. Stuck trips only on a positive overdue
// engagement with depth still above the release watermark: the caller
// reaches here only when the engaged signal has not released.
func paced(cfg ClaimPacingConfig, signal ClaimPacingSignal, engagedFor time.Duration) ClaimPacingDecision {
	return ClaimPacingDecision{
		Paced:      true,
		Signal:     signal,
		RetryAfter: cfg.GrantDelay,
		Stuck:      engagedFor > cfg.MaxEngagement,
	}
}

// ClaimPacingDeferral is the grant-delay channel: a paced ClaimNext
// returns it with no claim issued, so the runner backs off on the existing
// retry-after path instead of treating the paced grant as a failure.
type ClaimPacingDeferral struct {
	Signal ClaimPacingSignal `json:"signal"`
	Domain string            `json:"domain,omitempty"`
	// RetryAfter is the grant delay the runner must wait before claiming
	// again. It is always positive on a deferral the server returns.
	RetryAfter time.Duration `json:"retry_after_ns"`
	Stuck      bool          `json:"stuck,omitempty"`
}

// Validate checks the deferral carries a usable backoff. A blank signal is
// an unpaced non-deferral and fails: ClaimNextResponse.Validate only
// accepts a deferral on the unissued path.
func (e ClaimPacingDeferral) Validate() error {
	if err := e.Signal.Validate(); err != nil {
		return err
	}
	if e.RetryAfter <= 0 {
		return fmt.Errorf("deferral retry_after_ns must be positive")
	}
	return nil
}

// Error describes the paced grant for operator logs.
func (e ClaimPacingDeferral) Error() string {
	if e.Stuck {
		return fmt.Sprintf("claim pacing engaged on %s depth (domain %q): grant delayed %s, engagement stuck past its bound",
			e.Signal, e.Domain, e.RetryAfter)
	}
	return fmt.Sprintf("claim pacing engaged on %s depth (domain %q): grant delayed %s",
		e.Signal, e.Domain, e.RetryAfter)
}

// RetryAfterDelay exposes the grant delay on the retry-after path.
func (e ClaimPacingDeferral) RetryAfterDelay() time.Duration {
	return e.RetryAfter
}
