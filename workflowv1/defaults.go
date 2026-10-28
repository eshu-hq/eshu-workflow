// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import "time"

const (
	defaultClaimLeaseTTL            = 60 * time.Second
	defaultHeartbeatInterval        = 20 * time.Second
	defaultReapInterval             = 20 * time.Second
	defaultExpiredClaimLimit        = 100
	defaultExpiredClaimRequeueDelay = 5 * time.Second
	// defaultClaimMaxAttempts bounds collector retries on one workflow
	// work item. The server escalates a retryable failure to terminal
	// once the item's attempt count reaches this value. The default is
	// intentionally generous so transient throttles and timeouts still
	// recover; the guard is for a runaway loop where a permanent failure
	// drives the failed-retryable claim table into the millions.
	defaultClaimMaxAttempts = 10
)

// DefaultClaimLeaseTTL returns the default ownership lease on a new claim.
func DefaultClaimLeaseTTL() time.Duration {
	return defaultClaimLeaseTTL
}

// DefaultHeartbeatInterval returns the default claim heartbeat cadence.
func DefaultHeartbeatInterval() time.Duration {
	return defaultHeartbeatInterval
}

// DefaultReapInterval returns the default expired-claim reaper cadence.
func DefaultReapInterval() time.Duration {
	return defaultReapInterval
}

// DefaultExpiredClaimLimit returns the default per-reap reclaim bound.
func DefaultExpiredClaimLimit() int {
	return defaultExpiredClaimLimit
}

// DefaultExpiredClaimRequeueDelay returns the default delay before a
// reaped claim becomes visible again.
func DefaultExpiredClaimRequeueDelay() time.Duration {
	return defaultExpiredClaimRequeueDelay
}

// DefaultClaimMaxAttempts returns the bounded retry budget runners MUST
// apply unless the deployment overrides it.
func DefaultClaimMaxAttempts() int {
	return defaultClaimMaxAttempts
}
