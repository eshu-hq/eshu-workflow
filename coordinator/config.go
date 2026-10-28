// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package coordinator

import (
	"fmt"
	"time"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// Deployment modes. Dark reconciles durable instance state without
// planning or admitting work; active additionally plans, admits, reaps,
// and reconciles runs.
const (
	DeploymentModeDark   = "dark"
	DeploymentModeActive = "active"
)

const (
	defaultReconcileInterval    = 60 * time.Second
	defaultRunReconcileInterval = 30 * time.Second
	defaultScanInterval         = 5 * time.Minute
)

// Config captures the coordinator runtime settings.
type Config struct {
	// DeploymentMode is dark or active. Empty selects dark.
	DeploymentMode string
	// ClaimsEnabled gates planning and admission in active mode.
	ClaimsEnabled bool
	// ReconcileInterval bounds instance reconciliation. Zero selects
	// the default.
	ReconcileInterval time.Duration
	// RunReconcileInterval bounds drained-run reconciliation in active
	// mode. Zero selects the default.
	RunReconcileInterval time.Duration
	// ReapInterval bounds expired-claim reaping in active mode. Zero
	// selects the claim-plane default.
	ReapInterval time.Duration
	// ClaimLeaseTTL is the ownership lease new plans assume. Zero
	// selects the claim-plane default.
	ClaimLeaseTTL time.Duration
	// ExpiredClaimLimit bounds one reap pass. Zero selects the
	// claim-plane default.
	ExpiredClaimLimit int
	// ExpiredClaimRequeueDelay is the visibility delay for reaped
	// items. Negative selects the claim-plane default; zero requeues
	// immediately.
	ExpiredClaimRequeueDelay time.Duration
	// ScanInterval windows deterministic plan keys: two passes inside
	// one window plan the same run id, so replays converge. Zero
	// selects the default.
	ScanInterval time.Duration
	// CollectorInstances is the operator-declared instance set the
	// reconcile owns.
	CollectorInstances []workflowv1.DesiredCollectorInstance
}

// withDefaults fills every zero interval and the empty mode.
func (c Config) withDefaults() Config {
	if c.DeploymentMode == "" {
		c.DeploymentMode = DeploymentModeDark
	}
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = defaultReconcileInterval
	}
	if c.RunReconcileInterval == 0 {
		c.RunReconcileInterval = defaultRunReconcileInterval
	}
	if c.ReapInterval == 0 {
		c.ReapInterval = workflowv1.DefaultReapInterval()
	}
	if c.ClaimLeaseTTL == 0 {
		c.ClaimLeaseTTL = workflowv1.DefaultClaimLeaseTTL()
	}
	if c.ExpiredClaimLimit == 0 {
		c.ExpiredClaimLimit = workflowv1.DefaultExpiredClaimLimit()
	}
	if c.ExpiredClaimRequeueDelay == 0 {
		c.ExpiredClaimRequeueDelay = workflowv1.DefaultExpiredClaimRequeueDelay()
	}
	if c.ScanInterval == 0 {
		c.ScanInterval = defaultScanInterval
	}
	return c
}

// Validate checks the coordinator configuration.
func (c Config) Validate() error {
	if c.DeploymentMode != DeploymentModeDark && c.DeploymentMode != DeploymentModeActive {
		return fmt.Errorf("deployment mode %q must be %q or %q",
			c.DeploymentMode, DeploymentModeDark, DeploymentModeActive)
	}
	for name, interval := range map[string]time.Duration{
		"reconcile_interval":     c.ReconcileInterval,
		"run_reconcile_interval": c.RunReconcileInterval,
		"reap_interval":          c.ReapInterval,
		"claim_lease_ttl":        c.ClaimLeaseTTL,
		"scan_interval":          c.ScanInterval,
	} {
		if interval <= 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	if c.ExpiredClaimLimit <= 0 {
		return fmt.Errorf("expired claim limit must be positive")
	}
	for i := range c.CollectorInstances {
		if err := c.CollectorInstances[i].Validate(); err != nil {
			return fmt.Errorf("collector instance %d: %w", i, err)
		}
	}
	return nil
}
