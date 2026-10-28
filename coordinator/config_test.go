// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package coordinator_test

import (
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/coordinator"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

func validConfig() coordinator.Config {
	return coordinator.Config{
		DeploymentMode:           "active",
		ClaimsEnabled:            true,
		ReconcileInterval:        time.Minute,
		RunReconcileInterval:     30 * time.Second,
		ReapInterval:             20 * time.Second,
		ClaimLeaseTTL:            time.Minute,
		ExpiredClaimLimit:        100,
		ExpiredClaimRequeueDelay: 5 * time.Second,
		ScanInterval:             5 * time.Minute,
		StartupReconcileAttempts: 3,
		StartupReconcileInterval: time.Second,
		CollectorInstances:       []workflowv1.DesiredCollectorInstance{schedulableInstance("i-1", "jira")},
	}
}

func TestConfigValidation(t *testing.T) {
	if err := validConfig().Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	bad := validConfig()
	bad.DeploymentMode = "nocturnal"
	if err := bad.Validate(); err == nil {
		t.Fatal("unknown mode validated")
	}
	bad = validConfig()
	bad.ReconcileInterval = -time.Second
	if err := bad.Validate(); err == nil {
		t.Fatal("negative interval validated")
	}
	bad = validConfig()
	bad.ExpiredClaimLimit = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("zero claim limit validated")
	}
	bad = validConfig()
	bad.StartupReconcileAttempts = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("zero startup attempts validated")
	}
	bad = validConfig()
	bad.StartupReconcileInterval = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("zero startup interval validated")
	}
	bad = validConfig()
	bad.CollectorInstances = []workflowv1.DesiredCollectorInstance{{}}
	if err := bad.Validate(); err == nil {
		t.Fatal("blank instance validated")
	}
}
