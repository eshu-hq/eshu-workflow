// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestVersionFlagParses(t *testing.T) {
	opts, err := parseFlags([]string{"--version"})
	if err != nil {
		t.Fatalf("parse --version: %v", err)
	}
	if !opts.showVersion {
		t.Fatal("showVersion = false, want true")
	}
}

func TestClaimPlaneURLRequired(t *testing.T) {
	t.Setenv("WORKFLOW_COORDINATOR_CLAIM_PLANE_URL", "")
	if _, err := parseFlags([]string{}); err == nil {
		t.Fatal("missing claim-plane URL accepted")
	}
	if _, err := parseFlags([]string{"--claim-plane-url", "http://localhost:8080"}); err != nil {
		t.Fatalf("explicit URL rejected: %v", err)
	}
}

func TestInstancesFileLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "instances.json")
	raw := `[{"instance_id":"jira-primary","collector_kind":"jira","mode":"scheduled",
"enabled":true,"claims_enabled":true,
"configuration":"{\"targets\":[{\"scope_id\":\"scope-a\"}]}"}]`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatalf("write instances: %v", err)
	}
	instances, err := loadInstances(path)
	if err != nil {
		t.Fatalf("load instances: %v", err)
	}
	if len(instances) != 1 || instances[0].InstanceID != "jira-primary" {
		t.Fatalf("instances = %+v, want the declared jira instance", instances)
	}
	if _, err := loadInstances(path + ".missing"); err == nil {
		t.Fatal("missing instances file accepted")
	}
	if err := os.WriteFile(path, []byte(`[{"instance_id":""}`), 0o600); err != nil {
		t.Fatalf("write bad instances: %v", err)
	}
	if _, err := loadInstances(path); err == nil {
		t.Fatal("invalid instance accepted")
	}
	if instances, err := loadInstances(""); err != nil || instances != nil {
		t.Fatalf("empty path = %v, %v, want nil, nil", instances, err)
	}
}
