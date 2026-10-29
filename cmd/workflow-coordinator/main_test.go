// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/workflowv1"
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

func TestStartupFlagsParse(t *testing.T) {
	t.Setenv("WORKFLOW_COORDINATOR_CLAIM_PLANE_URL", "")
	opts, err := parseFlags([]string{
		"--claim-plane-url", "http://localhost:8080",
		"--startup-attempts", "7",
		"--startup-interval", "2s",
	})
	if err != nil {
		t.Fatalf("parse startup flags: %v", err)
	}
	if opts.startupAttempts != 7 {
		t.Fatalf("startupAttempts = %d, want 7", opts.startupAttempts)
	}
	if opts.startupInterval != 2*time.Second {
		t.Fatalf("startupInterval = %v, want 2s", opts.startupInterval)
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

func TestRunMainVersionPrintsAndExitsZero(t *testing.T) {
	out, err := os.CreateTemp(t.TempDir(), "version-*.txt")
	if err != nil {
		t.Fatalf("temp version output: %v", err)
	}
	defer func() { _ = out.Close() }()
	if code := runMain([]string{"--version"}, out); code != 0 {
		t.Fatalf("runMain --version = %d, want 0", code)
	}
	_ = out.Close()
	raw, err := os.ReadFile(out.Name())
	if err != nil {
		t.Fatalf("read version output: %v", err)
	}
	want := fmt.Sprintf("%s %d\n", versionName, workflowv1.ProtocolVersion)
	if string(raw) != want {
		t.Fatalf("version output %q, want exact %q", raw, want)
	}
}

func TestRunMainBadFlagExitsTwo(t *testing.T) {
	out, err := os.CreateTemp(t.TempDir(), "usage-*.txt")
	if err != nil {
		t.Fatalf("temp usage output: %v", err)
	}
	defer func() { _ = out.Close() }()
	if code := runMain([]string{"--no-such-flag"}, out); code != 2 {
		t.Fatalf("runMain bad flag = %d, want 2", code)
	}
}

func TestRunMainMissingURLExitsTwo(t *testing.T) {
	t.Setenv("WORKFLOW_COORDINATOR_CLAIM_PLANE_URL", "")
	out, err := os.CreateTemp(t.TempDir(), "missing-*.txt")
	if err != nil {
		t.Fatalf("temp output: %v", err)
	}
	defer func() { _ = out.Close() }()
	// A missing URL fails flag validation, the same exit path as a bad flag.
	if code := runMain([]string{}, out); code != 2 {
		t.Fatalf("runMain without URL = %d, want 2", code)
	}
}

func TestRunMainBadPlaneURLExitsOne(t *testing.T) {
	t.Setenv("WORKFLOW_COORDINATOR_CLAIM_PLANE_URL", "")
	out, err := os.CreateTemp(t.TempDir(), "badplane-*.txt")
	if err != nil {
		t.Fatalf("temp output: %v", err)
	}
	defer func() { _ = out.Close() }()
	// A non-empty but unparsable URL passes flag validation, then run fails
	// building the claim-plane client.
	if code := runMain([]string{"--claim-plane-url", "://bad-url"}, out); code != 1 {
		t.Fatalf("runMain bad plane URL = %d, want 1", code)
	}
}

func TestRunDarkModeEndsWithCanceledContext(t *testing.T) {
	plane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer plane.Close()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := run(ctx, logger, options{
		claimPlaneURL:  plane.URL,
		adminListen:    "127.0.0.1:0",
		deploymentMode: "dark",
	})
	if err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("run with canceled context = %v, want context canceled", err)
	}
}

func TestRunMainSuccessExitsZero(t *testing.T) {
	responses := map[string]string{
		"/workflow.v1/ListCollectorInstances":      `{"instances":[],"as_of":"2026-01-01T00:00:00Z"}`,
		"/workflow.v1/ReconcileCollectorInstances": `{"ok":true}`,
		"/workflow.v1/QueueDepthSnapshot":          `{"families":[],"as_of":"2026-01-01T00:00:00Z"}`,
	}
	plane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := responses[r.URL.Path]
		if !ok {
			t.Errorf("unexpected plane call to %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer plane.Close()
	out, err := os.CreateTemp(t.TempDir(), "success-*.txt")
	if err != nil {
		t.Fatalf("temp output: %v", err)
	}
	defer func() { _ = out.Close() }()
	// runMain ends only on SIGINT/SIGTERM: prove it is blocked in run,
	// deliver SIGTERM to self (its signal.NotifyContext converts the
	// signal into context cancellation instead of killing the test
	// binary), and expect the run-success exit 0.
	t.Setenv("WORKFLOW_COORDINATOR_CLAIM_PLANE_URL", plane.URL)
	done := make(chan int, 1)
	go func() {
		done <- runMain([]string{"--admin-listen", "127.0.0.1:0"}, out)
	}()
	select {
	case code := <-done:
		t.Fatalf("runMain returned %d without a signal; want it to block until SIGINT/SIGTERM", code)
	case <-time.After(500 * time.Millisecond):
	}
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("signal self: %v", err)
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("runMain success = %d, want 0", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("runMain did not exit within 15s of SIGTERM")
	}
}
