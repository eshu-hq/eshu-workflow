// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/eshu-hq/eshu-workflow/client"
	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// adminDSN returns the maintenance connection string for the test cluster.
func adminDSN() string {
	if dsn := os.Getenv("CLAIMPLANE_TEST_DATABASE_URL"); dsn != "" {
		return dsn
	}
	return "postgres://claimplane_test:claimplane_test@localhost:5432/claimplane_test_store?sslmode=disable"
}

// scratchDatabase creates an empty database for one test and drops it on
// cleanup, so run's migrate-and-serve path never races the other packages'
// shared test databases. It returns the DSN of the new database.
func scratchDatabase(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminDSN())
	if err != nil {
		t.Fatalf("connect to test cluster: %v", err)
	}
	defer func() { _ = admin.Close(context.Background()) }()

	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatalf("random suffix: %v", err)
	}
	name := "claimplane_test_cmd_" + hex.EncodeToString(suffix)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create scratch database: %v", err)
	}
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanCancel()
		conn, err := pgx.Connect(cleanCtx, adminDSN())
		if err != nil {
			t.Errorf("reconnect to drop %s: %v", name, err)
			return
		}
		defer func() { _ = conn.Close(context.Background()) }()
		if _, err := conn.Exec(cleanCtx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop scratch database %s: %v", name, err)
		}
	})

	parsed, err := url.Parse(adminDSN())
	if err != nil {
		t.Fatalf("parse admin DSN: %v", err)
	}
	parsed.Path = "/" + name
	return parsed.String()
}

// freeAddr returns a loopback address that was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return addr
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func getStatus(t *testing.T, target string) (int, string) {
	t.Helper()
	resp, err := http.Get(target) //nolint:gosec // loopback test target
	if err != nil {
		return 0, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// waitFor polls until cond holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestRunServesProtocolAndAdminThenShutsDownCleanly drives run end to end
// against a real database: it migrates, serves the protocol and admin
// listeners, answers a protocol read, reports ready, exports metrics, and
// returns nil when its context is canceled.
func TestRunServesProtocolAndAdminThenShutsDownCleanly(t *testing.T) {
	opts := options{
		listen:       freeAddr(t),
		adminListen:  freeAddr(t),
		databaseURL:  scratchDatabase(t),
		reapInterval: 20 * time.Millisecond,
		reapLimit:    10,
		requeueDelay: time.Second,
		quotas:       "jira=5",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, quietLogger(), opts) }()

	admin := "http://" + opts.adminListen
	waitFor(t, "readyz to report ready", func() bool {
		code, _ := getStatus(t, admin+"/readyz")
		return code == http.StatusOK
	})
	if code, body := getStatus(t, admin+"/healthz"); code != http.StatusOK || body != "ok" {
		t.Fatalf("healthz = %d %q, want 200 ok", code, body)
	}

	plane, err := client.New("http://" + opts.listen)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	snapshot, err := plane.QueueDepthSnapshot(ctx)
	if err != nil {
		t.Fatalf("QueueDepthSnapshot over the live protocol listener: %v", err)
	}
	if snapshot.GlobalPending != 0 || snapshot.GlobalClaimed != 0 {
		t.Fatalf("fresh database snapshot = %+v, want drained", snapshot)
	}

	// The reaper ticks every 20ms and observes reaps, so the reap counter
	// family must be exported once a tick has completed.
	var metrics string
	waitFor(t, "metrics to carry reaper telemetry", func() bool {
		_, metrics = getStatus(t, admin+"/metrics")
		return strings.Contains(metrics, "reap")
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v after cancellation, want nil", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not return after cancellation")
	}
}

func TestRunFailsOnUnreachableDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	opts := options{
		listen:       freeAddr(t),
		adminListen:  freeAddr(t),
		databaseURL:  "postgres://nobody:nothing@" + freeAddr(t) + "/none?sslmode=disable&connect_timeout=1",
		reapInterval: time.Second,
		reapLimit:    1,
	}
	err := run(ctx, quietLogger(), opts)
	if err == nil || !strings.Contains(err.Error(), "claim-plane database") {
		t.Fatalf("run = %v, want a claim-plane database error", err)
	}
}

func TestRunRejectsMalformedDatabaseURL(t *testing.T) {
	err := run(context.Background(), quietLogger(), options{databaseURL: "postgres://%zz"})
	if err == nil || !strings.Contains(err.Error(), "parse claim-plane database config") {
		t.Fatalf("run = %v, want a parse error", err)
	}
}

func TestRunRejectsInvalidPacingConfig(t *testing.T) {
	opts := options{
		databaseURL:  scratchDatabase(t),
		reapInterval: time.Second,
		reapLimit:    1,
	}
	// Half-configured pacing is a misconfiguration the process must refuse
	// to start with, not a silent default.
	opts.pacing = workflowv1.ClaimPacingConfig{DomainHigh: 10}
	err := run(context.Background(), quietLogger(), opts)
	if err == nil || !strings.Contains(err.Error(), "configure") {
		t.Fatalf("run = %v, want a configure error", err)
	}
}

// loopStore counts reaper ticks under a lock so the loop goroutine and the
// test can share it.
type loopStore struct {
	mu    sync.Mutex
	reaps int
}

func (s *loopStore) Reap(context.Context, time.Time, int, time.Duration) ([]workflowv1.Claim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reaps++
	return nil, nil
}

func (s *loopStore) ReconcileRuns(context.Context, time.Time) (int, error) { return 0, nil }

func (s *loopStore) QueueDepth(context.Context, time.Time) (workflowv1.QueueDepthSnapshot, error) {
	return workflowv1.QueueDepthSnapshot{}, nil
}

func (s *loopStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reaps
}

func TestReaperLoopTicksUntilContextCanceled(t *testing.T) {
	store := &loopStore{}
	r := newReaper(store, telemetry.NewInstruments(prometheus.NewRegistry()), quietLogger(),
		options{reapInterval: 5 * time.Millisecond, reapLimit: 1})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.loop(ctx)
		close(done)
	}()
	waitFor(t, "three reaper ticks", func() bool { return store.count() >= 3 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not return after cancellation")
	}
	stopped := store.count()
	time.Sleep(30 * time.Millisecond)
	if got := store.count(); got != stopped {
		t.Fatalf("reaper kept ticking after cancellation: %d -> %d", stopped, got)
	}
}

func TestRunMainExitCodes(t *testing.T) {
	out, err := os.CreateTemp(t.TempDir(), "out-*.txt")
	if err != nil {
		t.Fatalf("temp output: %v", err)
	}
	defer func() { _ = out.Close() }()

	if code := runMain([]string{"--version"}, out); code != 0 {
		t.Fatalf("runMain --version = %d, want 0", code)
	}
	raw, err := os.ReadFile(out.Name())
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	want := fmt.Sprintf("claim-plane %d\n", workflowv1.ProtocolVersion)
	if string(raw) != want {
		t.Fatalf("version output %q, want exact %q", raw, want)
	}

	if code := runMain([]string{"--no-such-flag"}, out); code != 2 {
		t.Fatalf("runMain bad flag = %d, want 2", code)
	}

	t.Setenv("CLAIMPLANE_DATABASE_URL", "")
	if code := runMain([]string{}, out); code != 2 {
		t.Fatalf("runMain without database URL = %d, want 2", code)
	}

	// Valid flags whose run fails (malformed quotas) exit 1.
	if code := runMain([]string{"--database-url", "postgres://localhost/x", "--quotas", "jira"}, out); code != 1 {
		t.Fatalf("runMain with failing run = %d, want 1", code)
	}
}
