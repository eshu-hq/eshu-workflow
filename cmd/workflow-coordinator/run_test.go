// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func freeLoopbackAddr(t *testing.T) string {
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

func httpGet(target string) (int, string) {
	resp, err := http.Get(target) //nolint:gosec // loopback test target
	if err != nil {
		return 0, err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func waitUntil(t *testing.T, what string, cond func() bool) {
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

// TestRunServesAdminEndpointsAndTracksPlaneHealth requires /healthz to
// stay up regardless of the plane, /readyz to follow plane reachability in
// both directions, and /metrics to export, while run stops cleanly on
// context cancellation.
func TestRunServesAdminEndpointsAndTracksPlaneHealth(t *testing.T) {
	var planeDown atomic.Bool
	plane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if planeDown.Load() && strings.HasSuffix(r.URL.Path, "QueueDepthSnapshot") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"code":"internal","message":"down"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "ListCollectorInstances"):
			_, _ = w.Write([]byte(`{"instances":[],"as_of":"2026-01-01T00:00:00Z"}`))
		case strings.HasSuffix(r.URL.Path, "QueueDepthSnapshot"):
			_, _ = w.Write([]byte(`{"families":[],"as_of":"2026-01-01T00:00:00Z"}`))
		default:
			_, _ = w.Write([]byte(`{"ok":true}`))
		}
	}))
	defer plane.Close()

	admin := freeLoopbackAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, slog.New(slog.NewJSONHandler(io.Discard, nil)), options{
			claimPlaneURL:  plane.URL,
			adminListen:    admin,
			deploymentMode: "dark",
		})
	}()

	base := "http://" + admin
	waitUntil(t, "readyz to report ready", func() bool {
		code, _ := httpGet(base + "/readyz")
		return code == http.StatusOK
	})
	if code, body := httpGet(base + "/healthz"); code != http.StatusOK || body != "ok" {
		t.Fatalf("healthz = %d %q, want 200 ok", code, body)
	}
	if code, body := httpGet(base + "/metrics"); code != http.StatusOK || body == "" {
		t.Fatalf("metrics = %d with %d bytes, want 200 with a body", code, len(body))
	}

	planeDown.Store(true)
	if code, _ := httpGet(base + "/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with the plane down = %d, want 503", code)
	}
	if code, _ := httpGet(base + "/healthz"); code != http.StatusOK {
		t.Fatalf("healthz with the plane down = %d, want 200 (liveness is independent)", code)
	}
	planeDown.Store(false)
	waitUntil(t, "readyz to recover", func() bool {
		code, _ := httpGet(base + "/readyz")
		return code == http.StatusOK
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

func TestRunFailsFastOnBadConfiguration(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	plane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer plane.Close()

	t.Run("unreadable instances file", func(t *testing.T) {
		err := run(context.Background(), logger, options{
			claimPlaneURL: plane.URL, adminListen: freeLoopbackAddr(t), deploymentMode: "dark",
			instancesFile: filepath.Join(t.TempDir(), "missing.json"),
		})
		if err == nil || !strings.Contains(err.Error(), "read instances file") {
			t.Fatalf("run = %v, want a read instances file error", err)
		}
	})
	t.Run("invalid instance declaration", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "instances.json")
		if err := os.WriteFile(path, []byte(`[{"instance_id":"x","collector_kind":"jira","mode":"sometimes"}]`), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		err := run(context.Background(), logger, options{
			claimPlaneURL: plane.URL, adminListen: freeLoopbackAddr(t), deploymentMode: "dark",
			instancesFile: path,
		})
		if err == nil || !strings.Contains(err.Error(), "instance 0") {
			t.Fatalf("run = %v, want an error naming instance 0", err)
		}
	})
	t.Run("malformed instances JSON", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "instances.json")
		if err := os.WriteFile(path, []byte(`{"not":"a list"}`), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
		err := run(context.Background(), logger, options{
			claimPlaneURL: plane.URL, adminListen: freeLoopbackAddr(t), deploymentMode: "dark",
			instancesFile: path,
		})
		if err == nil || !strings.Contains(err.Error(), "parse instances file") {
			t.Fatalf("run = %v, want a parse error", err)
		}
	})
	t.Run("unknown deployment mode", func(t *testing.T) {
		err := run(context.Background(), logger, options{
			claimPlaneURL: plane.URL, adminListen: freeLoopbackAddr(t), deploymentMode: "nocturnal",
		})
		if err == nil || !strings.Contains(err.Error(), "deployment mode") {
			t.Fatalf("run = %v, want a deployment mode error", err)
		}
	})
}
