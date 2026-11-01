// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package main

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestRunReturnsWhenProtocolListenerCannotBind requires run to fail fast
// when a listener cannot bind, instead of idling with only one listener up.
func TestRunReturnsWhenProtocolListenerCannotBind(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupy port: %v", err)
	}
	defer func() { _ = taken.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, quietLogger(), options{
			listen:       taken.Addr().String(),
			adminListen:  freeAddr(t),
			databaseURL:  scratchDatabase(t),
			reapInterval: time.Second,
			reapLimit:    1,
		})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run returned nil although the protocol listener could not bind")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run kept running 5s after the protocol listener failed to bind; it only exits on a signal")
	}
}
