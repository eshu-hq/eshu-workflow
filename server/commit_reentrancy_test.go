// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestCommitClaimFactsDoesNotBlockClaimPlane is the re-entrancy half of
// proof item 5: while one 4 MiB commit is blocked inside the proxy, the
// claim plane keeps serving, including a heartbeat the blocked core
// itself calls back into the satellite.
func TestCommitClaimFactsDoesNotBlockClaimPlane(t *testing.T) {
	// satelliteURL crosses from the test goroutine to the core-handler
	// goroutine with no channel or mutex edge between them (localhost
	// TCP I/O is invisible to the race detector), so it is shared
	// through an atomic.Value: Store in the test goroutine
	// synchronizes with Load in the handler goroutine. (Value rather
	// than atomic.String: the pinned toolchain on this box predates
	// the String wrapper; Value carries the same happens-before edge.)
	var satelliteURL atomic.Value
	entered := make(chan struct{})
	release := make(chan struct{})
	callbackDone := make(chan int, 1)
	coreSrv := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter, request *http.Request,
	) {
		_, _ = io.ReadAll(request.Body)
		close(entered)
		// Core calls back into the satellite while the proxy handler
		// is blocked: a shared lock would deadlock here.
		now := time.Now().UTC().Format(time.RFC3339Nano)
		hb, err := http.NewRequest(http.MethodPost, satelliteURL.Load().(string)+"/workflow.v1/HeartbeatClaim",
			strings.NewReader(fmt.Sprintf(
				`{"work_item_id":"item-1","claim_id":"c-1","fencing_token":1,`+
					`"owner_id":"o-1","observed_at":%q,"lease_duration_ns":60000000000}`, now)))
		if err != nil {
			callbackDone <- -1
			return
		}
		hb.Header.Set(workflowv1.VersionHeader, "1")
		hbResp, err := http.DefaultClient.Do(hb)
		if err != nil {
			callbackDone <- -1
			return
		}
		status := hbResp.StatusCode
		hbResp.Body.Close()
		callbackDone <- status
		<-release
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer coreSrv.Close()

	st := &fakeStore{}
	srv := newTestServerWithCommitTarget(t, st, coreSrv.URL)
	defer srv.Close()
	satelliteURL.Store(srv.URL)

	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	// A full-size result: the in-flight body a real commit carries.
	big := `"` + strings.Repeat("r", workflowv1.MaxCommitResultBytes-2) + `"`
	commitDone := make(chan int, 1)
	go func() {
		// Manual HTTP, no test helpers: FailNow must run on the test
		// goroutine, and this goroutine is not it.
		status := -1
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/workflow.v1/CommitClaimFacts",
			strings.NewReader(commitBodyForTest(now, big)))
		if err == nil {
			req.Header.Set(workflowv1.VersionHeader, "1")
			if resp, err := http.DefaultClient.Do(req); err == nil {
				status = resp.StatusCode
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}
		commitDone <- status
	}()

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("core never received the forwarded commit")
	}

	// Heartbeat and pin both answer while the commit is blocked.
	hbResp := post(t, srv, "/workflow.v1/HeartbeatClaim", fmt.Sprintf(
		`{"work_item_id":"item-1","claim_id":"c-1","fencing_token":1,`+
			`"owner_id":"o-1","observed_at":%q,"lease_duration_ns":60000000000}`,
		time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)))
	hbResp.Body.Close()
	if hbResp.StatusCode != http.StatusOK {
		t.Fatalf("heartbeat during blocked commit: status %d, want 200", hbResp.StatusCode)
	}
	select {
	case callbackStatus := <-callbackDone:
		if callbackStatus != http.StatusOK {
			t.Fatalf("core callback heartbeat: status %d, want 200", callbackStatus)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("core callback heartbeat never arrived")
	}
	close(release)
	select {
	case status := <-commitDone:
		if status != http.StatusOK {
			t.Fatalf("released commit: status %d, want 200", status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("commit never completed after release")
	}
}
