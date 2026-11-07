// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package server_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eshu-hq/eshu-workflow/server"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// commitCore is a fake core behind the proxy: it records what the
// satellite forwarded and answers with a canned status and body.
type commitCore struct {
	mu       sync.Mutex
	hits     int
	bodies   [][]byte
	versions []string
	status   int
	resp     string
}

func (c *commitCore) handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		raw, _ := io.ReadAll(request.Body)
		c.mu.Lock()
		c.hits++
		c.bodies = append(c.bodies, raw)
		c.versions = append(c.versions, request.Header.Get(workflowv1.VersionHeader))
		c.mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(c.status)
		_, _ = writer.Write([]byte(c.resp))
	})
}

func (c *commitCore) hitCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits
}

// firstBody returns a copy of the first forwarded body. The copy keeps
// the race detector honest: the handler goroutine owns the original.
func (c *commitCore) firstBody() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.bodies) == 0 {
		return nil
	}
	out := make([]byte, len(c.bodies[0]))
	copy(out, c.bodies[0])
	return out
}

func (c *commitCore) firstVersion() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.versions) == 0 {
		return ""
	}
	return c.versions[0]
}

// newTestServerWithCommitTarget builds a protocol server whose
// CommitClaimFacts route forwards to coreURL. Tests that never set a
// target keep using newTestServer: an unset target must fail closed.
func newTestServerWithCommitTarget(t *testing.T, st *fakeStore, coreURL string) *httptest.Server {
	t.Helper()
	srv, err := server.New(st, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv.SetCommitForwarder(coreURL, &http.Client{Timeout: 30 * time.Second})
	return httptest.NewServer(srv.Handler())
}

func commitBodyForTest(now string, result string) string {
	return fmt.Sprintf(
		`{"work_item_id":"item-1","claim_id":"c-1","fencing_token":7,"owner_id":"o-1",`+
			`"observed_at":%q,"lease_duration_ns":60000000000,`+
			`"idempotency_key":"idem-1","result_json":%s,`+
			`"scope_id":"scope-1","source_run_id":"sr-1","generation_id":"gen-1"}`,
		now, result)
}

func readAllForTest(t *testing.T, resp *http.Response) (int, string) {
	t.Helper()
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(raw)
}

// TestCommitClaimFactsForwardsRequestBytesVerbatim is the green half of
// proof item 1: the satellite must forward the exact bytes it received,
// including unknown fields inside the result it never decodes, and copy
// core's answer back untouched without touching its own store.
func TestCommitClaimFactsForwardsRequestBytesVerbatim(t *testing.T) {
	core := &commitCore{status: http.StatusOK, resp: `{"ok":true}`}
	coreSrv := httptest.NewServer(core.handler())
	defer coreSrv.Close()

	st := &fakeStore{}
	srv := newTestServerWithCommitTarget(t, st, coreSrv.URL)
	defer srv.Close()

	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	// Odd spacing plus a future field inside the result: a decode-and-
	// re-encode proxy would normalize or drop these bytes.
	sent := ` { "work_item_id" : "item-1" , "claim_id" : "c-1" , ` +
		`"fencing_token" : 7 , "owner_id" : "o-1" , "observed_at" : ` + fmt.Sprintf("%q", now) +
		` , "lease_duration_ns" : 60000000000 , "idempotency_key" : "idem-1" , ` +
		`"result_json" : { "fact_rows" : [] , "unknown_future_field" : [1,2,{}] } , ` +
		`"scope_id" : "scope-1" , "source_run_id" : "sr-1" , "generation_id" : "gen-1" } `
	resp := post(t, srv, "/workflow.v1/CommitClaimFacts", sent)
	status, body := readAllForTest(t, resp)
	if status != http.StatusOK || body != `{"ok":true}` {
		t.Fatalf("proxy answer = %d %q, want 200 %q", status, body, `{"ok":true}`)
	}
	if core.hitCount() != 1 {
		t.Fatalf("core hits = %d, want 1", core.hitCount())
	}
	if got := core.firstBody(); string(got) != sent {
		t.Fatalf("forwarded bytes differ:\n got %q\nwant %q", got, sent)
	}
	if got := core.firstVersion(); got != "1" {
		t.Fatalf("forwarded version header = %q, want %q", got, "1")
	}
	if len(st.calls) != 0 {
		t.Fatalf("proxy reached the ClaimStore: %v", st.calls)
	}
}

// TestCommitClaimFactsResultAtCapPasses requires a result of exactly
// MaxCommitResultBytes to forward: producers may legally emit it.
func TestCommitClaimFactsResultAtCapPasses(t *testing.T) {
	core := &commitCore{status: http.StatusOK, resp: `{"ok":true}`}
	coreSrv := httptest.NewServer(core.handler())
	defer coreSrv.Close()

	st := &fakeStore{}
	srv := newTestServerWithCommitTarget(t, st, coreSrv.URL)
	defer srv.Close()

	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	// RawMessage length counts the quotes, so the payload inside is cap-2.
	result := `"` + strings.Repeat("x", workflowv1.MaxCommitResultBytes-2) + `"`
	resp := post(t, srv, "/workflow.v1/CommitClaimFacts", commitBodyForTest(now, result))
	status, _ := readAllForTest(t, resp)
	if status != http.StatusOK {
		t.Fatalf("result at cap: status %d, want 200", status)
	}
	if core.hitCount() != 1 {
		t.Fatalf("core hits = %d, want 1", core.hitCount())
	}
}

// TestCommitClaimFactsOversizeResultFailsClosedWithZeroForwards is the
// terminal half of proof item 5: one byte past the result cap is
// invalid at the satellite and never reaches core.
func TestCommitClaimFactsOversizeResultFailsClosedWithZeroForwards(t *testing.T) {
	core := &commitCore{status: http.StatusOK, resp: `{"ok":true}`}
	coreSrv := httptest.NewServer(core.handler())
	defer coreSrv.Close()

	st := &fakeStore{}
	srv := newTestServerWithCommitTarget(t, st, coreSrv.URL)
	defer srv.Close()

	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	result := `"` + strings.Repeat("x", workflowv1.MaxCommitResultBytes-1) + `"`
	resp := post(t, srv, "/workflow.v1/CommitClaimFacts", commitBodyForTest(now, result))
	envelope := decodeEnvelope(t, resp)
	if resp.StatusCode != http.StatusBadRequest || envelope.Code != workflowv1.CodeInvalid {
		t.Fatalf("oversize result: status %d code %q, want 400 invalid",
			resp.StatusCode, envelope.Code)
	}
	if core.hitCount() != 0 {
		t.Fatalf("oversize result forwarded %d times, want 0", core.hitCount())
	}
	if len(st.calls) != 0 {
		t.Fatalf("oversize result reached the ClaimStore: %v", st.calls)
	}
}

// commitBodyOfSize builds a valid commit envelope padded to exactly n
// bytes so the body-cap boundary can be probed one byte at a time.
func commitBodyOfSize(t *testing.T, n int) string {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	template := commitBodyForTest(now, `{"fact_rows":[]}`)[:len(commitBodyForTest(now, `{"fact_rows":[]}`))-1] +
		`,"pad":"PADDING"}`
	base := len(template) - len("PADDING")
	if n < base {
		t.Fatalf("target size %d below unpadded envelope %d", n, base)
	}
	return strings.Replace(template, "PADDING", strings.Repeat("p", n-base), 1)
}

// TestCommitClaimFactsBodyCapBoundary requires the satellite to enforce
// the shared body cap itself: exactly MaxCommitBodyBytes forwards, one
// byte more is invalid with zero forwards.
func TestCommitClaimFactsBodyCapBoundary(t *testing.T) {
	core := &commitCore{status: http.StatusOK, resp: `{"ok":true}`}
	coreSrv := httptest.NewServer(core.handler())
	defer coreSrv.Close()

	st := &fakeStore{}
	srv := newTestServerWithCommitTarget(t, st, coreSrv.URL)
	defer srv.Close()

	resp := post(t, srv, "/workflow.v1/CommitClaimFacts",
		commitBodyOfSize(t, workflowv1.MaxCommitBodyBytes))
	status, _ := readAllForTest(t, resp)
	if status != http.StatusOK {
		t.Fatalf("body at cap: status %d, want 200", status)
	}
	if core.hitCount() != 1 {
		t.Fatalf("core hits = %d, want 1", core.hitCount())
	}

	resp = post(t, srv, "/workflow.v1/CommitClaimFacts",
		commitBodyOfSize(t, workflowv1.MaxCommitBodyBytes+1))
	envelope := decodeEnvelope(t, resp)
	if resp.StatusCode != http.StatusBadRequest || envelope.Code != workflowv1.CodeInvalid {
		t.Fatalf("body over cap: status %d code %q, want 400 invalid",
			resp.StatusCode, envelope.Code)
	}
	if core.hitCount() != 1 {
		t.Fatalf("over-cap body forwarded: core hits = %d, want 1", core.hitCount())
	}
}

// TestCommitClaimFactsCopiesRejectionsVerbatim is proof item 2 on the
// satellite side: every fence, lease, and conflict refusal core returns
// crosses the proxy with its status and envelope bytes intact, and the
// satellite's own store sees nothing.
func TestCommitClaimFactsCopiesRejectionsVerbatim(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"stale token", 409, `{"code":"stale_fence","message":"epoch 6 superseded by 7"}`},
		{"wrong owner", 409, `{"code":"stale_fence","message":"owner o-9 does not hold c-1"}`},
		{"expired lease", 409, `{"code":"lease_expired","message":"lease lapsed before commit"}`},
		{"completed claim", 409, `{"code":"claim_conflict","message":"claim c-1 already completed"}`},
		{"revoked grant", 409, `{"code":"stale_fence","message":"tenant grant revoked"}`},
		{"core invalid", 400, `{"code":"invalid","message":"result over core cap"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			core := &commitCore{status: tc.status, resp: tc.body}
			coreSrv := httptest.NewServer(core.handler())
			defer coreSrv.Close()

			st := &fakeStore{}
			srv := newTestServerWithCommitTarget(t, st, coreSrv.URL)
			defer srv.Close()

			now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
			resp := post(t, srv, "/workflow.v1/CommitClaimFacts",
				commitBodyForTest(now, `{"fact_rows":[]}`))
			status, body := readAllForTest(t, resp)
			if status != tc.status || body != tc.body {
				t.Fatalf("proxy answer = %d %q, want %d %q",
					status, body, tc.status, tc.body)
			}
			if len(st.calls) != 0 {
				t.Fatalf("rejected commit reached the ClaimStore: %v", st.calls)
			}
		})
	}
}

// TestSetCommitForwarderRejectsBadTargets requires malformed targets to
// leave the forwarder unset: a typo at startup keeps the dark default
// instead of forwarding at an accidental address.
func TestSetCommitForwarderRejectsBadTargets(t *testing.T) {
	st := &fakeStore{}
	srv, err := server.New(st, nil)
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	bad := []struct {
		name string
		url  string
		cli  *http.Client
	}{
		{"empty", "", &http.Client{}},
		{"blank", "   ", &http.Client{}},
		{"no scheme", "core:8080", &http.Client{}},
		{"no host", "http://", &http.Client{}},
		{"bad scheme", "gopher://core", &http.Client{}},
		{"unparseable", "http://[::1", &http.Client{}},
		{"nil client", "http://core:8080", nil},
	}
	for _, tc := range bad {
		srv.SetCommitForwarder(tc.url, tc.cli)
	}
	proxy := httptest.NewServer(srv.Handler())
	defer proxy.Close()

	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	resp := post(t, proxy, "/workflow.v1/CommitClaimFacts",
		commitBodyForTest(now, `{"fact_rows":[]}`))
	envelope := decodeEnvelope(t, resp)
	if resp.StatusCode != http.StatusInternalServerError || envelope.Code != "internal" {
		t.Fatalf("bad targets: status %d code %q, want 500 internal", resp.StatusCode, envelope.Code)
	}
}

// TestCommitClaimFactsNilForwarderFailsClosed requires the dark default
// (no core URL configured) to refuse with a 500 that is never an ack.
func TestCommitClaimFactsNilForwarderFailsClosed(t *testing.T) {
	st := &fakeStore{}
	srv := newTestServer(t, st)
	defer srv.Close()

	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	resp := post(t, srv, "/workflow.v1/CommitClaimFacts",
		commitBodyForTest(now, `{"fact_rows":[]}`))
	envelope := decodeEnvelope(t, resp)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("unset forwarder: status %d, want 500", resp.StatusCode)
	}
	if envelope.Code == "" || strings.Contains(envelope.Code, "ok") {
		t.Fatalf("unset forwarder: envelope code %q looks like an ack", envelope.Code)
	}
	if len(st.calls) != 0 {
		t.Fatalf("refused commit reached the ClaimStore: %v", st.calls)
	}
}

// TestCommitClaimFactsTransportFailureIsNeverAnAck requires a dead core
// to surface as a 500 internal: the caller must retry, never treat the
// attempt as committed (proof item 4 on the satellite side).
func TestCommitClaimFactsTransportFailureIsNeverAnAck(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close() // Refused connections from here on.

	st := &fakeStore{}
	srv := newTestServerWithCommitTarget(t, st, deadURL)
	defer srv.Close()

	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	resp := post(t, srv, "/workflow.v1/CommitClaimFacts",
		commitBodyForTest(now, `{"fact_rows":[]}`))
	envelope := decodeEnvelope(t, resp)
	if resp.StatusCode != http.StatusInternalServerError || envelope.Code != "internal" {
		t.Fatalf("dead core: status %d code %q, want 500 internal",
			resp.StatusCode, envelope.Code)
	}
	if len(st.calls) != 0 {
		t.Fatalf("failed forward reached the ClaimStore: %v", st.calls)
	}
}

// TestCommitClaimFactsRejectsVersionMismatch requires the version gate to
// run before any forward: a foreign major never reaches core.
func TestCommitClaimFactsRejectsVersionMismatch(t *testing.T) {
	core := &commitCore{status: http.StatusOK, resp: `{"ok":true}`}
	coreSrv := httptest.NewServer(core.handler())
	defer coreSrv.Close()

	st := &fakeStore{}
	srv := newTestServerWithCommitTarget(t, st, coreSrv.URL)
	defer srv.Close()

	now := time.Now().UTC().Truncate(time.Second).Format(time.RFC3339Nano)
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/workflow.v1/CommitClaimFacts",
		strings.NewReader(commitBodyForTest(now, `{"fact_rows":[]}`)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(workflowv1.VersionHeader, "2")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	envelope := decodeEnvelope(t, resp)
	if resp.StatusCode != http.StatusBadRequest || envelope.Code != workflowv1.CodeVersionMismatch {
		t.Fatalf("foreign major: status %d code %q, want 400 version_mismatch",
			resp.StatusCode, envelope.Code)
	}
	if core.hitCount() != 0 {
		t.Fatalf("version-mismatched commit forwarded %d times, want 0", core.hitCount())
	}
}
