// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"testing"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestListRunsRejectsMalformedCursor pins issue #20: a cursor that is not
// "<RFC3339Nano>|<run_id>" must fail as invalid, not return an empty page
// that a pager reads as end-of-list.
func TestListRunsRejectsMalformedCursor(t *testing.T) {
	st, ctx := openFreshStore(t)
	seedRun(t, ctx, st)

	for _, cursor := range []string{
		"garbage-without-separator",
		"not-a-time|run-1",
		"|run-1",
	} {
		_, err := st.ListRuns(ctx, workflowv1.ListRunsRequest{Cursor: cursor})
		if !isCode(err, workflowv1.CodeInvalid) {
			t.Errorf("cursor %q error = %v, want %s", cursor, err, workflowv1.CodeInvalid)
		}
	}
}
