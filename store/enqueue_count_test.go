// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore_test

import (
	"testing"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// TestEnqueueReportsInsertedCounts requires idempotent enqueue to report
// what the table actually took: the first batch inserts every row, and an
// ambiguous-timeout replay of the same batch accepts everything while
// inserting nothing.
func TestEnqueueReportsInsertedCounts(t *testing.T) {
	st, ctx := openFreshStore(t)
	runID := seedRun(t, ctx, st)
	items := []workflowv1.WorkItem{
		seedItem(runID, "count-0", false),
		seedItem(runID, "count-1", false),
	}
	inserted, err := st.Enqueue(ctx, items)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if inserted != 2 {
		t.Fatalf("inserted = %d, want 2", inserted)
	}
	inserted, err = st.Enqueue(ctx, items)
	if err != nil {
		t.Fatalf("replay enqueue: %v", err)
	}
	if inserted != 0 {
		t.Fatalf("replay inserted = %d, want 0", inserted)
	}
}
