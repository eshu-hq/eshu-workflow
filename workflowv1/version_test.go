// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import "testing"

func TestProtocolIdentity(t *testing.T) {
	if ProtocolName != "workflow.v1" {
		t.Fatalf("ProtocolName = %q, want workflow.v1", ProtocolName)
	}
	if ProtocolVersion != 1 {
		t.Fatalf("ProtocolVersion = %d, want 1", ProtocolVersion)
	}
}

func TestSupportedAcceptsCurrentMajorOnly(t *testing.T) {
	if !Supported(ProtocolVersion) {
		t.Fatal("Supported must accept the current protocol major")
	}
	for _, major := range []int{0, 2, 99} {
		if Supported(major) {
			t.Fatalf("Supported(%d) = true, want false", major)
		}
	}
}
