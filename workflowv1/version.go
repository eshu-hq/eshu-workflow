// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

const (
	// ProtocolName is the wire protocol name every endpoint serves under.
	ProtocolName = "workflow.v1"
	// ProtocolVersion is the current protocol major. A new closed-enum
	// member is a major bump while any reader rejects unknowns.
	ProtocolVersion = 1
	// VersionHeader carries the caller protocol major on every request.
	VersionHeader = "Workflow-Protocol-Version"
)

// Supported reports whether the server speaks the caller's major.
func Supported(major int) bool {
	return major == ProtocolVersion
}
