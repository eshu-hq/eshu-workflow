// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

// Package client drives the workflow claim plane over HTTP: every one of
// the 20 merged workflowv1 routes, including the six control-plane
// coordinator seams the in-tree claim-plane client used to reject with
// ErrClaimPlaneUnsupported. All coordinator state travels through this
// API; the package never touches a database and never imports core
// internals, so tenant and producer grants stay core-owned.
package client
