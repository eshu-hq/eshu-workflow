// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"fmt"
	"strconv"
	"strings"
)

// Protocol error codes. The code is the stable contract; message text is
// human detail and must never be parsed by callers.
const (
	CodeInvalid         = "invalid"
	CodeNotFound        = "not_found"
	CodeClaimConflict   = "claim_conflict"
	CodeStaleFence      = "stale_fence"
	CodeLeaseExpired    = "lease_expired"
	CodePaced           = "paced"
	CodeVersionMismatch = "version_mismatch"
)

// HTTP status per code. Stale-fence and lease-expiry are conflicts (409):
// the caller holds an epoch the server no longer honors and must
// re-claim, never retry the same mutation.
func httpStatusForCode(code string) int {
	switch code {
	case CodeInvalid, CodeVersionMismatch:
		return 400
	case CodeNotFound:
		return 404
	case CodeClaimConflict, CodeStaleFence, CodeLeaseExpired:
		return 409
	case CodePaced:
		return 429
	default:
		return 500
	}
}

// ProtocolError is a coded protocol failure.
type ProtocolError struct {
	Code    string
	Message string
	// RetryAfterSeconds carries the grant delay on paced errors.
	RetryAfterSeconds int64
}

// Error implements error.
func (e *ProtocolError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// HTTPStatus returns the HTTP status for the error code.
func (e *ProtocolError) HTTPStatus() int {
	return httpStatusForCode(e.Code)
}

// Envelope renders the JSON error body.
func (e *ProtocolError) Envelope() ErrorEnvelope {
	return ErrorEnvelope{Code: e.Code, Message: e.Message, RetryAfterSeconds: e.RetryAfterSeconds}
}

// CheckVersion validates the caller's VersionHeader value: it must be a
// bare major the server speaks. Anything else is a version mismatch, not
// an invalid request body.
func CheckVersion(header string) error {
	major, err := strconv.Atoi(strings.TrimSpace(header))
	if err != nil || !Supported(major) {
		return &ProtocolError{
			Code:    CodeVersionMismatch,
			Message: fmt.Sprintf("unsupported workflow protocol version %q, server speaks %d", header, ProtocolVersion),
		}
	}
	return nil
}
