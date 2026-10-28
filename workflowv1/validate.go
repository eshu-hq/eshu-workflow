// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import (
	"encoding/json"
	"fmt"
	"strings"
)

// normalizeJSONDocument treats a blank document as an empty object so an
// omitted configuration decodes to defaults instead of failing the read.
func normalizeJSONDocument(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "{}"
	}
	return trimmed
}

// validateJSONDocument rejects a configuration that is not valid JSON.
// Blank is valid: it normalizes to an empty object.
func validateJSONDocument(field, raw string) error {
	if !json.Valid([]byte(normalizeJSONDocument(raw))) {
		return fmt.Errorf("%s must be valid JSON", field)
	}
	return nil
}
