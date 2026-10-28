// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package workflowv1

import "fmt"

// ClaimQuota is the enforced claim budget for one collector family.
type ClaimQuota struct {
	MaxGenerationsPerHour int `json:"max_generations_per_hour"`
}

// Validate checks the quota budget.
func (q ClaimQuota) Validate() error {
	if q.MaxGenerationsPerHour < 0 {
		return fmt.Errorf("max_generations_per_hour must not be negative")
	}
	return nil
}

// QuotaAssignment binds one family to its enforced quota. Families absent
// from the server quota map claim unthrottled.
type QuotaAssignment struct {
	CollectorKind CollectorKind `json:"collector_kind"`
	Quota         ClaimQuota    `json:"quota"`
}

// Validate checks the quota assignment.
func (a QuotaAssignment) Validate() error {
	if err := a.CollectorKind.Validate(); err != nil {
		return err
	}
	return a.Quota.Validate()
}
