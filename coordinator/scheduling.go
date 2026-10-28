// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package coordinator

import (
	"fmt"
	"sort"

	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// schedulable reports whether the ported runner owns the instance: the
// instance is enabled and claim-enabled. Component-activation documents
// stay core-planned until their schema crosses the boundary (parity
// gap), so unlike the in-tree selector there is no configuration
// exclusion here.
func schedulable(instance workflowv1.CollectorInstance) bool {
	return instance.Enabled && instance.ClaimsEnabled
}

// familyOrder sequences one pass over the schedulable instances in
// weighted round-robin family order, then appends any family the
// rotation did not reach in sorted order. The rotation decides who
// plans first under contention; the sorted tail is the starvation
// bound: every schedulable family plans at least once per pass, even
// under skewed weights. Fairness inside the claim itself stays
// server-side in ClaimNext; this order is planning precedence only.
func familyOrder(instances []workflowv1.CollectorInstance) ([]workflowv1.CollectorInstance, error) {
	schedulableInstances := make([]workflowv1.CollectorInstance, 0, len(instances))
	for _, instance := range instances {
		if schedulable(instance) {
			schedulableInstances = append(schedulableInstances, instance)
		}
	}
	if len(schedulableInstances) == 0 {
		return nil, nil
	}
	candidates, err := workflowv1.FairnessCandidatesFromCollectorInstances(schedulableInstances)
	if err != nil {
		return nil, fmt.Errorf("fairness candidates: %w", err)
	}
	rotation, err := workflowv1.NewFamilyFairnessScheduler(candidates)
	if err != nil {
		return nil, fmt.Errorf("fairness scheduler: %w", err)
	}
	ordered := make([]workflowv1.CollectorInstance, 0, len(schedulableInstances))
	served := map[string]bool{}
	families := map[workflowv1.CollectorKind]bool{}
	for range schedulableInstances {
		target, ok := rotation.Next()
		if !ok {
			break
		}
		if families[target.CollectorKind] {
			continue
		}
		families[target.CollectorKind] = true
		for _, instance := range schedulableInstances {
			if instance.CollectorKind != target.CollectorKind || served[instance.InstanceID] {
				continue
			}
			served[instance.InstanceID] = true
			ordered = append(ordered, instance)
		}
	}
	// Starvation tail: families the rotation never reached still plan
	// this pass, in stable kind order.
	rest := make([]workflowv1.CollectorInstance, 0)
	for _, instance := range schedulableInstances {
		if !served[instance.InstanceID] {
			rest = append(rest, instance)
		}
	}
	sort.Slice(rest, func(i, j int) bool {
		if rest[i].CollectorKind != rest[j].CollectorKind {
			return rest[i].CollectorKind < rest[j].CollectorKind
		}
		return rest[i].InstanceID < rest[j].InstanceID
	})
	return append(ordered, rest...), nil
}
