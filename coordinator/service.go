// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/eshu-hq/eshu-workflow/client"
	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// ControlPlane is the narrow claim-plane surface the ported coordinator
// needs. The HTTP client implements it; tests substitute a fake. Every
// method travels a protocol route: the service holds no database handle
// and reads no table directly.
type ControlPlane interface {
	CreateRun(context.Context, workflowv1.Run) error
	EnqueueWorkItems(context.Context, []workflowv1.WorkItem) (int, error)
	ReconcileCollectorInstances(context.Context, time.Time, []workflowv1.DesiredCollectorInstance) error
	ListCollectorInstances(context.Context) ([]workflowv1.CollectorInstance, error)
	ListRuns(context.Context, workflowv1.ListRunsRequest) (workflowv1.ListRunsResponse, error)
	ReconcileWorkflowRuns(context.Context, time.Time) (int, error)
	ReapExpiredClaims(context.Context, time.Time, int, time.Duration) ([]workflowv1.Claim, error)
	GetRunStatus(context.Context, string) (workflowv1.GetRunStatusResponse, error)
	QueueDepthSnapshot(context.Context) (workflowv1.QueueDepthSnapshot, error)
}

// runListPageCap bounds the admission guard's ListRuns walk: twenty full
// pages cover twenty thousand open runs, past which the guard reports the
// bound instead of paging forever.
const runListPageCap = 20

// Service is the ported workflow coordinator runner.
type Service struct {
	Config   Config
	Plane    ControlPlane
	Planners *Registry
	Logger   *slog.Logger
	// Instruments records coordinator passes. Nil disables counting;
	// the command assigns a registry-backed handle.
	Instruments *telemetry.Instruments
	Clock       func() time.Time
}

func (s Service) now() time.Time {
	if s.Clock != nil {
		return s.Clock()
	}
	return time.Now()
}

func (s Service) logger() *slog.Logger {
	return telemetry.Logger(s.Logger)
}

// Run reconciles on its intervals until ctx ends: instance state always,
// and in active mode expired-claim reaping plus drained-run
// reconciliation.
func (s Service) Run(ctx context.Context) error {
	if s.Plane == nil {
		return fmt.Errorf("coordinator control plane is required")
	}
	if s.Planners == nil {
		return fmt.Errorf("coordinator planner registry is required")
	}
	s.Config = s.Config.withDefaults()
	if err := s.Config.Validate(); err != nil {
		return err
	}
	logger := s.logger()
	if err := s.Reconcile(ctx); err != nil {
		return fmt.Errorf("initial collector reconciliation: %w", err)
	}
	if s.Config.DeploymentMode == DeploymentModeActive {
		if err := s.Reap(ctx); err != nil {
			return fmt.Errorf("initial expired-claim reap: %w", err)
		}
		if err := s.ReconcileRuns(ctx); err != nil {
			return fmt.Errorf("initial workflow run reconciliation: %w", err)
		}
	}
	logger.Info("coordinator running",
		slog.String("deployment_mode", s.Config.DeploymentMode),
		slog.Bool("claims_enabled", s.Config.ClaimsEnabled),
		slog.Int("collector_instances", len(s.Config.CollectorInstances)))

	reconcileTicker := time.NewTicker(s.Config.ReconcileInterval)
	defer reconcileTicker.Stop()
	var reapTicker, runReconcileTicker *time.Ticker
	if s.Config.DeploymentMode == DeploymentModeActive {
		reapTicker = time.NewTicker(s.Config.ReapInterval)
		defer reapTicker.Stop()
		runReconcileTicker = time.NewTicker(s.Config.RunReconcileInterval)
		defer runReconcileTicker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-reconcileTicker.C:
			if err := s.Reconcile(ctx); err != nil {
				return fmt.Errorf("reconcile collector instances: %w", err)
			}
		case <-tickerChan(reapTicker):
			if err := s.Reap(ctx); err != nil {
				return fmt.Errorf("reap expired claims: %w", err)
			}
		case <-tickerChan(runReconcileTicker):
			if err := s.ReconcileRuns(ctx); err != nil {
				return fmt.Errorf("reconcile workflow runs: %w", err)
			}
		}
	}
}

func tickerChan(ticker *time.Ticker) <-chan time.Time {
	if ticker == nil {
		return nil
	}
	return ticker.C
}

// Reconcile declares the desired instances, observes durable state and
// queue depth, and in active mode with claims enabled plans and admits
// one run per schedulable family in fairness order. A failing family
// fails its own admission but never stops the pass: the returned error
// joins every family failure so the operator sees each sick family.
func (s Service) Reconcile(ctx context.Context) error {
	ctx, span := telemetry.Start(ctx, "coordinator.reconcile")
	defer span.End()
	logger := s.logger()
	observedAt := s.now().UTC()

	if err := s.Plane.ReconcileCollectorInstances(ctx, observedAt, s.Config.CollectorInstances); err != nil {
		telemetry.Fail(span, "reconcile", err.Error())
		s.Instruments.ObserveMutation("coordinator_reconcile", "error")
		return fmt.Errorf("reconcile collector instances: %w", err)
	}
	instances, err := s.Plane.ListCollectorInstances(ctx)
	if err != nil {
		telemetry.Fail(span, "reconcile", err.Error())
		s.Instruments.ObserveMutation("coordinator_reconcile", "error")
		return fmt.Errorf("list durable collector instances: %w", err)
	}
	s.observeDepths(ctx, logger)
	if s.Config.DeploymentMode != DeploymentModeActive || !s.Config.ClaimsEnabled {
		s.Instruments.ObserveMutation("coordinator_reconcile", "ok")
		return nil
	}
	ordered, err := familyOrder(instances)
	if err != nil {
		telemetry.Fail(span, "reconcile", err.Error())
		s.Instruments.ObserveMutation("coordinator_reconcile", "error")
		return fmt.Errorf("order families: %w", err)
	}
	var familyErrs []error
	for _, instance := range ordered {
		if err := s.planFamily(ctx, logger, instance, observedAt); err != nil {
			familyErrs = append(familyErrs, err)
		}
	}
	s.Instruments.ObserveMutation("coordinator_reconcile", "ok")
	return errors.Join(familyErrs...)
}

// observeDepths refreshes the family depth gauges from the read plane.
// Depth is observability only: a failed read logs and never fails the
// pass it rides on.
func (s Service) observeDepths(ctx context.Context, logger *slog.Logger) {
	snapshot, err := s.Plane.QueueDepthSnapshot(ctx)
	if err != nil {
		logger.Warn("queue depth observation failed", slog.String("error", err.Error()))
		return
	}
	for _, family := range snapshot.Families {
		s.Instruments.ObserveDepth(string(family.CollectorKind), "pending", family.Pending)
		s.Instruments.ObserveDepth(string(family.CollectorKind), "claimed", family.Claimed)
	}
}

// planFamily plans and admits one instance's work: parse declared
// targets, plan the deterministic run, skip when an open run already
// covers the scope set, else create the run and enqueue its items.
func (s Service) planFamily(
	ctx context.Context,
	logger *slog.Logger,
	instance workflowv1.CollectorInstance,
	observedAt time.Time,
) error {
	family := string(instance.CollectorKind)
	planner, ok := s.Planners.Lookup(instance.CollectorKind)
	if !ok || planner == nil {
		return fmt.Errorf("reconcile family %q: no planner registered", family)
	}
	targets, err := instanceTargets(instance.Configuration)
	if err != nil {
		return fmt.Errorf("reconcile family %q instance %q: %w", family, instance.InstanceID, err)
	}
	run, items, err := planner.Plan(ctx, PlanRequest{
		Family:     instance.CollectorKind,
		Instance:   instance,
		Targets:    targets,
		PlanKey:    planKeyFor(instance.CollectorKind, instance.InstanceID, observedAt, s.Config.ScanInterval),
		ObservedAt: observedAt,
	})
	if err != nil {
		return fmt.Errorf("reconcile family %q instance %q: plan: %w", family, instance.InstanceID, err)
	}
	if len(items) == 0 {
		return nil
	}
	covered, err := s.openRunCovers(ctx, run.RequestedCollector, run.RequestedScopeSet)
	if err != nil {
		return fmt.Errorf("reconcile family %q instance %q: guard: %w", family, instance.InstanceID, err)
	}
	if covered {
		logger.Info("duplicate plan skipped: an open run already covers the scope set",
			slog.String("family", family),
			slog.String("collector_instance_id", instance.InstanceID),
			slog.String("run_id", run.RunID))
		return nil
	}
	if err := s.Plane.CreateRun(ctx, run); err != nil {
		return fmt.Errorf("reconcile family %q instance %q: create run: %w", family, instance.InstanceID, err)
	}
	inserted, err := s.Plane.EnqueueWorkItems(ctx, items)
	if err != nil {
		return fmt.Errorf("reconcile family %q instance %q: enqueue: %w", family, instance.InstanceID, err)
	}
	admission := workflowv1.RunAdmission{EligibleTargets: len(items), InsertedWorkItems: inserted}
	if err := admission.Validate(); err != nil {
		return fmt.Errorf("reconcile family %q instance %q: admission: %w", family, instance.InstanceID, err)
	}
	s.Instruments.ObserveMutation("coordinator_admit", "ok")
	logger.Info("admitted scheduled work",
		slog.String("family", family),
		slog.String("collector_instance_id", instance.InstanceID),
		slog.String("run_id", run.RunID),
		slog.Int("eligible_targets", admission.EligibleTargets),
		slog.Int("inserted_work_items", admission.InsertedWorkItems))
	return nil
}

// openRunCovers reports whether a non-terminal run already owns the same
// collector and scope set. The walk pages the read plane newest-first
// and stops at the first cover or the page cap.
func (s Service) openRunCovers(ctx context.Context, collector, scopeSet string) (bool, error) {
	cursor := ""
	for range runListPageCap {
		page, err := s.Plane.ListRuns(ctx, workflowv1.ListRunsRequest{Limit: 100, Cursor: cursor})
		if err != nil {
			return false, err
		}
		for _, summary := range page.Runs {
			if summary.Status == workflowv1.RunStatusComplete || summary.Status == workflowv1.RunStatusFailed {
				continue
			}
			if summary.RequestedCollector == collector && summary.RequestedScopeSet == scopeSet {
				return true, nil
			}
		}
		if page.NextCursor == "" {
			return false, nil
		}
		cursor = page.NextCursor
	}
	return false, fmt.Errorf("open-run guard exceeded %d ListRuns pages", runListPageCap)
}

// Reap expires stale claims past now and requeues their work. It is a
// no-op outside active mode; the Run loop only ticks it there.
func (s Service) Reap(ctx context.Context) error {
	ctx, span := telemetry.Start(ctx, "coordinator.reap")
	defer span.End()
	if s.Config.DeploymentMode != DeploymentModeActive {
		return nil
	}
	now := s.now().UTC()
	reaped, err := s.Plane.ReapExpiredClaims(ctx, now, s.Config.ExpiredClaimLimit, s.Config.ExpiredClaimRequeueDelay)
	if err != nil {
		telemetry.Fail(span, "reap", err.Error())
		s.Instruments.ObserveMutation("coordinator_reap", "error")
		return fmt.Errorf("reap expired claims: %w", err)
	}
	s.Instruments.ObserveMutation("coordinator_reap", "ok")
	s.Instruments.ObserveReap(len(reaped))
	if len(reaped) > 0 {
		s.logger().Info("reaped expired claims", slog.Int("count", len(reaped)))
	}
	return nil
}

// ReconcileRuns advances drained runs to complete. It is a no-op outside
// active mode; the Run loop only ticks it there.
func (s Service) ReconcileRuns(ctx context.Context) error {
	ctx, span := telemetry.Start(ctx, "coordinator.reconcile_runs")
	defer span.End()
	if s.Config.DeploymentMode != DeploymentModeActive {
		return nil
	}
	transitioned, err := s.Plane.ReconcileWorkflowRuns(ctx, s.now().UTC())
	if err != nil {
		telemetry.Fail(span, "reconcile_runs", err.Error())
		s.Instruments.ObserveMutation("coordinator_run_reconcile", "error")
		return fmt.Errorf("reconcile workflow runs: %w", err)
	}
	s.Instruments.ObserveMutation("coordinator_run_reconcile", "ok")
	if transitioned > 0 {
		s.logger().Info("reconciled runs", slog.Int("count", transitioned))
	}
	return nil
}

// Compile-time proof the HTTP client drives the ported service: every
// coordinator state change travels the claim-plane API.
var _ ControlPlane = (*client.Client)(nil)
