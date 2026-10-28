// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

// Command workflow-coordinator runs the ported workflow coordinator: it
// reconciles desired collector instances and, in active mode with claims
// enabled, plans and admits scheduled work through the claim-plane HTTP
// API. It holds no database handle and reads no table directly; tenant
// and producer grants stay core-owned.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/eshu-hq/eshu-workflow/client"
	"github.com/eshu-hq/eshu-workflow/coordinator"
	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// versionName identifies this binary on the --version flag. The protocol
// major is the compatibility identity: the coordinator speaks workflow
// major 1 and nothing else.
const versionName = "workflow-coordinator"

type options struct {
	claimPlaneURL            string
	adminListen              string
	deploymentMode           string
	claimsEnabled            bool
	reconcileInterval        time.Duration
	runReconcileInterval     time.Duration
	reapInterval             time.Duration
	scanInterval             time.Duration
	expiredClaimLimit        int
	expiredClaimRequeueDelay time.Duration
	instancesFile            string
	showVersion              bool
}

func parseFlags(args []string) (options, error) {
	var opts options
	fs := flag.NewFlagSet("workflow-coordinator", flag.ContinueOnError)
	fs.StringVar(&opts.claimPlaneURL, "claim-plane-url", os.Getenv("WORKFLOW_COORDINATOR_CLAIM_PLANE_URL"),
		"claim-plane service base URL")
	fs.StringVar(&opts.adminListen, "admin-listen", ":8082", "health and metrics listener address")
	fs.StringVar(&opts.deploymentMode, "deployment-mode", coordinator.DeploymentModeDark,
		"coordinator deployment mode: dark or active")
	fs.BoolVar(&opts.claimsEnabled, "claims-enabled", false, "plan and admit work in active mode")
	fs.DurationVar(&opts.reconcileInterval, "reconcile-interval", 0, "instance reconciliation interval (0 selects default)")
	fs.DurationVar(&opts.runReconcileInterval, "run-reconcile-interval", 0, "drained-run reconciliation interval (0 selects default)")
	fs.DurationVar(&opts.reapInterval, "reap-interval", 0, "expired-claim reap interval (0 selects default)")
	fs.DurationVar(&opts.scanInterval, "scan-interval", 0, "deterministic plan-key window (0 selects default)")
	fs.IntVar(&opts.expiredClaimLimit, "reap-limit", 0, "claims reaped per pass (0 selects default)")
	fs.DurationVar(&opts.expiredClaimRequeueDelay, "requeue-delay", 0, "visibility delay for reaped items")
	fs.StringVar(&opts.instancesFile, "instances-file", "", "JSON file of desired collector instances (optional)")
	fs.BoolVar(&opts.showVersion, "version", false, "print the binary version and exit")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if opts.showVersion {
		return opts, nil
	}
	if strings.TrimSpace(opts.claimPlaneURL) == "" {
		return opts, fmt.Errorf("claim-plane URL is required (--claim-plane-url or WORKFLOW_COORDINATOR_CLAIM_PLANE_URL)")
	}
	return opts, nil
}

// loadInstances reads the operator-declared instance set. An absent file
// declares nothing: the reconcile then owns an empty set.
func loadInstances(path string) ([]workflowv1.DesiredCollectorInstance, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read instances file: %w", err)
	}
	var instances []workflowv1.DesiredCollectorInstance
	if err := json.Unmarshal(raw, &instances); err != nil {
		return nil, fmt.Errorf("parse instances file: %w", err)
	}
	for i := range instances {
		if err := instances[i].Validate(); err != nil {
			return nil, fmt.Errorf("instance %d: %w", i, err)
		}
	}
	return instances, nil
}

func run(ctx context.Context, logger *slog.Logger, opts options) error {
	plane, err := client.New(opts.claimPlaneURL)
	if err != nil {
		return err
	}
	instances, err := loadInstances(opts.instancesFile)
	if err != nil {
		return err
	}
	registry := prometheus.NewRegistry()
	instruments := telemetry.NewInstruments(registry)
	svc := coordinator.Service{
		Config: coordinator.Config{
			DeploymentMode:           opts.deploymentMode,
			ClaimsEnabled:            opts.claimsEnabled,
			ReconcileInterval:        opts.reconcileInterval,
			RunReconcileInterval:     opts.runReconcileInterval,
			ReapInterval:             opts.reapInterval,
			ScanInterval:             opts.scanInterval,
			ExpiredClaimLimit:        opts.expiredClaimLimit,
			ExpiredClaimRequeueDelay: opts.expiredClaimRequeueDelay,
			CollectorInstances:       instances,
		},
		Plane:       plane,
		Planners:    coordinator.DefaultRegistry(),
		Logger:      logger,
		Instruments: instruments,
	}

	adminMux := http.NewServeMux()
	adminMux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	adminMux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok"))
	})
	adminMux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		if _, err := plane.QueueDepthSnapshot(request.Context()); err != nil {
			http.Error(writer, "claim plane unreachable", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok"))
	})
	admin := &http.Server{
		Addr:              opts.adminListen,
		Handler:           adminMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		logger.Info("admin listener started", slog.String("addr", opts.adminListen))
		if err := admin.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("admin listener failed", slog.String("error", err.Error()))
			stop()
		}
	}()
	if err := svc.Run(ctx); err != nil {
		return err
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = admin.Shutdown(shutdownCtx)
	return nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	shutdown := telemetry.InstallLogExporter(logger)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(ctx)
	}()
	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		logger.Error("flag parse", slog.String("error", err.Error()))
		os.Exit(2)
	}
	if opts.showVersion {
		fmt.Printf("%s %d\n", versionName, workflowv1.ProtocolVersion)
		return
	}
	if err := run(context.Background(), logger, opts); err != nil {
		logger.Error("workflow-coordinator failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}
