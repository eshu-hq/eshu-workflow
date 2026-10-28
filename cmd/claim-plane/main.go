// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

// Command claim-plane serves the workflow claim plane: 14 protocol routes
// over one Postgres database, with an internal reaper and an admin
// listener for health and metrics.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	"github.com/eshu-hq/eshu-workflow/server"
	claimstore "github.com/eshu-hq/eshu-workflow/store"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

type options struct {
	listen       string
	adminListen  string
	databaseURL  string
	reapInterval time.Duration
	reapLimit    int
	requeueDelay time.Duration
	quotas       string
	pacing       workflowv1.ClaimPacingConfig
}

func parseFlags(args []string) (options, error) {
	var opts options
	fs := flag.NewFlagSet("claim-plane", flag.ContinueOnError)
	fs.StringVar(&opts.listen, "listen", ":8080", "protocol listener address")
	fs.StringVar(&opts.adminListen, "admin-listen", ":8081", "health and metrics listener address")
	fs.StringVar(&opts.databaseURL, "database-url", os.Getenv("CLAIMPLANE_DATABASE_URL"), "postgres DSN")
	fs.DurationVar(&opts.reapInterval, "reap-interval", workflowv1.DefaultReapInterval(), "reaper tick interval")
	fs.IntVar(&opts.reapLimit, "reap-limit", workflowv1.DefaultExpiredClaimLimit(), "claims reaped per tick")
	fs.DurationVar(&opts.requeueDelay, "requeue-delay", workflowv1.DefaultExpiredClaimRequeueDelay(), "visibility delay for reaped items")
	fs.StringVar(&opts.quotas, "quotas", "", "per-kind hourly quotas as kind=n,kind=n")
	fs.Int64Var(&opts.pacing.DomainHigh, "pacing-domain-high", 0, "per-family pending high watermark")
	fs.Int64Var(&opts.pacing.DomainLow, "pacing-domain-low", 0, "per-family pending low watermark")
	fs.Int64Var(&opts.pacing.GlobalHigh, "pacing-global-high", 0, "global pending high watermark")
	fs.Int64Var(&opts.pacing.GlobalLow, "pacing-global-low", 0, "global pending low watermark")
	fs.DurationVar(&opts.pacing.GrantDelay, "pacing-grant-delay", 0, "backoff carried on paced grants")
	fs.DurationVar(&opts.pacing.MaxEngagement, "pacing-max-engagement", 0, "engagement bound before stuck raises")
	fs.DurationVar(&opts.pacing.DepthCacheTTL, "pacing-depth-cache-ttl", 0, "depth observation reuse window")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if opts.databaseURL == "" {
		return opts, fmt.Errorf("database URL is required (--database-url or CLAIMPLANE_DATABASE_URL)")
	}
	if opts.reapInterval <= 0 {
		return opts, fmt.Errorf("reap interval must be positive")
	}
	if opts.reapLimit <= 0 {
		return opts, fmt.Errorf("reap limit must be positive")
	}
	return opts, nil
}

func parseQuotas(raw string) (map[string]int, error) {
	quotas := map[string]int{}
	if strings.TrimSpace(raw) == "" {
		return quotas, nil
	}
	for _, entry := range strings.Split(raw, ",") {
		kind, value, found := strings.Cut(entry, "=")
		if !found {
			return nil, fmt.Errorf("quota %q must be kind=n", entry)
		}
		limit, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || limit < 0 {
			return nil, fmt.Errorf("quota %q must be kind=n with a non-negative n", entry)
		}
		kind = strings.TrimSpace(kind)
		if kind == "" {
			return nil, fmt.Errorf("quota %q names no kind", entry)
		}
		quotas[kind] = limit
	}
	return quotas, nil
}

func run(ctx context.Context, logger *slog.Logger, opts options) error {
	quotas, err := parseQuotas(opts.quotas)
	if err != nil {
		return err
	}
	store, err := claimstore.Open(ctx, opts.databaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if err := store.Configure(claimstore.Config{Quotas: quotas, Pacing: opts.pacing, Logger: logger}); err != nil {
		return fmt.Errorf("configure: %w", err)
	}

	registry := prometheus.NewRegistry()
	instruments := telemetry.NewInstruments(registry)
	srv, err := server.New(store, logger)
	if err != nil {
		return err
	}
	srv.Instruments = instruments
	store.Instruments = instruments

	protocol := &http.Server{
		Addr:              opts.listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	adminMux := http.NewServeMux()
	adminMux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	adminMux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok"))
	})
	adminMux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		if err := store.PingTable(request.Context(), "workflow_claims"); err != nil {
			http.Error(writer, "not ready", http.StatusServiceUnavailable)
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

	reaper := newReaper(store, instruments, logger, opts)
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go reaper.loop(ctx)

	errCh := make(chan error, 2)
	go func() {
		logger.Info("protocol listener started", slog.String("addr", opts.listen))
		if err := protocol.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("protocol listener: %w", err)
			return
		}
		errCh <- nil
	}()
	go func() {
		logger.Info("admin listener started", slog.String("addr", opts.adminListen))
		if err := admin.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("admin listener: %w", err)
			return
		}
		errCh <- nil
	}()

	<-ctx.Done()
	logger.Info("shutdown started")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = protocol.Shutdown(shutdownCtx)
	_ = admin.Shutdown(shutdownCtx)
	for range 2 {
		if err := <-errCh; err != nil {
			return err
		}
	}
	return nil
}

// reaperStore is the narrow tick surface the reaper needs: expiring
// stale claims, reconciling drained runs, and reading queue depth. The
// pgx store implements it; tests substitute a fake.
type reaperStore interface {
	Reap(ctx context.Context, asOf time.Time, limit int, requeueDelay time.Duration) ([]workflowv1.Claim, error)
	ReconcileRuns(ctx context.Context, now time.Time) (int, error)
	QueueDepth(ctx context.Context, now time.Time) (workflowv1.QueueDepthSnapshot, error)
}

// reaper expires stale claims and reconciles drained runs on a tick.
type reaper struct {
	store       reaperStore
	instruments *telemetry.Instruments
	logger      *slog.Logger
	interval    time.Duration
	limit       int
	delay       time.Duration
}

func newReaper(
	store reaperStore,
	instruments *telemetry.Instruments,
	logger *slog.Logger,
	opts options,
) *reaper {
	return &reaper{
		store: store, instruments: instruments, logger: logger,
		interval: opts.reapInterval, limit: opts.reapLimit, delay: opts.requeueDelay,
	}
}

func (r *reaper) loop(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.tick(ctx, now.UTC())
		}
	}
}

func (r *reaper) tick(ctx context.Context, now time.Time) {
	reaped, err := r.store.Reap(ctx, now, r.limit, r.delay)
	if err != nil {
		r.logger.Error("reap tick", slog.String("error", err.Error()))
		return
	}
	if len(reaped) > 0 {
		r.logger.Info("reaped expired claims", slog.Int("count", len(reaped)))
	}
	r.instruments.ObserveReap(len(reaped))
	transitioned, err := r.store.ReconcileRuns(ctx, now)
	if err != nil {
		r.logger.Error("reconcile tick", slog.String("error", err.Error()))
		return
	}
	if transitioned > 0 {
		r.logger.Info("reconciled runs", slog.Int("count", transitioned))
	}
	snapshot, err := r.store.QueueDepth(ctx, now)
	if err != nil {
		r.logger.Error("depth tick", slog.String("error", err.Error()))
		return
	}
	for _, family := range snapshot.Families {
		r.instruments.ObserveDepth(string(family.CollectorKind), "pending", family.Pending)
		r.instruments.ObserveDepth(string(family.CollectorKind), "claimed", family.Claimed)
	}
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
	if err := run(context.Background(), logger, opts); err != nil {
		logger.Error("claim-plane failed", slog.String("error", err.Error()))
		os.Exit(1)
	}
}
