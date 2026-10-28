// SPDX-License-Identifier: MIT
// Copyright (c) 2025-2026 eshu-hq

package claimstore

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/eshu-hq/eshu-workflow/internal/telemetry"
	workflowv1 "github.com/eshu-hq/eshu-workflow/workflowv1"
)

// migrationFS embeds the fresh-install migrations in apply order.
//
//go:embed migrations/*.sql
var migrationFS embed.FS

// Config carries the server-side claim policy: pacing, quotas, and the
// degraded-mode quota floor.
type Config struct {
	// Pacing configures the shared hysteresis latch. Zero disables pacing.
	Pacing workflowv1.ClaimPacingConfig
	// Quotas maps collector kind to enforced generations per hour per
	// scope. Kinds absent from the map claim unthrottled.
	Quotas map[string]int
	// QuotaFloorGrantsPerHour bounds degraded-mode grants per component
	// per server-hour when the quota statement fails. Non-positive
	// selects DefaultQuotaFloorGrantsPerHour.
	QuotaFloorGrantsPerHour int
	// Logger receives degraded-mode fail-open records. Nil discards;
	// Configure replaces nil with a discard logger.
	Logger *slog.Logger
}

// DefaultQuotaFloorGrantsPerHour bounds degraded-mode grants when quota
// state is unreadable.
const DefaultQuotaFloorGrantsPerHour = 10

// Store persists claim-plane state in its own Postgres database. Unlike the
// in-tree store, heartbeat and complete enforce fencing only: the
// tenant-grant EXISTS join is gone because grant tables stay core-owned and
// grant authority moves to the commit pin plus core admission.
type Store struct {
	pool *pgxpool.Pool

	pacing workflowv1.ClaimPacingConfig
	quotas map[string]int
	floor  int
	logger *slog.Logger

	// Instruments records degraded-mode fail-open decisions. Nil
	// disables counting; the command assigns a registry-backed handle,
	// mirroring Server.Instruments.
	Instruments *telemetry.Instruments

	mu          sync.Mutex
	depthCache  cachedDepths
	floorUsed   map[floorKey]int
	floorWindow time.Time
}

type cachedDepths struct {
	at     time.Time
	ttl    time.Duration
	domain map[string]int64
	global int64
}

type floorKey struct {
	component string
	window    time.Time
}

// Open connects to the claim-plane database.
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse claim-plane database config: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open claim-plane database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping claim-plane database: %w", err)
	}
	return &Store{pool: pool, floorUsed: map[floorKey]int{}, logger: slog.New(slog.DiscardHandler)}, nil
}

// Configure applies claim policy. It must precede serving traffic.
func (s *Store) Configure(cfg Config) error {
	if err := cfg.Pacing.Validate(); err != nil {
		return fmt.Errorf("claim pacing config: %w", err)
	}
	for kind, limit := range cfg.Quotas {
		if limit < 0 {
			return fmt.Errorf("quota for %q must not be negative", kind)
		}
	}
	s.pacing = cfg.Pacing
	s.quotas = cfg.Quotas
	s.floor = cfg.QuotaFloorGrantsPerHour
	if s.floor <= 0 {
		s.floor = DefaultQuotaFloorGrantsPerHour
	}
	s.logger = telemetry.Logger(cfg.Logger)
	return nil
}

// Close drains the database pool.
func (s *Store) Close() {
	s.pool.Close()
}

// Migrate applies pending fresh-install migrations in filename order. Each
// file runs in its own transaction; applied versions are recorded in
// schema_migrations so reruns converge.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL
)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		raw, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		version := strings.TrimSuffix(name, ".sql")
		var applied bool
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, version).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if applied {
			continue
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(raw)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version, applied_at) VALUES ($1, $2)`,
			version, time.Now().UTC()); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}
	return nil
}

// PingTable reports whether a claim-plane table exists. It backs install
// verification and the ready probe.
func (s *Store) PingTable(ctx context.Context, table string) error {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(
SELECT 1 FROM information_schema.tables WHERE table_name = $1)`, table).Scan(&exists)
	if err != nil {
		return fmt.Errorf("ping table %s: %w", table, err)
	}
	if !exists {
		return fmt.Errorf("table %s is missing", table)
	}
	return nil
}

// DropTableForTest drops one table. Tests only: it simulates unreadable
// quota or latch state for degraded-mode proof.
func (s *Store) DropTableForTest(ctx context.Context, table string) error {
	_, err := s.pool.Exec(ctx, `DROP TABLE IF EXISTS `+table)
	return err
}

// ForgetMigrationForTest deletes one applied version so the next Migrate
// reapplies it. Tests only: it restores a table dropped for degraded-mode
// proof.
func (s *Store) ForgetMigrationForTest(ctx context.Context, version string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, version)
	return err
}

// ExecForTest runs one administrative statement. Tests only: it installs
// and removes the throwing trigger behind latch-write failure proof.
func (s *Store) ExecForTest(ctx context.Context, sql string) error {
	_, err := s.pool.Exec(ctx, sql)
	return err
}

// TruncateForTest empties claim-plane tables. Tests only. Tables dropped
// by degraded-mode proof are skipped: their cleanup restores them.
func (s *Store) TruncateForTest(ctx context.Context) error {
	for _, table := range []string{
		"workflow_run_progress", "workflow_run_completeness",
		"collector_instances", "component_quota_windows", "workflow_claim_pacing",
		"workflow_claims", "workflow_work_items", "workflow_runs",
	} {
		var regclass *string
		if err := s.pool.QueryRow(ctx, `SELECT to_regclass($1)`, "public."+table).Scan(&regclass); err != nil {
			return err
		}
		if regclass == nil {
			continue
		}
		if _, err := s.pool.Exec(ctx, `DELETE FROM `+table); err != nil {
			return err
		}
	}
	return nil
}

// queryRowRow abstracts pgx.Row for rowToClaimItem scanning.
func scanClaimedItem(row pgx.Row) (workflowv1.ClaimedWorkItem, error) {
	var item workflowv1.WorkItem
	var claim workflowv1.Claim
	var leaseExpires *time.Time
	err := row.Scan(
		&item.WorkItemID, &item.RunID, &item.CollectorKind, &item.CollectorInstanceID,
		&item.SourceSystem, &item.ScopeID, &item.TenantID, &item.WorkspaceID,
		&item.SubjectClass, &item.PolicyRevisionHash, &item.AcceptanceUnitID,
		&item.SourceRunID, &item.GenerationID, &item.FairnessKey,
		&item.ClaimPriority, &item.ClaimEcosystem, &item.Status,
		&item.AttemptCount, &item.CurrentClaimID, &item.CurrentFencingToken,
		&item.CurrentOwnerID, &leaseExpires, &item.CreatedAt, &item.UpdatedAt,
		&claim.ClaimID, &claim.FencingToken, &claim.OwnerID, &claim.Status,
		&claim.ClaimedAt, &claim.HeartbeatAt, &claim.LeaseExpiresAt,
		&claim.CreatedAt, &claim.UpdatedAt,
	)
	if err != nil {
		return workflowv1.ClaimedWorkItem{}, err
	}
	claim.WorkItemID = item.WorkItemID
	if leaseExpires != nil {
		item.LeaseExpiresAt = *leaseExpires
	}
	return workflowv1.ClaimedWorkItem{WorkItem: item, Claim: claim}, nil
}
