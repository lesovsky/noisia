// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package xminhorizonholder defines implementation of a workload which deliberately
// pins the cluster-wide xmin horizon by holding a long-lived REPEATABLE READ snapshot
// while a churn loop generates dead tuples that VACUUM cannot reclaim — a bloat sibling
// of the slow-line family.
//
// This file is the dependency-free foundation: the Config surface, its validation, the
// NewWorkload constructor, and the copied infrastructure helpers (sanitize, randomSuffix,
// formatBytes, rowsForSize, prepare, dropTable, cleanup). Unlike its checkpoint-storm
// sibling there is no forcer threshold, hence no dirty-percentage config field nor its
// floor, and no startup privilege gate. The runtime organ — the long-lived holder, the churn workers,
// the reporter, and Run — is added on top of this scaffold (it does not live in this
// file).
//
// The seed table is dropped on a fresh connection at exit. Workload duration is
// controlled by a context created outside and passed to Run.
package xminhorizonholder

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
)

const (
	// minTableSize is the validate() floor (64 MiB). Below it the heap is too small to
	// accumulate enough dead tuples for the held xmin horizon to produce meaningful,
	// observable bloat — the silent no-op the guard exists to prevent.
	minTableSize int64 = 64 << 20

	// headerBytesPerRow is the rough per-row overhead (≈ 24-byte tuple header + 8-byte id
	// bigint) used by the payload-aware size→rows estimate. The real bytes-per-row is
	// headerBytesPerRow + PayloadBytes. It only needs to land the seed in the right
	// ballpark of the target size; the panel reports the application bytes it dirtied, so
	// this estimate never affects the self-report.
	headerBytesPerRow int64 = 32
)

// Config defines configuration settings for xmin-horizon-holder workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	// It is a secret and must never be logged.
	Conninfo string
	// TableSize defines the target seed-table size in bytes (from --table-size,
	// base-2 parsed). It is converted to a row count via a payload-aware estimate.
	TableSize int64
	// PayloadBytes defines the payload size in bytes written per UPDATE (>= 1).
	PayloadBytes int
	// Rate defines UPDATE statements rate per second PER worker; 0 means unlimited.
	Rate float64
	// ReportInterval defines the escalation panel print cadence.
	ReportInterval time.Duration
	// Jobs defines how many concurrent churn workers to run, taken from the global
	// --jobs flag (>= 1).
	Jobs uint16
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Conninfo == "" {
		return fmt.Errorf("conninfo must not be empty")
	}

	// Floor guard (anti-self-defeat): below 64 MiB the heap is too small for the held xmin
	// horizon to produce observable bloat. Recommend raising --table-size.
	if c.TableSize < minTableSize {
		return fmt.Errorf("table size (%d) must be at least %d bytes: raise --table-size", c.TableSize, minTableSize)
	}

	if c.PayloadBytes < 1 {
		return fmt.Errorf("payload bytes must be greater than zero")
	}

	if c.Rate < 0 {
		return fmt.Errorf("rate must not be negative")
	}

	if c.ReportInterval <= 0 {
		return fmt.Errorf("report interval must be positive")
	}

	// One worker already drives churn; there is no jobs >= 2 invariant.
	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be greater than zero")
	}

	return nil
}

// workload implements noisia.Workload interface. tableIdent and rows are populated at
// seed time; the follow-up engine reads them when fanning out workers and the holder.
type workload struct {
	config     Config
	logger     log.Logger
	tableIdent string // double-quoted identifier used in CREATE/DROP/work SQL
	rows       int64  // seed row count derived from TableSize via rowsForSize
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config Config, logger log.Logger) (noisia.Workload, error) {
	err := config.validate()
	if err != nil {
		return nil, err
	}

	return &workload{config: config, logger: logger}, nil
}

// Run is a placeholder so *workload satisfies the noisia.Workload interface that
// NewWorkload returns from this foundation task. The real engine — the long-lived
// REPEATABLE READ holder, the churn workers, the reporter, and the orchestration spine —
// is added in Task 2, which replaces this stub. It is intentionally NOT the engine: it
// fails loudly rather than silently no-op'ing if ever invoked before Task 2 lands.
func (w *workload) Run(ctx context.Context) error {
	return fmt.Errorf("xmin-horizon-holder: run engine not yet implemented")
}

// rowsForSize converts a target byte size into a seed row count via a payload-aware
// bytes-per-row estimate (headerBytesPerRow + payloadBytes), with a floor of one row so
// a tiny (or nonsensical) input never yields a zero-row table.
func rowsForSize(tableSize, payloadBytes int64) int64 {
	bytesPerRow := headerBytesPerRow + payloadBytes
	n := tableSize / bytesPerRow
	if n < 1 {
		return 1
	}
	return n
}

// prepare creates the seed table and inserts N rows of K-byte payload in a single
// transaction. The heap is (id bigint PRIMARY KEY, payload bytea): the churn loop UPDATEs
// these rows in place, producing dead tuples that VACUUM cannot reclaim while the holder
// pins the xmin horizon. The identifier is interpolated (it cannot be a bind param); the
// payload buffer is bound as $1 and the row count as $2.
func prepare(ctx context.Context, conn db.Conn, tableIdent string, rows int64, payloadBytes int) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, _, err = tx.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, payload bytea)", tableIdent))
	if err != nil {
		return err
	}

	// The payload content is intentionally a fixed zero-filled buffer: dead tuples are
	// produced by the UPDATE regardless of byte equality, so the value is irrelevant.
	payload := make([]byte, payloadBytes)
	// Seed in a single set-based statement: one server-side round-trip instead of N
	// network round-trips, which matters for large N over a remote link.
	insertSQL := fmt.Sprintf("INSERT INTO %s (id, payload) SELECT g, $1 FROM generate_series(1, $2) AS g", tableIdent)
	_, _, err = tx.Exec(ctx, insertSQL, payload, rows)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// dropTable drops the seed table over the provided connection. It is split out from
// cleanup so the DROP itself can be exercised over an independently provided conn
// (self-sufficiency), and so cleanup owns only the fresh-connection lifecycle.
func dropTable(ctx context.Context, conn db.Conn, tableIdent string) error {
	_, _, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableIdent))
	return err
}

// cleanup drops the seed table on a fresh context.Background() (the run ctx is already
// cancelled at exit), bounded by a short timeout so a hung target cannot block forever.
// It opens its OWN fresh connection rather than reusing a worker conn: a Ctrl+C landing
// mid-UPDATE can poison a worker conn, so a DROP over it would fail and orphan the table
// (ADR-002-2). Any failure is logged via Warnf naming the table so nothing is silently
// orphaned. Every error passes through sanitize.
func (w *workload) cleanup(tableIdent string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Open a fresh connection: a worker conn may be dead after a mid-UPDATE cancel.
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		w.logger.Warnf("xmin-horizon-holder: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}
	defer func() { _ = conn.Close() }()

	// Attribute the cleanup DROP in pg_stat_activity. Best-effort: the drop and its
	// honest logging are what matter, so a failed SET must not abort cleanup.
	_, _, _ = conn.Exec(ctx, "SET application_name = 'noisia'")

	if err := dropTable(ctx, conn, tableIdent); err != nil {
		w.logger.Warnf("xmin-horizon-holder: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}

	w.logger.Infof("xmin-horizon-holder: table dropped")
}

// randomSuffix returns a random string of length n drawn from [a-z0-9]. The charset
// guarantees an injection-safe SQL identifier, and a per-run suffix keeps reruns from
// colliding on the seed table name.
func randomSuffix(n int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// sanitize returns an error message stripped of any conninfo fragment that could leak
// the DSN (e.g. a password). It keeps only the leading part of a pgx error up to the
// first occurrence of a conninfo-like token.
func sanitize(err error) string {
	if err == nil {
		return ""
	}

	msg := err.Error()
	for _, tok := range []string{"password=", "host=", "user=", "dbname=", "database=", "sslmode=", "://"} {
		if idx := strings.Index(msg, tok); idx >= 0 {
			return "connection error (details suppressed)"
		}
	}

	return msg
}

// formatBytes renders a byte count as a human-readable string using binary units
// (base 1024): bytes below 1024 stay as "<n>B"; larger values are scaled to the
// largest fitting unit (KB/MB/GB) with one fractional digit, e.g. "4.2GB".
func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}

	// The unit ladder caps at GB on purpose: the workload's dirtied counter stays in the
	// GB range, so values >= 1 TB render as a large GB number, not TB.
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 2; v /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMG"[exp])
}
