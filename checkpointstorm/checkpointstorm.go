// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package checkpointstorm defines implementation of a workload which deliberately drives
// a CHECKPOINT storm on a PostgreSQL instance — an I/O sibling of the slow-line family,
// distinct from wal-flood (ADR-003-2).
//
// It seeds one large table (id bigint PRIMARY KEY, payload bytea), verifies at startup
// that the connecting role may issue CHECKPOINT (superuser OR pg_checkpoint membership),
// then fans out --jobs scattered random-id UPDATE workers while a single serial forcer
// issues synchronous CHECKPOINTs whenever the accumulated dirty-row count crosses a
// threshold. The forced flushes hammer the storage layer rather than burning CPU.
//
// The workload self-reports honestly: it keeps an in-process counter of the bytes it has
// dirtied (UPDATEs × payload) and never polls live server state at runtime. The seed
// table is dropped on a fresh connection at exit. Workload duration is controlled by a
// context created outside and passed to Run.
//
// This file currently provides the package foundation: Config/validate, the startup
// CHECKPOINT-privilege check, the seed prepare path, the size→rows estimate, and cleanup.
// The escalation engine (Run, scattered-churn workers, the serial CHECKPOINT forcer, and
// the reporter) is added by a follow-up task.
package checkpointstorm

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
	// accumulate enough dirty pages for a forced CHECKPOINT to do meaningful I/O work —
	// the silent no-op the guard exists to prevent.
	minTableSize int64 = 64 << 20

	// minDirtyPct is the validate() floor (5%) for the dirty-trigger percentage. Below it
	// the threshold rounds toward a tiny number of rows and the forcer would fire almost
	// continuously, defeating the cadence signal (anti-self-defeat).
	minDirtyPct = 5

	// headerBytesPerRow is the rough per-row overhead (≈ 24-byte tuple header + 8-byte id
	// bigint) used by the payload-aware size→rows estimate. The real bytes-per-row is
	// headerBytesPerRow + PayloadBytes. It only needs to land the seed in the right
	// ballpark of the target size; the panel reports the application bytes it dirtied, so
	// this estimate never affects the self-report.
	headerBytesPerRow int64 = 32
)

// checkpointPrivilegeQuery is the startup CHECKPOINT-privilege precondition (Decision 3).
// It is a STATIC string literal: current_user is the SQL function (not a Go value),
// 'pg_checkpoint' is a literal, and no role name or version is ever assembled via
// fmt.Sprintf. It is PG<15-safe — to_regrole('pg_checkpoint') IS NOT NULL short-circuits
// pg_has_role so the latter is never evaluated when the role is absent (to_regrole
// returns NULL instead of erroring on a missing role). A true result means the role may
// issue CHECKPOINT (superuser OR pg_checkpoint member); false/empty means it may not.
const checkpointPrivilegeQuery = "SELECT rolsuper OR (to_regrole('pg_checkpoint') IS NOT NULL AND pg_has_role(current_user, 'pg_checkpoint', 'MEMBER')) FROM pg_roles WHERE rolname = current_user"

// Config defines configuration settings for checkpoint-storm workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	// It is a secret and must never be logged.
	Conninfo string
	// TableSize defines the target seed-table size in bytes (from --table-size,
	// base-2 parsed). It is converted to a row count via a payload-aware estimate.
	TableSize int64
	// DirtyPct defines the percentage of rows that must be dirtied before the forcer
	// issues a synchronous CHECKPOINT (>= 5; floored anti-self-defeat).
	DirtyPct int
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

	// Floor guard (anti-self-defeat): below 64 MiB the heap is too small for a forced
	// CHECKPOINT to do meaningful I/O. Recommend raising --table-size.
	if c.TableSize < minTableSize {
		return fmt.Errorf("table size (%d) must be at least %d bytes: raise --table-size", c.TableSize, minTableSize)
	}

	// Floor guard (anti-self-defeat): below 5% the trigger fires almost continuously.
	if c.DirtyPct < minDirtyPct {
		return fmt.Errorf("dirty pct (%d) must be at least %d: raise --dirty-pct", c.DirtyPct, minDirtyPct)
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
// seed time; the follow-up engine reads them when fanning out workers and the forcer.
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

// Run is the workload entry point required by noisia.Workload. The escalation engine
// (seed, privilege gate, scattered-churn workers, the serial CHECKPOINT forcer, and the
// reporter) is implemented by a follow-up task; this foundation provides only the
// building blocks it composes.
func (w *workload) Run(ctx context.Context) error {
	return fmt.Errorf("checkpoint-storm: workload engine not implemented yet")
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

// checkCheckpointPrivilege is the startup functional gate (Decision 3, ADR-005-1): it
// reads the static, PG<15-safe checkpointPrivilegeQuery via Query + Next/Scan (db.Conn
// has no QueryRow) and returns a clear error when the connecting role may not issue
// CHECKPOINT. A false result AND an empty result (no row at all) are both treated as
// "no privilege". Every query error passes through sanitize so a pgx error can never
// leak a DSN fragment.
func checkCheckpointPrivilege(ctx context.Context, conn db.Conn) error {
	rows, err := conn.Query(ctx, checkpointPrivilegeQuery)
	if err != nil {
		return fmt.Errorf("checkpoint privilege check failed: %s", sanitize(err))
	}
	defer rows.Close()

	granted := false
	for rows.Next() {
		if err := rows.Scan(&granted); err != nil {
			return fmt.Errorf("checkpoint privilege check failed: %s", sanitize(err))
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("checkpoint privilege check failed: %s", sanitize(err))
	}

	if !granted {
		return fmt.Errorf("connecting role may not issue CHECKPOINT: it must be a superuser or a member of pg_checkpoint")
	}

	return nil
}

// prepare creates the seed table and inserts N rows of K-byte payload in a single
// transaction. The heap is (id bigint PRIMARY KEY, payload bytea): the churn loop UPDATEs
// these rows in place, dirtying pages that the forcer flushes via CHECKPOINT. The
// identifier is interpolated (it cannot be a bind param); the payload buffer is bound as
// $1 and the row count as $2.
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

	// The payload content is intentionally a fixed zero-filled buffer: dirty pages are
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
		w.logger.Warnf("checkpoint-storm: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}
	defer func() { _ = conn.Close() }()

	// Attribute the cleanup DROP in pg_stat_activity. Best-effort: the drop and its
	// honest logging are what matter, so a failed SET must not abort cleanup.
	_, _, _ = conn.Exec(ctx, "SET application_name = 'noisia'")

	if err := dropTable(ctx, conn, tableIdent); err != nil {
		w.logger.Warnf("checkpoint-storm: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}

	w.logger.Infof("checkpoint-storm: table dropped")
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
	// GB range (Decision 7), so values >= 1 TB render as a large GB number, not TB.
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 2; v /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMG"[exp])
}
