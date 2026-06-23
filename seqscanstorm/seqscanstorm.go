// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package seqscanstorm defines implementation of a workload which deliberately drives
// a sustained, CPU-bound sequential-scan storm on a PostgreSQL instance.
//
// It seeds one large table whose filter column (payload) carries NO index, then fans
// out --jobs per-worker connections that loop a forced full-table Seq Scan
// (count(*) ... WHERE payload = <empty-predicate>). Because payload is never indexed
// and the predicate matches nothing, the planner is forced into a Seq Scan over the
// whole heap on every query, burning CPU on brute-force scan+filter rather than I/O
// (once the table is warm in page cache).
//
// The workload self-reports honestly: it keeps an in-process counter of logical bytes
// scanned (completed queries × the real, once-read table size) and never polls live
// server state at runtime. The seed table is dropped on a fresh connection at exit.
// Workload duration is controlled by a context created outside and passed to Run.
//
// This file currently provides the package foundation: Config/validate, the seed
// prepare path, the one-time relation-size read, and cleanup. The escalation engine
// (Run, warm-up, workers, reporter) is added by a follow-up task.
package seqscanstorm

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
	// bytesPerRowEstimate is the rough on-disk size of one seeded heap tuple (24-byte
	// header + two 8-byte bigint columns + item pointer, page overhead folded in). It
	// only needs to land the seed in the right ballpark of the target size: the panel
	// reports the REAL pg_relation_size, so this estimate never affects self-report.
	bytesPerRowEstimate int64 = 48

	// minTableSize is the validate() floor (64 MiB). Below it a modern server scans the
	// table in a few milliseconds, parse/plan overhead dominates, and no sustained CPU
	// storm forms — the silent no-op the guard exists to prevent.
	minTableSize int64 = 64 << 20

	// emptyPredicateValue is the out-of-range payload filter constant used by the worker
	// query (WHERE payload = emptyPredicateValue). payload is seeded as 1..N, so 0 never
	// matches: every query deterministically scans the whole table and returns one row.
	emptyPredicateValue = 0
)

// Config defines configuration settings for seqscan-storm workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	// It is a secret and must never be logged.
	Conninfo string
	// TableSize defines the target seed-table size in bytes (from --table-size,
	// base-2 parsed). It is converted to a row count via bytesPerRowEstimate.
	TableSize int64
	// Jobs defines how many concurrent scan workers to run, taken from the global
	// --jobs flag (>= 1). One worker already saturates one core.
	Jobs uint16
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Conninfo == "" {
		return fmt.Errorf("conninfo must not be empty")
	}

	// Floor guard (anti-self-defeat): below 64 MiB the scan is too cheap to form a
	// sustained CPU storm. Recommend raising --table-size.
	if c.TableSize < minTableSize {
		return fmt.Errorf("table size (%d) must be at least %d bytes: raise --table-size", c.TableSize, minTableSize)
	}

	// One worker already saturates one core; there is no jobs >= 2 invariant.
	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be greater than zero")
	}

	return nil
}

// workload implements noisia.Workload interface. realTableSize is populated once at
// seed time from pg_relation_size (or falls back to the configured TableSize); the
// follow-up engine reads it for the logical-bytes panel.
type workload struct {
	config        Config
	logger        log.Logger
	tableIdent    string // double-quoted identifier used in CREATE/DROP/work SQL
	tableName     string // unquoted relname, bound into pg_relation_size/pg_stat lookups
	realTableSize int64
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config Config, logger log.Logger) (noisia.Workload, error) {
	err := config.validate()
	if err != nil {
		return nil, err
	}

	return &workload{config: config, logger: logger}, nil
}

// Run is implemented by the follow-up escalation-engine task (warm-up, per-worker
// connections, scan loop, reporter). The foundation task ships only the lifecycle
// scaffolding (Config/validate, prepare, readRelationSize, cleanup) this builds on.
func (w *workload) Run(ctx context.Context) error {
	// TODO(task-02): seed connection, warm-up, worker fan-out, reporter.
	return fmt.Errorf("seqscan-storm: Run not implemented yet")
}

// rowsForSize converts a target byte size into a seed row count via the fixed
// bytes-per-row estimate, with a floor of one row so a tiny (or nonsensical) input
// never yields a zero-row, never-scanning table.
func rowsForSize(tableSize int64) int64 {
	n := tableSize / bytesPerRowEstimate
	if n < 1 {
		return 1
	}
	return n
}

// prepare creates the seed table and inserts N rows in a single transaction. The
// table is (id bigint PRIMARY KEY, payload bigint) with NO index on payload: the
// un-indexed filter column is what forces a Seq Scan on the worker query. payload is
// seeded as the generate_series value g (payload = id), so a WHERE payload = 0 filter
// matches nothing. The identifier is interpolated (it cannot be a bind param); the row
// count is bound as $1.
func prepare(ctx context.Context, conn db.Conn, tableIdent string, rows int64) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, _, err = tx.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, payload bigint)", tableIdent))
	if err != nil {
		return err
	}

	// Seed in a single set-based statement: one server-side round-trip instead of N
	// network round-trips, which matters for large N over a remote link.
	insertSQL := fmt.Sprintf("INSERT INTO %s (id, payload) SELECT g, g FROM generate_series(1, $1) AS g", tableIdent)
	_, _, err = tx.Exec(ctx, insertSQL, rows)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// readRelationSize reads the real on-disk size of the seed table once, modeled on
// backendkiller.readBackendMemory (Query + rows.Next/Scan loop; db.Conn has no
// QueryRow). The unquoted relname is passed as a bind arg.
//
// pg_relation_size returns the MAIN fork only (heap, excluding the PK B-tree). This is
// intentional and honest: the worker's Seq Scan traverses the heap, not the PK index,
// so heap size is the correct per-scan logical size.
func readRelationSize(ctx context.Context, conn db.Conn, relname string) (int64, error) {
	rows, err := conn.Query(ctx, "SELECT pg_relation_size($1)", relname)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var sz int64
	for rows.Next() {
		if err := rows.Scan(&sz); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	return sz, nil
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
// mid-scan can poison a worker conn, so a DROP over it would fail and orphan the table
// (ADR-002-2). Any failure is logged via Warnf naming the table so nothing is silently
// orphaned. Every error passes through sanitize.
func (w *workload) cleanup(tableIdent string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Open a fresh connection: a worker conn may be dead after a mid-scan cancel.
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		w.logger.Warnf("seqscan-storm: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}
	defer func() { _ = conn.Close() }()

	// Attribute the cleanup DROP in pg_stat_activity. Best-effort: the drop and its
	// honest logging are what matter, so a failed SET must not abort cleanup.
	_, _, _ = conn.Exec(ctx, "SET application_name = 'noisia'")

	if err := dropTable(ctx, conn, tableIdent); err != nil {
		w.logger.Warnf("seqscan-storm: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}

	w.logger.Infof("seqscan-storm: table dropped")
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
// largest fitting unit with one fractional digit, e.g. "4.2GB".
//
// Unlike the sibling workloads' GB-capped copy, the ladder is extended to TB and PB:
// this workload's logical scanned counter routinely exceeds 1 TB (a multi-hundred-MiB
// table × thousands of scans), which a GB-capped helper would render as "1000.0GB".
func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}

	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 4; v /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGTP"[exp])
}
