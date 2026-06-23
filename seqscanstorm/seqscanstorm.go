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
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
)

// reportInterval is the escalation-panel print cadence. Unlike the sibling workloads,
// seqscan-storm's Config exposes no ReportInterval field (by design — the panel cadence
// is not a teaching knob), so the interval is a package-level value rather than a Config
// knob. It is a var (not a const) solely so tests can shorten it; production never
// reassigns it.
var reportInterval = time.Second

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

// Run opens a dedicated seed connection to build the un-indexed table, reads its real
// on-disk size once, warms its cache with a single full pass (best-effort), then fans
// out up to Jobs per-worker connections that loop a forced full-table Seq Scan while a
// ticker reports logical bytes scanned. Each worker owns its own connection (no shared
// pool, so the pool default max_conns cannot cap parallelism below --jobs, ADR-003-1).
// The seed table is dropped in a defer on a fresh context.Background() (ctx is already
// cancelled at exit). The only stop mechanism is the supplied ctx.
func (w *workload) Run(ctx context.Context) error {
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		// Never echo the raw error: it may carry DSN fragments (e.g. password=…).
		return fmt.Errorf("connect to target failed: %s", sanitize(err))
	}
	defer func() { _ = conn.Close() }()

	// Attribute the seeding backend in pg_stat_activity. db.Connect (unlike the pool
	// path) does not set application_name, so set it locally here.
	_, _, err = conn.Exec(ctx, "SET application_name = 'noisia'")
	if err != nil {
		return fmt.Errorf("set application_name failed: %s", sanitize(err))
	}

	// Build the per-run table name once. The suffix is restricted to [a-z0-9], which
	// alone makes the identifier injection-safe; double-quoting is belt-and-suspenders.
	// The identifier (which cannot be a bind parameter) is reused verbatim in
	// CREATE/DROP/work SQL; the row count and relname are bind args.
	w.tableIdent = "\"noisia_seqscan_" + randomSuffix(8) + "\""
	w.tableName = strings.Trim(w.tableIdent, "\"")

	// Cleanup is registered right after a successful connect so the table is dropped even
	// if seeding fails. It runs on a fresh context.Background() because ctx is already
	// cancelled at exit and a drop on a cancelled ctx would fail (ADR-002-2).
	defer w.cleanup(w.tableIdent)

	// Seed the large un-indexed table. Seeding a multi-hundred-MiB table takes a while,
	// and the reporter starts only after warm-up, so emit explicit feedback.
	rows := rowsForSize(w.config.TableSize)
	w.logger.Infof("seqscan-storm: seeding %d rows (~%s), this may take a while...", rows, formatBytes(w.config.TableSize))
	err = prepare(ctx, conn, w.tableIdent, rows)
	if err != nil {
		// A clean ctx stop must not be reported as a failure.
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("seed table failed: %s", sanitize(err))
	}

	// Read the real on-disk heap size once (the panel's per-scan logical size). On
	// failure, warn (sanitized) and fall back to the configured TableSize as an
	// approximate size — the panel still reports a sensible scanned figure.
	w.realTableSize, err = readRelationSize(ctx, conn, w.tableName)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		w.logger.Warnf("seqscan-storm: reading table size failed, using configured --table-size (approx): %s", sanitize(err))
		w.realTableSize = w.config.TableSize
	}

	// Warm-up pre-phase (best-effort) on the seed connection: a clean ctx stop here is a
	// clean exit, not an error; a real query error warns and proceeds to escalation.
	w.warmup(ctx, conn)
	if ctx.Err() != nil {
		return nil
	}

	w.logger.Infof("seqscan-storm: starting %d scan workers — watch server CPU (pgcenter/top)", w.config.Jobs)

	// queries holds the total completed full scans across all workers; sessions holds the
	// live worker count. Both are shared and read by the reporter, which must never touch
	// a (not concurrency-safe) Conn (ADR-002-3).
	var queries atomic.Int64
	var sessions atomic.Int64

	// start is taken AFTER warm-up returns (uptime counts from escalation start).
	start := time.Now()
	tickerDone := make(chan struct{})
	var reporterWg sync.WaitGroup
	reporterWg.Add(1)
	go func() {
		defer reporterWg.Done()
		w.runReporter(ctx, tickerDone, start, &queries, &sessions)
	}()

	// Fan out exactly Jobs workers, each on its own connection. The worker query is a
	// forced full Seq Scan: payload is seeded as 1..N and never indexed, so the empty
	// predicate (payload = $1, bound to emptyPredicateValue=0) matches nothing and the
	// planner must scan the whole heap. The table identifier (which cannot be a bind
	// parameter) is interpolated; the predicate value is bound as $1.
	scanSQL := fmt.Sprintf("SELECT count(*) FROM %s WHERE payload = $1", w.tableIdent)

	var wg sync.WaitGroup
	errs := make([]error, w.config.Jobs)
	wg.Add(int(w.config.Jobs))
	for i := 0; i < int(w.config.Jobs); i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = w.runWorker(ctx, scanSQL, &queries, &sessions)
		}(i)
	}
	wg.Wait()

	close(tickerDone)
	reporterWg.Wait()

	// Surface the first init error (a real setup defect — e.g. exhausted connections)
	// reported by any worker; a degraded run (>=1 worker alive) or a clean stop leaves
	// all errs nil — Run errors only when ZERO workers were ever live.
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

// warmup runs one full-table scan over the seed connection to load the heap into the
// page cache, so the first demo seconds show CPU-bound scanning rather than iowait
// (Decision 6). It is best-effort: a clean ctx stop exits silently (the caller checks
// ctx.Err()), and a real query error is warned (sanitized) and swallowed so escalation
// proceeds anyway — the escalation scans warm the cache themselves.
func (w *workload) warmup(ctx context.Context, conn db.Conn) {
	w.logger.Infof("seqscan-storm: warming up cache: full scan of %s (%s)", w.tableIdent, formatBytes(w.realTableSize))

	warmSQL := fmt.Sprintf("SELECT count(*) FROM %s", w.tableIdent)
	rows, err := conn.Query(ctx, warmSQL)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Warnf("seqscan-storm: warm-up scan failed (continuing to escalation): %s", sanitize(err))
		return
	}
	defer rows.Close()

	for rows.Next() {
		// The count value is irrelevant; the point is to touch every heap page.
	}
	if rerr := rows.Err(); rerr != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Warnf("seqscan-storm: warm-up scan failed (continuing to escalation): %s", sanitize(rerr))
	}
}

// runWorker opens its OWN dedicated connection and drives the scan loop. A per-worker
// connection guarantees Jobs concurrent in-flight scans regardless of any pool sizing
// (ADR-003-1). Right after connect it issues SET application_name then the determinism
// SET max_parallel_workers_per_gather = 0; if EITHER the connect or the determinism SET
// fails, the worker is SKIPPED (Decision 5) — a worker without the determinism SET could
// let parallel query engage, breaking the "1 worker = 1 scan = 1 core" model and the
// self-report. A skip degrades the run (Warn + nil) as long as another worker is live;
// only when no worker was ever live (sessions == 0) is the sanitized init error returned.
// Every connection/SET error passes through sanitize so a pgx error can never leak a DSN.
func (w *workload) runWorker(ctx context.Context, scanSQL string, queries, sessions *atomic.Int64) error {
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		return skipWorkerLog(ctx, w.logger, "worker connect failed", err, sessions)
	}
	defer func() { _ = conn.Close() }()

	return runScanWorkerWithConn(ctx, conn, w.logger, scanSQL, queries, sessions)
}

// runScanWorkerWithConn drives the post-connect worker lifecycle over an already-open
// conn: best-effort SET application_name, the mandatory determinism SET (Decision 5,
// skip-on-failure), the live-session increment, and the scan loop. It is split from
// runWorker so the SET/skip/scan logic can be unit-tested over a conn double without a
// live database (runWorker itself only adds the db.Connect that needs a real server).
func runScanWorkerWithConn(ctx context.Context, conn db.Conn, logger log.Logger, scanSQL string, queries, sessions *atomic.Int64) error {
	// db.Connect does not set application_name; do it best-effort so the worker is
	// attributable in pg_stat_activity. A failed SET must not abort the worker (it is
	// cosmetic), but its error still passes through sanitize so the DSN is never logged.
	if _, _, serr := conn.Exec(ctx, "SET application_name = 'noisia'"); serr != nil {
		logger.Warnf("seqscan-storm: set application_name failed: %s", sanitize(serr))
	}

	// Determinism SET (Decision 5): functional, not cosmetic. If it fails, skip the
	// worker entirely — do NOT run a non-deterministic worker.
	if _, _, serr := conn.Exec(ctx, "SET max_parallel_workers_per_gather = 0"); serr != nil {
		return skipWorkerLog(ctx, logger, "worker set max_parallel_workers_per_gather failed", serr, sessions)
	}

	// Count this session as live exactly once, after both SETs and before the scan loop.
	sessions.Add(1)

	return runScan(ctx, conn, logger, scanSQL, queries, sessions)
}

// skipWorkerLog is the common skip path for a worker that cannot start (failed connect
// or failed determinism SET, Decision 5). A clean ctx stop returns nil; if at least one
// other session is live it degrades (Warn + nil); only when no session ever started
// (sessions == 0) is the sanitized init error returned, so a broken setup cannot
// masquerade as a successful run while still allowing a degraded run to continue.
func skipWorkerLog(ctx context.Context, logger log.Logger, what string, err error, sessions *atomic.Int64) error {
	if ctx.Err() != nil {
		return nil
	}
	if sessions.Load() > 0 {
		logger.Warnf("seqscan-storm: %s (degraded): %s", what, sanitize(err))
		return nil
	}
	return fmt.Errorf("%s: %s", what, sanitize(err))
}

// runScan is one worker's forced-Seq-Scan loop. It owns its Conn and loops the count(*)
// query (db.Conn has no QueryRow, so Query + Next/Scan + Close), incrementing the SHARED
// queries counter for every scan that completes successfully. The count(*) result (= 0)
// is irrelevant; only successful completion counts.
//
// Returns nil on a clean ctx stop. On a query error under a live ctx after at least one
// successful scan anywhere (queries > 0), it logs a degradation warning, decrements
// sessions EXACTLY once, and returns nil — the session was lost, not the run. The first
// query error while queries is still 0 is returned as a sanitized init error, so a broken
// setup cannot masquerade as a working scan loop.
//
// sessions is decremented ONLY here and ONLY on the degraded death path (query error,
// queries>0, ctx still live): never on a clean ctx-cancel exit and never on conn.Close
// (Close is the caller's defer), so the live-session count never goes negative and a
// ctx-cancelled worker (shutting down normally) is not mistaken for a degraded death.
func runScan(ctx context.Context, conn db.Conn, logger log.Logger, scanSQL string, queries, sessions *atomic.Int64) error {
	for {
		err := scanOnce(ctx, conn, scanSQL)
		if err != nil {
			// A clean ctx stop must not be reported as a failure (and must not decrement).
			if ctx.Err() != nil {
				return nil
			}

			if queries.Load() > 0 {
				// Mid-run loss of this session: degradation, not death of the run.
				// Decrement the live session count exactly once and let the other
				// workers carry on.
				logger.Warnf("seqscan-storm: worker lost connection: %s", sanitize(err))
				sessions.Add(-1)
				return nil
			}

			// First scan failed with no prior success anywhere: setup defect.
			return fmt.Errorf("scan failed: %s", sanitize(err))
		}

		queries.Add(1)

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

// scanOnce runs one count(*) scan to completion: Query, drain the single result row,
// check rows.Err(), and Close — a scan is "successful" only after a clean Scan and a
// nil rows.Err(), so a half-read result never counts as a completed scan. The empty
// predicate is passed as bind arg $1, not interpolated into the SQL text.
func scanOnce(ctx context.Context, conn db.Conn, scanSQL string) error {
	rows, err := conn.Query(ctx, scanSQL, emptyPredicateValue)
	if err != nil {
		return err
	}
	defer rows.Close()

	var n int64
	for rows.Next() {
		if err := rows.Scan(&n); err != nil {
			return err
		}
	}
	return rows.Err()
}

// runReporter prints the escalation panel every reportInterval, reading only the shared
// atomic queries and sessions counters. It never queries a Conn (ADR-002-3). scanned is
// the logical bytes scanned = completed queries × the real (once-read) table size; rate
// is the per-interval delta of scanned, both rendered via formatBytes.
func (w *workload) runReporter(ctx context.Context, done <-chan struct{}, start time.Time, queries, sessions *atomic.Int64) {
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()

	var prev int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			q := queries.Load()
			cur := q * w.realTableSize
			uptime := time.Since(start)
			rate := int64(float64(cur-prev) / reportInterval.Seconds())
			prev = cur

			w.logger.Infof("seqscan-storm: scanned=%s rate=%s/s queries=%d sessions=%d uptime=%s",
				formatBytes(cur), formatBytes(rate), q, sessions.Load(), uptime.Truncate(time.Second))
		}
	}
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
