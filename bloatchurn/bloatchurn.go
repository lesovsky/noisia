// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bloatchurn defines implementation of a workload which deliberately generates
// remediable table-and-index bloat via a rate attack: it floods a seed table with UPDATEs
// faster than autovacuum (left enabled) can reclaim the dead tuples. It is the sanctioned
// "rate attack" twin of xmin-horizon-holder (the "horizon attack", ADR-007-4) — built by
// copying that package's shape, removing every holder organ, and adding the bloat-shaping
// seed/churn (ADR-007-4).
//
// The bloat mechanism is one move with a double effect (Decision 2): the seed table carries
// an updated_at timestamptz column with the SINGLE index on it, and every churn UPDATE sets
// updated_at = now(). Updating an indexed column forbids HOT (the heap bloats) and the
// monotonic now() bloats the btree on its right edge (a REINDEX CONCURRENTLY lesson). Churn
// touches only the lower half of the table (hotFraction = 0.5, Decision 3) — never the tail —
// so the live tail heap pages keep VACUUM from truncating the file and the bloat survives for
// the post-stop repair demo.
//
// It hosts the Config surface, its validation, the NewWorkload constructor, the copied
// infrastructure helpers (sanitize, randomSuffix, formatBytes, rowsForSize, prepare,
// dropTable, cleanup), and the runtime engine: Run, the scattered-churn workers, and the
// self-report reporter.
//
// The seed table is dropped on a fresh connection at exit (unless KeepTable). Workload
// duration is controlled by a context created outside and passed to Run.
package bloatchurn

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
	"golang.org/x/time/rate"
)

const (
	// minTableSize is the validate() floor (64 MiB). Below it the heap is too small to
	// accumulate enough dead tuples for the rate attack to produce meaningful, observable
	// bloat — the silent no-op the guard exists to prevent.
	minTableSize int64 = 64 << 20

	// headerBytesPerRow is the rough per-row overhead (≈ 24-byte tuple header + 8-byte id
	// bigint + 8-byte updated_at timestamptz) used by the payload-aware size→rows estimate.
	// The real bytes-per-row is headerBytesPerRow + PayloadBytes. It only needs to land the
	// seed in the right ballpark of the target size; the panel reports the application bytes
	// it dirtied, so this estimate never affects the self-report.
	headerBytesPerRow int64 = 40

	// hotFraction is the share of the seed table the churn loop targets: only ids in the
	// lower half [1, floor(hotFraction*rows)] are ever UPDATEd (Decision 3). The untouched
	// tail keeps its live heap pages, which prevents VACUUM from truncating the file and
	// erasing the bloat — so the bloat survives for a post-stop repair demo.
	hotFraction = 0.5
)

// Config defines configuration settings for bloat-churn workload.
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
	// KeepTable keeps the seed table on graceful exit instead of dropping it.
	KeepTable bool
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Conninfo == "" {
		return fmt.Errorf("conninfo must not be empty")
	}

	// Floor guard (anti-self-defeat): below 64 MiB the heap is too small for the rate attack
	// to produce observable bloat. Recommend raising --table-size.
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
// seed time; the follow-up engine reads them when fanning out workers.
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

// Run opens a dedicated seed connection, sets application_name, seeds the
// (id bigint PK, payload bytea, updated_at timestamptz) table with the single index on
// updated_at, then fans out Jobs scattered hot-prefix UPDATE churn workers (each on its own
// db.Connect, ADR-003-1) and a ticker reporter that prints the self-report panel from atomics
// only. The seed table is dropped in a defer on a fresh context.Background() (ADR-002-2),
// unless KeepTable keeps it. The only stop mechanism is the supplied ctx.
func (w *workload) Run(ctx context.Context) error {
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		// Never echo the raw error: it may carry DSN fragments (e.g. password=…).
		return fmt.Errorf("connect to target failed: %s", sanitize(err))
	}
	defer func() { _ = conn.Close() }()

	// Attribute the seeding backend in pg_stat_activity. db.Connect (unlike the pool path)
	// does not set application_name, so set it locally here.
	_, _, err = conn.Exec(ctx, "SET application_name = 'noisia'")
	if err != nil {
		return fmt.Errorf("set application_name failed: %s", sanitize(err))
	}

	// Build the per-run table name once. The suffix is restricted to [a-z0-9], which alone
	// makes the identifier injection-safe; double-quoting is belt-and-suspenders.
	w.tableIdent = "\"noisia_bloatchurn_" + randomSuffix(8) + "\""

	// Cleanup is registered before seeding so a mid-seed cancel still drops the partial table.
	// It runs on a fresh context.Background() because ctx is already cancelled at exit and a
	// drop on a cancelled ctx would fail (ADR-002-2).
	defer w.cleanup(w.tableIdent)

	// Startup line: intent + table + target size + where to watch the effect.
	w.logger.Infof("bloat-churn: %d workers churning %s (target %s), updating an indexed column to defeat HOT — watch table+index bloat via pgstattuple, dead tuples (pg_stat_user_tables.n_dead_tup) and the table+index size",
		w.config.Jobs, w.tableIdent, formatBytes(w.config.TableSize))

	// Seed the large heap churned in place. Seeding a multi-hundred-MiB table is slow and the
	// reporter starts only after it, so emit explicit feedback.
	w.rows = rowsForSize(w.config.TableSize, int64(w.config.PayloadBytes))
	w.logger.Infof("bloat-churn: seeding %d rows (~%s), this may take a while...", w.rows, formatBytes(w.config.TableSize))
	err = prepare(ctx, conn, w.tableIdent, w.rows, w.config.PayloadBytes)
	if err != nil {
		// A clean ctx stop must not be reported as a failure.
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("seed table failed: %s", sanitize(err))
	}

	// Shared runtime atomics, read by the reporter — never a Conn (ADR-002-3). churned is the
	// successful UPDATE count (panel field); sessions is the live-worker count (degradation
	// accounting, not in the panel).
	var churned, sessions atomic.Int64

	start := time.Now()
	tickerDone := make(chan struct{})
	var reporterWg sync.WaitGroup
	reporterWg.Add(1)
	go func() {
		defer reporterWg.Done()
		runReporter(ctx, tickerDone, start, w.config.ReportInterval, w.logger, w.config.PayloadBytes, &churned)
	}()

	// Fan out exactly Jobs scattered-churn workers, each on its own connection (ADR-003-1). The
	// table identifier (which cannot be a bind parameter) is interpolated; payload is bound as
	// $1 and the random id as $2. Setting the indexed updated_at column on every UPDATE forbids
	// HOT and bloats the btree right edge (Decision 2).
	updateSQL := fmt.Sprintf("UPDATE %s SET updated_at = now(), payload = $1 WHERE id = $2", w.tableIdent)

	var wg sync.WaitGroup
	errs := make([]error, w.config.Jobs)
	wg.Add(int(w.config.Jobs))
	for i := 0; i < int(w.config.Jobs); i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = w.runWorker(ctx, updateSQL, &churned, &sessions)
		}(i)
	}
	wg.Wait()

	// Workers have all returned (ctx cancelled or every worker died); stop the reporter.
	close(tickerDone)
	reporterWg.Wait()

	// Surface the first init error (a real setup defect — e.g. exhausted connections) reported
	// by any worker; a degraded run (>=1 worker alive) or a clean stop leaves all errs nil —
	// Run errors only when ZERO workers were ever live.
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

// runWorker opens its OWN dedicated connection and drives the scattered-churn loop. A
// per-worker connection guarantees Jobs concurrent in-flight UPDATEs regardless of any pool
// sizing (ADR-003-1). A failed connect degrades the run (Warn + nil) as long as another
// worker is live; only when no worker was ever live (sessions == 0) is the sanitized init
// error returned. Every connection error passes through sanitize so a pgx error can never
// leak a DSN.
func (w *workload) runWorker(ctx context.Context, updateSQL string, churned, sessions *atomic.Int64) error {
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if sessions.Load() > 0 {
			w.logger.Warnf("bloat-churn: worker connect failed (degraded): %s", sanitize(err))
			return nil
		}
		return fmt.Errorf("worker connect failed: %s", sanitize(err))
	}
	defer func() { _ = conn.Close() }()

	// Churn only the hot prefix (Decision 3): ids in [1, hotRows], where hotRows is the lower
	// hotFraction of the table. floor(hotFraction*rows) can be 0 on a pathologically tiny table,
	// which would panic rand.Int63n(0); guard it to at least 1 (the 64 MiB floor makes this
	// purely defensive). The tail (hotRows, rows] is never touched.
	hotRows := int64(float64(w.rows) * hotFraction)
	if hotRows < 1 {
		hotRows = 1
	}

	return runWorkerWithConn(ctx, conn, w.logger, w.config, updateSQL, hotRows, churned, sessions)
}

// runWorkerWithConn drives the post-connect worker lifecycle over an already-open conn:
// a best-effort SET application_name (a failed SET is cosmetic and does NOT abort the worker),
// the live-session increment, and the rate-limited scattered-UPDATE loop. It is split from
// runWorker so the SET + churn logic can be unit-tested over a conn double without a live
// database, and it takes hotRows (not rows) so the hot-prefix bound is directly unit-testable.
//
// Each statement targets a RANDOM id = rand.Int63n(hotRows)+1 (scattered across the hot
// prefix only, Decision 3), bound as $2; payload is a fixed zero-filled buffer bound as $1.
// On each successful UPDATE it adds 1 to churned (the panel counter).
//
// Returns nil on a clean ctx stop. On an UPDATE error under a live ctx after at least one
// successful UPDATE anywhere (churned > 0), it logs a degradation warning, decrements sessions
// EXACTLY once, and returns nil — the session was lost, not the run. The first UPDATE error
// while churned is still 0 is returned as a sanitized init error.
func runWorkerWithConn(ctx context.Context, conn db.Conn, logger log.Logger, config Config, updateSQL string, hotRows int64, churned, sessions *atomic.Int64) error {
	// db.Connect does not set application_name; do it best-effort so the worker is attributable
	// in pg_stat_activity. A failed SET must not abort the worker (it is cosmetic), but its error
	// still passes through sanitize so the DSN is never logged.
	if _, _, serr := conn.Exec(ctx, "SET application_name = 'noisia'"); serr != nil {
		logger.Warnf("bloat-churn: set application_name failed: %s", sanitize(serr))
	}

	// Count this session as live exactly once, after the SET and before the churn loop.
	sessions.Add(1)

	lim := rate.Limit(config.Rate)
	if config.Rate == 0 {
		lim = rate.Inf
	}
	limiter := rate.NewLimiter(lim, 1)

	// The payload must be INCOMPRESSIBLE (random bytes, not zeros): an all-zeros bytea
	// compresses to almost nothing, so a payload at/above the ~2KB TOAST threshold would be
	// compressed away instead of adding the expected on-disk write pressure; random bytes
	// keep the dirtied bytes honest. Filled once and reused for every UPDATE — no per-statement
	// cost, and re-updating with the same buffer still produces a fresh dead tuple every time.
	payload := make([]byte, config.PayloadBytes)
	for i := range payload {
		payload[i] = byte(rand.Intn(256))
	}

	for {
		if err := limiter.Wait(ctx); err != nil {
			// Wait only errors when ctx is done: a clean ctx stop, not a failure.
			return nil
		}

		// Scattered dirtying over the hot prefix only (Decision 3). Bound as $2.
		id := rand.Int63n(hotRows) + 1
		_, _, err := conn.Exec(ctx, updateSQL, payload, id)
		if err != nil {
			// A clean ctx stop must not be reported as a failure (and must not decrement).
			if ctx.Err() != nil {
				return nil
			}

			if churned.Load() > 0 {
				// Mid-run loss of this session: degradation, not death of the run. Decrement the
				// live session count exactly once and let the other workers carry on.
				logger.Warnf("bloat-churn: worker lost connection: %s", sanitize(err))
				sessions.Add(-1)
				return nil
			}

			// First UPDATE failed with no prior success anywhere: setup defect.
			return fmt.Errorf("churn update failed: %s", sanitize(err))
		}

		churned.Add(1)

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

// runReporter prints the self-report panel every interval, reading only the shared atomics. It
// never queries a Conn (ADR-002-3). The panel is exactly
// "bloat-churn: churned=<N> dirtied=<bytes> (<rate>/min) elapsed=<Z>": dirtied is the
// application bytes dirtied (churned × PayloadBytes), rate is churned per elapsed-minute
// (guarded against a divide-by-~0 on the first sub-minute tick).
func runReporter(ctx context.Context, done <-chan struct{}, start time.Time, interval time.Duration, logger log.Logger, payloadBytes int, churned *atomic.Int64) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			elapsed := time.Since(start)
			ch := churned.Load()

			// Guard division on the first sub-minute tick: scale by elapsed minutes, but never
			// divide by ~0 (which would print +Inf).
			minutes := elapsed.Minutes()
			perMin := 0.0
			if minutes > 0 {
				perMin = float64(ch) / minutes
			}

			dirtied := formatBytes(ch * int64(payloadBytes))

			logger.Infof("bloat-churn: churned=%d dirtied=%s (%.1f/min) elapsed=%s",
				ch, dirtied, perMin, elapsed.Truncate(time.Second))
		}
	}
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

// prepare creates the seed table, its single index, and inserts N rows of K-byte payload in a
// single transaction. The heap is (id bigint PRIMARY KEY, payload bytea, updated_at timestamptz)
// with the ONLY index on updated_at: the churn loop UPDATEs updated_at = now() in place, which
// forbids HOT (heap bloats) and bloats the btree right edge (Decision 2), producing dead tuples
// that autovacuum cannot keep up with. The identifier is interpolated (it cannot be a bind
// param); the payload buffer is bound as $1 and the row count as $2.
func prepare(ctx context.Context, conn db.Conn, tableIdent string, rows int64, payloadBytes int) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, _, err = tx.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, payload bytea, updated_at timestamptz)", tableIdent))
	if err != nil {
		return err
	}

	// The single index on updated_at is the bloat target: monotonic now() values bloat the
	// btree on its right edge, and indexing the churned column forbids HOT (Decision 2).
	_, _, err = tx.Exec(ctx, fmt.Sprintf("CREATE INDEX ON %s (updated_at)", tableIdent))
	if err != nil {
		return err
	}

	// The payload must be INCOMPRESSIBLE (random bytes, not zeros): an all-zeros bytea
	// compresses to almost nothing, so the seed would silently fall far short of the requested
	// on-disk size (with the inline default it stays in the heap; if a user raises PayloadBytes
	// above the ~2KB TOAST threshold, random bytes keep the size honest in the TOAST fork rather
	// than vanishing). The buffer is reused for every row — heap tuples are not deduplicated, so
	// the heap still reaches the target size.
	payload := make([]byte, payloadBytes)
	for i := range payload {
		payload[i] = byte(rand.Intn(256))
	}
	// Seed in a single set-based statement: one server-side round-trip instead of N
	// network round-trips, which matters for large N over a remote link.
	insertSQL := fmt.Sprintf("INSERT INTO %s (id, payload, updated_at) SELECT g, $1, now() FROM generate_series(1, $2) AS g", tableIdent)
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
// With KeepTable the table is left in place and its name is logged so the operator can run
// the post-stop repair demo and drop it manually (Decision 5, slotbloat KeepSlot precedent).
// Otherwise it opens its OWN fresh connection rather than reusing a worker conn: a Ctrl+C
// landing mid-UPDATE can poison a worker conn, so a DROP over it would fail and orphan the
// table (ADR-002-2). Any failure is logged via Warnf naming the table so nothing is silently
// orphaned. Every error passes through sanitize.
func (w *workload) cleanup(tableIdent string) {
	if w.config.KeepTable {
		w.logger.Infof("bloat-churn: table kept: %s — drop manually", tableIdent)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Open a fresh connection: a worker conn may be dead after a mid-UPDATE cancel.
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		w.logger.Warnf("bloat-churn: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}
	defer func() { _ = conn.Close() }()

	// Attribute the cleanup DROP in pg_stat_activity. Best-effort: the drop and its
	// honest logging are what matter, so a failed SET must not abort cleanup.
	_, _, _ = conn.Exec(ctx, "SET application_name = 'noisia'")

	if err := dropTable(ctx, conn, tableIdent); err != nil {
		w.logger.Warnf("bloat-churn: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}

	w.logger.Infof("bloat-churn: table dropped")
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
