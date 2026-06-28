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
// Run orchestrates the engine: seed the table, fan out the scattered-churn workers, run
// the single serial CHECKPOINT forcer on its own connection, and tick the self-report
// panel — all under one ctx, with cleanup on a fresh connection at exit.
package checkpointstorm

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

// forcerSleep is the poll interval the forcer sleeps between checks of sinceCheckpoint
// while below the trigger threshold, so it never busy-spins a core (Decision 4). It is a
// var (not a const) solely so tests can keep the below-threshold window short; production
// never reassigns it.
var forcerSleep = time.Millisecond

// Run opens a dedicated seed connection, sets application_name, verifies the CHECKPOINT
// privilege (honest startup error if absent — the workload does not start and no table is
// created), seeds the (id bigint PK, payload bytea) table, then fans out Jobs scattered
// random-id UPDATE workers (each on its own db.Connect, ADR-003-1) while a single serial
// forcer goroutine — on its own connection — issues a synchronous CHECKPOINT whenever the
// accumulated dirty-row count crosses the rows-based trigger. A ticker reporter prints the
// self-report panel from atomics only. The seed table is dropped in a defer on a fresh
// context.Background() (ctx is already cancelled at exit, ADR-002-2). The only stop
// mechanism is the supplied ctx.
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

	// Functional gate (Decision 3, ADR-005-1): if the connecting role may not issue
	// CHECKPOINT, fail honestly at startup BEFORE creating any table — the workload does
	// not start.
	if err := checkCheckpointPrivilege(ctx, conn); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	// Build the per-run table name once. The suffix is restricted to [a-z0-9], which alone
	// makes the identifier injection-safe; double-quoting is belt-and-suspenders. The
	// identifier (which cannot be a bind parameter) is reused verbatim in CREATE/UPDATE/DROP;
	// payload and id are bind args.
	w.tableIdent = "\"noisia_chkptstorm_" + randomSuffix(8) + "\""

	// Cleanup is registered right after the privilege gate passes and BEFORE seeding so a
	// mid-seed cancel still drops the partial table. It runs on a fresh context.Background()
	// because ctx is already cancelled at exit and a drop on a cancelled ctx would fail
	// (ADR-002-2).
	defer w.cleanup(w.tableIdent)

	// Startup line: intent + table + target size + dirty-pct + where to watch the IO.
	w.logger.Infof("checkpoint-storm: %d workers churning %s (target %s), forcing CHECKPOINT every %d%% dirty — watch checkpoint IO (pgcenter buffers_checkpoint / iostat %%util)",
		w.config.Jobs, w.tableIdent, formatBytes(w.config.TableSize), w.config.DirtyPct)

	// Seed the large heap churned in place. Seeding a multi-hundred-MiB table is slow and the
	// reporter starts only after it, so emit explicit feedback.
	w.rows = rowsForSize(w.config.TableSize, int64(w.config.PayloadBytes))
	w.logger.Infof("checkpoint-storm: seeding %d rows (~%s), this may take a while...", w.rows, formatBytes(w.config.TableSize))
	err = prepare(ctx, conn, w.tableIdent, w.rows, w.config.PayloadBytes)
	if err != nil {
		// A clean ctx stop must not be reported as a failure.
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("seed table failed: %s", sanitize(err))
	}

	// Shared runtime atomics, read by the reporter — never a Conn (ADR-002-3). dirtied is
	// successful UPDATEs × PayloadBytes (panel bytes); sinceCheckpoint is UPDATEs since the
	// last checkpoint (forcer trigger), incremented by workers and decremented by the forcer;
	// checkpoints is the total forced checkpoints; flushNanos is the last CHECKPOINT call
	// duration; sessions is the live-worker count (degradation accounting, not in the panel).
	var dirtied, sinceCheckpoint, checkpoints, flushNanos, sessions atomic.Int64

	start := time.Now()
	tickerDone := make(chan struct{})
	var reporterWg sync.WaitGroup
	reporterWg.Add(1)
	go func() {
		defer reporterWg.Done()
		runReporter(ctx, tickerDone, start, w.config.ReportInterval, w.logger, &dirtied, &checkpoints, &flushNanos)
	}()

	// Launch the single serial forcer on its OWN connection (Decision 4). Its death is a
	// degradation, not a Run failure (Decision 5), so it runs under its own WaitGroup and its
	// outcome never enters the worker "zero live = error" rule.
	threshold := forcerThreshold(w.config.DirtyPct, w.rows)
	var forcerWg sync.WaitGroup
	forcerWg.Add(1)
	go func() {
		defer forcerWg.Done()
		w.runForcerWorker(ctx, threshold, &sinceCheckpoint, &checkpoints, &flushNanos)
	}()

	// Fan out exactly Jobs scattered-churn workers, each on its own connection (ADR-003-1).
	// The table identifier (which cannot be a bind parameter) is interpolated; payload is
	// bound as $1 and the random id as $2.
	updateSQL := fmt.Sprintf("UPDATE %s SET payload = $1 WHERE id = $2", w.tableIdent)

	var wg sync.WaitGroup
	errs := make([]error, w.config.Jobs)
	wg.Add(int(w.config.Jobs))
	for i := 0; i < int(w.config.Jobs); i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = w.runWorker(ctx, updateSQL, &dirtied, &sinceCheckpoint, &sessions)
		}(i)
	}
	wg.Wait()

	// Workers have all returned (ctx cancelled or every worker died); stop the forcer and the
	// reporter.
	forcerWg.Wait()
	close(tickerDone)
	reporterWg.Wait()

	// Surface the first init error (a real setup defect — e.g. exhausted connections) reported
	// by any worker; a degraded run (>=1 worker alive) or a clean stop leaves all errs nil —
	// Run errors only when ZERO workers were ever live. The forcer is NOT in this rule.
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

// forcerThreshold computes the rows-based dirty trigger: max(1, DirtyPct/100 × rows)
// (Decision 2). The max(1, …) floor prevents a degenerate threshold of 0 (which would make
// the forcer fire back-to-back no-op CHECKPOINTs); the validate() floors keep rows large
// enough that the threshold is normally ≫ 1.
func forcerThreshold(dirtyPct int, rows int64) int64 {
	t := int64(dirtyPct) * rows / 100
	if t < 1 {
		return 1
	}
	return t
}

// runWorker opens its OWN dedicated connection and drives the scattered-churn loop. A
// per-worker connection guarantees Jobs concurrent in-flight UPDATEs regardless of any pool
// sizing (ADR-003-1). A failed connect degrades the run (Warn + nil) as long as another
// worker is live; only when no worker was ever live (sessions == 0) is the sanitized init
// error returned. Every connection error passes through sanitize so a pgx error can never
// leak a DSN.
func (w *workload) runWorker(ctx context.Context, updateSQL string, dirtied, sinceCheckpoint, sessions *atomic.Int64) error {
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if sessions.Load() > 0 {
			w.logger.Warnf("checkpoint-storm: worker connect failed (degraded): %s", sanitize(err))
			return nil
		}
		return fmt.Errorf("worker connect failed: %s", sanitize(err))
	}
	defer func() { _ = conn.Close() }()

	return runWorkerWithConn(ctx, conn, w.logger, w.config, updateSQL, w.rows, dirtied, sinceCheckpoint, sessions)
}

// runWorkerWithConn drives the post-connect worker lifecycle over an already-open conn:
// a best-effort SET application_name (a failed SET is cosmetic and does NOT abort the
// worker — unlike seqscan-storm there is NO second functional determinism SET here), the
// live-session increment, and the rate-limited scattered-UPDATE loop. It is split from
// runWorker so the SET + churn logic can be unit-tested over a conn double without a live
// database (runWorker itself only adds the db.Connect that needs a real server).
//
// Each statement targets a RANDOM id = rand.Int63n(rows)+1 (scattered across the whole
// heap, Decision 6), bound as $2; payload is a fixed random-filled buffer bound as $1. On
// each successful UPDATE it adds PayloadBytes to dirtied and 1 to sinceCheckpoint.
//
// Returns nil on a clean ctx stop. On a UPDATE error under a live ctx after at least one
// successful UPDATE anywhere (dirtied > 0), it logs a degradation warning, decrements
// sessions EXACTLY once, and returns nil — the session was lost, not the run. The first
// UPDATE error while dirtied is still 0 is returned as a sanitized init error.
func runWorkerWithConn(ctx context.Context, conn db.Conn, logger log.Logger, config Config, updateSQL string, rows int64, dirtied, sinceCheckpoint, sessions *atomic.Int64) error {
	// db.Connect does not set application_name; do it best-effort so the worker is
	// attributable in pg_stat_activity. A failed SET must not abort the worker (it is
	// cosmetic), but its error still passes through sanitize so the DSN is never logged.
	if _, _, serr := conn.Exec(ctx, "SET application_name = 'noisia'"); serr != nil {
		logger.Warnf("checkpoint-storm: set application_name failed: %s", sanitize(serr))
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
	// compressed/TOASTed away out-of-line instead of dirtying a real heap page. Random bytes
	// keep the write landing on a distinct heap page. Filled once and reused for every UPDATE.
	payload := make([]byte, config.PayloadBytes)
	for i := range payload {
		payload[i] = byte(rand.Intn(256))
	}

	for {
		if err := limiter.Wait(ctx); err != nil {
			// Wait only errors when ctx is done (or the limiter is misconfigured, which it is
			// not here): a clean ctx stop, not a failure.
			return nil
		}

		// Scattered dirtying: a random id over the whole table (Decision 6). Bound as $2.
		id := rand.Int63n(rows) + 1
		_, _, err := conn.Exec(ctx, updateSQL, payload, id)
		if err != nil {
			// A clean ctx stop must not be reported as a failure (and must not decrement).
			if ctx.Err() != nil {
				return nil
			}

			if dirtied.Load() > 0 {
				// Mid-run loss of this session: degradation, not death of the run. Decrement
				// the live session count exactly once and let the other workers carry on.
				logger.Warnf("checkpoint-storm: worker lost connection: %s", sanitize(err))
				sessions.Add(-1)
				return nil
			}

			// First UPDATE failed with no prior success anywhere: setup defect.
			return fmt.Errorf("churn update failed: %s", sanitize(err))
		}

		dirtied.Add(int64(config.PayloadBytes))
		sinceCheckpoint.Add(1)

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

// runForcerWorker owns the forcer's dedicated db.Connect and its one best-effort reconnect,
// then runs the serial forcer loop. It is the connection-owning wrapper around the pure
// runForcer loop (mirroring runWorker / runWorkerWithConn): runForcer is testable over a
// conn double, while runForcerWorker holds the real db.Connect that needs a live server.
//
// The forcer is a singleton (Decision 4/5): its death is a degradation, not a Run failure,
// so this function returns nothing. A failed initial connect under a live ctx is warned
// (sanitized) and the forcer simply does not run — Run continues on the churn workers.
func (w *workload) runForcerWorker(ctx context.Context, threshold int64, sinceCheckpoint, checkpoints, flushNanos *atomic.Int64) {
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.logger.Warnf("checkpoint-storm: forcer connect failed — checkpoint forcing stopped, storm degraded to plain churn: %s", sanitize(err))
		return
	}
	defer func() { _ = conn.Close() }()

	// Attribute the forcer backend in pg_stat_activity. Best-effort: a failed SET is cosmetic
	// and must not stop the forcer, but its error still passes through sanitize.
	if _, _, serr := conn.Exec(ctx, "SET application_name = 'noisia'"); serr != nil {
		w.logger.Warnf("checkpoint-storm: forcer set application_name failed: %s", sanitize(serr))
	}

	reconnect := func(rctx context.Context) (db.Conn, error) { return db.Connect(rctx, w.config.Conninfo) }
	_ = runForcer(ctx, conn, w.logger, threshold, sinceCheckpoint, checkpoints, flushNanos, reconnect)
}

// runForcer is the single serial forcer loop over an already-open conn (Decision 1/4). While
// sinceCheckpoint is below the threshold it sleeps forcerSleep between polls (it never spins a
// core); when sinceCheckpoint crosses the threshold it issues a synchronous CHECKPOINT
// (conn.Exec(ctx, "CHECKPOINT"), no bind args), times the call client-side
// (flushNanos.Store(int64(time.Since(t))), Decision 8), decrements sinceCheckpoint by the
// observed threshold (Add(-threshold), NOT Store(0) — preserves read→reset-window increments,
// Decision 2), and increments checkpoints. Checkpoints are strictly serial: the next trigger
// is evaluated only after the current CHECKPOINT returns.
//
// On a CHECKPOINT error under a live ctx it makes ONE best-effort reconnect (via the supplied
// reconnect func); if the reconnect fails it warns loudly ("checkpoint forcing stopped — storm
// degraded to plain churn") and returns — the forcer exits but Run does NOT fail (Decision 5).
// A clean ctx stop returns nil. Every error passes through sanitize.
func runForcer(ctx context.Context, conn db.Conn, logger log.Logger, threshold int64, sinceCheckpoint, checkpoints, flushNanos *atomic.Int64, reconnect func(context.Context) (db.Conn, error)) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		if sinceCheckpoint.Load() < threshold {
			// Below threshold: sleep a short interval between polls, not a busy-spin. The
			// sleep is interruptible by ctx so shutdown is prompt.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(forcerSleep):
			}
			continue
		}

		t := time.Now()
		_, _, err := conn.Exec(ctx, "CHECKPOINT")
		if err != nil {
			// A clean ctx stop must not be reported as a failure.
			if ctx.Err() != nil {
				return nil
			}

			// Best-effort ONE reconnect; on failure the forcer degrades to plain churn and
			// exits. Run does not fail because the singleton forcer died (Decision 5).
			logger.Warnf("checkpoint-storm: CHECKPOINT failed, reconnecting: %s", sanitize(err))
			if reconnect == nil {
				logger.Warnf("checkpoint-storm: checkpoint forcing stopped — storm degraded to plain churn")
				return nil
			}
			newConn, rerr := reconnect(ctx)
			if rerr != nil {
				logger.Warnf("checkpoint-storm: checkpoint forcing stopped — storm degraded to plain churn: %s", sanitize(rerr))
				return nil
			}
			conn = newConn
			continue
		}

		flushNanos.Store(int64(time.Since(t)))
		// Decrement by the observed threshold, NOT Store(0): preserves any worker increments
		// that landed between the read and the reset (Decision 2).
		sinceCheckpoint.Add(-threshold)
		checkpoints.Add(1)
	}
}

// runReporter prints the self-report panel every interval, reading only the shared atomics.
// It never queries a Conn (ADR-002-3). The panel is exactly
// "checkpoint-storm: dirtied=<bytes> checkpoints=<N> (<M>/min) flush=<T> elapsed=<Z>": dirtied
// via the GB-capped formatBytes (Decision 7), M/min = checkpoints / elapsed-minutes, flush the
// last stored CHECKPOINT call duration (Decision 8). There is NO sessions field in the panel.
func runReporter(ctx context.Context, done <-chan struct{}, start time.Time, interval time.Duration, logger log.Logger, dirtied, checkpoints, flushNanos *atomic.Int64) {
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
			cps := checkpoints.Load()

			// Guard division on the first sub-minute tick: scale by elapsed minutes, but never
			// divide by ~0 (which would print +Inf).
			minutes := elapsed.Minutes()
			perMin := 0.0
			if minutes > 0 {
				perMin = float64(cps) / minutes
			}

			flush := time.Duration(flushNanos.Load())

			logger.Infof("checkpoint-storm: dirtied=%s checkpoints=%d (%.1f/min) flush=%s elapsed=%s",
				formatBytes(dirtied.Load()), cps, perMin, flush.Truncate(time.Millisecond), elapsed.Truncate(time.Second))
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

	// The payload must be INCOMPRESSIBLE (random bytes, not zeros): an all-zeros bytea
	// compresses to almost nothing, so a payload at/above the ~2KB TOAST threshold would be
	// compressed/TOASTed away out-of-line, leaving the main heap far smaller than the
	// requested table-size — too few distinct pages to dirty. Random bytes keep the heap at
	// its on-disk size so the forced CHECKPOINT flushes many distinct pages. Filled once.
	payload := make([]byte, payloadBytes)
	for i := range payload {
		payload[i] = byte(rand.Intn(256))
	}
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
