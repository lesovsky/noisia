// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package xminhorizonholder defines implementation of a workload which deliberately
// pins the cluster-wide xmin horizon by holding a long-lived REPEATABLE READ snapshot
// while a churn loop generates dead tuples that VACUUM cannot reclaim — a bloat sibling
// of the slow-line family.
//
// It hosts the Config surface, its validation, the NewWorkload constructor, the copied
// infrastructure helpers (sanitize, randomSuffix, formatBytes, rowsForSize, prepare,
// dropTable, cleanup), and the runtime engine: Run, the synchronously-established mandatory
// holder, its idle/reconnect loop, the scattered-churn workers, and the self-report reporter.
// Unlike its checkpoint-storm sibling there is no forcer threshold, hence no dirty-percentage
// config field nor its floor, and no startup privilege gate (Decision 5).
//
// The seed table is dropped on a fresh connection at exit. Workload duration is
// controlled by a context created outside and passed to Run.
package xminhorizonholder

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

// holderBeginSQL pins the holder's REPEATABLE READ snapshot (Decision 1). It is a STATIC
// string literal — never assembled via fmt.Sprintf — because db.Conn.Begin maps to pgx
// BeginTx(ctx, TxOptions{}) which emits the literal "begin" (server-default READ COMMITTED)
// and cannot carry an isolation level, and db.Conn exposes no BeginTx. A raw Exec of this
// literal is the only path through the existing db wrapper to a stable REPEATABLE READ
// snapshot that freezes backend_xmin for the whole run.
const holderBeginSQL = "BEGIN ISOLATION LEVEL REPEATABLE READ"

// holderPingInterval is the cadence at which the holder loop issues a lightweight liveness
// ping inside the held transaction to detect a mid-run connection drop (e.g. a target
// idle_in_transaction_session_timeout). It is a var (not a const) solely so tests can keep
// the interval short; production never reassigns it.
var holderPingInterval = time.Second

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

// Run opens a dedicated seed connection, sets application_name, seeds the
// (id bigint PK, payload bytea) table, then establishes the MANDATORY holder synchronously
// on its own connection (Decision 3) — SET application_name → raw BEGIN ISOLATION LEVEL
// REPEATABLE READ → SELECT 1 (pins backend_xmin) → SELECT txid_current() (pins backend_xid).
// Any failure establishing the holder returns a sanitized error and the workload does NOT
// start. heldSince is set at the successful pin BEFORE the reporter goroutine starts
// (Decision 9). After the pin, a holder goroutine idles the connection and does one
// reconnect on a mid-run drop (Decision 4), Jobs scattered random-id UPDATE churn workers
// fan out (each on its own db.Connect, ADR-003-1), and a ticker reporter prints the
// self-report panel from atomics only. The seed table is dropped in a defer on a fresh
// context.Background() (ADR-002-2). The only stop mechanism is the supplied ctx.
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
	w.tableIdent = "\"noisia_xminhold_" + randomSuffix(8) + "\""

	// Cleanup is registered before seeding so a mid-seed cancel still drops the partial table.
	// It runs on a fresh context.Background() because ctx is already cancelled at exit and a
	// drop on a cancelled ctx would fail (ADR-002-2).
	defer w.cleanup(w.tableIdent)

	// Startup line: intent + table + target size + where to watch the effect.
	w.logger.Infof("xmin-horizon-holder: %d workers churning %s (target %s), holding a REPEATABLE READ snapshot to freeze the xmin horizon — watch dead tuples (pg_stat_user_tables.n_dead_tup) and backend_xmin/backend_xid in pg_stat_activity",
		w.config.Jobs, w.tableIdent, formatBytes(w.config.TableSize))

	// Seed the large heap churned in place. Seeding a multi-hundred-MiB table is slow and the
	// reporter starts only after it, so emit explicit feedback.
	w.rows = rowsForSize(w.config.TableSize, int64(w.config.PayloadBytes))
	w.logger.Infof("xmin-horizon-holder: seeding %d rows (~%s), this may take a while...", w.rows, formatBytes(w.config.TableSize))
	err = prepare(ctx, conn, w.tableIdent, w.rows, w.config.PayloadBytes)
	if err != nil {
		// A clean ctx stop must not be reported as a failure.
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("seed table failed: %s", sanitize(err))
	}

	// Shared runtime atomics, read by the reporter — never a Conn (ADR-002-3). churned is the
	// successful UPDATE count (panel field, no threshold); heldSince is the UnixNano of the
	// current snapshot pin (reset on reconnect); holderRestarts is the count of successful
	// holder reconnects; sessions is the live-worker count (degradation accounting, not in the
	// panel).
	var churned, heldSince, holderRestarts, sessions atomic.Int64

	// Establish the MANDATORY holder synchronously on its OWN connection BEFORE fanning out any
	// churn worker (Decision 3). Any failure here is fatal: the frozen horizon is the defining
	// organ, so the workload must not start as plain bloat-churn.
	holderConn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("holder connect failed: %s", sanitize(err))
	}
	if err := establishHolder(ctx, holderConn, &heldSince); err != nil {
		_ = holderConn.Close()
		if ctx.Err() != nil {
			return nil
		}
		// establishHolder already sanitizes every step error.
		return err
	}

	// The reporter starts only AFTER heldSince is set at the successful pin (Decision 9), so
	// the first tick never reads the epoch and prints a nonsense multi-decade held.
	start := time.Now()
	tickerDone := make(chan struct{})
	var reporterWg sync.WaitGroup
	reporterWg.Add(1)
	go func() {
		defer reporterWg.Done()
		runReporter(ctx, tickerDone, start, w.config.ReportInterval, w.logger, &churned, &holderRestarts, &heldSince)
	}()

	// Hand the pinned holder connection to a goroutine that idles it until ctx.Done() and does
	// one reconnect on a mid-run drop (Decision 4). The holder's death (after a failed reconnect)
	// is a degradation, not a Run failure, so it runs under its OWN WaitGroup and never enters
	// the worker "zero live = error" rule.
	reconnect := func(rctx context.Context) (db.Conn, error) { return db.Connect(rctx, w.config.Conninfo) }
	var holderWg sync.WaitGroup
	holderWg.Add(1)
	go func() {
		defer holderWg.Done()
		_ = runHolder(ctx, holderConn, w.logger, &heldSince, &holderRestarts, reconnect)
	}()

	// Fan out exactly Jobs scattered-churn workers, each on its own connection (ADR-003-1). The
	// table identifier (which cannot be a bind parameter) is interpolated; payload is bound as
	// $1 and the random id as $2.
	updateSQL := fmt.Sprintf("UPDATE %s SET payload = $1 WHERE id = $2", w.tableIdent)

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

	// Workers have all returned (ctx cancelled or every worker died); stop the holder and the
	// reporter.
	holderWg.Wait()
	close(tickerDone)
	reporterWg.Wait()

	// Surface the first init error (a real setup defect — e.g. exhausted connections) reported
	// by any worker; a degraded run (>=1 worker alive) or a clean stop leaves all errs nil —
	// Run errors only when ZERO workers were ever live. The holder is NOT in this rule.
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

// establishHolder pins the holder's snapshot over an already-open conn by issuing, in order:
// SET application_name='noisia', the static BEGIN ISOLATION LEVEL REPEATABLE READ literal,
// SELECT 1 (pins backend_xmin via the first snapshot-taking statement), and SELECT
// txid_current() (forces an xid → pins backend_xid, Decision 2). Return values are not needed
// (held is a client-side duration) and db.Conn lacks QueryRow, so every step uses Exec. On
// success it stores time.Now().UnixNano() into heldSince (the pin instant, set here so the
// reconnect path RESETS it honestly). Any step failure returns a sanitized error and leaves
// heldSince untouched — the mandatory-start gate (Decision 3) relies on this.
func establishHolder(ctx context.Context, conn db.Conn, heldSince *atomic.Int64) error {
	if _, _, err := conn.Exec(ctx, "SET application_name = 'noisia'"); err != nil {
		return fmt.Errorf("holder set application_name failed: %s", sanitize(err))
	}
	if _, _, err := conn.Exec(ctx, holderBeginSQL); err != nil {
		return fmt.Errorf("holder begin failed: %s", sanitize(err))
	}
	if _, _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
		return fmt.Errorf("holder snapshot pin failed: %s", sanitize(err))
	}
	if _, _, err := conn.Exec(ctx, "SELECT txid_current()"); err != nil {
		return fmt.Errorf("holder xid pin failed: %s", sanitize(err))
	}

	heldSince.Store(time.Now().UnixNano())
	return nil
}

// runHolder idles the pinned holder connection until ctx.Done(), issuing a lightweight
// liveness ping every holderPingInterval to detect a mid-run drop. The held REPEATABLE READ
// transaction stays idle-in-transaction between pings; the ping is the only way to observe a
// dropped backend through the db.Conn wrapper (which exposes only Begin/Exec/Query/Close).
//
// On a ping error under a live ctx it makes ONE best-effort reconnect (via the supplied
// reconnect func) and re-establishes the snapshot on the new conn (new BEGIN+SELECTs). A
// successful reconnect RESETS heldSince (the horizon genuinely unfroze in the gap, so held
// must restart) and increments holderRestarts to make the reset visible (Decision 4). If the
// reconnect or the re-establish fails, it warns (sanitized) that the horizon is no longer held
// and returns — the holder exits but Run does NOT fail (degradation, Decision 4). A clean ctx
// stop returns nil. Every error passes through sanitize. It owns the holder connection's
// lifecycle and closes it on exit (snapshot released, Decision 10).
func runHolder(ctx context.Context, conn db.Conn, logger log.Logger, heldSince, holderRestarts *atomic.Int64, reconnect func(context.Context) (db.Conn, error)) error {
	defer func() { _ = conn.Close() }()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(holderPingInterval):
		}

		// Liveness ping inside the held transaction; an error means the backend dropped.
		if _, _, err := conn.Exec(ctx, "SELECT 1"); err != nil {
			// A clean ctx stop must not be reported as a drop.
			if ctx.Err() != nil {
				return nil
			}

			logger.Warnf("xmin-horizon-holder: holder connection lost, reconnecting: %s", sanitize(err))

			newConn, rerr := reconnect(ctx)
			if rerr != nil {
				logger.Warnf("xmin-horizon-holder: holder reconnect failed — xmin horizon no longer held: %s", sanitize(rerr))
				return nil
			}
			if eerr := establishHolder(ctx, newConn, heldSince); eerr != nil {
				_ = newConn.Close()
				logger.Warnf("xmin-horizon-holder: holder re-establish failed — xmin horizon no longer held: %s", sanitize(eerr))
				return nil
			}

			// The dead conn is replaced; close it and adopt the freshly-pinned one. heldSince was
			// already reset by establishHolder; surface the reset via holder-restarts.
			_ = conn.Close()
			conn = newConn
			holderRestarts.Add(1)
		}
	}
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
			w.logger.Warnf("xmin-horizon-holder: worker connect failed (degraded): %s", sanitize(err))
			return nil
		}
		return fmt.Errorf("worker connect failed: %s", sanitize(err))
	}
	defer func() { _ = conn.Close() }()

	return runWorkerWithConn(ctx, conn, w.logger, w.config, updateSQL, w.rows, churned, sessions)
}

// runWorkerWithConn drives the post-connect worker lifecycle over an already-open conn:
// a best-effort SET application_name (a failed SET is cosmetic and does NOT abort the worker),
// the live-session increment, and the rate-limited scattered-UPDATE loop. It is split from
// runWorker so the SET + churn logic can be unit-tested over a conn double without a live
// database.
//
// Each statement targets a RANDOM id = rand.Int63n(rows)+1 (scattered across the whole heap,
// Decision 6), bound as $2; payload is a fixed zero-filled buffer bound as $1. On each
// successful UPDATE it adds 1 to churned (the panel counter).
//
// Returns nil on a clean ctx stop. On an UPDATE error under a live ctx after at least one
// successful UPDATE anywhere (churned > 0), it logs a degradation warning, decrements sessions
// EXACTLY once, and returns nil — the session was lost, not the run. The first UPDATE error
// while churned is still 0 is returned as a sanitized init error. A wraparound read-only
// refusal surfaces naturally here as such an UPDATE error (Decision 8 — no special-casing).
func runWorkerWithConn(ctx context.Context, conn db.Conn, logger log.Logger, config Config, updateSQL string, rows int64, churned, sessions *atomic.Int64) error {
	// db.Connect does not set application_name; do it best-effort so the worker is attributable
	// in pg_stat_activity. A failed SET must not abort the worker (it is cosmetic), but its error
	// still passes through sanitize so the DSN is never logged.
	if _, _, serr := conn.Exec(ctx, "SET application_name = 'noisia'"); serr != nil {
		logger.Warnf("xmin-horizon-holder: set application_name failed: %s", sanitize(serr))
	}

	// Count this session as live exactly once, after the SET and before the churn loop.
	sessions.Add(1)

	lim := rate.Limit(config.Rate)
	if config.Rate == 0 {
		lim = rate.Inf
	}
	limiter := rate.NewLimiter(lim, 1)

	payload := make([]byte, config.PayloadBytes)

	for {
		if err := limiter.Wait(ctx); err != nil {
			// Wait only errors when ctx is done: a clean ctx stop, not a failure.
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

			if churned.Load() > 0 {
				// Mid-run loss of this session: degradation, not death of the run. Decrement the
				// live session count exactly once and let the other workers carry on.
				logger.Warnf("xmin-horizon-holder: worker lost connection: %s", sanitize(err))
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
// "xmin-horizon-holder: held=<dur> churned=<N> (<rate>/min) holder-restarts=<n> elapsed=<Z>":
// held is time.Since(heldSince), rate is churned per elapsed-minute (guarded against a
// divide-by-~0 on the first sub-minute tick). There is NO dirtied/flush/checkpoints/sessions
// field (Decision 9).
func runReporter(ctx context.Context, done <-chan struct{}, start time.Time, interval time.Duration, logger log.Logger, churned, holderRestarts, heldSince *atomic.Int64) {
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

			// held is the age of the CURRENT snapshot pin; a reconnect resets heldSince so held
			// restarts honestly (Decision 4/9).
			held := time.Duration(0)
			if hs := heldSince.Load(); hs > 0 {
				held = time.Since(time.Unix(0, hs))
			}

			logger.Infof("xmin-horizon-holder: held=%s churned=%d (%.1f/min) holder-restarts=%d elapsed=%s",
				held.Truncate(time.Second), ch, perMin, holderRestarts.Load(), elapsed.Truncate(time.Second))
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
