// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package hotrowcontention defines implementation of workload which deliberately
// drives row-lock and buffer-content contention on a PostgreSQL instance.
//
// It is the deliberate INVERSE of wal-flood. Where wal-flood partitions UPDATEs over
// DISJOINT id ranges so writers never serialize on row locks (maximizing aggregate WAL
// throughput), hot-row-contention makes the opposite choice: it seeds a tiny table of
// HotRows "hot" rows and fans out --jobs autocommit UPDATE-churn workers that SHARE
// those rows (multiple sessions per row). The many `UPDATE counter = counter + 1`
// statements serialize on a single row-lock and contend on LWLock:BufferContent,
// saturating server CPU and collapsing TPS. The result is degradation observable from
// the OUTSIDE — not the death of the instance.
//
// There is no payload and no rate limiter: the goal is maximum contention, so UPDATEs
// run at full speed. The workload self-reports honestly from in-process atomics
// (attempts written and live sessions); it never polls server state. Each worker owns
// its own connection (no shared pool, so the pool default max_conns cannot cap
// parallelism below --jobs). Workload duration is controlled by a context created
// outside and passed to Run.
package hotrowcontention

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

// Config defines configuration settings for hot-row-contention workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	// It is a secret and must never be logged.
	Conninfo string
	// Jobs defines how many concurrent churn workers (sessions) to run, taken from the
	// global --jobs flag. Workers SHARE the hot rows; must satisfy Jobs >= 2*HotRows.
	Jobs uint16
	// HotRows defines the number of shared hot rows (focuses) contended over (>= 1).
	HotRows int
	// ReportInterval defines the report panel print cadence.
	ReportInterval time.Duration
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Conninfo == "" {
		return fmt.Errorf("conninfo must not be empty")
	}

	if c.HotRows < 1 {
		return fmt.Errorf("hot rows must be greater than zero")
	}

	if c.ReportInterval <= 0 {
		return fmt.Errorf("report interval must be positive")
	}

	// Contention precondition (inverse of wal-flood's Rows >= Jobs guard): there must be
	// at least two sessions per hot row, otherwise no contention is created. Both sides
	// are int (Jobs widened) so there is no overflow.
	if int(c.Jobs) < 2*c.HotRows {
		return fmt.Errorf("jobs (%d) must be at least twice hot-rows (%d) to create contention: raise --jobs or lower --hot-rows", c.Jobs, c.HotRows)
	}

	return nil
}

// workload implements noisia.Workload interface.
type workload struct {
	config Config
	logger log.Logger
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config Config, logger log.Logger) (noisia.Workload, error) {
	err := config.validate()
	if err != nil {
		return nil, err
	}

	return &workload{config, logger}, nil
}

// Run opens a dedicated connection to seed the small hot-row table, then fans out Jobs
// long-lived churn workers that SHARE the hot rows while a ticker reports progress.
// Each worker owns its own connection (no shared pool, so the pool default max_conns
// cannot cap parallelism below --jobs). The seed table is dropped in a defer on a fresh
// context.Background() (ctx is already cancelled at exit).
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
	// CREATE/UPDATE/DROP; the hot-row count is a bind arg.
	tableIdent := "\"noisia_hotrow_" + randomSuffix(8) + "\""

	// Cleanup is registered right after a successful connect so the table is dropped even
	// if seeding fails. It runs on a fresh context.Background() because ctx is already
	// cancelled at exit and a drop on a cancelled ctx would fail (ADR-002-2).
	defer w.cleanup(tableIdent)

	// Seed the hot rows. HotRows is small, so seeding is near-instant; emit brief feedback.
	w.logger.Infof("hot-row-contention: seeding table %s with %d hot rows...", tableIdent, w.config.HotRows)
	err = prepare(ctx, conn, tableIdent, w.config.HotRows)
	if err != nil {
		// A clean ctx stop must not be reported as a failure.
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("seed table failed: %s", sanitize(err))
	}
	w.logger.Infof("hot-row-contention: %d sessions over %d hot rows (~%d sessions/row) — watch server CPU (pgcenter/top)",
		w.config.Jobs, w.config.HotRows, int(w.config.Jobs)/w.config.HotRows)

	// attempts holds the total successful UPDATEs across all workers. sessions holds the
	// live worker count. Both are shared and read by the reporter, which must never touch
	// a (not concurrency-safe) Conn.
	var attempts atomic.Int64
	var sessions atomic.Int64

	// start and tickerDone are taken BEFORE workers spawn (uptime counts from churn start).
	start := time.Now()
	tickerDone := make(chan struct{})
	var reporterWg sync.WaitGroup
	reporterWg.Add(1)
	go func() {
		defer reporterWg.Done()
		w.runReporter(ctx, tickerDone, start, &attempts, &sessions)
	}()

	// Fan out exactly Jobs long-lived workers. Worker i hammers hot row (i mod HotRows)+1,
	// so multiple workers SHARE each hot row — this is the contention (no partition()).
	updateSQL := fmt.Sprintf("UPDATE %s SET counter = counter + 1 WHERE id = $1", tableIdent)

	var wg sync.WaitGroup
	errs := make([]error, w.config.Jobs)
	wg.Add(int(w.config.Jobs))
	for i := 0; i < int(w.config.Jobs); i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = w.runWorker(ctx, updateSQL, idx, &attempts, &sessions)
		}(i)
	}
	wg.Wait()

	close(tickerDone)
	reporterWg.Wait()

	// Surface the first init error (a real setup defect — e.g. exhausted connections)
	// reported by any worker; a degraded run or a clean stop leaves all errs nil.
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

// runWorker opens its OWN dedicated connection and drives the churn loop against the hot
// row id = (workerIndex mod HotRows)+1. A per-worker connection guarantees Jobs
// concurrent in-flight UPDATEs regardless of any pool sizing. Every connection error
// passes through sanitize so a pgx error can never leak a DSN fragment.
//
// On connect failure: a clean ctx stop returns nil; if at least one other session is
// live (sessions>0) the failure is a degraded condition (Warn + nil); only when no
// session ever connected (sessions==0) is the sanitized init error returned.
func (w *workload) runWorker(ctx context.Context, updateSQL string, workerIndex int, attempts, sessions *atomic.Int64) error {
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		if sessions.Load() > 0 {
			// Other workers are alive (e.g. jobs > max_connections): degrade, don't abort.
			w.logger.Warnf("hot-row-contention: worker %d connect failed (degraded): %s", workerIndex, sanitize(err))
			return nil
		}
		return fmt.Errorf("worker connect failed: %s", sanitize(err))
	}
	// Count this session as live exactly once, before churn. The single decrement on an
	// Exec error lives in runChurn; Close (in the defer below) does NOT decrement.
	sessions.Add(1)

	// db.Connect does not set application_name; do it best-effort so the worker is
	// attributable in pg_stat_activity. A failed SET must not abort the worker, but its
	// error still passes through sanitize so the DSN is never logged.
	if _, _, serr := conn.Exec(ctx, "SET application_name = 'noisia'"); serr != nil {
		w.logger.Warnf("hot-row-contention: set application_name failed: %s", sanitize(serr))
	}

	defer func() { _ = conn.Close() }()

	id := (workerIndex % w.config.HotRows) + 1
	return runChurn(ctx, conn, w.logger, updateSQL, id, attempts, sessions)
}

// runChurn is one worker's autocommit UPDATE loop. It owns its Conn and hammers a single
// shared hot row id, adding 1 to the SHARED attempts counter for every successful UPDATE.
//
// Returns nil on a clean ctx stop (no decrement of sessions). On an Exec error after at
// least one successful UPDATE anywhere (attempts>0) while the context is still live, it
// logs a degradation warning, decrements sessions EXACTLY once, and returns nil — this is
// degradation, not a climax. The first Exec error while attempts is still 0 is returned
// as a sanitized init error, so a broken setup cannot masquerade as a successful run; one
// worker's early error cannot do so either once any worker has written (Decision 3/5).
//
// sessions is decremented ONLY here and ONLY on an Exec error: never on a clean ctx-cancel
// exit and never on conn.Close (Close is the caller's defer), so the counter never goes
// negative.
func runChurn(ctx context.Context, conn db.Conn, logger log.Logger, updateSQL string, id int, attempts, sessions *atomic.Int64) error {
	for {
		_, _, err := conn.Exec(ctx, updateSQL, id)
		if err != nil {
			// A clean ctx stop must not be reported as a failure (and must not decrement).
			if ctx.Err() != nil {
				return nil
			}

			if attempts.Load() > 0 {
				// Mid-run loss of this session: degradation, not death. Decrement the live
				// session count exactly once and keep the run going on the other workers.
				logger.Warnf("hot-row-contention: worker lost connection: %s", sanitize(err))
				sessions.Add(-1)
				return nil
			}

			// First Exec failed with no prior success anywhere: setup defect.
			return fmt.Errorf("churn update failed: %s", sanitize(err))
		}

		attempts.Add(1)

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

// runReporter prints the report panel every ReportInterval, reading only the shared
// atomic attempts and sessions counters. It never queries a Conn.
func (w *workload) runReporter(ctx context.Context, done <-chan struct{}, start time.Time, attempts, sessions *atomic.Int64) {
	ticker := time.NewTicker(w.config.ReportInterval)
	defer ticker.Stop()

	var prev int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			cur := attempts.Load()
			uptime := time.Since(start)
			r := float64(cur-prev) / w.config.ReportInterval.Seconds()
			prev = cur

			w.logger.Infof("hot-row-contention: attempts=%d rate=%.0f/s sessions=%d uptime=%s", cur, r, sessions.Load(), uptime.Truncate(time.Second))
		}
	}
}

// prepare creates the hot-row table and inserts HotRows rows in a single transaction.
// counter is bigint so a single row read in the shared-contention test can grow without
// overflow. The heap is deliberately tiny: the churn loop UPDATEs these rows in place.
func prepare(ctx context.Context, conn db.Conn, tableIdent string, hotRows int) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, _, err = tx.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, counter bigint NOT NULL)", tableIdent))
	if err != nil {
		return err
	}

	// Seed in a single set-based statement: one server-side round-trip instead of N.
	insertSQL := fmt.Sprintf("INSERT INTO %s (id, counter) SELECT g, 0 FROM generate_series(1, $1) AS g", tableIdent)
	_, _, err = tx.Exec(ctx, insertSQL, hotRows)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// cleanup drops the seed table on a fresh context.Background() (ctx is already cancelled
// at exit), bounded by a short timeout so a hung (not dead) target cannot block Run's
// return forever. It opens its OWN fresh connection rather than reusing a worker conn: a
// Ctrl+C landing mid-UPDATE aborts the in-flight query and poisons that conn, so a DROP
// over it would fail and orphan the table (ADR-002-2). Any failure is logged via Warnf
// with the table name so nothing is silently orphaned. Every error passes through sanitize.
func (w *workload) cleanup(tableIdent string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Open a fresh connection: a worker conn may be dead after a mid-UPDATE cancel.
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		// The target may be unreachable. Nothing was dropped — say so honestly so the
		// table is not silently assumed gone.
		w.logger.Warnf("hot-row-contention: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}
	defer func() { _ = conn.Close() }()

	// Attribute the cleanup DROP in pg_stat_activity. Best-effort: the drop and its honest
	// logging are what matter, so a failed SET must not abort cleanup — ignore the error
	// and proceed. Conninfo is never logged.
	_, _, _ = conn.Exec(ctx, "SET application_name = 'noisia'")

	_, _, err = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableIdent))
	if err != nil {
		w.logger.Warnf("hot-row-contention: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}

	w.logger.Infof("hot-row-contention: table dropped")
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

// sanitize returns an error message stripped of any conninfo fragment that could leak the
// DSN (e.g. a password). It keeps only the leading part of a pgx error up to the first
// occurrence of a conninfo-like token.
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
