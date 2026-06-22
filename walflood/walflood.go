// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package walflood defines implementation of workload which deliberately drives a
// high rate of WAL generation on a primary PostgreSQL instance.
//
// Unlike slot-bloat (which pins WAL with a forgotten replication slot while the
// instance stays idle), wal-flood has NO replication slot: it seeds one regular
// table with N rows of K-byte payload and fans out --jobs long-lived, rate-limited
// UPDATE-churn workers over DISJOINT id sub-ranges of that single table. Each worker
// owns its own connection and a contiguous, non-overlapping slice of the row set, so
// writers never serialize on row locks and the aggregate WAL rate scales with --jobs.
//
// The workload self-reports honestly: it keeps an in-process counter of the
// application payload bytes it has written (count × K) and prints an escalation
// panel every report-interval, labeled payload-written. It never polls server WAL
// state, so the log stays truthful even when the target becomes unreachable at the
// moment of catastrophe. Escalation to disk-full is environment-dependent, not a
// guarantee: on capable I/O the WAL rate plateaus instead of filling the disk.
// Workload duration is controlled by a context created outside and passed to Run.
package walflood

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

// Config defines configuration settings for wal-flood workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	// It is a secret and must never be logged.
	Conninfo string
	// Rate defines UPDATE statements rate per second PER worker; 0 means unlimited (rate.Inf).
	Rate float64
	// Rows defines the total number of seed rows, partitioned into Jobs disjoint
	// id ranges and churned in place (>= 1, and >= Jobs).
	Rows int
	// PayloadBytes defines the payload size in bytes per UPDATE (>= 1).
	PayloadBytes int
	// ReportInterval defines the escalation panel print cadence.
	ReportInterval time.Duration
	// Jobs defines how many concurrent churn workers to run, taken from the global
	// --jobs flag (>= 1). Each worker owns one disjoint id sub-range.
	Jobs uint16
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Conninfo == "" {
		return fmt.Errorf("conninfo must not be empty")
	}

	if c.Rate < 0 {
		return fmt.Errorf("rate must not be negative")
	}

	if c.Rows < 1 {
		return fmt.Errorf("rows must be greater than zero")
	}

	if c.PayloadBytes < 1 {
		return fmt.Errorf("payload bytes must be greater than zero")
	}

	if c.ReportInterval <= 0 {
		return fmt.Errorf("report interval must be positive")
	}

	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be greater than zero")
	}

	// Partition rule (Decision 2): every worker must own at least one row, so the
	// row set cannot be smaller than the number of workers.
	if c.Rows < int(c.Jobs) {
		return fmt.Errorf("rows must not be less than jobs")
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

// Run opens a dedicated connection to seed one fixed-heap table, then fans out Jobs
// long-lived churn workers over disjoint id ranges while a ticker reports progress.
// Each worker owns its own connection (no shared pool, so the pool default max_conns
// cannot cap parallelism below --jobs). The seed table is dropped in a defer on a
// fresh context.Background() (ctx is already cancelled at exit).
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
	// CREATE/UPDATE/DROP; payload and id are bind args.
	tableIdent := "\"noisia_walflood_" + randomSuffix(8) + "\""

	// Cleanup is registered right after a successful connect so the table is dropped
	// even if seeding fails. It runs on a fresh context.Background() because ctx is
	// already cancelled at exit and a drop on a cancelled ctx would fail (ADR-002-2).
	defer w.cleanup(tableIdent)

	// Seed the fixed row set churned in place. Seeding can take a while for large
	// rows/payload, and the reporter starts only after it, so emit explicit feedback.
	w.logger.Infof("wal-flood: seeding %d rows (~%s), this may take a while...", w.config.Rows, formatBytes(int64(w.config.Rows)*int64(w.config.PayloadBytes)))
	err = prepare(ctx, conn, tableIdent, w.config.Rows, w.config.PayloadBytes)
	if err != nil {
		// A clean ctx stop must not be reported as a failure.
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("seed table failed: %s", sanitize(err))
	}
	w.logger.Infof("wal-flood: seeding done, starting %d churn workers", w.config.Jobs)

	// counter holds the total payload bytes successfully written by all workers
	// (successful UPDATEs × K). It is shared across workers and read by the reporter,
	// which must never touch a (not concurrency-safe) Conn.
	var counter atomic.Int64

	start := time.Now()
	tickerDone := make(chan struct{})
	var reporterWg sync.WaitGroup
	reporterWg.Add(1)
	go func() {
		defer reporterWg.Done()
		w.runReporter(ctx, tickerDone, start, &counter)
	}()

	// Fan out exactly Jobs long-lived workers, each over its own disjoint id range.
	ranges := partition(w.config.Rows, w.config.Jobs)
	updateSQL := fmt.Sprintf("UPDATE %s SET payload = $1 WHERE id = $2", tableIdent)

	var wg sync.WaitGroup
	errs := make([]error, len(ranges))
	wg.Add(len(ranges))
	for i, r := range ranges {
		go func(idx, lo, hi int) {
			defer wg.Done()
			errs[idx] = w.runWorker(ctx, updateSQL, lo, hi, &counter)
		}(i, r[0], r[1])
	}
	wg.Wait()

	close(tickerDone)
	reporterWg.Wait()

	// Surface the first init error (a real setup defect — e.g. exhausted connections)
	// reported by any worker; a clean stop or a climax leaves all errs nil.
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

// runWorker opens its OWN dedicated connection and drives the churn loop over the
// [lo, hi] id sub-range. A per-worker connection guarantees Jobs concurrent in-flight
// UPDATEs regardless of any pool sizing. Every connection error passes through
// sanitize so a pgx error can never leak a DSN fragment.
func (w *workload) runWorker(ctx context.Context, updateSQL string, lo, hi int, counter *atomic.Int64) error {
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("worker connect failed: %s", sanitize(err))
	}
	defer func() { _ = conn.Close() }()

	// db.Connect does not set application_name; do it best-effort so the worker is
	// attributable in pg_stat_activity. A failed SET must not abort the worker, but
	// its error still passes through sanitize so the DSN is never logged.
	if _, _, serr := conn.Exec(ctx, "SET application_name = 'noisia'"); serr != nil {
		w.logger.Warnf("wal-flood: set application_name failed: %s", sanitize(serr))
	}

	return runChurn(ctx, conn, w.logger, w.config, updateSQL, lo, hi, counter)
}

// runChurn is one worker's rate-limited UPDATE loop. It owns its Conn and cycles id
// only within its disjoint [lo, hi] sub-range, adding K to the SHARED counter for
// every successful UPDATE.
//
// Returns nil on a clean ctx stop. On a connection loss after at least one successful
// UPDATE anywhere (shared counter>0) while the context is still live, it logs the
// climax line and returns nil. The first Exec error while the shared counter is still
// 0 is returned as an init error, so a broken setup cannot masquerade as a successful
// disk-full event — and one worker's early error cannot do so either once any worker
// has written (Decision 3).
func runChurn(ctx context.Context, conn db.Conn, logger log.Logger, config Config, updateSQL string, lo, hi int, counter *atomic.Int64) error {
	lim := rate.Limit(config.Rate)
	if config.Rate == 0 {
		lim = rate.Inf
	}
	limiter := rate.NewLimiter(lim, 1)

	payload := make([]byte, config.PayloadBytes)
	id := lo - 1

	for {
		if limiter.Allow() {
			// Cycle id over the worker's disjoint range [lo, hi].
			id++
			if id > hi {
				id = lo
			}

			_, _, err := conn.Exec(ctx, updateSQL, payload, id)
			if err != nil {
				// A clean ctx stop must not be reported as a failure.
				if ctx.Err() != nil {
					return nil
				}

				if counter.Load() > 0 {
					logger.Infof("wal-flood: connection lost — target likely disk-full/restarted")
					return nil
				}

				// First Exec failed with no prior success anywhere: setup defect.
				return fmt.Errorf("churn update failed: %s", sanitize(err))
			}

			counter.Add(int64(config.PayloadBytes))
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

// runReporter prints the escalation panel every ReportInterval, reading only the
// shared atomic payload-bytes counter. It never queries a Conn.
func (w *workload) runReporter(ctx context.Context, done <-chan struct{}, start time.Time, counter *atomic.Int64) {
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
			cur := counter.Load()
			elapsed := time.Since(start)
			rate := int64(float64(cur-prev) / w.config.ReportInterval.Seconds())
			prev = cur

			w.logger.Infof("wal-flood: payload-written=%s rate=%s/s elapsed=%s", formatBytes(cur), formatBytes(rate), elapsed.Truncate(time.Second))
		}
	}
}

// prepare creates the seed table and inserts N rows of K-byte payload in a single
// transaction. The heap is deliberately fixed: the churn loop UPDATEs these rows in
// place, so only WAL grows, not the table.
func prepare(ctx context.Context, conn db.Conn, tableIdent string, rows, payloadBytes int) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, _, err = tx.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, payload bytea)", tableIdent))
	if err != nil {
		return err
	}

	// The payload content is intentionally a fixed zero-filled buffer: WAL is
	// generated by the UPDATE regardless of byte equality, so the value is irrelevant.
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

// cleanup drops the seed table on a fresh context.Background() (ctx is already
// cancelled at exit), bounded by a short timeout so a hung (not dead) target cannot
// block Run's return forever. It opens its OWN fresh connection rather than reusing a
// worker conn: a Ctrl+C landing mid-UPDATE aborts the in-flight query and poisons that
// conn, so a DROP over it would fail and orphan the table (ADR-002-2). Any failure is
// logged via Warnf with the table name so nothing is silently orphaned. Every error
// passes through sanitize.
func (w *workload) cleanup(tableIdent string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Open a fresh connection: a worker conn may be dead after a mid-UPDATE cancel.
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		// The target may be unreachable (e.g. in PANIC at real disk-full). Nothing was
		// dropped — say so honestly so the table is not silently assumed gone.
		w.logger.Warnf("wal-flood: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}
	defer func() { _ = conn.Close() }()

	// Attribute the cleanup DROP in pg_stat_activity. Best-effort: the drop and its
	// honest logging are what matter, so a failed SET must not abort cleanup — ignore
	// the error and proceed. Conninfo is never logged.
	_, _, _ = conn.Exec(ctx, "SET application_name = 'noisia'")

	_, _, err = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableIdent))
	if err != nil {
		w.logger.Warnf("wal-flood: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}

	w.logger.Infof("wal-flood: table dropped")
}

// partition splits the id space 1..rows into jobs contiguous, non-overlapping
// sub-ranges and returns, for each worker, an inclusive [lo, hi] pair. Worker w
// (0-based) owns [w*rows/jobs + 1 .. (w+1)*rows/jobs] (Decision 2). The integer
// division spreads any remainder into the tail ranges with no gap or overlap.
// Precondition: rows >= jobs (enforced by Config.validate), so no range is empty.
func partition(rows int, jobs uint16) [][2]int {
	j := int(jobs)
	ranges := make([][2]int, 0, j)
	for w := 0; w < j; w++ {
		lo := w*rows/j + 1
		hi := (w + 1) * rows / j
		ranges = append(ranges, [2]int{lo, hi})
	}
	return ranges
}

// randomSuffix returns a random string of length n drawn from [a-z0-9]. The charset
// guarantees an injection-safe SQL identifier, and a per-run suffix keeps reruns
// from colliding on the seed table name.
func randomSuffix(n int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// sanitize returns an error message stripped of any conninfo fragment that could
// leak the DSN (e.g. a password). It keeps only the leading part of a pgx error
// up to the first occurrence of a conninfo-like token.
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

	// The unit ladder caps at GB on purpose: the workload's payload counter never
	// reaches terabytes, so values >= 1 TB render as a large GB number, not TB.
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 2; v /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMG"[exp])
}
