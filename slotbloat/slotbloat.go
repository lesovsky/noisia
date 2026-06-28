// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package slotbloat defines implementation of workload which deliberately drives
// a PostgreSQL instance towards a disk-full PANIC via a forgotten replication slot.
//
// A single dedicated connection (not a pool) creates an un-consumed physical
// replication slot with immediately_reserve := true, which freezes restart_lsn at
// creation and pins WAL without any consumer. It then seeds a dedicated regular
// table with N rows of K-byte payload and drives a single-threaded rate-limited
// UPDATE-churn loop over that fixed row set. Because the slot pins WAL while the
// heap stays flat (UPDATE in place), pg_wal grows unbounded until the disk fills
// and the instance PANICs.
//
// The workload self-reports honestly: it keeps an in-process counter of the
// application payload bytes it has written (count × K) and prints an escalation
// panel every report-interval via logger.Infof, labeled payload-written. It never
// polls server WAL state, so the log stays truthful even when the target becomes
// unreachable at the moment of catastrophe. On graceful stop the slot and table
// are dropped on a fresh context.Background(); with --keep-slot both are kept and
// their names are logged.
// Workload duration is controlled by a context created outside and passed to Run.
package slotbloat

import (
	"context"
	"errors"
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

// Config defines configuration settings for slot-bloat workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	// It is a secret and must never be logged.
	Conninfo string
	// Rate defines UPDATE statements rate per second; 0 means unlimited (rate.Inf).
	Rate float64
	// Rows defines the number of seed rows churned in place (>= 1).
	Rows int
	// PayloadBytes defines the payload size in bytes per row/UPDATE (>= 1).
	PayloadBytes int
	// ReportInterval defines the escalation panel print cadence.
	ReportInterval time.Duration
	// KeepSlot keeps the slot and table on graceful exit instead of dropping them.
	KeepSlot bool
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

// Run opens a single dedicated connection, creates the WAL-pinning replication
// slot and seed table, then drives the UPDATE-churn loop while reporting progress
// on a separate ticker goroutine. Cleanup of the slot and table runs in a defer on
// a fresh context.Background() (ctx is already cancelled at exit).
func (w *workload) Run(ctx context.Context) error {
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		// Never echo the raw error: it may carry DSN fragments (e.g. password=…).
		return fmt.Errorf("connect to target failed: %s", sanitize(err))
	}

	defer func() { _ = conn.Close() }()

	// Attribute the offending backend in pg_stat_activity. db.Connect (unlike the
	// pool path) does not set application_name, so set it locally here.
	_, _, err = conn.Exec(ctx, "SET application_name = 'noisia'")
	if err != nil {
		return fmt.Errorf("set application_name failed: %s", sanitize(err))
	}

	// Build the per-run object names once. The suffix is restricted to [a-z0-9],
	// which alone makes the identifier injection-safe; double-quoting is
	// belt-and-suspenders. The table identifier (which cannot be a bind parameter)
	// is reused verbatim in CREATE/UPDATE/DROP; the slot name is passed as a bind
	// arg to the slot functions.
	base := "noisia_slotbloat_" + randomSuffix(8)
	slotName := base
	tableIdent := "\"" + base + "\""

	// Cleanup is registered right after a successful connect so the slot/table are
	// dropped even if seeding fails. It runs on a fresh context.Background() because
	// ctx is already cancelled at exit and a drop on a cancelled ctx would fail.
	defer w.cleanup(slotName, tableIdent)

	// Create the un-consumed physical slot with immediately_reserve := true, which
	// freezes restart_lsn at creation and pins WAL without any consumer.
	_, _, err = conn.Exec(ctx, "SELECT pg_create_physical_replication_slot($1, true)", slotName)
	if err != nil {
		// A clean ctx stop must not be reported as a failure.
		if ctx.Err() != nil {
			return nil
		}
		// No rows churned yet: this is a setup defect (most often a missing
		// REPLICATION privilege), not a successful disk-full event.
		return fmt.Errorf("create replication slot failed (the connecting role needs the REPLICATION privilege): %s", sanitize(err))
	}

	// Seed the fixed row set churned in place. Seeding can take a while for large
	// rows/payload, and the reporter starts only after it (printing payload-written=0B
	// during the seed would be misleading), so emit explicit seed-phase feedback here.
	w.logger.Infof("slot-bloat: seeding %d rows (~%s), this may take a while...", w.config.Rows, formatBytes(int64(w.config.Rows)*int64(w.config.PayloadBytes)))
	err = prepare(ctx, conn, tableIdent, w.config.Rows, w.config.PayloadBytes)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("seed table failed: %s", sanitize(err))
	}
	w.logger.Infof("slot-bloat: seeding done, starting churn")

	// counter holds the number of payload bytes successfully written by UPDATEs
	// (successful UPDATEs × K). It is read by the report ticker goroutine, which
	// must never touch the (not concurrency-safe) Conn.
	var counter atomic.Int64

	start := time.Now()
	tickerDone := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.runReporter(ctx, tickerDone, start, &counter)
	}()

	updateSQL := fmt.Sprintf("UPDATE %s SET payload = $1 WHERE id = $2", tableIdent)
	err = runChurn(ctx, conn, w.logger, w.config, updateSQL, &counter)

	close(tickerDone)
	wg.Wait()

	return err
}

// runReporter prints the escalation panel every ReportInterval, reading only the
// atomic payload-bytes counter. It never queries the Conn.
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

			w.logger.Infof("slot-bloat: payload-written=%s rate=%s/s elapsed=%s", formatBytes(cur), formatBytes(rate), elapsed.Truncate(time.Second))
		}
	}
}

// prepare creates the seed table and inserts N rows of K-byte payload in a single
// transaction. The heap is deliberately fixed: the churn loop UPDATEs these rows in
// place, so disk-full is unambiguously attributable to pinned WAL, not table growth.
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

	// The payload must be INCOMPRESSIBLE (random bytes, not zeros): a zero-filled bytea
	// at the default 8192-byte size is stored out-of-line in TOAST and pglz-compresses to
	// almost nothing, so the actual WAL pinned per UPDATE falls far below the reported
	// payload-written and the slot-bloat pressure is far weaker than the counter implies.
	// Random bytes do not compress, so the real WAL volume matches the nominal payload
	// size. Filled once and reused for every row — no per-statement cost.
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

// runChurn is the single-threaded rate-limited UPDATE loop. It owns the Conn: it
// issues each UPDATE over the fixed row set (cycling id over 1..N) and adds K to
// counter for every successful UPDATE.
//
// Returns nil on a clean ctx stop. On a connection loss after at least one
// successful UPDATE (counter>0) while the context is still live, it logs the climax
// line and returns nil. The first Exec error (counter==0) is returned as an init
// error, so a broken setup cannot masquerade as a successful disk-full event.
func runChurn(ctx context.Context, conn db.Conn, logger log.Logger, config Config, updateSQL string, counter *atomic.Int64) error {
	lim := rate.Limit(config.Rate)
	if config.Rate == 0 {
		lim = rate.Inf
	}
	limiter := rate.NewLimiter(lim, 1)

	// The payload must be INCOMPRESSIBLE (random bytes, not zeros): a zero-filled bytea
	// at the default 8192-byte size is TOASTed out-of-line and pglz-compresses to almost
	// nothing, so the real WAL pinned per UPDATE would fall far below the reported
	// payload-written. Random bytes do not compress, so the actual WAL volume matches the
	// nominal payload size. Filled once and reused for every UPDATE — no per-statement cost.
	payload := make([]byte, config.PayloadBytes)
	for i := range payload {
		payload[i] = byte(rand.Intn(256))
	}
	var id int

	for {
		if limiter.Allow() {
			// Cycle id over 1..N.
			id++
			if id > config.Rows {
				id = 1
			}

			_, _, err := conn.Exec(ctx, updateSQL, payload, id)
			if err != nil {
				// A clean ctx stop must not be reported as a failure.
				if ctx.Err() != nil {
					return nil
				}

				if counter.Load() > 0 {
					logger.Infof("slot-bloat: connection lost — target likely disk-full/restarted")
					return nil
				}

				// First Exec failed with no prior success: this is a setup defect,
				// not a successful disk-full event.
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

// cleanup drops the slot and table on a fresh context.Background() (ctx is already
// cancelled at exit), bounded by a short timeout so a hung (not dead) target cannot
// block Run's return forever. It opens its OWN fresh connection rather than reusing
// the workload conn: a Ctrl+C landing mid-UPDATE aborts the in-flight query and
// poisons that conn, so a DROP over it would fail and orphan the slot. By default
// both are dropped and "slot dropped" is logged only on a fully successful drop; any
// failure is logged via Warnf with the object names so nothing is silently orphaned —
// the slot-drop error is surfaced in preference to the table-drop error since an
// orphaned slot is the more dangerous leak. With KeepSlot both are kept and their
// names are logged. Every error passes through sanitize.
func (w *workload) cleanup(slotName, tableIdent string) {
	if w.config.KeepSlot {
		w.logger.Infof("slot-bloat: slot kept: %s, table kept: %s — drop manually", slotName, tableIdent)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Open a fresh connection: the workload conn may be dead after a mid-UPDATE cancel.
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		// The target may be unreachable (e.g. in PANIC at real disk-full). Nothing was
		// dropped — say so honestly so the slot is not silently assumed gone.
		w.logger.Warnf("slot-bloat: cleanup failed for slot %s, table %s: %s — drop manually", slotName, tableIdent, sanitize(err))
		return
	}
	defer func() { _ = conn.Close() }()

	// Attribute the cleanup DROPs in pg_stat_activity (like Run does). Best-effort:
	// the drops and their honest logging are what matter, so a failed SET must not
	// abort cleanup — ignore the error and proceed. Conninfo is never logged.
	_, _, _ = conn.Exec(ctx, "SET application_name = 'noisia'")

	_, _, terr := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableIdent))
	_, _, serr := conn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slotName)
	if terr != nil || serr != nil {
		// Surface the slot-drop error first (the orphaned slot is the more dangerous
		// leak); join both so the table-drop error is not lost.
		w.logger.Warnf("slot-bloat: cleanup failed for slot %s, table %s: %s — drop manually", slotName, tableIdent, sanitize(errors.Join(serr, terr)))
		return
	}

	w.logger.Infof("slot-bloat: slot dropped")
}

// randomSuffix returns a random string of length n drawn from [a-z0-9]. The charset
// guarantees an injection-safe SQL identifier, and a per-run suffix keeps reruns
// from colliding on the persistent slot/table names.
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
