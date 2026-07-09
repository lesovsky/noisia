// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package waitxacts defines implementation of workload which builds a stable queue
// of blocked sessions behind a single locked table.
//
// One dedicated holder connection cycles an ACCESS EXCLUSIVE lock on ONE table
// (the first of the top-write tables, or a dedicated fixture table): it holds the
// lock a random interval in [LocktimeMin..LocktimeMax], rolls back, and re-acquires
// almost immediately (a flicker, so the blocked-session count never falls to zero
// between cycles). A rate-limited manager opens N separate client connections, each
// blocking on that table with a SELECT, so the count of established (and therefore
// blocked) connections climbs monotonically and — when unbounded or driven fast
// enough — exhausts max_connections. A connect error (e.g. too-many-clients) is the
// expected observable: it increments a refused counter, is logged rate-limited, and
// never crashes the run. A reporter ticker prints a self-report line from atomics.
//
// Workload duration is controlled by a context created outside and passed to Run.
package waitxacts

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
	"github.com/lesovsky/noisia/targeting"
	"golang.org/x/time/rate"
)

const (
	// fixtureTable is the dedicated table locked when no top-write table is targeted.
	fixtureTable = "_noisia_waitxacts_workload"
	// waiterPauseInterval is the pause a waiter makes between re-issuing its blocking
	// SELECT — it prevents a busy-spin in the holder's brief OFF window (Decision 7).
	waiterPauseInterval = 100 * time.Millisecond
	// holderReLockBackoff is the pause before the holder re-establishes its lock after
	// a mid-run connection loss (Decision 7).
	holderReLockBackoff = time.Second
	// managerPollInterval paces the manager's spawn/cap check loop; the actual spawn
	// rate is governed by the rate limiter, this only bounds the loop's CPU use.
	managerPollInterval = 10 * time.Millisecond
	// refusedLogPerSecond throttles the rate-limited "connect refused" log so a cascade
	// does not flood the log (Decision 7: at most once per second).
	refusedLogPerSecond = 1
)

// Config defines configuration settings for waiting transactions workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	// It is a secret and must never be logged.
	Conninfo string
	// Fixture forces the dedicated fixture table instead of a real top-write table.
	Fixture bool
	// LocktimeMin defines a lower threshold of the lock hold interval.
	LocktimeMin time.Duration
	// LocktimeMax defines an upper threshold of the lock hold interval.
	LocktimeMax time.Duration
	// Waiters caps the number of blocked waiter connections; 0 means unlimited.
	Waiters uint
	// WaitersRate defines how many waiter connections are opened per second (> 0).
	WaitersRate float64
	// ReportInterval defines the self-report line print cadence (> 0).
	ReportInterval time.Duration
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.WaitersRate <= 0 {
		return fmt.Errorf("waiters rate must be greater than zero")
	}

	if c.ReportInterval <= 0 {
		return fmt.Errorf("report interval must be positive")
	}

	if c.LocktimeMin <= 0 || c.LocktimeMax <= 0 {
		return fmt.Errorf("min and max lock time must be greater than zero")
	}

	if c.LocktimeMin > c.LocktimeMax {
		return fmt.Errorf("min lock time must be less or equal to max lock time")
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

// Run selects the target table (using a setup pool that is closed right after setup),
// launches the holder + waiter manager + reporter, and blocks until the context is
// cancelled. On exit every holder/waiter goroutine is drained through a shared
// WaitGroup BEFORE the fixture table is dropped via a fresh connection.
func (w *workload) Run(ctx context.Context) error {
	tableIdent, usingFixture, err := w.setup(ctx)
	if err != nil {
		// setup opens the pool / reads the catalog / creates the fixture — its pgx
		// errors can carry DSN fragments (host=/user=/…), so never surface them raw:
		// this error propagates out to cmd/app.go's logger (matches walflood.Run).
		return fmt.Errorf("waitxacts setup failed: %s", sanitize(err))
	}

	// Drop the fixture table on exit via a fresh connection (ADR-002-2). Registered
	// only when we created it — a real top-write table must never be dropped.
	if usingFixture {
		defer w.cleanup(tableIdent)
	}

	// A derived context lets a startup lock failure tear everything down deterministically.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var established, refused atomic.Int64

	// Reporter: reads only atomics, never touches a Conn (ADR-002-3).
	tickerDone := make(chan struct{})
	var reporterWg sync.WaitGroup
	reporterWg.Add(1)
	go func() {
		defer reporterWg.Done()
		w.runReporter(runCtx, tickerDone, tableIdent, &established, &refused)
	}()

	// wg is the shared drain barrier for the holder and all waiters (Decision 9).
	var wg sync.WaitGroup

	// Holder: reports its first-lock result on holderStart. A startup failure is fatal.
	holderStart := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.runHolder(runCtx, tableIdent, holderStart)
	}()

	var runErr error
	select {
	case <-ctx.Done():
		// Cancelled before the holder established: fall through to the drain.
	case err := <-holderStart:
		if err != nil {
			runErr = err
			cancel() // startup lock failed — tear everything down
		}
	}

	if runErr == nil && ctx.Err() == nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.runManager(runCtx, &wg, tableIdent, &established, &refused)
		}()

		<-ctx.Done()
		cancel()
	}

	// Drain barrier: wait for the holder and every waiter to exit (and close their
	// connections) BEFORE the deferred fresh-connection cleanup drops the table.
	wg.Wait()
	close(tickerDone)
	reporterWg.Wait()

	return runErr
}

// setup opens a short-lived pool to pick the target table and prepare the fixture,
// then closes the pool (Decision 8) so it does not hold a slot for the whole run.
func (w *workload) setup(ctx context.Context) (tableIdent string, usingFixture bool, err error) {
	pool, err := db.NewPostgresDB(ctx, w.config.Conninfo)
	if err != nil {
		return "", false, err
	}
	defer pool.Close()

	// Prefer the top-write table unless fixture mode is forced.
	if !w.config.Fixture {
		tables, terr := targeting.TopWriteTables(pool, 1)
		if terr != nil {
			return "", false, terr
		}
		if len(tables) > 0 {
			tableIdent = tables[0]
		}
	}

	// No real table to target: use the dedicated fixture table.
	if tableIdent == "" {
		tableIdent = fixtureTable
		usingFixture = true
		if perr := prepare(ctx, pool, tableIdent); perr != nil {
			return "", false, perr
		}
	}

	return tableIdent, usingFixture, nil
}

// runHolder drives one dedicated connection through the flicker cycle. It reports the
// result of the FIRST lock acquisition on startup (a failure there is fatal), then
// keeps cycling with best-effort re-establishment on a mid-run connection loss.
func (w *workload) runHolder(ctx context.Context, tableIdent string, startup chan<- error) {
	var conn db.Conn
	established := false
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// (Re)connect if needed.
		if conn == nil {
			c, cerr := db.Connect(ctx, w.config.Conninfo)
			if cerr != nil {
				if ctx.Err() != nil {
					return
				}
				if !established {
					startup <- fmt.Errorf("holder connect failed: %s", sanitize(cerr))
					return
				}
				w.logger.Warnf("waitxacts: holder reconnect failed, retrying: %s", sanitize(cerr))
				if !sleepCtx(ctx, holderReLockBackoff) {
					return
				}
				continue
			}
			// db.Connect does not set application_name; do it best-effort so the holder
			// is attributable in pg_stat_activity. A failed SET must not abort the holder.
			if _, _, serr := c.Exec(ctx, "SET application_name = 'noisia'"); serr != nil {
				w.logger.Warnf("waitxacts: holder set application_name failed: %s", sanitize(serr))
			}
			conn = c
		}

		// On the first cycle, report a successful startup the moment the lock is
		// acquired (not after the hold, which lasts up to LocktimeMax) so the manager
		// can start spawning waiters immediately.
		var onLocked func()
		if !established {
			onLocked = func() {
				startup <- nil
				established = true
			}
		}

		// One lock cycle: BEGIN, LOCK, hold, ROLLBACK.
		if herr := w.holdCycle(ctx, conn, tableIdent, onLocked); herr != nil {
			if ctx.Err() != nil {
				return
			}
			if !established {
				startup <- fmt.Errorf("holder initial lock failed: %s", sanitize(herr))
				return
			}
			w.logger.Warnf("waitxacts: holder lost lock, re-establishing: %s", sanitize(herr))
			_ = conn.Close()
			conn = nil
			if !sleepCtx(ctx, holderReLockBackoff) {
				return
			}
			continue
		}
		// OFF window = 0: loop and re-acquire immediately (flicker).
	}
}

// holdCycle acquires an ACCESS EXCLUSIVE lock in an explicit transaction, holds it a
// random interval in [LocktimeMin..LocktimeMax], and rolls back. On ctx cancellation
// during the hold it returns ctx.Err() and the caller closes the conn (which aborts
// the transaction and releases the lock server-side).
func (w *workload) holdCycle(ctx context.Context, conn db.Conn, tableIdent string, onLocked func()) error {
	if _, _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		return err
	}

	// The identifier is a trusted catalog source (fixture constant or schema.relname
	// from pg_stat_user_tables) — never CLI input — so it is used as a bare %s
	// (Decision 10; workloads must not import pgx to quote it).
	if _, _, err := conn.Exec(ctx, fmt.Sprintf("LOCK TABLE %s IN ACCESS EXCLUSIVE MODE", tableIdent)); err != nil {
		_, _, _ = conn.Exec(ctx, "ROLLBACK")
		return err
	}

	// The lock is held now; notify the caller before the (long) hold begins.
	if onLocked != nil {
		onLocked()
	}

	timer := time.NewTimer(w.randomLocktime())
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	if _, _, err := conn.Exec(ctx, "ROLLBACK"); err != nil {
		return err
	}
	return nil
}

// randomLocktime returns a random hold interval in [LocktimeMin..LocktimeMax].
func (w *workload) randomLocktime() time.Duration {
	minNs := w.config.LocktimeMin.Nanoseconds()
	maxNs := w.config.LocktimeMax.Nanoseconds()
	if maxNs <= minNs {
		return time.Duration(minNs)
	}
	return time.Duration(minNs + rand.Int63n(maxNs-minNs))
}

// runManager spawns waiter goroutines at WaitersRate until the established gauge
// reaches the Waiters cap (0 = unlimited), refilling killed waiters within the rate
// budget. It stops spawning as soon as the context is cancelled.
func (w *workload) runManager(ctx context.Context, wg *sync.WaitGroup, tableIdent string, established, refused *atomic.Int64) {
	limiter := rate.NewLimiter(rate.Limit(w.config.WaitersRate), 1)
	// A dedicated 1/s limiter throttles refusal logging to a fixed cadence regardless
	// of WaitersRate (Decision 7). Shared across waiters so the whole pile logs at most
	// once per second during a cascade.
	logLimiter := rate.NewLimiter(rate.Limit(refusedLogPerSecond), 1)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Enforce a hard cap: the gauge is incremented HERE (a slot reservation),
		// synchronously in this single manager goroutine, so the next iteration's read
		// already reflects this spawn — the manager can never overshoot Waiters even at
		// a high WaitersRate. Only this goroutine ever increments the gauge; waiters
		// decrement it on exit (so killed waiters free a slot and get refilled).
		if w.config.Waiters == 0 || established.Load() < int64(w.config.Waiters) {
			if limiter.Allow() {
				established.Add(1)
				wg.Add(1)
				go func() {
					defer wg.Done()
					w.runWaiter(ctx, tableIdent, established, refused, logLimiter)
				}()
			}
		}

		if !sleepCtx(ctx, managerPollInterval) {
			return
		}
	}
}

// runWaiter opens its OWN dedicated connection and blocks on the locked table. The
// manager already reserved this waiter's slot in the established gauge, so runWaiter
// ALWAYS decrements it exactly once on exit (freeing the slot for a refill), whether
// the connect succeeds, is refused, or the waiter is later killed. A connect error is
// the expected too-many-clients observable: it increments refused, is logged
// rate-limited, keeps the pile, and never crashes the run.
func (w *workload) runWaiter(ctx context.Context, tableIdent string, established, refused *atomic.Int64, logLimiter *rate.Limiter) {
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		// Release the reserved slot so the manager can refill within the rate budget.
		established.Add(-1)
		if ctx.Err() != nil {
			return
		}
		// Any connect error while opening a waiter is treated as the expected
		// too-many-clients refusal (no SQLSTATE parse): count it, log rate-limited,
		// keep the pile.
		refused.Add(1)
		if logLimiter.Allow() {
			w.logger.Warnf("waitxacts: waiter connect refused: %s", sanitize(err))
		}
		return
	}

	defer func() {
		_ = conn.Close()
		established.Add(-1)
	}()

	// db.Connect does not set application_name; do it best-effort so the waiter is
	// attributable in pg_stat_activity (needed for the counting/refusal mechanics).
	if _, _, serr := conn.Exec(ctx, "SET application_name = 'noisia'"); serr != nil {
		w.logger.Warnf("waitxacts: waiter set application_name failed: %s", sanitize(serr))
	}

	query := fmt.Sprintf("SELECT * FROM %s", tableIdent)
	for {
		// This blocks on the held lock until the holder's OFF window, then re-issues.
		if _, _, qerr := conn.Exec(ctx, query); qerr != nil {
			// A ctx-cancel or a server-side reset ends this waiter; the manager refills.
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(waiterPauseInterval):
		}
	}
}

// runReporter prints one self-report line every ReportInterval, reading ONLY the
// atomics (ADR-002-3). It never touches a Conn and never polls pg_stat_activity.
func (w *workload) runReporter(ctx context.Context, done <-chan struct{}, tableIdent string, established, refused *atomic.Int64) {
	ticker := time.NewTicker(w.config.ReportInterval)
	defer ticker.Stop()

	target := "unlimited"
	if w.config.Waiters > 0 {
		target = fmt.Sprintf("%d", w.config.Waiters)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			w.logger.Infof("waitxacts: established=%d target=%s refused=%d locked_table=%s",
				established.Load(), target, refused.Load(), tableIdent)
		}
	}
}

// prepare creates the fixture lock table. An empty table under ACCESS EXCLUSIVE
// blocks a SELECT just as well, so no rows are needed.
func prepare(ctx context.Context, pool db.DB, tableIdent string) error {
	_, _, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (payload bigint)", tableIdent))
	return err
}

// cleanup drops the fixture table on a fresh context.Background() (the run ctx is
// already cancelled at exit), bounded by a short timeout. It opens its OWN fresh
// connection rather than reusing a holder/waiter conn: a Ctrl+C landing mid-query
// poisons those conns, so a DROP over them would fail and orphan the table
// (ADR-002-2). Every error passes through sanitize so a pgx error cannot leak a DSN.
func (w *workload) cleanup(tableIdent string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		w.logger.Warnf("waitxacts: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}
	defer func() { _ = conn.Close() }()

	// Attribute the cleanup DROP in pg_stat_activity. Best-effort.
	_, _, _ = conn.Exec(ctx, "SET application_name = 'noisia'")

	_, _, err = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableIdent))
	if err != nil {
		w.logger.Warnf("waitxacts: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}

	w.logger.Infof("waitxacts: fixture table dropped")
}

// sleepCtx sleeps for d or returns early if ctx is cancelled. It returns false when
// the context was cancelled (the caller should stop).
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
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
