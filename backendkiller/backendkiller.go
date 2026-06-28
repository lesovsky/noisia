// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package backendkiller defines implementation of workload which deliberately
// drives a single PostgreSQL backend towards an out-of-memory condition.
//
// A single dedicated connection (not a pool) issues, in a single-threaded
// rate-limited loop, unique literal server-side prepared statements of the form
// "PREPARE noisia_bk_<i> AS SELECT 0 AS c0, 1 AS c1, …" and immediately EXECUTEs
// each one once. PREPARE caches the statement's parse/rewrite tree; the first
// EXECUTE builds and caches its generic plan as well, so each pair grows the
// backend's plan cache (and RSS) faster than PREPARE alone. The statements are
// never DEALLOCATEd, so the plan cache (and the backend's RSS) grows without
// bound. Eventually the OOM killer reaps the backend and the postmaster restarts
// the whole instance.
//
// The workload self-reports: it keeps an in-process counter of created statements
// and prints an escalation panel every report-interval via logger.Infof. When its
// own connection is lost after at least one successful PREPARE (the target was
// likely OOM-restarted), it logs a climax line and returns nil.
// Workload duration is controlled by a context created outside and passed to Run.
package backendkiller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"golang.org/x/time/rate"
)

// Config defines configuration settings for backend-killer workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	// It is a secret and must never be logged.
	Conninfo string
	// Rate defines PREPARE statements rate per second; 0 means unlimited (rate.Inf).
	Rate float64
	// PlanSize defines the target-list width per PREPARE (plan heaviness).
	PlanSize int
	// ShowMemory enables appending the own-backend memory estimate to the panel.
	ShowMemory bool
	// ReportInterval defines the escalation panel print cadence.
	ReportInterval time.Duration
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Conninfo == "" {
		return fmt.Errorf("conninfo must not be empty")
	}

	if c.Rate < 0 {
		return fmt.Errorf("rate must not be negative")
	}

	if c.PlanSize < 1 {
		return fmt.Errorf("plan size must be greater than zero")
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

// Run opens a single dedicated connection and drives the plan-cache-leak loop
// while reporting progress on a separate ticker goroutine.
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

	// counter holds the number of successfully created prepared statements.
	// mem holds the last own-backend memory estimate (bytes). Both are read by the
	// report ticker goroutine, which must never touch the (not concurrency-safe) Conn.
	var counter atomic.Int64
	var mem atomic.Int64

	start := time.Now()
	tickerDone := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.runReporter(ctx, tickerDone, start, &counter, &mem)
	}()

	err = runLoop(ctx, conn, w.logger, w.config, &counter, &mem)

	close(tickerDone)
	wg.Wait()

	return err
}

// runReporter prints the escalation panel every ReportInterval, reading only the
// atomic counter and memory estimate. It never queries the Conn.
func (w *workload) runReporter(ctx context.Context, done <-chan struct{}, start time.Time, counter, mem *atomic.Int64) {
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
			r := float64(cur-prev) / w.config.ReportInterval.Seconds()
			prev = cur

			if w.config.ShowMemory {
				if m := mem.Load(); m > 0 {
					w.logger.Infof("backend-killer: prepared stmts=%d rate=%.0f/s elapsed=%s + backend mem≈%d bytes", cur, r, elapsed.Truncate(time.Second), m)
					continue
				}
			}

			w.logger.Infof("backend-killer: prepared stmts=%d rate=%.0f/s elapsed=%s", cur, r, elapsed.Truncate(time.Second))
		}
	}
}

// runLoop is the single-threaded rate-limited PREPARE+EXECUTE loop. It owns the
// Conn: each iteration issues a PREPARE then EXECUTEs it once and, when enabled,
// performs the periodic own-backend memory read. It increments counter once per
// completed PREPARE+EXECUTE pair (the pair is one statement; EXECUTE is a memory
// amplifier, not a separate metric) and publishes the memory estimate via mem.
//
// Returns nil on a clean ctx stop. On a connection loss after at least one
// completed pair (counter>0) while the context is still live, it logs the climax
// line and returns nil. An Exec error on the very first statement, in either the
// PREPARE or the EXECUTE phase (counter==0), is returned as an init error, so a
// broken builder cannot masquerade as a successful OOM event.
func runLoop(ctx context.Context, conn db.Conn, logger log.Logger, config Config, counter, mem *atomic.Int64) error {
	lim := rate.Limit(config.Rate)
	if config.Rate == 0 {
		lim = rate.Inf
	}
	limiter := rate.NewLimiter(lim, 1)

	start := time.Now()
	var i int64
	var lastMemoryRead time.Time
	var memWarned bool

	// fail maps an Exec error (from either the PREPARE or the EXECUTE) to runLoop's
	// return value: nil on a clean ctx stop, nil after logging the climax once at
	// least one statement landed (the target was likely OOM-restarted), or an init
	// error when the very first statement failed (a setup/builder defect, never a
	// climax). Both phases share this because an OOM can reap the backend at either.
	fail := func(err error) error {
		// A clean ctx stop must not be reported as a failure.
		if ctx.Err() != nil {
			return nil
		}

		if counter.Load() > 0 {
			logger.Infof("backend-killer: connection lost after %s, %d statements issued — target likely OOM-restarted", time.Since(start).Truncate(time.Second), counter.Load())
			return nil
		}

		return fmt.Errorf("issue prepared statement failed: %s", sanitize(err))
	}

	for {
		if limiter.Allow() {
			q := buildPrepare(i, config.PlanSize)
			id := i
			i++

			if _, _, err := conn.Exec(ctx, q); err != nil {
				return fail(err)
			}

			// EXECUTE the freshly prepared statement once. PREPARE caches only the
			// parse/rewrite tree; the first EXECUTE builds and caches the generic
			// plan as well (these statements have no bind args, so the generic plan
			// is used immediately). That roughly doubles the per-statement memory and
			// reaches OOM sooner. The result row is discarded.
			if _, _, err := conn.Exec(ctx, buildExecute(id)); err != nil {
				return fail(err)
			}

			// Count the statement only once its EXECUTE has also landed: the generic
			// plan (the bulk of the leaked memory) is cached only after EXECUTE, and
			// the counter==0 init-error guard in fail() must reject a first-EXECUTE
			// builder defect just as it rejects a first-PREPARE one. counter therefore
			// tracks completed PREPARE+EXECUTE pairs.
			counter.Add(1)

			// Optional own-backend memory read on the report cadence. Performed by
			// this (Conn-owning) goroutine only.
			if config.ShowMemory && time.Since(lastMemoryRead) >= config.ReportInterval {
				lastMemoryRead = time.Now()
				used, rerr := readBackendMemory(ctx, conn)
				if rerr != nil {
					// Per-tick recoverable (read failure / PG<14): warn once, drop
					// the field, keep going. Never a climax.
					if !memWarned && ctx.Err() == nil {
						logger.Warnf("backend-killer: reading backend memory failed: %s, dropping memory field", sanitize(rerr))
						memWarned = true
					}
				} else {
					mem.Store(used)
				}
			}
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

// buildPrepare builds a unique heavy server-side PREPARE statement:
// "PREPARE noisia_bk_<i> AS SELECT 0 AS c0, 1 AS c1, … , (planSize-1) AS c{planSize-1}".
// It uses only validated ints with %d formatting — there is no bind argument and
// no external input, so there is no injection surface.
func buildPrepare(i int64, planSize int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "PREPARE noisia_bk_%d AS SELECT ", i)
	for j := 0; j < planSize; j++ {
		if j > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%d AS c%d", j, j)
	}
	return b.String()
}

// buildExecute builds the EXECUTE statement for the prepared statement id created
// by buildPrepare: "EXECUTE noisia_bk_<i>". The statement takes no parameters, so
// there is no bind argument and no injection surface.
func buildExecute(i int64) string {
	return fmt.Sprintf("EXECUTE noisia_bk_%d", i)
}

// readBackendMemory reads the total bytes used by the current backend's memory
// contexts (PG 14+). On PG<14 the relation is absent and an error is returned.
func readBackendMemory(ctx context.Context, conn db.Conn) (int64, error) {
	rows, err := conn.Query(ctx, "SELECT sum(used_bytes) FROM pg_backend_memory_contexts")
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var used int64
	for rows.Next() {
		if err := rows.Scan(&used); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	return used, nil
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
