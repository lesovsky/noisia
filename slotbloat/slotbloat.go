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
	"fmt"
	"strings"
	"time"

	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/log"
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

// Run drives the slot-bloat workload.
//
// TODO(task-02): implement the full mechanics — single db.Connect connection,
// physical slot creation (immediately_reserve), seed-table prepare(), the
// rate-limited UPDATE-churn loop with the payload-bytes counter, the reporter
// goroutine, init-error/climax/ctx.Done handling, and best-effort cleanup on a
// separate context.Background() honouring KeepSlot. This task lays the package
// foundation only; there is no DB interaction yet.
func (w *workload) Run(ctx context.Context) error {
	return fmt.Errorf("not implemented")
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
