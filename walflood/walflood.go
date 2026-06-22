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
	"time"

	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/log"
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

// Run drives the WAL flood. The seed, the Jobs-way disjoint-range churn fan-out, the
// reporter and the cleanup are implemented in Task 02.
func (w *workload) Run(ctx context.Context) error {
	// TODO(task-02): seed table, spawn Jobs churn workers over disjoint ranges,
	// run the reporter ticker, and drop the seed table on a fresh connection.
	return nil
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
