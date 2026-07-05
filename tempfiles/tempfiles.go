// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package tempfiles defines a workload which executes queries that produce on-disk
// temporary files because their sort genuinely needs more memory than the server's
// work_mem — NOT because work_mem is artificially lowered. On a default-configured
// PostgreSQL (work_mem = 4MB) every query spills to an external merge; raising work_mem
// (per-role / per-database / session) makes the same query run fully in memory, so the
// "size work_mem correctly" remediation can actually be demonstrated.
//
// On start the workload seeds its own per-run dataset table
// (noisia_tempfiles_<random8>, a wide incompressible (id, payload) heap of tens of MiB)
// on a dedicated connection, following the bloatchurn/slotbloat convention. It then fans
// out Jobs workers; each opens its own connection pool and, in a rate-limited loop, fires
// a big "ORDER BY random()" sort over the dataset in a dedicated goroutine — asynchronously,
// so a slow spilling query does not throttle the required rate (this is why a pool, not a
// single connection, is used per worker: several spilling queries may be in flight at once,
// the deliberate exception to ADR-003-1). The dataset table is dropped on exit on a fresh
// connection (ADR-002-2). Total temp bytes are reported before/after via
// pg_stat_get_db_temp_bytes(); this is database-wide and cumulative, so the delta may
// include temp bytes produced by concurrent workload in the same database.
//
// Workload duration is controlled by a context created outside and passed to Run. The
// context is the only stop mechanism: when it expires the loop drains in-flight queries
// and stops.
package tempfiles

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"golang.org/x/time/rate"
)

// datasetSize is the target on-disk size of the per-run dataset table. A full sort of the
// table must comfortably exceed the default work_mem (4MB) so every query spills to an
// external merge on a default-configured server; 64 MiB leaves a wide margin (robust to a
// future default-work_mem bump) while still seeding fast enough for short runs. It is a var,
// not a const, only so tests can shrink the seed for speed.
var datasetSize int64 = 64 << 20

const (
	// payloadBytes is the width of the incompressible payload column per row. It is kept
	// inline (below the ~2KB TOAST threshold) so the bytes sit in the heap and are carried
	// through the sort. The buffer MUST be filled with random (incompressible) bytes, never
	// zeros: an all-zeros bytea pglz-compresses to almost nothing, so the table — and the
	// sort volume — would collapse far below datasetSize and might not spill (ADR-008-2).
	payloadBytes int = 256

	// headerBytesPerRow is the rough per-row overhead (≈ 24-byte tuple header + 8-byte id
	// bigint) used by the size→rows estimate. It only needs to land the seed in the right
	// ballpark of the target size.
	headerBytesPerRow int64 = 32
)

// Config defines configuration settings for temp files workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	// It is a secret and must never be logged.
	Conninfo string
	// Jobs defines how many workers should be created for producing temp files.
	Jobs uint16
	// Rate defines rate interval for queries executing (queries per second, per worker).
	Rate float64
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be greater than zero")
	}

	if c.Rate <= 0 {
		return fmt.Errorf("temp files queries rate must be positive")
	}

	return nil
}

// workload implements noisia.Workload interface. tableIdent is populated at seed time and
// read by cleanup and the workers.
type workload struct {
	config     Config
	logger     log.Logger
	tableIdent string // double-quoted identifier used in CREATE/DROP/work SQL
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config Config, logger log.Logger) (noisia.Workload, error) {
	err := config.validate()
	if err != nil {
		return nil, err
	}

	return &workload{config: config, logger: logger}, nil
}

// Run seeds the dataset table on a dedicated connection, fans out Jobs workers that sort it
// (spilling temp files), then drops the table on exit and reports the temp-bytes delta. The
// delta is imperfect: pg_stat_get_db_temp_bytes is database-wide and cumulative, so there is
// no way to know exactly how many temp bytes this workload alone produced. The only stop
// mechanism is the supplied ctx.
func (w *workload) Run(ctx context.Context) error {
	bytesBefore, err := countTempBytes(w.config.Conninfo)
	if err != nil {
		return fmt.Errorf("count temp bytes failed: %s", sanitize(err))
	}

	// Seed on a dedicated connection. db.Connect (unlike the pool) does not set
	// application_name, so set it locally so the seeding backend is attributable.
	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		// Never echo the raw error: a pgx connect error may carry DSN fragments.
		return fmt.Errorf("connect to target failed: %s", sanitize(err))
	}

	if _, _, err := conn.Exec(ctx, "SET application_name = 'noisia'"); err != nil {
		_ = conn.Close()
		return fmt.Errorf("set application_name failed: %s", sanitize(err))
	}

	// Build the per-run table name once. The suffix is restricted to [a-z0-9], which alone
	// makes the identifier injection-safe; double-quoting is belt-and-suspenders.
	w.tableIdent = "\"noisia_tempfiles_" + randomSuffix(8) + "\""

	// Cleanup is registered before seeding so a mid-seed cancel still drops the partial table.
	// It opens its own fresh connection on context.Background() because ctx is already
	// cancelled at exit and a drop on a cancelled ctx would fail (ADR-002-2).
	defer w.cleanup(w.tableIdent)

	rows := rowsForSize(datasetSize, int64(payloadBytes))
	w.logger.Infof("tempfiles: seeding dataset %s (~%s, %d rows) — queries sort it and spill temp files; watch pg_stat_database.temp_files/temp_bytes and log_temp_files",
		w.tableIdent, formatBytes(datasetSize), rows)
	err = prepare(ctx, conn, w.tableIdent, rows)
	_ = conn.Close() // the seed connection is not needed once the dataset exists
	if err != nil {
		// A clean ctx stop must not be reported as a failure.
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("seed dataset failed: %s", sanitize(err))
	}

	// The sort query carries the full (id, payload) row set through an ORDER BY random(),
	// whose volume ≈ the table size, so it spills to an external merge on the default work_mem.
	// The table identifier cannot be a bind parameter; it is interpolated (injection-safe suffix).
	query := fmt.Sprintf("SELECT id, payload FROM %s ORDER BY random()", w.tableIdent)

	workers := int(w.config.Jobs)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if err := runWorker(ctx, w.logger, w.config, query); err != nil {
				w.logger.Warnf("start tempfiles worker failed: %s, continue", sanitize(err))
			}
		}()
	}

	wg.Wait()

	bytesAfter, err := countTempBytes(w.config.Conninfo)
	if err != nil {
		return fmt.Errorf("count temp bytes failed: %s", sanitize(err))
	}
	w.logger.Infof("generated %d temp bytes (might include temp bytes produced by concurrent workload)", bytesAfter-bytesBefore)

	return nil
}

// runWorker opens its own connection pool and starts the tempfiles loop. A pool (not a single
// connection) is used because the loop executes queries asynchronously and several spilling
// queries might be in flight concurrently — a single pgx connection is not safe for concurrent
// queries. The per-worker pool is the deliberate exception to ADR-003-1: because each worker
// has its OWN pool, --jobs parallelism is not capped by a shared pool.
func runWorker(ctx context.Context, logger log.Logger, config Config, query string) error {
	logger.Info("start tempfiles worker")

	// db.NewPostgresDB sets application_name=noisia on the pool, so no manual SET here.
	pool, err := db.NewPostgresDB(ctx, config.Conninfo)
	if err != nil {
		return err
	}

	defer pool.Close()

	// startLoop returns nil on a clean ctx stop and never surfaces a per-query error (those are
	// logged and the loop continues), so this only ever returns nil today; the plumbing is kept
	// for symmetry with the sibling workers.
	err = startLoop(ctx, pool, logger, config.Rate, query)
	if err != nil {
		return err
	}

	logger.Infof("tempfiles worker finished")
	return nil
}

// startLoop executes the sort query in a loop at the required rate until the context expires.
func startLoop(ctx context.Context, pool db.DB, logger log.Logger, r float64, query string) error {
	var wg sync.WaitGroup

	limiter := rate.NewLimiter(rate.Limit(r), 1)
	for {
		if limiter.Allow() {
			wg.Add(1)

			// Because a spilling query can run for a while, execute it asynchronously so it
			// does not hold up the required rate. Don't wait for it to finish here.
			go func() {
				defer wg.Done()
				// Ignore errors related to context expiration.
				if err := execQuery(ctx, pool, query); err != nil && ctx.Err() == nil {
					logger.Warnf("executing tempfiles query failed: %s, continue", sanitize(err))
				}
			}()
		}

		select {
		case <-ctx.Done():
			wg.Wait()
			return nil
		default:
		}
	}
}

// execQuery executes the sort query which spills to a temp file when its sort exceeds work_mem.
// It issues exactly this one query — no SET work_mem — so raising work_mem globally / per-role /
// per-database can actually remove the spill.
func execQuery(ctx context.Context, pool db.DB, query string) error {
	_, _, err := pool.Exec(ctx, query)
	return err
}

// prepare creates the per-run dataset table and seeds it with N rows of an incompressible
// payload in a single transaction. The table is (id bigint, payload bytea) with NO index: the
// workload only sorts it, never looks rows up by key. The identifier is interpolated (it cannot
// be a bind param); the payload buffer is bound as $1 and the row count as $2.
func prepare(ctx context.Context, conn db.Conn, tableIdent string, rows int64) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, _, err = tx.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id bigint, payload bytea)", tableIdent))
	if err != nil {
		return err
	}

	// The payload must be INCOMPRESSIBLE (random bytes, not zeros): an all-zeros bytea
	// compresses to almost nothing, so the seed — and the sort volume — would fall far short
	// of datasetSize and might not spill (ADR-008-2). The buffer is filled once and reused for
	// every row (heap tuples are not deduplicated, so the table still reaches the target size).
	payload := make([]byte, payloadBytes)
	for i := range payload {
		payload[i] = byte(rand.Intn(256))
	}

	// Seed in a single set-based statement: one server-side round-trip instead of N network
	// round-trips, which matters for large N over a remote link.
	insertSQL := fmt.Sprintf("INSERT INTO %s (id, payload) SELECT g, $1 FROM generate_series(1, $2) AS g", tableIdent)
	_, _, err = tx.Exec(ctx, insertSQL, payload, rows)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// dropTable drops the dataset table over the provided connection. It is split out from cleanup
// so the DROP can be exercised over an independently provided conn (self-sufficiency).
func dropTable(ctx context.Context, conn db.Conn, tableIdent string) error {
	_, _, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tableIdent))
	return err
}

// cleanup drops the dataset table on a fresh context.Background() (the run ctx is already
// cancelled at exit), bounded by a short timeout so a hung target cannot block forever. It
// opens its OWN fresh connection rather than reusing a worker/pool conn: a Ctrl+C landing
// mid-query can poison a connection, so a DROP over it would fail and orphan the table
// (ADR-002-2). Any failure is logged via Warnf naming the table so nothing is silently
// orphaned. Every error passes through sanitize so a pgx error can never leak a DSN.
func (w *workload) cleanup(tableIdent string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := db.Connect(ctx, w.config.Conninfo)
	if err != nil {
		w.logger.Warnf("tempfiles: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}
	defer func() { _ = conn.Close() }()

	// Attribute the cleanup DROP in pg_stat_activity. Best-effort: the drop and its honest
	// logging are what matter, so a failed SET must not abort cleanup.
	_, _, _ = conn.Exec(ctx, "SET application_name = 'noisia'")

	if err := dropTable(ctx, conn, tableIdent); err != nil {
		w.logger.Warnf("tempfiles: cleanup failed for table %s: %s — drop manually", tableIdent, sanitize(err))
		return
	}

	w.logger.Infof("tempfiles: dataset table dropped")
}

// countTempBytes queries current database statistics about temp bytes written. A private
// context is used here, because this is an auxiliary routine and is not related to the main
// workload.
func countTempBytes(conninfo string) (int, error) {
	bytes := -1 // zero could be returned from database and it is valid value

	conn, err := db.Connect(context.Background(), conninfo)
	if err != nil {
		return bytes, err
	}

	defer func() { _ = conn.Close() }()

	rows, err := conn.Query(context.Background(), "SELECT pg_stat_get_db_temp_bytes(oid) from pg_database where datname = current_database()")
	if err != nil {
		return bytes, err
	}

	for rows.Next() {
		err = rows.Scan(&bytes)
		if err != nil {
			return bytes, err
		}
	}

	return bytes, nil
}

// rowsForSize converts a target byte size into a seed row count via a payload-aware
// bytes-per-row estimate (headerBytesPerRow + payloadBytes), with a floor of one row so a
// tiny (or nonsensical) input never yields a zero-row table.
func rowsForSize(tableSize, payloadBytes int64) int64 {
	bytesPerRow := headerBytesPerRow + payloadBytes
	n := tableSize / bytesPerRow
	if n < 1 {
		return 1
	}
	return n
}

// randomSuffix returns a random string of length n drawn from [a-z0-9]. The charset
// guarantees an injection-safe SQL identifier, and a per-run suffix keeps reruns from
// colliding on the dataset table name.
func randomSuffix(n int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// sanitize returns an error message stripped of any conninfo fragment that could leak the DSN
// (e.g. a password). If the message contains a conninfo-like token, the details are suppressed.
func sanitize(err error) string {
	if err == nil {
		return ""
	}

	msg := err.Error()
	for _, tok := range []string{"password=", "host=", "user=", "dbname=", "database=", "sslmode=", "://"} {
		if strings.Contains(msg, tok) {
			return "connection error (details suppressed)"
		}
	}

	return msg
}

// formatBytes renders a byte count as a human-readable string using binary units (base 1024):
// bytes below 1024 stay as "<n>B"; larger values are scaled to the largest fitting unit
// (KB/MB/GB) with one fractional digit, e.g. "64.0MB".
func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}

	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 2; v /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMG"[exp])
}
