// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package checkpointstorm

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"github.com/stretchr/testify/assert"
)

func TestConfig_validate(t *testing.T) {
	base := Config{
		Conninfo:       "host=127.0.0.1 dbname=postgres",
		TableSize:      minTableSize,
		DirtyPct:       minDirtyPct,
		PayloadBytes:   1,
		Rate:           0,
		ReportInterval: time.Second,
		Jobs:           4,
	}

	assert.NoError(t, base.validate(), "valid baseline must pass")

	tests := []struct {
		name string
		mut  func(c *Config)
	}{
		{"empty conninfo", func(c *Config) { c.Conninfo = "" }},
		{"table size below floor", func(c *Config) { c.TableSize = minTableSize - 1 }},
		{"zero table size", func(c *Config) { c.TableSize = 0 }},
		{"dirty pct below floor", func(c *Config) { c.DirtyPct = minDirtyPct - 1 }},
		{"zero payload bytes", func(c *Config) { c.PayloadBytes = 0 }},
		{"negative rate", func(c *Config) { c.Rate = -1 }},
		{"zero report interval", func(c *Config) { c.ReportInterval = 0 }},
		{"zero jobs", func(c *Config) { c.Jobs = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := base
			tt.mut(&c)
			assert.Error(t, c.validate(), "invalid config must be rejected")
		})
	}

	// Acceptance: TableSize exactly at the floor passes (guard is <, not <=).
	c := base
	c.TableSize = minTableSize
	assert.NoError(t, c.validate(), "table size equal to the floor must pass")

	// Acceptance: DirtyPct exactly at the floor passes (guard is <, not <=).
	c = base
	c.DirtyPct = minDirtyPct
	assert.NoError(t, c.validate(), "dirty pct equal to the floor must pass")

	// Acceptance: Jobs == 1 passes (no Jobs >= 2 invariant).
	c = base
	c.Jobs = 1
	assert.NoError(t, c.validate(), "jobs == 1 must pass")

	// Acceptance: Rate == 0 passes (0 means unlimited).
	c = base
	c.Rate = 0
	assert.NoError(t, c.validate(), "rate == 0 must pass")
}

func Test_rowsForSize(t *testing.T) {
	testcases := []struct {
		tableSize    int64
		payloadBytes int64
		want         int64
	}{
		{tableSize: minTableSize, payloadBytes: 1, want: minTableSize / (headerBytesPerRow + 1)},
		{tableSize: 500 << 20, payloadBytes: 1024, want: (500 << 20) / (headerBytesPerRow + 1024)},
		{tableSize: headerBytesPerRow + 8, payloadBytes: 8, want: 1},
		{tableSize: 1, payloadBytes: 1, want: 1},  // floor: never zero rows
		{tableSize: 0, payloadBytes: 1, want: 1},  // floor: never zero rows
		{tableSize: -1, payloadBytes: 1, want: 1}, // floor holds even for nonsensical input
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, rowsForSize(tc.tableSize, tc.payloadBytes),
			"tableSize=%d payloadBytes=%d", tc.tableSize, tc.payloadBytes)
	}
}

func Test_formatBytes(t *testing.T) {
	testcases := []struct {
		n    int64
		want string
	}{
		{n: 0, want: "0B"},                      // zero
		{n: 42, want: "42B"},                    // within B range
		{n: 1023, want: "1023B"},                // just below the KB boundary
		{n: 1024, want: "1.0KB"},                // exactly the KB boundary
		{n: 512 * 1024, want: "512.0KB"},        // within KB range
		{n: 1024 * 1024, want: "1.0MB"},         // exactly the MB boundary
		{n: 180 * 1024 * 1024, want: "180.0MB"}, // within MB range
		{n: 1024 * 1024 * 1024, want: "1.0GB"},  // exactly the GB boundary
		{n: 4509715660, want: "4.2GB"},          // within GB range
		{n: 1 << 40, want: "1024.0GB"},          // GB-capped: a TB-scale value renders as GB, not TB
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, formatBytes(tc.n))
	}
}

func Test_randomSuffix(t *testing.T) {
	re := regexp.MustCompile(`^[a-z0-9]+$`)

	for i := 0; i < 100; i++ {
		s := randomSuffix(8)
		assert.Len(t, s, 8, "suffix must have the requested length")
		assert.Regexp(t, re, s, "suffix must be an injection-safe identifier")
	}
}

func Test_sanitize(t *testing.T) {
	testcases := []struct {
		name       string
		err        error
		suppressed bool
		want       string
	}{
		{name: "password token", err: fmt.Errorf("failed: password=SENTINEL_SECRET host=db"), suppressed: true},
		{name: "host token", err: fmt.Errorf("dial error host=SENTINEL_SECRET"), suppressed: true},
		{name: "user token", err: fmt.Errorf("auth failed user=SENTINEL_SECRET"), suppressed: true},
		{name: "dbname token", err: fmt.Errorf("connect failed dbname=SENTINEL_SECRET"), suppressed: true},
		{name: "database token", err: fmt.Errorf("dial error database=SENTINEL_SECRET"), suppressed: true},
		{name: "sslmode token", err: fmt.Errorf("tls error sslmode=SENTINEL_SECRET"), suppressed: true},
		{name: "url form", err: fmt.Errorf("failed: postgres://user:SENTINEL_SECRET@db/noisia"), suppressed: true},
		{name: "privilege query error", err: fmt.Errorf("priv check failed: dial tcp host=SENTINEL_SECRET refused"), suppressed: true},
		{name: "benign", err: fmt.Errorf("server closed the connection unexpectedly"), suppressed: false, want: "server closed the connection unexpectedly"},
		{name: "nil", err: nil, suppressed: false, want: ""},
	}

	for _, tc := range testcases {
		got := sanitize(tc.err)
		assert.NotContains(t, got, "SENTINEL_SECRET", tc.name)
		if tc.suppressed {
			assert.Equal(t, "connection error (details suppressed)", got, tc.name)
		} else {
			assert.Equal(t, tc.want, got, tc.name)
		}
	}
}

func Test_checkpointPrivilegeQuery_isStaticLiteral(t *testing.T) {
	// Decision 3: the privilege-check SQL is the only net-new query with no sibling
	// precedent. It must be a static string literal — no role name or version assembled
	// via fmt.Sprintf. CI is PG15-only, so a guard regression cannot be caught at runtime;
	// catch it here by asserting the query string contains no format verbs and carries the
	// PG<15-safe to_regrole guard plus the current_user predicate.
	for _, verb := range []string{"%s", "%v", "%d", "%q"} {
		assert.NotContains(t, checkpointPrivilegeQuery, verb,
			"privilege query must be a static literal (no format verb %q)", verb)
	}
	assert.Contains(t, checkpointPrivilegeQuery, "to_regrole('pg_checkpoint')",
		"privilege query must guard PG<15 via to_regrole('pg_checkpoint')")
	assert.Contains(t, checkpointPrivilegeQuery, "current_user",
		"privilege query must scope membership to current_user")
}

// captureLogger is a log.Logger test double that records every line so a test can
// assert log shape without real output.
type captureLogger struct {
	mu        sync.Mutex
	infoLines []string
	warnLines []string
}

func (l *captureLogger) Info(msg string) {}
func (l *captureLogger) Infof(format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infoLines = append(l.infoLines, fmt.Sprintf(format, v...))
}
func (l *captureLogger) Warn(msg string) {}
func (l *captureLogger) Warnf(format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warnLines = append(l.warnLines, fmt.Sprintf(format, v...))
}
func (l *captureLogger) Error(msg string)                       {}
func (l *captureLogger) Errorf(format string, v ...interface{}) {}

func (l *captureLogger) warns() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.warnLines))
	copy(out, l.warnLines)
	return out
}

func (l *captureLogger) infos() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.infoLines))
	copy(out, l.infoLines)
	return out
}

// fakeRows is a minimal pgx.Rows double for the db.Conn.Query path in
// checkCheckpointPrivilege. It yields a single configurable bool (the granted flag)
// over one Next/Scan, or no rows at all (empty result), or a scan error. Only
// Next/Scan/Err/Close are exercised; the rest are stubbed.
type fakeRows struct {
	granted bool
	empty   bool
	scanErr error
	pos     int
}

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return nil }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Next() bool {
	if r.empty || r.pos > 0 {
		return false
	}
	r.pos++
	return true
}
func (r *fakeRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if len(dest) > 0 {
		if p, ok := dest[0].(*bool); ok {
			*p = r.granted
		}
	}
	return nil
}
func (r *fakeRows) Values() ([]any, error) { return nil, nil }
func (r *fakeRows) RawValues() [][]byte    { return nil }
func (r *fakeRows) Conn() *pgx.Conn        { return nil }

// queryConn is a db.Conn double that captures the SQL of the single Query
// checkCheckpointPrivilege issues, and returns a configured fakeRows or a query error.
type queryConn struct {
	rows     pgx.Rows
	queryErr error
	gotSQL   string
}

func (c *queryConn) Begin(ctx context.Context) (db.Tx, error) { panic("not implemented") }
func (c *queryConn) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
	panic("not implemented")
}
func (c *queryConn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	c.gotSQL = sql
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return c.rows, nil
}
func (c *queryConn) Close() error { return nil }

func Test_checkCheckpointPrivilege_grantedPasses(t *testing.T) {
	// The catalog query returns true (superuser or pg_checkpoint member) -> no error.
	conn := &queryConn{rows: &fakeRows{granted: true}}

	err := checkCheckpointPrivilege(context.Background(), conn)
	assert.NoError(t, err, "a granted privilege must pass")
	assert.Equal(t, checkpointPrivilegeQuery, conn.gotSQL, "the static privilege query must be issued verbatim")
}

func Test_checkCheckpointPrivilege_deniedFailsHonestly(t *testing.T) {
	// A false result and an empty result (no row) are BOTH treated as "no privilege":
	// the workload must fail honestly at startup naming the requirement.
	t.Run("false result", func(t *testing.T) {
		conn := &queryConn{rows: &fakeRows{granted: false}}
		err := checkCheckpointPrivilege(context.Background(), conn)
		assert.Error(t, err, "a false privilege result must fail")
		assert.Contains(t, err.Error(), "pg_checkpoint", "the error must name the pg_checkpoint requirement")
	})

	t.Run("empty result", func(t *testing.T) {
		conn := &queryConn{rows: &fakeRows{empty: true}}
		err := checkCheckpointPrivilege(context.Background(), conn)
		assert.Error(t, err, "an empty privilege result must fail")
		assert.Contains(t, err.Error(), "pg_checkpoint", "the error must name the pg_checkpoint requirement")
	})
}

func Test_checkCheckpointPrivilege_sanitizesError(t *testing.T) {
	// A query error that carries a conninfo fragment must be sanitized — the DSN must
	// never leak through the returned privilege-check error.
	conn := &queryConn{queryErr: fmt.Errorf("priv query failed: password=SENTINEL_SECRET reset")}

	err := checkCheckpointPrivilege(context.Background(), conn)
	assert.Error(t, err)
	assert.NotContains(t, err.Error(), "SENTINEL_SECRET", "the privilege-check error must be sanitized")
}

// txRecorder is a db.Tx double that records every statement Exec'd inside the
// transaction (with its bind args) and whether Commit/Rollback were called, so
// prepare()'s SQL and transaction lifecycle can be asserted without a live DB.
type txRecorder struct {
	mu         sync.Mutex
	execs      []recordedExec
	committed  bool
	rolledBack bool
}

type recordedExec struct {
	sql  string
	args []interface{}
}

func (t *txRecorder) Commit(ctx context.Context) error   { t.committed = true; return nil }
func (t *txRecorder) Rollback(ctx context.Context) error { t.rolledBack = true; return nil }
func (t *txRecorder) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.execs = append(t.execs, recordedExec{sql: sql, args: args})
	return 0, "", nil
}
func (t *txRecorder) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	panic("not implemented")
}

// txConn is a db.Conn double whose Begin returns a shared txRecorder, so a test can
// inspect what prepare() emitted inside the transaction and that it committed.
type txConn struct {
	tx         *txRecorder
	beginCalls int
}

func (c *txConn) Begin(ctx context.Context) (db.Tx, error) {
	c.beginCalls++
	return c.tx, nil
}
func (c *txConn) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
	panic("prepare must run its statements inside the transaction, not on the bare conn")
}
func (c *txConn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	panic("not implemented")
}
func (c *txConn) Close() error { return nil }

func Test_prepare_emitsCreateAndSeedSQL(t *testing.T) {
	// prepare() encodes the seed invariant in SQL. Drive it over a tx double and assert:
	// it runs inside an explicit, committed transaction; CREATE TABLE has
	// (id bigint PRIMARY KEY, payload bytea); the seed is a single set-based
	// INSERT ... SELECT $1 FROM generate_series(1, $2) whose payload is a fixed
	// random-filled make([]byte, PayloadBytes) bound as $1 and the row count bound as $2.
	const rows int64 = 12345
	const payloadBytes = 256
	tableIdent := "\"noisia_chkptstorm_" + randomSuffix(8) + "\""

	tx := &txRecorder{}
	conn := &txConn{tx: tx}

	err := prepare(context.Background(), conn, tableIdent, rows, payloadBytes)
	assert.NoError(t, err)

	// Explicit transaction that committed.
	assert.Equal(t, 1, conn.beginCalls, "prepare must open exactly one transaction")
	assert.True(t, tx.committed, "prepare must commit the seed transaction")

	tx.mu.Lock()
	execs := tx.execs
	tx.mu.Unlock()
	assert.Len(t, execs, 2, "prepare must emit exactly CREATE TABLE then INSERT")

	createSQL := execs[0].sql
	assert.Contains(t, createSQL, "CREATE TABLE "+tableIdent, "first statement must create the seed table")
	assert.Contains(t, createSQL, "id bigint PRIMARY KEY", "id must be a bigint PRIMARY KEY")
	assert.Contains(t, createSQL, "payload bytea", "payload must be a bytea column")

	insert := execs[1]
	assert.Contains(t, insert.sql, "INSERT INTO "+tableIdent, "second statement must seed the table")
	assert.Contains(t, insert.sql, "SELECT g, $1 FROM generate_series(1, $2)",
		"seed must be a single set-based INSERT ... SELECT with payload as $1 and row count as $2 (walflood two-bind form)")
	// The row count must be BOUND as $2, not string-interpolated into the SQL text.
	assert.NotContains(t, insert.sql, fmt.Sprintf("%d", rows), "row count must not be interpolated into the SQL text")
	// Payload content is now RANDOM (incompressible) so the heap reaches its on-disk size,
	// so assert only that $1 is a []byte of the right length (not its bytes) and $2 the rows.
	assert.Len(t, insert.args, 2, "seed must bind payload ($1) and row count ($2)")
	payloadArg, ok := insert.args[0].([]byte)
	assert.True(t, ok, "payload must be bound as a []byte")
	assert.Len(t, payloadArg, payloadBytes, "payload buffer must be PayloadBytes long")
	assert.Equal(t, rows, insert.args[1], "row count must be bound as $2")
}

// recordingConn is a db.Conn double that records every SQL statement Exec'd, so a test
// can assert dropTable drops the named table over an independently provided conn.
type recordingConn struct {
	mu     sync.Mutex
	execs  []string
	closed bool
}

func (c *recordingConn) Begin(ctx context.Context) (db.Tx, error) { panic("not implemented") }
func (c *recordingConn) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execs = append(c.execs, sql)
	return 0, "", nil
}
func (c *recordingConn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	panic("not implemented")
}
func (c *recordingConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *recordingConn) statements() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.execs))
	copy(out, c.execs)
	return out
}

func Test_dropTable_dropsTable(t *testing.T) {
	// cleanup's DROP must not depend on any live workload connection: dropTable issues
	// the DROP over an independently provided conn. Prove it over a recording double —
	// no live DB needed for this unit-level self-sufficiency check (ADR-002-2).
	tableIdent := "\"noisia_chkptstorm_" + randomSuffix(8) + "\""
	conn := &recordingConn{}

	err := dropTable(context.Background(), conn, tableIdent)
	assert.NoError(t, err)

	joined := strings.Join(conn.statements(), "\n")
	assert.Contains(t, joined, "DROP TABLE IF EXISTS "+tableIdent, "cleanup must drop the named table")
}

func Test_cleanup_warnsNamingTableOnConnectFailure(t *testing.T) {
	// When the fresh cleanup connection cannot be opened, cleanup must warn naming the
	// table (sanitized) so the orphaned table can be dropped manually. Conninfo is bad
	// here, so db.Connect fails fast and the warning path is exercised without a live DB.
	logger := &captureLogger{}
	w := &workload{
		config: Config{
			Conninfo:       "host=127.0.0.1 port=1 dbname=postgres connect_timeout=1",
			TableSize:      minTableSize,
			DirtyPct:       minDirtyPct,
			PayloadBytes:   1,
			ReportInterval: time.Second,
			Jobs:           1,
		},
		logger: logger,
	}

	tableIdent := "\"noisia_chkptstorm_" + randomSuffix(8) + "\""
	w.cleanup(tableIdent)

	joined := strings.Join(logger.warns(), "\n")
	assert.Contains(t, joined, tableIdent, "cleanup warning must name the table for manual drop")
}

// stormConn is a db.Conn double for the escalation engine: it records the SQL of every
// Exec (the SET application_name, the worker UPDATE, the forcer CHECKPOINT) together with
// its bind args, can be configured to fail a specific statement after a number of
// successes, and self-cancels the context after stopAfter matching Execs so the
// otherwise-unbounded loops terminate without relying on wall-clock.
type stormConn struct {
	mu sync.Mutex

	execs []recordedExec

	// failOn: substring of an Exec SQL -> error returned once okBefore matching Execs
	// have already succeeded. Lets a test drive "first UPDATE fails" or "CHECKPOINT fails
	// after N successes".
	failOn   string
	failErr  error
	okBefore int

	// stopAfter: cancel ctx after this many Execs whose SQL contains stopSQL have run, so
	// the loop exits cleanly. stopSQL == "" matches any Exec.
	stopAfter int
	stopSQL   string
	cancel    context.CancelFunc
}

func (c *stormConn) Begin(ctx context.Context) (db.Tx, error) { panic("not implemented") }

func (c *stormConn) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execs = append(c.execs, recordedExec{sql: sql, args: args})

	if c.failOn != "" && strings.Contains(sql, c.failOn) {
		// Count only the matching statement toward okBefore.
		matched := 0
		for _, e := range c.execs {
			if strings.Contains(e.sql, c.failOn) {
				matched++
			}
		}
		if matched > c.okBefore {
			return 0, "", c.failErr
		}
	}

	if c.stopAfter > 0 && c.cancel != nil {
		matched := 0
		for _, e := range c.execs {
			if c.stopSQL == "" || strings.Contains(e.sql, c.stopSQL) {
				matched++
			}
		}
		if matched >= c.stopAfter {
			c.cancel()
		}
	}

	return 0, "", nil
}

func (c *stormConn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	panic("not implemented")
}
func (c *stormConn) Close() error { return nil }

func (c *stormConn) recorded() []recordedExec {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]recordedExec, len(c.execs))
	copy(out, c.execs)
	return out
}

func (c *stormConn) countSQL(sub string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, e := range c.execs {
		if strings.Contains(e.sql, sub) {
			n++
		}
	}
	return n
}

func Test_forcerThreshold(t *testing.T) {
	// threshold = max(1, DirtyPct/100 × rows), with the max(1, …) floor for tiny inputs.
	testcases := []struct {
		dirtyPct int
		rows     int64
		want     int64
	}{
		{dirtyPct: 10, rows: 1000, want: 100},    // 10% of 1000
		{dirtyPct: 5, rows: 200000, want: 10000}, // 5% of 200000
		{dirtyPct: 25, rows: 8, want: 2},         // 25% of 8 = 2
		{dirtyPct: 5, rows: 1, want: 1},          // floor: 5% of 1 rounds to 0 -> 1
		{dirtyPct: 5, rows: 10, want: 1},         // floor: 5% of 10 = 0 (int) -> 1
		{dirtyPct: 1, rows: 1, want: 1},          // floor holds for the smallest inputs
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, forcerThreshold(tc.dirtyPct, tc.rows),
			"dirtyPct=%d rows=%d", tc.dirtyPct, tc.rows)
	}
}

func Test_runForcer_firesAndResets(t *testing.T) {
	// With sinceCheckpoint already >= threshold the forcer must issue CHECKPOINT, decrement
	// sinceCheckpoint by threshold (NOT to zero — overshoot preserved), increment checkpoints,
	// and record a non-zero flushNanos. The conn self-cancels after the first CHECKPOINT so the
	// loop exits cleanly (one checkpoint is enough to prove the mechanism).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &stormConn{stopAfter: 1, stopSQL: "CHECKPOINT", cancel: cancel}

	const threshold int64 = 100
	var sinceCheckpoint, checkpoints, flushNanos atomic.Int64
	sinceCheckpoint.Store(130) // 30 of overshoot beyond the threshold

	err := runForcer(ctx, conn, log.NewDefaultLogger("error"), threshold,
		&sinceCheckpoint, &checkpoints, &flushNanos, func(context.Context) (db.Conn, error) {
			return nil, fmt.Errorf("reconnect must not be called on the happy path")
		})

	assert.NoError(t, err, "a clean ctx cancel must not surface as an error")
	assert.Equal(t, int64(1), checkpoints.Load(), "exactly one CHECKPOINT must have fired")
	assert.Equal(t, int64(30), sinceCheckpoint.Load(),
		"sinceCheckpoint must be decremented by threshold (130-100=30), NOT reset to 0")
	assert.Greater(t, flushNanos.Load(), int64(0), "the CHECKPOINT call must record a non-zero flush duration")
	assert.Equal(t, 1, conn.countSQL("CHECKPOINT"), "the forcer must issue exactly the CHECKPOINT command")

	for _, e := range conn.recorded() {
		if strings.Contains(e.sql, "CHECKPOINT") {
			assert.Empty(t, e.args, "CHECKPOINT must be issued with no bind args")
		}
	}
}

func Test_runForcer_belowThresholdSleepsNotSpins(t *testing.T) {
	// Below the threshold the forcer must NOT issue CHECKPOINT (it sleeps between polls). Over
	// a short live window with sinceCheckpoint < threshold, no CHECKPOINT must be recorded.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	conn := &stormConn{}

	const threshold int64 = 1000
	var sinceCheckpoint, checkpoints, flushNanos atomic.Int64
	sinceCheckpoint.Store(10) // well below the threshold

	err := runForcer(ctx, conn, log.NewDefaultLogger("error"), threshold,
		&sinceCheckpoint, &checkpoints, &flushNanos, nil)

	assert.NoError(t, err, "a clean ctx-deadline stop must not surface as an error")
	assert.Equal(t, 0, conn.countSQL("CHECKPOINT"), "below threshold the forcer must not fire a CHECKPOINT")
	assert.Equal(t, int64(0), checkpoints.Load(), "no checkpoint must have been counted below threshold")
}

func Test_runForcer_reconnectThenDegrades(t *testing.T) {
	// A CHECKPOINT error under a live ctx triggers exactly ONE reconnect attempt; when the
	// reconnect fails, the forcer warns ("checkpoint forcing stopped") and returns nil — Run
	// must not fail because the forcer died.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := &captureLogger{}

	// The very first CHECKPOINT fails with a DSN-bearing error.
	conn := &stormConn{
		failOn:   "CHECKPOINT",
		failErr:  fmt.Errorf("CHECKPOINT failed: password=SENTINEL_SECRET reset"),
		okBefore: 0,
	}

	const threshold int64 = 10
	var sinceCheckpoint, checkpoints, flushNanos atomic.Int64
	sinceCheckpoint.Store(20) // over threshold so the first poll fires a CHECKPOINT

	reconnects := 0
	err := runForcer(ctx, conn, logger, threshold,
		&sinceCheckpoint, &checkpoints, &flushNanos, func(context.Context) (db.Conn, error) {
			reconnects++
			return nil, fmt.Errorf("reconnect failed: host=SENTINEL_SECRET refused")
		})

	assert.NoError(t, err, "a dead forcer degrades (returns nil), it does not fail Run")
	assert.Equal(t, 1, reconnects, "the forcer must attempt exactly one reconnect")

	joined := strings.Join(logger.warns(), "\n")
	assert.Contains(t, joined, "checkpoint forcing stopped", "the degradation must be warned loudly")
	assert.NotContains(t, joined, "SENTINEL_SECRET", "the degradation warning must be sanitized")
}

func Test_runWorkerWithConn_scatteredUpdateLoop(t *testing.T) {
	// The worker first SETs application_name, then loops the random-id UPDATE: id is bound as
	// $2 (in [1, rows], never interpolated) and payload as $1. Each success increments dirtied
	// (+PayloadBytes) and sinceCheckpoint; a clean ctx cancel returns nil.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &stormConn{stopAfter: 8, stopSQL: "UPDATE", cancel: cancel}

	const rows int64 = 500
	const payloadBytes = 256
	cfg := Config{PayloadBytes: payloadBytes, Rate: 0}
	updateSQL := "UPDATE t SET payload = $1 WHERE id = $2"

	var dirtied, sinceCheckpoint, sessions atomic.Int64

	err := runWorkerWithConn(ctx, conn, log.NewDefaultLogger("error"), cfg, updateSQL, rows,
		&dirtied, &sinceCheckpoint, &sessions)
	assert.NoError(t, err, "a clean ctx cancel must not surface as an error")

	recorded := conn.recorded()
	assert.GreaterOrEqual(t, len(recorded), 2, "worker must SET application_name then UPDATE")
	assert.Contains(t, recorded[0].sql, "SET application_name = 'noisia'", "first Exec must be the SET")

	updates := 0
	for _, e := range recorded {
		if !strings.Contains(e.sql, "UPDATE") {
			continue
		}
		updates++
		assert.Equal(t, updateSQL, e.sql, "the worker must loop exactly the scattered UPDATE")
		assert.Len(t, e.args, 2, "the UPDATE must bind payload ($1) and id ($2)")
		id, ok := e.args[1].(int64)
		assert.True(t, ok, "id must be bound as an int64")
		assert.GreaterOrEqual(t, id, int64(1), "random id must be >= 1")
		assert.LessOrEqual(t, id, rows, "random id must be <= rows")
	}
	assert.GreaterOrEqual(t, updates, 1, "the worker must have issued at least one UPDATE")
	assert.Equal(t, int64(updates)*int64(payloadBytes), dirtied.Load(),
		"dirtied must accumulate PayloadBytes per successful UPDATE")
	assert.Equal(t, int64(updates), sinceCheckpoint.Load(),
		"sinceCheckpoint must increment once per successful UPDATE")
	assert.Equal(t, int64(1), sessions.Load(), "a fully-started worker counts as exactly one live session")
}

func Test_runWorkerWithConn_initErrorOnFirstUpdate(t *testing.T) {
	// First UPDATE fails with zero workers ever live -> sanitized init error returned; sessions
	// must NOT be decremented (a worker that never churned is a setup defect, not a degradation).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn := &stormConn{
		failOn:   "UPDATE",
		failErr:  fmt.Errorf("update failed: password=SENTINEL_SECRET reset"),
		okBefore: 0,
	}

	const rows int64 = 100
	cfg := Config{PayloadBytes: 1, Rate: 0}
	var dirtied, sinceCheckpoint, sessions atomic.Int64

	err := runWorkerWithConn(ctx, conn, log.NewDefaultLogger("error"), cfg,
		"UPDATE t SET payload = $1 WHERE id = $2", rows, &dirtied, &sinceCheckpoint, &sessions)

	assert.Error(t, err, "a first-UPDATE failure with no prior success is a returned init error")
	assert.NotContains(t, err.Error(), "SENTINEL_SECRET", "the init error must be sanitized")
	assert.Equal(t, int64(0), dirtied.Load(), "no UPDATE succeeded, so dirtied stays 0")
	// The worker incremented sessions before entering the loop; the init-error path must NOT
	// DECREMENT it (decrement is the degraded-death path only). "Zero ever live" is judged by
	// dirtied (work done), not by this counter.
	assert.Equal(t, int64(1), sessions.Load(), "the init-error path must not decrement sessions")
}

func Test_runWorkerWithConn_degradedDecrementsSessionsOnce(t *testing.T) {
	// After some successful UPDATEs the conn fails mid-loop while ctx is live and a sibling is
	// live: the worker must warn, return nil (degraded, not fatal), and decrement sessions
	// EXACTLY once.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := &captureLogger{}

	conn := &stormConn{
		failOn:   "UPDATE",
		failErr:  fmt.Errorf("worker conn dropped"),
		okBefore: 3, // first 3 UPDATEs succeed, then it fails
	}

	const rows int64 = 100
	cfg := Config{PayloadBytes: 1, Rate: 0}
	var dirtied, sinceCheckpoint, sessions atomic.Int64
	sessions.Store(1) // a sibling worker is already live (this worker will become the 2nd)

	err := runWorkerWithConn(ctx, conn, logger, cfg,
		"UPDATE t SET payload = $1 WHERE id = $2", rows, &dirtied, &sinceCheckpoint, &sessions)

	assert.NoError(t, err, "a degraded mid-run loss returns nil, not an error")
	assert.GreaterOrEqual(t, dirtied.Load(), int64(3), "the UPDATEs that succeeded before the loss must have counted")
	assert.Equal(t, int64(1), sessions.Load(),
		"the worker counts itself live (1->2) then the degraded death decrements once (2->1)")

	joined := strings.Join(logger.warns(), "\n")
	assert.Contains(t, joined, "worker lost connection", "the degraded loss must be warned")
}

func Test_runReporter_panelFormat(t *testing.T) {
	// runReporter prints the panel each interval, reading only the atomics. Assert the exact
	// shape: dirtied via formatBytes, the (M/min) rate, flush as a duration, elapsed, and that
	// the panel carries NO sessions field.
	logger := &captureLogger{}

	var dirtied, checkpoints, flushNanos atomic.Int64
	dirtied.Store(4509715660) // 4.2GB via the GB-capped formatBytes
	checkpoints.Store(12)
	flushNanos.Store(int64(250 * time.Millisecond))

	done := make(chan struct{})
	reporterDone := make(chan struct{})
	interval := 30 * time.Millisecond
	// elapsed origin three minutes back so M/min = 12/3 = 4.0 deterministically (the few-ms
	// drift across one tick is far below a whole minute, so the truncated rate is stable).
	start := time.Now().Add(-3 * time.Minute)
	go func() {
		runReporter(context.Background(), done, start, interval, log.Logger(logger), &dirtied, &checkpoints, &flushNanos)
		close(reporterDone)
	}()

	time.Sleep(interval + 150*time.Millisecond)
	close(done)
	<-reporterDone

	lines := logger.infos()
	assert.GreaterOrEqual(t, len(lines), 1, "at least one panel line must be printed")

	first := lines[0]
	assert.Contains(t, first, "checkpoint-storm: dirtied=4.2GB", "dirtied must be rendered via GB-capped formatBytes")
	assert.Contains(t, first, "checkpoints=12", "panel must report the forced-checkpoint count")
	assert.Contains(t, first, "(4.0/min)", "panel must report checkpoints/min")
	assert.Contains(t, first, "flush=250ms", "panel must report the last CHECKPOINT call duration")
	assert.Contains(t, first, "elapsed=", "panel must report elapsed")
	assert.NotContains(t, first, "sessions", "the panel must NOT carry a sessions field")
}

func Test_sanitize_netNewForcerPaths(t *testing.T) {
	// Net-new error paths (forcer CHECKPOINT + reconnect, per-worker SET application_name) must
	// never leak conninfo: drive each over a conn double with a DSN-bearing error and assert no
	// logged line contains the sentinel token.
	t.Run("forcer CHECKPOINT and reconnect", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		logger := &captureLogger{}
		conn := &stormConn{
			failOn:  "CHECKPOINT",
			failErr: fmt.Errorf("CHECKPOINT failed: password=SENTINEL_SECRET reset"),
		}
		var sinceCheckpoint, checkpoints, flushNanos atomic.Int64
		sinceCheckpoint.Store(50)

		err := runForcer(ctx, conn, logger, 10, &sinceCheckpoint, &checkpoints, &flushNanos,
			func(context.Context) (db.Conn, error) {
				return nil, fmt.Errorf("reconnect failed: host=SENTINEL_SECRET refused")
			})
		assert.NoError(t, err)
		for _, line := range append(logger.warns(), logger.infos()...) {
			assert.NotContains(t, line, "SENTINEL_SECRET", "no forcer log line may carry conninfo")
		}
	})

	t.Run("per-worker SET application_name", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		// The SET is best-effort: it fails but the worker keeps going, so self-cancel after a
		// couple of UPDATEs to bound the loop.
		conn := &stormConn{
			failOn:    "application_name",
			failErr:   fmt.Errorf("SET failed: password=SENTINEL_SECRET broken conn"),
			okBefore:  0,
			stopAfter: 2,
			stopSQL:   "UPDATE",
			cancel:    cancel,
		}
		logger := &captureLogger{}
		cfg := Config{PayloadBytes: 1, Rate: 0}
		var dirtied, sinceCheckpoint, sessions atomic.Int64

		err := runWorkerWithConn(ctx, conn, logger, cfg,
			"UPDATE t SET payload = $1 WHERE id = $2", 100, &dirtied, &sinceCheckpoint, &sessions)
		assert.NoError(t, err, "a best-effort SET failure must not abort the worker")
		for _, line := range append(logger.warns(), logger.infos()...) {
			assert.NotContains(t, line, "SENTINEL_SECRET", "the SET warning must be sanitized")
		}
	})
}

func Test_NewWorkload(t *testing.T) {
	// NewWorkload must call validate() FIRST and propagate its error: a config that
	// validate() rejects yields (nil, err) and never a half-built workload. A valid
	// baseline yields a non-nil workload and no error. No DB is touched.
	logger := log.NewDefaultLogger("error")

	base := Config{
		Conninfo:       "host=127.0.0.1 dbname=postgres",
		TableSize:      minTableSize,
		DirtyPct:       minDirtyPct,
		PayloadBytes:   1,
		ReportInterval: time.Second,
		Jobs:           4,
	}

	w, err := NewWorkload(base, logger)
	assert.NoError(t, err)
	assert.NotNil(t, w, "valid config must yield a non-nil workload")

	tests := []struct {
		name string
		mut  func(c *Config)
	}{
		{"empty conninfo", func(c *Config) { c.Conninfo = "" }},
		{"table size below floor", func(c *Config) { c.TableSize = minTableSize - 1 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := base
			tt.mut(&c)
			got, err := NewWorkload(c, logger)
			assert.Error(t, err, "invalid config must be rejected before a workload is built")
			assert.Nil(t, got, "rejected config must yield a nil workload")
		})
	}
}
