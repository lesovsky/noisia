// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package seqscanstorm

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"github.com/stretchr/testify/assert"
)

func TestConfig_validate(t *testing.T) {
	base := Config{
		Conninfo:  "host=127.0.0.1 dbname=postgres",
		TableSize: minTableSize,
		Jobs:      4,
	}

	assert.NoError(t, base.validate(), "valid baseline must pass")

	// Conninfo-empty guard goes first.
	tests := []struct {
		name string
		mut  func(c *Config)
	}{
		{"empty conninfo", func(c *Config) { c.Conninfo = "" }},
		{"table size below floor", func(c *Config) { c.TableSize = minTableSize - 1 }},
		{"zero table size", func(c *Config) { c.TableSize = 0 }},
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

	// Acceptance: Jobs == 1 passes (no Jobs >= 2 invariant).
	c = base
	c.Jobs = 1
	assert.NoError(t, c.validate(), "jobs == 1 must pass")

	// The floor-guard message must carry the recommendation to raise --table-size.
	c = base
	c.TableSize = minTableSize - 1
	err := c.validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "--table-size", "floor guard must recommend raising --table-size")
}

func Test_rowsForSize(t *testing.T) {
	testcases := []struct {
		tableSize int64
		want      int64
	}{
		{tableSize: minTableSize, want: minTableSize / bytesPerRowEstimate},
		{tableSize: 500 << 20, want: (500 << 20) / bytesPerRowEstimate},
		{tableSize: bytesPerRowEstimate, want: 1},
		{tableSize: 1, want: 1},  // floor: never zero rows
		{tableSize: 0, want: 1},  // floor: never zero rows
		{tableSize: -1, want: 1}, // floor holds even for nonsensical input
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, rowsForSize(tc.tableSize), "tableSize=%d", tc.tableSize)
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
		{n: 1 << 40, want: "1.0TB"},             // exactly the TB boundary (new tier)
		{n: 3 << 40, want: "3.0TB"},             // multi-TB renders as TB, not GB
		{n: 1 << 50, want: "1.0PB"},             // exactly the PB boundary (new tier)
		{n: 2 << 50, want: "2.0PB"},             // within PB range, the ceiling
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
		{name: "relation size error", err: fmt.Errorf("read size failed: dial tcp host=SENTINEL_SECRET refused"), suppressed: true},
		{name: "cleanup drop error", err: fmt.Errorf("DROP TABLE failed: password=SENTINEL_SECRET timeout"), suppressed: true},
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

// captureLogger is a log.Logger test double that records every line so a test can
// assert log shape (e.g. cleanup warnings naming the table) without real output.
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

// recordingConn is a db.Conn double that records every SQL statement Exec'd, so a
// test can assert cleanup drops the named table over an independently provided conn
// (self-sufficiency, ADR-002-2) without a live database.
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

func Test_dropTable_selfSufficient(t *testing.T) {
	// cleanup's DROP must not depend on any live workload connection: dropTable issues
	// the DROP over an independently provided conn. Prove it over a recording double —
	// no live DB needed for this unit-level self-sufficiency check (ADR-002-2). The
	// live-PG variant of TestWorkload_cleanup_selfSufficient is deferred to Task 04.
	tableIdent := "\"noisia_seqscan_" + randomSuffix(8) + "\""
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
		config: Config{Conninfo: "host=127.0.0.1 port=1 dbname=postgres connect_timeout=1", TableSize: minTableSize, Jobs: 1},
		logger: logger,
	}

	tableIdent := "\"noisia_seqscan_" + randomSuffix(8) + "\""
	w.cleanup(tableIdent)

	joined := strings.Join(logger.warns(), "\n")
	assert.Contains(t, joined, tableIdent, "cleanup warning must name the table for manual drop")
}

func Test_NewWorkload(t *testing.T) {
	// NewWorkload must call validate() FIRST and propagate its error: a config that
	// validate() rejects yields (nil, err) and never a half-built workload. A valid
	// baseline yields a non-nil workload and no error. No DB is touched.
	logger := log.NewDefaultLogger("error")

	base := Config{
		Conninfo:  "host=127.0.0.1 dbname=postgres",
		TableSize: minTableSize,
		Jobs:      4,
	}

	// Valid baseline: non-nil workload, nil error.
	w, err := NewWorkload(base, logger)
	assert.NoError(t, err)
	assert.NotNil(t, w, "valid config must yield a non-nil workload")

	// Invalid configs: validate() runs first, so NewWorkload returns (nil, err).
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

func Test_prepare(t *testing.T) {
	// prepare() encodes the core seqscan-storm invariant in SQL. Drive it over a tx
	// double and assert: it runs inside an explicit, committed transaction; CREATE TABLE
	// has id bigint PRIMARY KEY and payload bigint and NO index on payload; the seed is a
	// single set-based INSERT ... SELECT g, g FROM generate_series(1, $1) whose row count
	// is BOUND as $1 (not interpolated). A regression (indexing payload, interpolating the
	// row count, or losing the transaction) must fail this test.
	const rows int64 = 12345
	tableIdent := "\"noisia_seqscan_" + randomSuffix(8) + "\""

	tx := &txRecorder{}
	conn := &txConn{tx: tx}

	err := prepare(context.Background(), conn, tableIdent, rows)
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
	assert.Contains(t, createSQL, "payload bigint", "payload must be a plain bigint column")

	// The seqscan-storm invariant: payload carries NO index. If any statement built an
	// index on payload, the worker query could use it and the Seq Scan storm collapses.
	for _, e := range execs {
		upper := strings.ToUpper(e.sql)
		assert.NotContains(t, upper, "CREATE INDEX", "payload must never be indexed")
		assert.NotContains(t, upper, "INDEX ON", "payload must never be indexed")
	}

	insert := execs[1]
	assert.Contains(t, insert.sql, "INSERT INTO "+tableIdent, "second statement must seed the table")
	assert.Contains(t, insert.sql, "SELECT g, g FROM generate_series(1, $1)",
		"seed must be a single set-based INSERT ... SELECT over generate_series with the row count as $1")
	// The row count must be BOUND as $1, not string-interpolated into the SQL text.
	assert.NotContains(t, insert.sql, fmt.Sprintf("%d", rows), "row count must not be interpolated into the SQL text")
	assert.Equal(t, []interface{}{rows}, insert.args, "row count must be passed as bind arg $1")
}

// fakeRows is a minimal pgx.Rows double for the db.Conn.Query path in readRelationSize.
// It yields a single configurable int64 (the relation size) over one Next/Scan, or a
// scan error, modeling pg_relation_size's single-row result. The unused pgx.Rows methods
// are stubbed: readRelationSize only calls Next, Scan, Err and Close.
type fakeRows struct {
	size    int64
	scanErr error
	pos     int
}

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return nil }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Next() bool {
	if r.pos > 0 {
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
		if p, ok := dest[0].(*int64); ok {
			*p = r.size
		}
	}
	return nil
}
func (r *fakeRows) Values() ([]any, error) { return nil, nil }
func (r *fakeRows) RawValues() [][]byte    { return nil }
func (r *fakeRows) Conn() *pgx.Conn        { return nil }

// queryConn is a db.Conn double that captures the SQL and bind args of the single Query
// readRelationSize issues, and returns a configured fakeRows or a query error.
type queryConn struct {
	rows     pgx.Rows
	queryErr error
	gotSQL   string
	gotArgs  []interface{}
}

func (c *queryConn) Begin(ctx context.Context) (db.Tx, error) { panic("not implemented") }
func (c *queryConn) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
	panic("not implemented")
}
func (c *queryConn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	c.gotSQL = sql
	c.gotArgs = args
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return c.rows, nil
}
func (c *queryConn) Close() error { return nil }

func Test_readRelationSize(t *testing.T) {
	// Happy path: readRelationSize returns the size scanned from the single result row,
	// and passes the UNQUOTED relname as bind arg $1 (pg_relation_size takes a value, not
	// an identifier — quoting it would break the lookup).
	t.Run("happy path", func(t *testing.T) {
		const want int64 = 188 << 20
		const relname = "noisia_seqscan_abcd1234"
		conn := &queryConn{rows: &fakeRows{size: want}}

		got, err := readRelationSize(context.Background(), conn, relname)
		assert.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, []interface{}{relname}, conn.gotArgs, "relname must be passed UNQUOTED as bind arg $1")
		assert.NotContains(t, conn.gotSQL, relname, "relname must not be interpolated into the SQL text")
	})

	// Query error path: the error from conn.Query propagates and the size is zero.
	t.Run("query error", func(t *testing.T) {
		conn := &queryConn{queryErr: fmt.Errorf("relation does not exist")}

		got, err := readRelationSize(context.Background(), conn, "noisia_seqscan_x")
		assert.Error(t, err)
		assert.Equal(t, int64(0), got)
	})
}
