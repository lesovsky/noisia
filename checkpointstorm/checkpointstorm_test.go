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
	// zero-filled make([]byte, PayloadBytes) bound as $1 and the row count bound as $2.
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
	assert.Equal(t, []interface{}{make([]byte, payloadBytes), rows}, insert.args,
		"payload must be a fixed zero-filled buffer ($1) and the row count bound as $2")
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
