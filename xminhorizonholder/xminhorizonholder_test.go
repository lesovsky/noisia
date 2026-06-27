// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xminhorizonholder

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
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"github.com/stretchr/testify/assert"
)

func TestConfig_validate(t *testing.T) {
	base := Config{
		Conninfo:       "host=127.0.0.1 dbname=postgres",
		TableSize:      minTableSize,
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
	tableIdent := "\"noisia_xminhold_" + randomSuffix(8) + "\""

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
	tableIdent := "\"noisia_xminhold_" + randomSuffix(8) + "\""
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
			PayloadBytes:   1,
			ReportInterval: time.Second,
			Jobs:           1,
		},
		logger: logger,
	}

	tableIdent := "\"noisia_xminhold_" + randomSuffix(8) + "\""
	w.cleanup(tableIdent)

	joined := strings.Join(logger.warns(), "\n")
	assert.Contains(t, joined, tableIdent, "cleanup warning must name the table for manual drop")
	// The operator-log label must be this package's, never a leftover from the verbatim copy.
	assert.Contains(t, joined, "xmin-horizon-holder:", "cleanup warning must carry the xmin-horizon-holder label")
	assert.NotContains(t, joined, "checkpoint-storm:", "cleanup warning must not carry the copied checkpoint-storm label")
	// A connect failure error must be sanitized: no conninfo fragment may leak into the warning.
	assert.NotContains(t, joined, "127.0.0.1", "cleanup warning must not leak a DSN fragment")
}

func Test_NewWorkload(t *testing.T) {
	// NewWorkload must call validate() FIRST and propagate its error: a config that
	// validate() rejects yields (nil, err) and never a half-built workload. A valid
	// baseline yields a non-nil workload and no error. No DB is touched.
	logger := log.NewDefaultLogger("error")

	base := Config{
		Conninfo:       "host=127.0.0.1 dbname=postgres",
		TableSize:      minTableSize,
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

// stormConn is a db.Conn double for the engine: it records the SQL of every Exec (the SET
// application_name, the holder BEGIN/SELECT sequence, the holder liveness ping, the worker
// UPDATE) together with its bind args, can be configured to fail a specific statement after
// a number of successes, and self-cancels the context after stopAfter matching Execs so the
// otherwise-unbounded loops terminate without relying on wall-clock. Adapted verbatim from
// checkpointstorm's stormConn.
type stormConn struct {
	mu sync.Mutex

	execs []recordedExec

	// failOn: substring of an Exec SQL -> error returned once okBefore matching Execs have
	// already succeeded. Lets a test drive "first UPDATE fails" or "ping fails".
	failOn   string
	failErr  error
	okBefore int

	// stopAfter: cancel ctx after this many Execs whose SQL contains stopSQL have run, so the
	// loop exits cleanly. stopSQL == "" matches any Exec.
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

func Test_holderBeginSQL_isStaticLiteral(t *testing.T) {
	// Decision 1: the holder snapshot is pinned via a raw BEGIN with a static isolation level
	// literal — never assembled via fmt.Sprintf. CI is PG15-only, so a regression cannot be
	// caught at runtime; catch it here by asserting the literal carries no format verb and the
	// REPEATABLE READ isolation level.
	for _, verb := range []string{"%s", "%v", "%d", "%q"} {
		assert.NotContains(t, holderBeginSQL, verb, "holder BEGIN must be a static literal (no format verb %q)", verb)
	}
	assert.Equal(t, "BEGIN ISOLATION LEVEL REPEATABLE READ", holderBeginSQL,
		"holder BEGIN must request the REPEATABLE READ isolation level")
}

func Test_establishHolder_emitsOrderedStatements(t *testing.T) {
	// The holder establish sequence must issue exactly, in order: SET application_name, BEGIN
	// ISOLATION LEVEL REPEATABLE READ (pins nothing yet), SELECT 1 (pins backend_xmin), SELECT
	// txid_current() (pins backend_xid). heldSince must be set to a non-epoch value on success.
	conn := &stormConn{}
	var heldSince atomic.Int64

	err := establishHolder(context.Background(), conn, &heldSince)
	assert.NoError(t, err)

	recorded := conn.recorded()
	assert.Len(t, recorded, 4, "establish must issue exactly four statements in order")
	assert.Equal(t, "SET application_name = 'noisia'", recorded[0].sql)
	assert.Equal(t, "BEGIN ISOLATION LEVEL REPEATABLE READ", recorded[1].sql)
	assert.Equal(t, "SELECT 1", recorded[2].sql)
	assert.Equal(t, "SELECT txid_current()", recorded[3].sql)
	assert.Greater(t, heldSince.Load(), int64(0), "heldSince must be set to a non-epoch value at the successful pin")
}

func Test_establishHolder_failsAtAnyStep(t *testing.T) {
	// A failure at any establish step (SET, BEGIN, SELECT 1, txid_current) must return an error
	// so the mandatory-start gate holds and the workload does not start. heldSince must stay epoch.
	steps := []string{"SET application_name", "BEGIN ISOLATION LEVEL REPEATABLE READ", "SELECT 1", "SELECT txid_current()"}
	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			conn := &stormConn{failOn: step, failErr: fmt.Errorf("boom at %s", step), okBefore: 0}
			var heldSince atomic.Int64

			err := establishHolder(context.Background(), conn, &heldSince)
			assert.Error(t, err, "a failure at %s must return an error (mandatory start)", step)
			assert.Equal(t, int64(0), heldSince.Load(), "heldSince must stay epoch when the pin fails")
		})
	}
}

func Test_establishHolder_sanitizesError(t *testing.T) {
	// A holder step failing with a DSN-bearing error must return a sanitized error — the
	// conninfo fragment must never leak into the returned/logged line (security finding).
	conn := &stormConn{
		failOn:   "BEGIN",
		failErr:  fmt.Errorf("begin failed: password=SENTINEL_SECRET reset"),
		okBefore: 0,
	}
	var heldSince atomic.Int64

	err := establishHolder(context.Background(), conn, &heldSince)
	assert.Error(t, err)
	assert.NotContains(t, err.Error(), "SENTINEL_SECRET", "the holder establish error must be sanitized")
}

func Test_holderLoop_reconnectResetsHeldAndBumpsRestarts(t *testing.T) {
	// A mid-run liveness-ping error under a live ctx triggers exactly ONE reconnect that
	// re-issues the BEGIN/SELECT 1/txid_current() sequence on the fresh conn, resets heldSince
	// to a newer value, and increments holderRestarts to 1.
	old := holderPingInterval
	holderPingInterval = time.Millisecond
	defer func() { holderPingInterval = old }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := &captureLogger{}

	// The held conn's first liveness ping fails (a mid-run drop).
	conn1 := &stormConn{failOn: "SELECT 1", failErr: fmt.Errorf("holder conn dropped"), okBefore: 0}
	// The reconnect returns a fresh conn that re-establishes cleanly, then self-cancels after
	// txid_current() so the loop exits without relying on wall-clock.
	conn2 := &stormConn{stopAfter: 1, stopSQL: "txid_current", cancel: cancel}

	var heldSince, holderRestarts atomic.Int64
	pre := time.Now().Add(-time.Hour).UnixNano()
	heldSince.Store(pre)

	reconnects := 0
	err := runHolder(ctx, conn1, logger, &heldSince, &holderRestarts, func(context.Context) (db.Conn, error) {
		reconnects++
		return conn2, nil
	})

	assert.NoError(t, err, "a clean ctx cancel must not surface as an error")
	assert.Equal(t, 1, reconnects, "the holder must attempt exactly one reconnect")
	assert.Equal(t, int64(1), holderRestarts.Load(), "a successful reconnect must bump holder-restarts to 1")
	assert.Greater(t, heldSince.Load(), pre, "the reconnect must RESET heldSince to a newer value")

	joined := strings.Join(stormSQLs(conn2.recorded()), "\n")
	assert.Contains(t, joined, "BEGIN ISOLATION LEVEL REPEATABLE READ", "reconnect must re-issue the BEGIN")
	assert.Contains(t, joined, "SELECT 1", "reconnect must re-pin the snapshot")
	assert.Contains(t, joined, "SELECT txid_current()", "reconnect must re-pin the xid")
}

func Test_holderLoop_secondFailureWarnsAndExits(t *testing.T) {
	// The second failure after a drop — whether the reconnect itself fails OR the reconnect
	// succeeds but the re-establish on the fresh conn fails — logs a sanitized warning and the
	// holder returns WITHOUT failing Run; holder-restarts must NOT be incremented in either case.
	old := holderPingInterval
	holderPingInterval = time.Millisecond
	defer func() { holderPingInterval = old }()

	t.Run("reconnect fails", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		logger := &captureLogger{}

		conn1 := &stormConn{
			failOn:   "SELECT 1",
			failErr:  fmt.Errorf("ping failed: password=SENTINEL_SECRET drop"),
			okBefore: 0,
		}

		var heldSince, holderRestarts atomic.Int64
		reconnects := 0
		err := runHolder(ctx, conn1, logger, &heldSince, &holderRestarts, func(context.Context) (db.Conn, error) {
			reconnects++
			return nil, fmt.Errorf("reconnect failed: host=SENTINEL_SECRET refused")
		})

		assert.NoError(t, err, "a dead holder degrades (returns nil), it does not fail Run")
		assert.Equal(t, 1, reconnects, "the holder must attempt exactly one reconnect")
		assert.Equal(t, int64(0), holderRestarts.Load(), "a failed reconnect must not count as a restart")

		joined := strings.Join(logger.warns(), "\n")
		assert.Contains(t, joined, "no longer held", "the degradation must be warned loudly")
		assert.NotContains(t, joined, "SENTINEL_SECRET", "every holder warning must be sanitized")
	})

	t.Run("reconnect succeeds but re-establish fails", func(t *testing.T) {
		// The reconnect returns a fresh conn, but its BEGIN fails with a DSN-bearing error: the
		// holder must warn (sanitized) that the horizon is no longer held, return nil, and NOT
		// bump holder-restarts.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		logger := &captureLogger{}

		conn1 := &stormConn{failOn: "SELECT 1", failErr: fmt.Errorf("holder conn dropped"), okBefore: 0}
		conn2 := &stormConn{failOn: "BEGIN", failErr: fmt.Errorf("begin failed: password=SENTINEL_SECRET reset"), okBefore: 0}

		var heldSince, holderRestarts atomic.Int64
		reconnects := 0
		err := runHolder(ctx, conn1, logger, &heldSince, &holderRestarts, func(context.Context) (db.Conn, error) {
			reconnects++
			return conn2, nil
		})

		assert.NoError(t, err, "a failed re-establish degrades (returns nil), it does not fail Run")
		assert.Equal(t, 1, reconnects, "the holder must attempt exactly one reconnect")
		assert.Equal(t, int64(0), holderRestarts.Load(), "a failed re-establish must not count as a restart")
		assert.Equal(t, int64(0), heldSince.Load(), "a failed re-establish must not set heldSince")

		joined := strings.Join(logger.warns(), "\n")
		assert.Contains(t, joined, "re-establish failed", "the degradation must be warned loudly")
		assert.NotContains(t, joined, "SENTINEL_SECRET", "the re-establish warning must be sanitized")
	})
}

func Test_runWorkerWithConn_scatteredUpdateLoop(t *testing.T) {
	// The worker first SETs application_name, then loops the random-id UPDATE: id is bound as $2
	// (in [1, rows], never interpolated) and payload as $1. Each success increments churned; a
	// clean ctx cancel returns nil.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &stormConn{stopAfter: 8, stopSQL: "UPDATE", cancel: cancel}

	const rows int64 = 500
	const payloadBytes = 256
	cfg := Config{PayloadBytes: payloadBytes, Rate: 0}
	updateSQL := "UPDATE t SET payload = $1 WHERE id = $2"

	var churned, sessions atomic.Int64

	err := runWorkerWithConn(ctx, conn, log.NewDefaultLogger("error"), cfg, updateSQL, rows, &churned, &sessions)
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
	assert.Equal(t, int64(updates), churned.Load(), "churned must increment once per successful UPDATE")
	assert.Equal(t, int64(1), sessions.Load(), "a fully-started worker counts as exactly one live session")
}

func Test_runWorkerWithConn_initErrorVsDegraded(t *testing.T) {
	t.Run("init error on first update", func(t *testing.T) {
		// First UPDATE fails with nothing ever churned -> sanitized init error returned; sessions
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
		var churned, sessions atomic.Int64

		err := runWorkerWithConn(ctx, conn, log.NewDefaultLogger("error"), cfg,
			"UPDATE t SET payload = $1 WHERE id = $2", rows, &churned, &sessions)

		assert.Error(t, err, "a first-UPDATE failure with nothing ever churned is a returned init error")
		assert.NotContains(t, err.Error(), "SENTINEL_SECRET", "the init error must be sanitized")
		assert.Equal(t, int64(0), churned.Load(), "no UPDATE succeeded, so churned stays 0")
		assert.Equal(t, int64(1), sessions.Load(), "the init-error path must not decrement sessions")
	})

	t.Run("degraded decrements sessions once", func(t *testing.T) {
		// After some successful UPDATEs the conn fails mid-loop while ctx is live and a sibling is
		// live: the worker must warn, return nil (degraded, not fatal), and decrement sessions once.
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
		var churned, sessions atomic.Int64
		sessions.Store(1) // a sibling worker is already live (this worker will become the 2nd)

		err := runWorkerWithConn(ctx, conn, logger, cfg,
			"UPDATE t SET payload = $1 WHERE id = $2", rows, &churned, &sessions)

		assert.NoError(t, err, "a degraded mid-run loss returns nil, not an error")
		assert.GreaterOrEqual(t, churned.Load(), int64(3), "the UPDATEs that succeeded before the loss must have counted")
		assert.Equal(t, int64(1), sessions.Load(), "the worker counts itself live (1->2) then the degraded death decrements once (2->1)")

		joined := strings.Join(logger.warns(), "\n")
		assert.Contains(t, joined, "worker lost connection", "the degraded loss must be warned")
	})
}

func Test_runReporter_panelFormat(t *testing.T) {
	// runReporter prints the panel each interval, reading only the atomics. Assert the exact
	// field set — held, churned, the (rate/min), holder-restarts, elapsed — and that the panel
	// carries NONE of dirtied/flush/checkpoints/sessions.
	logger := &captureLogger{}

	var churned, holderRestarts, heldSince atomic.Int64
	churned.Store(12)
	holderRestarts.Store(2)
	heldSince.Store(time.Now().Add(-90 * time.Second).UnixNano())

	done := make(chan struct{})
	reporterDone := make(chan struct{})
	interval := 30 * time.Millisecond
	// elapsed origin three minutes back so rate = 12/3 = 4.0/min deterministically.
	start := time.Now().Add(-3 * time.Minute)
	go func() {
		runReporter(context.Background(), done, start, interval, log.Logger(logger), &churned, &holderRestarts, &heldSince)
		close(reporterDone)
	}()

	time.Sleep(interval + 150*time.Millisecond)
	close(done)
	<-reporterDone

	lines := logger.infos()
	assert.GreaterOrEqual(t, len(lines), 1, "at least one panel line must be printed")

	first := lines[0]
	assert.Contains(t, first, "xmin-horizon-holder: held=", "panel must report the held duration")
	assert.Contains(t, first, "churned=12", "panel must report the churned count")
	assert.Contains(t, first, "(4.0/min)", "panel must report churned/min")
	assert.Contains(t, first, "holder-restarts=2", "panel must report the holder-restarts count")
	assert.Contains(t, first, "elapsed=", "panel must report elapsed")
	assert.NotContains(t, first, "dirtied", "the panel must NOT carry a dirtied field")
	assert.NotContains(t, first, "flush", "the panel must NOT carry a flush field")
	assert.NotContains(t, first, "checkpoints", "the panel must NOT carry a checkpoints field")
	assert.NotContains(t, first, "sessions", "the panel must NOT carry a sessions field")
}

// stormSQLs extracts the SQL text of recorded execs for order/content assertions.
func stormSQLs(execs []recordedExec) []string {
	out := make([]string, 0, len(execs))
	for _, e := range execs {
		out = append(out, e.sql)
	}
	return out
}
