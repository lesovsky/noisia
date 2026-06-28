// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backendkiller

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"github.com/stretchr/testify/assert"
)

func TestConfig_validate(t *testing.T) {
	testcases := []struct {
		valid  bool
		config Config
	}{
		// valid cases
		{valid: true, config: Config{Conninfo: "host=127.0.0.1", Rate: 0, PlanSize: 1, ReportInterval: time.Second}},
		{valid: true, config: Config{Conninfo: "host=127.0.0.1", Rate: 5, PlanSize: 1000, ReportInterval: time.Second}},
		// invalid cases
		{valid: false, config: Config{Conninfo: "", Rate: 1, PlanSize: 1, ReportInterval: time.Second}},                    // empty conninfo
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: -1, PlanSize: 1, ReportInterval: time.Second}},     // negative rate
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: 1, PlanSize: 0, ReportInterval: time.Second}},      // plan-size < 1
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: 1, PlanSize: 1, ReportInterval: 0}},                // report-interval <= 0
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: 1, PlanSize: 1, ReportInterval: -1 * time.Second}}, // negative report-interval
	}

	for _, tc := range testcases {
		if tc.valid {
			assert.NoError(t, tc.config.validate())
		} else {
			assert.Error(t, tc.config.validate())
		}
	}
}

func Test_buildPrepare(t *testing.T) {
	const planSize = 5

	q0 := buildPrepare(0, planSize)
	q7 := buildPrepare(7, planSize)

	// Statement name must be unique per i.
	assert.Contains(t, q0, "PREPARE noisia_bk_0 AS SELECT ")
	assert.Contains(t, q7, "PREPARE noisia_bk_7 AS SELECT ")
	assert.NotEqual(t, q0, q7)

	// Exactly planSize target-list expressions: planSize-1 separating commas and
	// planSize " AS c" expression markers.
	assert.Equal(t, planSize-1, strings.Count(q0, ","))
	assert.Equal(t, planSize, strings.Count(q0, " AS c"))

	// Every distinct expression "j AS cj" present.
	for j := 0; j < planSize; j++ {
		assert.Contains(t, q0, fmt.Sprintf("%d AS c%d", j, j))
	}

	// Statement is composed solely of the prefix, ints and the cN identifiers —
	// no bind args, no unexpected interpolated text.
	assert.Regexp(t, `^PREPARE noisia_bk_\d+ AS SELECT (\d+ AS c\d+)(, \d+ AS c\d+)*$`, q0)
}

func Test_buildExecute(t *testing.T) {
	e0 := buildExecute(0)
	e7 := buildExecute(7)

	// Statement name must match the one PREPARE created for the same id and be
	// unique per id.
	assert.Equal(t, "EXECUTE noisia_bk_0", e0)
	assert.Equal(t, "EXECUTE noisia_bk_7", e7)
	assert.NotEqual(t, e0, e7)

	// Composed solely of the prefix and an int id — no bind args, no injection
	// surface — including a large id where %d formatting could differ.
	assert.Regexp(t, `^EXECUTE noisia_bk_\d+$`, buildExecute(1_000_000))
}

func Test_sanitize(t *testing.T) {
	testcases := []struct {
		name       string
		err        error
		suppressed bool
	}{
		{name: "password token", err: fmt.Errorf("failed: password=SENTINEL_SECRET host=db"), suppressed: true},
		{name: "url form", err: fmt.Errorf("failed: postgres://user:SENTINEL_SECRET@db/noisia"), suppressed: true},
		{name: "database token", err: fmt.Errorf("dial error database=noisia_invalid"), suppressed: true},
		{name: "benign", err: fmt.Errorf("server closed the connection unexpectedly"), suppressed: false},
		{name: "nil", err: nil, suppressed: false},
	}

	for _, tc := range testcases {
		got := sanitize(tc.err)
		assert.NotContains(t, got, "SENTINEL_SECRET", tc.name)
		if tc.suppressed {
			assert.Equal(t, "connection error (details suppressed)", got, tc.name)
		}
	}
}

func TestWorkload_Run(t *testing.T) {
	config := Config{
		Conninfo:       db.TestConninfo,
		Rate:           50,
		PlanSize:       10,
		ReportInterval: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("info"))
	assert.NoError(t, err)
	err = w.Run(ctx)
	assert.Nil(t, err)
}

func TestWorkload_Run_invalidConninfo(t *testing.T) {
	// Use a recognizable sentinel in the conninfo and assert it never leaks into
	// the returned error (Decision 9: no Conninfo in any log/error).
	config := Config{
		Conninfo:       "database=noisia_invalid password=SENTINEL_SECRET",
		Rate:           1,
		PlanSize:       10,
		ReportInterval: time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)
	err = w.Run(ctx)
	assert.Error(t, err)
	assert.NotContains(t, err.Error(), "SENTINEL_SECRET")
}

func TestWorkload_Run_applicationName(t *testing.T) {
	// Decision 8: the dedicated connection reports application_name=noisia.
	conn, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = conn.Close() }()

	_, _, err = conn.Exec(context.Background(), "SET application_name = 'noisia'")
	assert.NoError(t, err)

	rows, err := conn.Query(context.Background(), "SELECT current_setting('application_name')")
	assert.NoError(t, err)
	var appName string
	for rows.Next() {
		assert.NoError(t, rows.Scan(&appName))
	}
	rows.Close()
	assert.Equal(t, "noisia", appName)
}

func Test_runLoop_monotonicGrowth(t *testing.T) {
	conn, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var counter atomic.Int64
	var mem atomic.Int64

	config := Config{
		Conninfo: db.TestConninfo,
		Rate:     50,
		PlanSize: 10,
		// ShowMemory exercises readBackendMemory's happy path on the PG15
		// container; the short interval ensures the read fires within the window.
		ShowMemory:     true,
		ReportInterval: 100 * time.Millisecond,
	}

	// Take a mid-run snapshot via the atomic counter while runLoop runs in a
	// goroutine, then a final snapshot after it returns. Baseline is 0.
	var mid atomic.Int64
	go func() {
		time.Sleep(400 * time.Millisecond)
		mid.Store(counter.Load())
	}()

	err = runLoop(ctx, conn, log.NewDefaultLogger("error"), config, &counter, &mem)
	assert.NoError(t, err)

	final := counter.Load()
	midVal := mid.Load()

	// Sustained creation over the ~1s window at Rate=50 (not a single increment).
	assert.GreaterOrEqual(t, final, int64(20))
	// Monotonic: baseline(0) <= mid <= final.
	assert.Positive(t, midVal)
	assert.GreaterOrEqual(t, final, midVal)
	// --show-memory happy path on PG15: the own-backend memory read returns a value.
	assert.Positive(t, mem.Load())
}

// fakeConn is a minimal db.Conn used to exercise the Decision 5 climax / first-Exec
// guard branches in runLoop without a real database. Only Exec is invoked
// (ShowMemory is false in these tests, so Query is never called).
type fakeConn struct {
	execErr      error    // returned once execOKBefore successful Execs have happened
	execOKBefore int      // number of successful Execs before execErr is returned
	calls        int      // total Exec calls (PREPARE + EXECUTE)
	sqls         []string // every SQL string passed to Exec, in order
}

func (f *fakeConn) Begin(ctx context.Context) (db.Tx, error) { panic("not implemented") }
func (f *fakeConn) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
	f.calls++
	f.sqls = append(f.sqls, sql)
	if f.calls > f.execOKBefore {
		return 0, "", f.execErr
	}
	return 0, "PREPARE", nil
}
func (f *fakeConn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	panic("not implemented")
}
func (f *fakeConn) Close() error { return nil }

func Test_runLoop_executeFollowsPrepare(t *testing.T) {
	// Each loop iteration issues PREPARE then EXECUTE for the same statement id.
	// Stop after the second PREPARE so the recorded SQL is deterministic.
	conn := &fakeConn{execErr: fmt.Errorf("connection reset by peer"), execOKBefore: 2}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var counter, mem atomic.Int64
	config := Config{Conninfo: "x", Rate: 0, PlanSize: 2, ReportInterval: time.Second}

	err := runLoop(ctx, conn, log.NewDefaultLogger("error"), config, &counter, &mem)
	assert.NoError(t, err)

	// PREPARE for id 0, then EXECUTE for the same id 0, then PREPARE for id 1 (which fails).
	assert.True(t, strings.HasPrefix(conn.sqls[0], "PREPARE noisia_bk_0 "))
	assert.Equal(t, "EXECUTE noisia_bk_0", conn.sqls[1])
	assert.True(t, strings.HasPrefix(conn.sqls[2], "PREPARE noisia_bk_1 "))
}

func Test_runLoop_climaxAfterSuccess(t *testing.T) {
	// Decision 5: an Exec error under a live ctx with counter>0 is the climax —
	// runLoop logs the climax line and returns nil. Here the error lands on the
	// EXECUTE of the second statement: PREPARE(0) ok, EXECUTE(0) ok, PREPARE(1) ok,
	// EXECUTE(1) fails. counter tracks completed PREPARE+EXECUTE pairs, so the first
	// pair leaves it at 1.
	conn := &fakeConn{execErr: fmt.Errorf("connection reset by peer"), execOKBefore: 3}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var counter, mem atomic.Int64
	config := Config{Conninfo: "x", Rate: 0, PlanSize: 2, ReportInterval: time.Second}

	err := runLoop(ctx, conn, log.NewDefaultLogger("error"), config, &counter, &mem)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), counter.Load())
	// The failing call was an EXECUTE — an OOM during plan caching is a valid climax.
	assert.True(t, strings.HasPrefix(conn.sqls[len(conn.sqls)-1], "EXECUTE "))
}

func Test_runLoop_firstExecuteError(t *testing.T) {
	// Mirror of Test_runLoop_firstExecError on the EXECUTE phase: PREPARE(0) ok,
	// then EXECUTE(0) fails with no completed pair yet (counter==0). A first-EXECUTE
	// builder defect must surface as an init error, never as a climax.
	conn := &fakeConn{execErr: fmt.Errorf("prepared statement does not exist"), execOKBefore: 1}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var counter, mem atomic.Int64
	config := Config{Conninfo: "x", Rate: 0, PlanSize: 2, ReportInterval: time.Second}

	err := runLoop(ctx, conn, log.NewDefaultLogger("error"), config, &counter, &mem)
	assert.Error(t, err)
	assert.Equal(t, int64(0), counter.Load())
	// The failure was on the EXECUTE, not the PREPARE.
	assert.True(t, strings.HasPrefix(conn.sqls[len(conn.sqls)-1], "EXECUTE "))
}

func Test_runLoop_firstExecError(t *testing.T) {
	// Decision 5: the first Exec error (counter==0) is a setup/builder defect and
	// is returned as an init error, never masquerading as a climax.
	conn := &fakeConn{execErr: fmt.Errorf("syntax error"), execOKBefore: 0}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var counter, mem atomic.Int64
	config := Config{Conninfo: "x", Rate: 0, PlanSize: 2, ReportInterval: time.Second}

	err := runLoop(ctx, conn, log.NewDefaultLogger("error"), config, &counter, &mem)
	assert.Error(t, err)
	assert.Equal(t, int64(0), counter.Load())
}
