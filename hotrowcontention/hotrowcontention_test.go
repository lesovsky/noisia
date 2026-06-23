// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package hotrowcontention

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
		Jobs:           4,
		HotRows:        1,
		ReportInterval: time.Second,
	}

	assert.NoError(t, base.validate(), "valid baseline must pass")

	tests := []struct {
		name string
		mut  func(c *Config)
	}{
		{"empty conninfo", func(c *Config) { c.Conninfo = "" }},
		{"zero hot rows", func(c *Config) { c.HotRows = 0 }},
		{"negative hot rows", func(c *Config) { c.HotRows = -1 }},
		{"zero report interval", func(c *Config) { c.ReportInterval = 0 }},
		{"negative report interval", func(c *Config) { c.ReportInterval = -time.Second }},
		{"single job", func(c *Config) { c.Jobs = 1; c.HotRows = 1 }},                    // 1 < 2*1
		{"jobs less than twice hot rows", func(c *Config) { c.Jobs = 3; c.HotRows = 2 }}, // 3 < 2*2
		{"hot rows greater than jobs", func(c *Config) { c.HotRows = 100; c.Jobs = 10 }}, // 10 < 2*100
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := base
			tt.mut(&c)
			assert.Error(t, c.validate(), "invalid config must be rejected")
		})
	}
}

func Test_randomSuffix(t *testing.T) {
	re := regexp.MustCompile(`^[a-z0-9]+$`)

	// Sample repeatedly: the charset guarantee must hold for every draw, not one.
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
		want       string // expected output for the non-suppressed (pass-through) cases
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

// captureLogger is a log.Logger test double that records every Infof/Warnf line so a
// test can assert the report-panel/warning shape without touching real logging output.
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

func (l *captureLogger) lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.infoLines))
	copy(out, l.infoLines)
	return out
}

func (l *captureLogger) warnings() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.warnLines))
	copy(out, l.warnLines)
	return out
}

// fakeConn is a minimal db.Conn used to exercise the churn init-error / degradation /
// ctx.Done branches without a real database. Only Exec is invoked.
type fakeConn struct {
	execErr      error // returned once execOKBefore successful Execs have happened
	execOKBefore int   // number of successful Execs before execErr is returned
	mu           sync.Mutex
	calls        int
}

func (f *fakeConn) Begin(ctx context.Context) (db.Tx, error) { panic("not implemented") }
func (f *fakeConn) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls > f.execOKBefore {
		return 0, "", f.execErr
	}
	return 0, "UPDATE", nil
}
func (f *fakeConn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	panic("not implemented")
}
func (f *fakeConn) Close() error { return nil }

func Test_runChurn_firstExecError(t *testing.T) {
	// The first Exec error with the shared attempts counter still at 0 is a setup defect
	// and is returned as an init error, never masquerading as degradation. The returned
	// error must also be sanitized — a DSN-carrying pgx error must not leak through.
	conn := &fakeConn{execErr: fmt.Errorf("dial tcp failed host=SENTINEL_SECRET refused"), execOKBefore: 0}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var attempts, sessions atomic.Int64
	sessions.Store(1) // a connected worker had incremented it

	err := runChurn(ctx, conn, log.NewDefaultLogger("error"), "UPDATE t SET counter = counter + 1 WHERE id = $1", 1, &attempts, &sessions)
	assert.Error(t, err)
	assert.NotContains(t, err.Error(), "SENTINEL_SECRET", "init error must be sanitized")
	assert.Equal(t, int64(0), attempts.Load())
	// First-error init path must NOT decrement sessions.
	assert.Equal(t, int64(1), sessions.Load())
}

func Test_runChurn_lostConnectionAfterSuccess(t *testing.T) {
	// An Exec error under a live ctx with attempts>0 is mid-run degradation — runChurn
	// logs a warning, decrements sessions exactly once, and returns nil.
	conn := &fakeConn{execErr: fmt.Errorf("connection reset by peer"), execOKBefore: 3}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	logger := &captureLogger{}
	var attempts, sessions atomic.Int64
	sessions.Store(2)

	err := runChurn(ctx, conn, logger, "UPDATE t SET counter = counter + 1 WHERE id = $1", 1, &attempts, &sessions)
	assert.NoError(t, err)
	// 3 successful UPDATEs were counted as attempts.
	assert.Equal(t, int64(3), attempts.Load())
	// sessions decremented exactly once (2 -> 1).
	assert.Equal(t, int64(1), sessions.Load())
	// The degradation warning must actually be logged.
	assert.Contains(t, strings.Join(logger.warnings(), "\n"), "lost connection", "degradation warning must be logged")
}

func Test_runChurn_ctxDoneClean(t *testing.T) {
	// A cancelled ctx must yield a clean return nil with no error and no decrement of sessions.
	conn := &fakeConn{execErr: fmt.Errorf("context canceled"), execOKBefore: 0}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var attempts, sessions atomic.Int64
	sessions.Store(1)

	err := runChurn(ctx, conn, log.NewDefaultLogger("error"), "UPDATE t SET counter = counter + 1 WHERE id = $1", 1, &attempts, &sessions)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), attempts.Load())
	// Clean ctx-cancel must NOT decrement sessions.
	assert.Equal(t, int64(1), sessions.Load())
}

func Test_runReporter(t *testing.T) {
	// runReporter prints the report panel each ReportInterval, reading only the atomic
	// counters. Drive it for a tick against controlled counters and assert the panel
	// shape and the attempts-delta rate.
	const interval = 50 * time.Millisecond
	logger := &captureLogger{}
	w := &workload{
		config: Config{ReportInterval: interval},
		logger: logger,
	}

	var attempts, sessions atomic.Int64
	// 2000 attempts before the first tick: rate = 2000 / 0.05s = 40000/s.
	attempts.Store(2000)
	sessions.Store(5)

	done := make(chan struct{})
	reporterDone := make(chan struct{})
	go func() {
		w.runReporter(context.Background(), done, time.Now(), &attempts, &sessions)
		close(reporterDone)
	}()

	time.Sleep(interval + 20*time.Millisecond)
	close(done)
	<-reporterDone

	lines := logger.lines()
	assert.GreaterOrEqual(t, len(lines), 1, "at least one panel line must be printed")

	first := lines[0]
	assert.Contains(t, first, "attempts=2000", "panel must report the attempts counter")
	assert.Contains(t, first, "rate=40000/s", "rate must be the attempts delta over the interval")
	assert.Contains(t, first, "sessions=5", "panel must report live sessions")
	assert.Contains(t, first, "uptime=", "panel must report uptime")
}

// -----------------------------------------------------------------------------
// Integration tests (testcontainers, real PostgreSQL via db.TestConninfo).
//
// They assert the OBSERVABLE proxies of the workload against a live server. All
// state waits use bounded condition-polling, never a fixed sleep as the wait
// mechanism (pg_stat_* is updated asynchronously; a fixed sleep is flaky or slow).
// -----------------------------------------------------------------------------

func TestWorkload_Run_jobsHonored(t *testing.T) {
	// With Jobs=N, at least N noisia backends are concurrently active mid-run. This holds
	// INDEPENDENT of the host CPU count because each worker owns its own db.Connect
	// (Decision 1) — a shared pool's default max_conns would cap this below Jobs.
	const jobs = 4
	config := Config{
		Conninfo:       db.TestConninfo,
		Jobs:           jobs,
		HotRows:        2,
		ReportInterval: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()

	// Poll until at least Jobs noisia backends are concurrently active. The observer
	// conn does not set application_name, so it is not counted.
	var n int
	for i := 0; i < 150; i++ {
		n = countNoisiaBackends(t, obs)
		if n >= jobs {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, n, jobs, "at least Jobs worker backends must be concurrently active, independent of host CPU")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_sharedContention(t *testing.T) {
	// With HotRows < Jobs, multiple sessions hammer the SAME hot row. Poll counter of
	// id=1: it must climb under load — proof that sessions SHARE rows (central
	// correctness concern, ADR-003-2), not spread over disjoint rows.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeHotRow(t, obs) // clean slate so discover cannot pick up an orphan table

	config := Config{
		Conninfo:       db.TestConninfo,
		Jobs:           4,
		HotRows:        1, // single hot row -> all sessions share it
		ReportInterval: 100 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait for the seed table to appear.
	var table string
	for i := 0; i < 50; i++ {
		table = discoverHotRowTable(t, obs)
		if table != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.NotEmpty(t, table, "seed table must appear")
	tableIdent := "\"" + table + "\""

	// Poll the counter of the single hot row id=1: it must strictly grow under load.
	var first, last int64
	for i := 0; i < 200; i++ {
		c := hotRowCounter(t, obs, tableIdent)
		if first == 0 && c > 0 {
			first = c
		}
		if first > 0 && c > first {
			last = c
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	assert.NoError(t, <-done)

	assert.Greater(t, first, int64(0), "the shared hot row counter must have been incremented")
	assert.Greater(t, last, first, "the shared hot row counter must climb under multiple sessions")
}

func TestWorkload_Run_cleanupOnExit(t *testing.T) {
	// During run the seed table exists with HotRows rows; graceful exit drops it.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeHotRow(t, obs) // start from a clean slate, independent of test order

	const hotRows = 3
	config := Config{
		Conninfo:       db.TestConninfo,
		Jobs:           6,
		HotRows:        hotRows,
		ReportInterval: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait until the seed table appears, then assert it has exactly HotRows rows.
	var table string
	for i := 0; i < 50; i++ {
		table = discoverHotRowTable(t, obs)
		if table != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.NotEmpty(t, table, "seed table must appear")
	assert.Equal(t, hotRows, countRows(t, obs, "\""+table+"\""))

	cancel()
	assert.NoError(t, <-done)

	assert.Empty(t, discoverHotRowTable(t, obs), "seed table must be dropped on graceful exit")
}

func TestWorkload_cleanup_selfSufficient(t *testing.T) {
	// cleanup must not depend on any live workload connection: it opens its OWN fresh
	// connection. Prove it deterministically — manually create a matching table, then
	// invoke cleanup directly with no workload running, and assert it is gone (ADR-002-2).
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeHotRow(t, obs)
	defer purgeHotRow(t, obs)

	base := "noisia_hotrow_" + randomSuffix(8)
	tableIdent := "\"" + base + "\""

	_, _, err = obs.Exec(context.Background(), "CREATE TABLE "+tableIdent+" (id int PRIMARY KEY, counter bigint NOT NULL)")
	assert.NoError(t, err)
	assert.NotEmpty(t, discoverHotRowTable(t, obs), "table must exist before cleanup")

	gw, err := NewWorkload(Config{
		Conninfo:       db.TestConninfo,
		Jobs:           2,
		HotRows:        1,
		ReportInterval: 200 * time.Millisecond,
	}, log.NewDefaultLogger("error"))
	assert.NoError(t, err)
	gw.(*workload).cleanup(tableIdent)

	assert.Empty(t, discoverHotRowTable(t, obs), "table must be dropped by self-sufficient cleanup")
}

// discoverHotRowTable returns the single noisia_hotrow_% table name present in the
// target DB, or "" if none. Used by integration tests since the suffix is random.
func discoverHotRowTable(t *testing.T, conn db.Conn) string {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE tablename LIKE 'noisia_hotrow_%'")
	assert.NoError(t, err)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		assert.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	assert.NoError(t, rows.Err())
	// At most one hot-row table should exist at a time; more than one means a prior test
	// orphaned its table, which would mask a real cleanup regression.
	assert.LessOrEqual(t, len(names), 1, "at most one hot-row table should exist at a time")
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

// countRows returns count(*) for the given table identifier.
func countRows(t *testing.T, conn db.Conn, tableIdent string) int {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT count(*) FROM "+tableIdent)
	assert.NoError(t, err)
	defer rows.Close()
	var n int
	for rows.Next() {
		assert.NoError(t, rows.Scan(&n))
	}
	assert.NoError(t, rows.Err())
	return n
}

// hotRowCounter returns the counter value of the hot row id=1.
func hotRowCounter(t *testing.T, conn db.Conn, tableIdent string) int64 {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT counter FROM "+tableIdent+" WHERE id = 1")
	assert.NoError(t, err)
	defer rows.Close()
	var n int64
	for rows.Next() {
		assert.NoError(t, rows.Scan(&n))
	}
	assert.NoError(t, rows.Err())
	return n
}

// countNoisiaBackends returns the number of backends with application_name='noisia'.
func countNoisiaBackends(t *testing.T, conn db.Conn) int {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT count(*) FROM pg_stat_activity WHERE application_name = 'noisia'")
	assert.NoError(t, err)
	defer rows.Close()
	var n int
	for rows.Next() {
		assert.NoError(t, rows.Scan(&n))
	}
	assert.NoError(t, rows.Err())
	return n
}

// purgeHotRow drops every leftover noisia_hotrow_% table so a test starts from a clean
// slate, independent of test order.
func purgeHotRow(t *testing.T, conn db.Conn) {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE tablename LIKE 'noisia_hotrow_%'")
	assert.NoError(t, err)
	var names []string
	for rows.Next() {
		var n string
		assert.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}
	rows.Close()
	for _, n := range names {
		_, _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS \""+n+"\"")
	}
}
