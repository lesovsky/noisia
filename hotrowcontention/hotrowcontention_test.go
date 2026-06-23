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

// recordingConn is a db.Conn double that records the id bind argument of every UPDATE so
// a test can assert which hot row a worker targets. After stopAfter Execs it cancels the
// context, bounding the rate.Inf churn loop without relying on wall-clock.
type recordingConn struct {
	mu        sync.Mutex
	ids       map[int]struct{}
	calls     int
	stopAfter int
	cancel    context.CancelFunc
}

func (c *recordingConn) Begin(ctx context.Context) (db.Tx, error) { panic("not implemented") }
func (c *recordingConn) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ids == nil {
		c.ids = make(map[int]struct{})
	}
	if len(args) == 1 {
		if id, ok := args[0].(int); ok {
			c.ids[id] = struct{}{}
		}
	}
	c.calls++
	if c.stopAfter > 0 && c.calls >= c.stopAfter && c.cancel != nil {
		c.cancel()
	}
	return 0, "UPDATE", nil
}
func (c *recordingConn) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	panic("not implemented")
}
func (c *recordingConn) Close() error { return nil }

func (c *recordingConn) seenIDs() map[int]struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[int]struct{}, len(c.ids))
	for k := range c.ids {
		out[k] = struct{}{}
	}
	return out
}

func Test_runChurn_sharesHotRows(t *testing.T) {
	// The deliberate INVERSE of wal-flood's disjoint partition: worker i targets row
	// (i mod HotRows)+1, so several worker indices fold onto the SAME row. Drive runChurn
	// per worker index with a recordingConn that captures the id bind arg, and assert the
	// (i mod HotRows)+1 sharing mapping with zero DB and zero flakiness.
	const hotRows = 2

	// Each worker churns exactly the id hotRowID maps it to; record what id it UPDATEs.
	idForWorker := make(map[int]int)
	for idx := 0; idx < 4; idx++ {
		ctx, cancel := context.WithCancel(context.Background())
		rec := &recordingConn{stopAfter: 3, cancel: cancel}

		var attempts, sessions atomic.Int64
		id := hotRowID(idx, hotRows)
		_ = runChurn(ctx, rec, log.NewDefaultLogger("error"), "UPDATE t SET counter = counter + 1 WHERE id = $1", id, &attempts, &sessions)
		cancel()

		seen := rec.seenIDs()
		assert.Len(t, seen, 1, "a worker churns exactly one shared hot row")
		_, ok := seen[id]
		assert.True(t, ok, "worker %d must UPDATE its mapped hot row id=%d", idx, id)
		idForWorker[idx] = id
	}

	// The mapping must fold multiple worker indices onto the same row and span exactly the
	// HotRows shared rows {1,2}: idx 0,2 -> id 1; idx 1,3 -> id 2.
	assert.Equal(t, 1, idForWorker[0])
	assert.Equal(t, 2, idForWorker[1])
	assert.Equal(t, 1, idForWorker[2], "worker 2 must SHARE row 1 with worker 0")
	assert.Equal(t, 2, idForWorker[3], "worker 3 must SHARE row 2 with worker 1")

	seenIDs := map[int]struct{}{}
	for _, id := range idForWorker {
		seenIDs[id] = struct{}{}
	}
	assert.Equal(t, map[int]struct{}{1: {}, 2: {}}, seenIDs, "workers must span exactly the HotRows shared rows")
}

func Test_runChurn_attemptsGatesInitVsDegradation(t *testing.T) {
	// The init-vs-degradation rule is keyed on the SHARED attempts atomic, so it must be
	// proven under concurrency: worker A succeeds a few Execs (attempts>0 globally), then
	// worker B's FIRST Exec fails under a live ctx. Because attempts>0, B's failure is
	// degradation (return nil, sessions decremented once), NOT an init error.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	logger := &captureLogger{}
	var attempts, sessions atomic.Int64
	sessions.Store(2) // two live workers

	// Worker A: succeeds 3 Execs, raising the shared attempts>0, then would error — but we
	// drive it first and let it lift attempts before B's first Exec.
	connA := &fakeConn{execErr: fmt.Errorf("connection reset by peer"), execOKBefore: 3}
	// Worker B: fails on its very first Exec.
	connB := &fakeConn{execErr: fmt.Errorf("connection reset by peer"), execOKBefore: 0}

	// Run A to completion first so attempts is provably > 0 before B starts (deterministic;
	// no reliance on goroutine scheduling for the gate to be observed).
	errA := runChurn(ctx, connA, logger, "UPDATE t SET counter = counter + 1 WHERE id = $1", 1, &attempts, &sessions)
	assert.NoError(t, errA, "worker A degrades after success (attempts>0), returns nil")
	assert.Greater(t, attempts.Load(), int64(0), "worker A must have raised the shared attempts counter")
	assert.Equal(t, int64(1), sessions.Load(), "worker A decremented sessions exactly once (2 -> 1)")

	errB := runChurn(ctx, connB, logger, "UPDATE t SET counter = counter + 1 WHERE id = $1", 2, &attempts, &sessions)
	assert.NoError(t, errB, "worker B's first failure with attempts>0 globally is degradation, not an init error")
	assert.Equal(t, int64(0), sessions.Load(), "worker B decremented sessions exactly once (1 -> 0)")
}

func Test_runChurn_twoWorkersConcurrentFirstFailure(t *testing.T) {
	// Both workers fail their very first Exec before any success anywhere (shared attempts
	// == 0) => both return init errors and sessions is left unchanged (the inverse aggregation
	// gate). This mirrors walflood's concurrent first-failure test.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	logger := log.NewDefaultLogger("error")
	var attempts, sessions atomic.Int64
	sessions.Store(2)

	conn1 := &fakeConn{execErr: fmt.Errorf("permission denied"), execOKBefore: 0}
	conn2 := &fakeConn{execErr: fmt.Errorf("permission denied"), execOKBefore: 0}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = runChurn(ctx, conn1, logger, "UPDATE t SET counter = counter + 1 WHERE id = $1", 1, &attempts, &sessions)
	}()
	go func() {
		defer wg.Done()
		errs[1] = runChurn(ctx, conn2, logger, "UPDATE t SET counter = counter + 1 WHERE id = $1", 2, &attempts, &sessions)
	}()
	wg.Wait()

	assert.Error(t, errs[0])
	assert.Error(t, errs[1])
	assert.Equal(t, int64(0), attempts.Load())
	// Init-error path must NOT decrement sessions.
	assert.Equal(t, int64(2), sessions.Load())
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
	// Central correctness concern (ADR-003-2): prove that MULTIPLE LIVE SESSIONS SHARE the
	// hot rows, not merely that "a counter grows" (a single worker would satisfy that too).
	// HotRows=2, Jobs=4 forces two workers onto each of id=1 and id=2 via (i mod HotRows)+1.
	// We require BOTH id=1 and id=2 counters to climb AT THE MOMENT >= Jobs 'noisia' backends
	// are concurrently live — so the climbing counters are attributed to many shared sessions,
	// not one worker and not a disjoint partition.
	const jobs = 4
	const hotRows = 2

	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeHotRow(t, obs) // clean slate so discover cannot pick up an orphan table

	config := Config{
		Conninfo:       db.TestConninfo,
		Jobs:           jobs,
		HotRows:        hotRows, // fewer rows than jobs -> sessions share each row
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

	// Bounded poll-loop: capture the FIRST observed counter for each shared row, then wait
	// for BOTH to strictly grow while at least Jobs 'noisia' backends are concurrently live.
	// grew==true only when both rows climbed under >= Jobs live sessions.
	var first1, first2, last1, last2 int64
	var backendsDuringGrowth int
	grew := false
	for i := 0; i < 200; i++ {
		c1 := hotRowCounterID(t, obs, tableIdent, 1)
		c2 := hotRowCounterID(t, obs, tableIdent, 2)
		if first1 == 0 && c1 > 0 {
			first1 = c1
		}
		if first2 == 0 && c2 > 0 {
			first2 = c2
		}
		n := countNoisiaBackends(t, obs)
		if first1 > 0 && first2 > 0 && c1 > first1 && c2 > first2 && n >= jobs {
			last1, last2 = c1, c2
			backendsDuringGrowth = n
			grew = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	assert.NoError(t, <-done)

	if !grew {
		t.Fatalf("shared-row counters did not both grow under >= %d live backends within window "+
			"(first id=1=%d, first id=2=%d, last n='noisia' backends seen); expected both shared rows "+
			"to climb while multiple sessions share them", jobs, first1, first2)
	}

	// Both shared rows were incremented and both climbed — proof workers share the foci.
	assert.Greater(t, first1, int64(0), "shared hot row id=1 must have been incremented")
	assert.Greater(t, first2, int64(0), "shared hot row id=2 must have been incremented")
	assert.Greater(t, last1, first1, "shared hot row id=1 counter must climb under multiple sessions")
	assert.Greater(t, last2, first2, "shared hot row id=2 counter must climb under multiple sessions")
	// The climbing counters are attributed to >= Jobs concurrently-live sessions sharing the
	// rows, not to a single worker or a disjoint partition.
	assert.GreaterOrEqual(t, backendsDuringGrowth, jobs,
		"at least Jobs live 'noisia' backends must share the hot rows while their counters climb")
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

// hotRowCounterID returns the counter value of the given hot row id.
func hotRowCounterID(t *testing.T, conn db.Conn, tableIdent string, id int) int64 {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT counter FROM "+tableIdent+" WHERE id = $1", id)
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
