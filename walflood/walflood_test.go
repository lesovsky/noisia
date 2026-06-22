// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package walflood

import (
	"context"
	"fmt"
	"regexp"
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
		Rate:           0,
		Rows:           1000,
		PayloadBytes:   8192,
		ReportInterval: time.Second,
		Jobs:           4,
	}

	assert.NoError(t, base.validate(), "valid baseline must pass")

	tests := []struct {
		name string
		mut  func(c *Config)
	}{
		{"empty conninfo", func(c *Config) { c.Conninfo = "" }},
		{"negative rate", func(c *Config) { c.Rate = -1 }},
		{"zero rows", func(c *Config) { c.Rows = 0 }},
		{"zero payload bytes", func(c *Config) { c.PayloadBytes = 0 }},
		{"zero report interval", func(c *Config) { c.ReportInterval = 0 }},
		{"negative report interval", func(c *Config) { c.ReportInterval = -time.Second }},
		{"zero jobs", func(c *Config) { c.Jobs = 0 }},
		{"rows less than jobs", func(c *Config) { c.Rows = 2; c.Jobs = 3 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := base
			tt.mut(&c)
			assert.Error(t, c.validate(), "invalid config must be rejected")
		})
	}
}

func Test_partition(t *testing.T) {
	cases := []struct {
		rows int
		jobs uint16
	}{
		{1000, 4}, // even split
		{10, 3},   // rows % jobs != 0 (remainder spread into tail ranges)
		{11, 3},   // larger remainder (2) lands in the tail ranges
		{5, 5},    // rows == jobs (one id per worker)
		{100, 1},  // single worker owns the whole range
		{7, 2},    // odd remainder
	}

	for _, c := range cases {
		ranges := partition(c.rows, c.jobs)

		assert.Len(t, ranges, int(c.jobs), "must return exactly Jobs ranges")
		assert.Equal(t, 1, ranges[0][0], "coverage must start at id 1")
		assert.Equal(t, c.rows, ranges[len(ranges)-1][1], "coverage must end at id Rows")

		for i, r := range ranges {
			assert.LessOrEqual(t, r[0], r[1], "range must be non-empty (lo <= hi)")
			if i > 0 {
				assert.Equal(t, ranges[i-1][1]+1, r[0],
					"ranges must be contiguous with no gap or overlap")
			}
		}
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
		// seed/cleanup error paths: a wrapped DSN fragment must still collapse.
		{name: "seed error", err: fmt.Errorf("seed table failed: dial tcp host=SENTINEL_SECRET refused"), suppressed: true},
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
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, formatBytes(tc.n))
	}
}

// captureLogger is a log.Logger test double that records every Infof line so a test
// can assert the escalation-panel shape without touching real logging output.
type captureLogger struct {
	mu        sync.Mutex
	infoLines []string
}

func (l *captureLogger) Info(msg string) {}
func (l *captureLogger) Infof(format string, v ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.infoLines = append(l.infoLines, fmt.Sprintf(format, v...))
}
func (l *captureLogger) Warn(msg string)                        {}
func (l *captureLogger) Warnf(format string, v ...interface{})  {}
func (l *captureLogger) Error(msg string)                       {}
func (l *captureLogger) Errorf(format string, v ...interface{}) {}

func (l *captureLogger) lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.infoLines))
	copy(out, l.infoLines)
	return out
}

// fakeConn is a minimal db.Conn used to exercise the churn init-error / climax /
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
	// The first Exec error with the shared counter still at 0 is a setup defect and is
	// returned as an init error, never masquerading as a climax.
	conn := &fakeConn{execErr: fmt.Errorf("permission denied"), execOKBefore: 0}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var counter atomic.Int64
	config := Config{Conninfo: "x", Rate: 0, Rows: 5, PayloadBytes: 8, ReportInterval: time.Second, Jobs: 1}

	err := runChurn(ctx, conn, log.NewDefaultLogger("error"), config, "UPDATE t SET payload=$1 WHERE id=$2", 1, 5, &counter)
	assert.Error(t, err)
	assert.Equal(t, int64(0), counter.Load())
}

func Test_runChurn_climaxAfterSuccess(t *testing.T) {
	// An Exec error under a live ctx with counter>0 is the climax — runChurn logs the
	// climax line and returns nil.
	conn := &fakeConn{execErr: fmt.Errorf("connection reset by peer"), execOKBefore: 3}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var counter atomic.Int64
	config := Config{Conninfo: "x", Rate: 0, Rows: 5, PayloadBytes: 8, ReportInterval: time.Second, Jobs: 1}

	err := runChurn(ctx, conn, log.NewDefaultLogger("error"), config, "UPDATE t SET payload=$1 WHERE id=$2", 1, 5, &counter)
	assert.NoError(t, err)
	// 3 successful UPDATEs × 8 bytes.
	assert.Equal(t, int64(24), counter.Load())
}

func Test_runChurn_ctxDoneClean(t *testing.T) {
	// A cancelled ctx must yield a clean return nil — the error is not masked into a failure.
	conn := &fakeConn{execErr: fmt.Errorf("context canceled"), execOKBefore: 0}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var counter atomic.Int64
	config := Config{Conninfo: "x", Rate: 0, Rows: 5, PayloadBytes: 8, ReportInterval: time.Second, Jobs: 1}

	err := runChurn(ctx, conn, log.NewDefaultLogger("error"), config, "UPDATE t SET payload=$1 WHERE id=$2", 1, 5, &counter)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), counter.Load())
}

func Test_runChurn_twoWorkersConcurrentFirstFailure(t *testing.T) {
	// Two workers each fail their very first Exec before any success anywhere (shared
	// counter == 0) => both return init errors (the shared-counter aggregation rule).
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var counter atomic.Int64
	config := Config{Conninfo: "x", Rate: 0, Rows: 4, PayloadBytes: 8, ReportInterval: time.Second, Jobs: 2}
	logger := log.NewDefaultLogger("error")
	conn1 := &fakeConn{execErr: fmt.Errorf("permission denied"), execOKBefore: 0}
	conn2 := &fakeConn{execErr: fmt.Errorf("permission denied"), execOKBefore: 0}

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = runChurn(ctx, conn1, logger, config, "UPDATE t SET payload=$1 WHERE id=$2", 1, 2, &counter)
	}()
	go func() {
		defer wg.Done()
		errs[1] = runChurn(ctx, conn2, logger, config, "UPDATE t SET payload=$1 WHERE id=$2", 3, 4, &counter)
	}()
	wg.Wait()

	assert.Error(t, errs[0])
	assert.Error(t, errs[1])
	assert.Equal(t, int64(0), counter.Load())
}

func Test_runChurn_churnsWithinRange(t *testing.T) {
	// A worker must only UPDATE ids inside its disjoint [lo, hi] sub-range (the
	// anti-self-defeat guarantee), cycling within the range.
	rec := &recordingConn{}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var counter atomic.Int64
	config := Config{Conninfo: "x", Rate: 0, Rows: 10, PayloadBytes: 8, ReportInterval: time.Second, Jobs: 2}

	// Worker owns ids 6..10.
	_ = runChurn(ctx, rec, log.NewDefaultLogger("error"), config, "UPDATE t SET payload=$1 WHERE id=$2", 6, 10, &counter)

	ids := rec.seenIDs()
	assert.NotEmpty(t, ids, "worker must have issued at least one UPDATE")
	for id := range ids {
		assert.GreaterOrEqual(t, id, 6, "id must not fall below the worker's range")
		assert.LessOrEqual(t, id, 10, "id must not exceed the worker's range")
	}
}

// recordingConn is a db.Conn double that records the id bind argument of every UPDATE
// so a test can assert a worker stays within its disjoint range.
type recordingConn struct {
	mu  sync.Mutex
	ids map[int]struct{}
}

func (c *recordingConn) Begin(ctx context.Context) (db.Tx, error) { panic("not implemented") }
func (c *recordingConn) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ids == nil {
		c.ids = make(map[int]struct{})
	}
	if len(args) == 2 {
		if id, ok := args[1].(int); ok {
			c.ids[id] = struct{}{}
		}
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

func Test_runReporter(t *testing.T) {
	// runReporter prints the escalation panel each ReportInterval, reading only the
	// atomic counter. Drive it for a tick against a controlled counter and assert the
	// panel shape, human-readable formatting (formatBytes) and the counter-delta rate.
	const interval = 50 * time.Millisecond
	logger := &captureLogger{}
	w := &workload{
		config: Config{ReportInterval: interval},
		logger: logger,
	}

	var counter atomic.Int64
	// 2 MiB written before the first tick: rate = 2 MiB / 0.05s = 40 MiB/s.
	counter.Store(2 * 1024 * 1024)

	done := make(chan struct{})
	reporterDone := make(chan struct{})
	go func() {
		w.runReporter(context.Background(), done, time.Now(), &counter)
		close(reporterDone)
	}()

	time.Sleep(interval + 20*time.Millisecond)
	close(done)
	<-reporterDone

	lines := logger.lines()
	assert.GreaterOrEqual(t, len(lines), 1, "at least one panel line must be printed")

	first := lines[0]
	assert.Contains(t, first, "wal-flood: payload-written=2.0MB", "payload-written must be human-readable from formatBytes")
	assert.Contains(t, first, "rate=40.0MB/s", "rate must be the counter delta over the interval, human-readable")
	assert.Contains(t, first, "elapsed=", "panel must report elapsed time")
}
