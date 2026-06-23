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
	"github.com/lesovsky/noisia/db"
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
