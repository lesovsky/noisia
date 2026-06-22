// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package slotbloat

import (
	"context"
	"fmt"
	"net/url"
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
	testcases := []struct {
		valid  bool
		config Config
	}{
		// valid cases
		{valid: true, config: Config{Conninfo: "host=127.0.0.1", Rate: 0, Rows: 1, PayloadBytes: 1, ReportInterval: time.Second}},
		{valid: true, config: Config{Conninfo: "host=127.0.0.1", Rate: 5, Rows: 1000, PayloadBytes: 8192, ReportInterval: time.Second}},
		// invalid cases
		{valid: false, config: Config{Conninfo: "", Rate: 1, Rows: 1, PayloadBytes: 1, ReportInterval: time.Second}},                    // empty conninfo
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: -1, Rows: 1, PayloadBytes: 1, ReportInterval: time.Second}},     // negative rate
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: 1, Rows: 0, PayloadBytes: 1, ReportInterval: time.Second}},      // rows < 1
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: 1, Rows: 1, PayloadBytes: 0, ReportInterval: time.Second}},      // payload-bytes < 1
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: 1, Rows: 1, PayloadBytes: 1, ReportInterval: 0}},                // report-interval == 0
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: 1, Rows: 1, PayloadBytes: 1, ReportInterval: -1 * time.Second}}, // report-interval < 0
	}

	for _, tc := range testcases {
		if tc.valid {
			assert.NoError(t, tc.config.validate())
		} else {
			assert.Error(t, tc.config.validate())
		}
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
		// prepare()/cleanup error paths: a wrapped DSN fragment must still collapse.
		{name: "prepare seed error", err: fmt.Errorf("seed table failed: dial tcp host=SENTINEL_SECRET refused"), suppressed: true},
		{name: "cleanup drop error", err: fmt.Errorf("DROP TABLE failed: password=SENTINEL_SECRET timeout"), suppressed: true},
		{name: "benign", err: fmt.Errorf("server closed the connection unexpectedly"), suppressed: false, want: "server closed the connection unexpectedly"},
		{name: "benign prepare", err: fmt.Errorf("permission denied for schema public"), suppressed: false, want: "permission denied for schema public"},
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

func Test_runReporter(t *testing.T) {
	// runReporter prints the escalation panel each ReportInterval, reading only the
	// atomic counter. Drive it for a couple of ticks against a controlled counter and
	// assert the panel shape plus the human-readable formatting (formatBytes) and the
	// counter-delta rate. Deterministic: small interval, stop via the done channel.
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

	// Allow the first tick to fire and be recorded.
	time.Sleep(interval + 20*time.Millisecond)
	close(done)
	<-reporterDone

	lines := logger.lines()
	assert.GreaterOrEqual(t, len(lines), 1, "at least one panel line must be printed")

	// First panel: payload-written reflects the counter, rate is the delta over the
	// interval, both human-readable; elapsed is present.
	first := lines[0]
	assert.Contains(t, first, "slot-bloat: payload-written=2.0MB", "payload-written must be human-readable from formatBytes")
	assert.Contains(t, first, "rate=40.0MB/s", "rate must be the counter delta over the interval, human-readable")
	assert.Contains(t, first, "elapsed=", "panel must report elapsed time")
}

// fakeConn is a minimal db.Conn used to exercise the runChurn init-error / climax /
// ctx.Done branches without a real database. Only Exec is invoked.
type fakeConn struct {
	execErr      error // returned once execOKBefore successful Execs have happened
	execOKBefore int   // number of successful Execs before execErr is returned
	calls        int
}

func (f *fakeConn) Begin(ctx context.Context) (db.Tx, error) { panic("not implemented") }
func (f *fakeConn) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
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

func Test_runChurn_firstSlotError(t *testing.T) {
	// The first Exec error (counter==0) is a setup defect (e.g. missing REPLICATION)
	// and is returned as an init error, never masquerading as a climax.
	conn := &fakeConn{execErr: fmt.Errorf("permission denied"), execOKBefore: 0}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var counter atomic.Int64
	config := Config{Conninfo: "x", Rate: 0, Rows: 5, PayloadBytes: 8, ReportInterval: time.Second}

	err := runChurn(ctx, conn, log.NewDefaultLogger("error"), config, "UPDATE t SET payload=$1 WHERE id=$2", &counter)
	assert.Error(t, err)
	assert.Equal(t, int64(0), counter.Load())
}

func Test_runChurn_climaxAfterSuccess(t *testing.T) {
	// An Exec error under a live ctx with counter>0 is the climax — runChurn logs
	// the climax line and returns nil.
	conn := &fakeConn{execErr: fmt.Errorf("connection reset by peer"), execOKBefore: 3}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var counter atomic.Int64
	config := Config{Conninfo: "x", Rate: 0, Rows: 5, PayloadBytes: 8, ReportInterval: time.Second}

	err := runChurn(ctx, conn, log.NewDefaultLogger("error"), config, "UPDATE t SET payload=$1 WHERE id=$2", &counter)
	assert.NoError(t, err)
	// 3 successful UPDATEs × 8 bytes.
	assert.Equal(t, int64(24), counter.Load())
}

func Test_runChurn_ctxDoneClean(t *testing.T) {
	// A cancelled ctx (mirroring create/seed cancellation surfacing in the loop)
	// must yield a clean return nil — the error is not masked into a failure.
	conn := &fakeConn{execErr: fmt.Errorf("context canceled"), execOKBefore: 0}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var counter atomic.Int64
	config := Config{Conninfo: "x", Rate: 0, Rows: 5, PayloadBytes: 8, ReportInterval: time.Second}

	err := runChurn(ctx, conn, log.NewDefaultLogger("error"), config, "UPDATE t SET payload=$1 WHERE id=$2", &counter)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), counter.Load())
}

// discoverSlotBloatTable returns the single noisia_slotbloat_% table name present in
// the target DB, or "" if none. Used by integration tests since the suffix is random.
func discoverSlotBloatTable(t *testing.T, conn db.Conn) string {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE tablename LIKE 'noisia_slotbloat_%'")
	assert.NoError(t, err)
	defer rows.Close()
	var name string
	for rows.Next() {
		assert.NoError(t, rows.Scan(&name))
	}
	assert.NoError(t, rows.Err())
	return name
}

func TestWorkload_Run_slotCreated(t *testing.T) {
	// Slot is created with immediately_reserve: restart_lsn is non-null right after
	// creation and slot_type is physical. Observed mid-run via a separate connection.
	config := Config{
		Conninfo:       db.TestConninfo,
		Rate:           50,
		Rows:           10,
		PayloadBytes:   64,
		ReportInterval: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Give Run time to create the slot and seed the table.
	time.Sleep(700 * time.Millisecond)

	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	rows, err := obs.Query(context.Background(),
		"SELECT slot_type, restart_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name LIKE 'noisia_slotbloat_%'")
	assert.NoError(t, err)
	var slotType string
	var restartLSNSet bool
	found := false
	for rows.Next() {
		found = true
		assert.NoError(t, rows.Scan(&slotType, &restartLSNSet))
	}
	rows.Close()
	_ = obs.Close()

	assert.True(t, found, "slot must exist mid-run")
	assert.Equal(t, "physical", slotType)
	assert.True(t, restartLSNSet, "restart_lsn must be non-null with immediately_reserve")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_seedPhaseFeedback(t *testing.T) {
	// During the (potentially long) seed phase there must be explicit feedback: a
	// "seeding" line before prepare() and a "seeding done, starting churn" line after,
	// both emitted before the first escalation panel — otherwise a large seed looks
	// stuck (no output) while WAL grows on the server.
	logger := &captureLogger{}
	config := Config{
		Conninfo:       db.TestConninfo,
		Rate:           50,
		Rows:           5,
		PayloadBytes:   64,
		ReportInterval: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	w, err := NewWorkload(config, logger)
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Give Run time to seed and start churn (the first panel fires only after the
	// report interval), then cancel.
	time.Sleep(300 * time.Millisecond)
	cancel()
	assert.NoError(t, <-done)

	lines := logger.lines()

	seedingIdx, seedingDoneIdx, firstPanelIdx := -1, -1, -1
	for i, line := range lines {
		switch {
		case seedingIdx == -1 && strings.Contains(line, "slot-bloat: seeding ") && strings.Contains(line, "rows"):
			seedingIdx = i
		case seedingDoneIdx == -1 && strings.Contains(line, "slot-bloat: seeding done, starting churn"):
			seedingDoneIdx = i
		case firstPanelIdx == -1 && strings.Contains(line, "payload-written="):
			firstPanelIdx = i
		}
	}

	assert.NotEqual(t, -1, seedingIdx, "a seeding line must be emitted")
	assert.NotEqual(t, -1, seedingDoneIdx, "a seeding-done line must be emitted")
	assert.Less(t, seedingIdx, seedingDoneIdx, "seeding line must precede seeding-done line")
	if firstPanelIdx != -1 {
		assert.Less(t, seedingDoneIdx, firstPanelIdx, "seed feedback must precede the first panel")
	}
}

func TestWorkload_Run_seedRowCount(t *testing.T) {
	// After prepare() the seed table holds exactly N rows.
	const rows = 7
	conn, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = conn.Close() }()

	tableIdent := "\"noisia_slotbloat_seedtest\""
	defer func() { _, _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+tableIdent) }()

	err = prepare(context.Background(), conn, tableIdent, rows, 32)
	assert.NoError(t, err)

	cnt := countRows(t, conn, tableIdent)
	assert.Equal(t, rows, cnt)
}

func TestWorkload_Run_flatHeap(t *testing.T) {
	// The payload counter grows during churn while the heap stays flat: count(*)
	// remains == N across the run.
	const rows = 8
	config := Config{
		Conninfo:       db.TestConninfo,
		Rate:           100,
		Rows:           rows,
		PayloadBytes:   128,
		ReportInterval: 100 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()

	// Wait until the table appears, then assert flat heap mid-run.
	var table string
	for i := 0; i < 50; i++ {
		table = discoverSlotBloatTable(t, obs)
		if table != "" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.NotEmpty(t, table, "seed table must appear")

	tableIdent := "\"" + table + "\""
	assert.Equal(t, rows, countRows(t, obs, tableIdent))

	// Let churn run, then re-check the row count is still N (flat heap).
	time.Sleep(400 * time.Millisecond)
	assert.Equal(t, rows, countRows(t, obs, tableIdent))

	// The churn loop must actually have issued UPDATEs against the seed table —
	// otherwise a flat heap alone would also pass with zero writes. n_tup_upd is
	// updated asynchronously, so poll for it (mirroring the table-appears poll above).
	var upd int64
	for i := 0; i < 50; i++ {
		upd = tupUpd(t, obs, table)
		if upd > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.Greater(t, upd, int64(0), "the churn loop must have issued UPDATEs against the seed table")

	cancel()
	assert.NoError(t, <-done)
}

// tupUpd returns n_tup_upd for the given relation from pg_stat_user_tables. Used to
// prove the churn loop actually wrote (stats are updated asynchronously).
func tupUpd(t *testing.T, conn db.Conn, relname string) int64 {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT coalesce(n_tup_upd, 0) FROM pg_stat_user_tables WHERE relname = $1", relname)
	assert.NoError(t, err)
	defer rows.Close()
	var n int64
	for rows.Next() {
		assert.NoError(t, rows.Scan(&n))
	}
	assert.NoError(t, rows.Err())
	return n
}

func TestWorkload_Run_cleanupDropsByDefault(t *testing.T) {
	// Graceful exit drops both the slot and the table by default.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeSlotBloat(t, obs) // start from a clean slate, independent of test order

	config := Config{
		Conninfo:       db.TestConninfo,
		Rate:           50,
		Rows:           5,
		PayloadBytes:   64,
		ReportInterval: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)
	assert.NoError(t, w.Run(ctx))

	assert.Equal(t, 0, countSlots(t, obs), "slot must be dropped")
	assert.Empty(t, discoverSlotBloatTable(t, obs), "table must be dropped")
}

func TestWorkload_Run_cleanupDropsAfterMidChurnCancel(t *testing.T) {
	// On Ctrl+C landing mid-UPDATE (unlimited rate), pgx aborts the in-flight query
	// and poisons the workload's single connection. Cleanup must still drop the slot
	// and table — it must not depend on the (now dead) workload conn. This reproduces
	// the stand bug where the slot was orphaned after a mid-churn cancel.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeSlotBloat(t, obs) // start from a clean slate, independent of test order
	defer purgeSlotBloat(t, obs)

	config := Config{
		Conninfo:       db.TestConninfo,
		Rate:           0, // unlimited: the cancel lands mid-UPDATE and poisons the conn
		Rows:           5,
		PayloadBytes:   1024,
		ReportInterval: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait until the slot and table actually exist, so churn is active.
	var table string
	for i := 0; i < 100; i++ {
		table = discoverSlotBloatTable(t, obs)
		if table != "" && countSlots(t, obs) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.NotEmpty(t, table, "seed table must appear before cancel")
	assert.Equal(t, 1, countSlots(t, obs), "slot must exist before cancel")

	// Cancel while churn is hammering: the cancel lands mid-UPDATE and poisons the
	// workload conn.
	cancel()
	<-done

	assert.Equal(t, 0, countSlots(t, obs), "slot must be dropped even after mid-churn cancel")
	assert.Empty(t, discoverSlotBloatTable(t, obs), "table must be dropped even after mid-churn cancel")
}

func TestWorkload_Run_keepSlot(t *testing.T) {
	// With KeepSlot the slot and table survive graceful exit.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeSlotBloat(t, obs) // start from a clean slate, independent of test order

	config := Config{
		Conninfo:       db.TestConninfo,
		Rate:           50,
		Rows:           5,
		PayloadBytes:   64,
		ReportInterval: 200 * time.Millisecond,
		KeepSlot:       true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)
	assert.NoError(t, w.Run(ctx))

	table := discoverSlotBloatTable(t, obs)
	assert.NotEmpty(t, table, "table must be kept")
	assert.Equal(t, 1, countSlots(t, obs), "slot must be kept")

	// Manual cleanup so the kept objects don't leak into other tests.
	purgeSlotBloat(t, obs)
}

func TestWorkload_Run_applicationName(t *testing.T) {
	// The dedicated connection reports application_name=noisia mid-run.
	config := Config{
		Conninfo:       db.TestConninfo,
		Rate:           50,
		Rows:           5,
		PayloadBytes:   64,
		ReportInterval: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	time.Sleep(500 * time.Millisecond)

	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	rows, err := obs.Query(context.Background(),
		"SELECT count(*) FROM pg_stat_activity WHERE application_name = 'noisia'")
	assert.NoError(t, err)
	var n int
	for rows.Next() {
		assert.NoError(t, rows.Scan(&n))
	}
	rows.Close()
	_ = obs.Close()
	assert.GreaterOrEqual(t, n, 1, "the workload connection must report application_name=noisia")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_initErrorNoReplication(t *testing.T) {
	// A role without REPLICATION cannot create the slot: Run returns an init error
	// mentioning the privilege, and the Conninfo (with a sentinel) never leaks.
	admin, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = admin.Close() }()

	_, _, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS noisia_norepl")
	_, _, err = admin.Exec(context.Background(),
		"CREATE ROLE noisia_norepl LOGIN PASSWORD 'SENTINEL_SECRET' NOREPLICATION")
	assert.NoError(t, err)
	// Needs CREATE on public for the (never-reached) seed step; grant it so the only
	// failure is the missing REPLICATION privilege.
	_, _, _ = admin.Exec(context.Background(), "GRANT CREATE ON SCHEMA public TO noisia_norepl")
	defer func() { _, _, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS noisia_norepl") }()

	// Build the role's conninfo from the test DSN, swapping in user/password.
	conninfo := swapUserPassword(t, db.TestConninfo, "noisia_norepl", "SENTINEL_SECRET")

	config := Config{
		Conninfo:       conninfo,
		Rate:           50,
		Rows:           5,
		PayloadBytes:   64,
		ReportInterval: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)
	err = w.Run(ctx)
	assert.Error(t, err)
	assert.Contains(t, strings.ToUpper(err.Error()), "REPLICATION")
	assert.NotContains(t, err.Error(), "SENTINEL_SECRET")
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

// countSlots returns the number of noisia_slotbloat_% replication slots.
func countSlots(t *testing.T, conn db.Conn) int {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'noisia_slotbloat_%'")
	assert.NoError(t, err)
	defer rows.Close()
	var n int
	for rows.Next() {
		assert.NoError(t, rows.Scan(&n))
	}
	assert.NoError(t, rows.Err())
	return n
}

// purgeSlotBloat removes any leftover noisia_slotbloat_% slots and tables, so a
// cleanup-sensitive test starts from a clean slate regardless of test order.
func purgeSlotBloat(t *testing.T, conn db.Conn) {
	t.Helper()

	slotRows, err := conn.Query(context.Background(),
		"SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'noisia_slotbloat_%'")
	assert.NoError(t, err)
	var slots []string
	for slotRows.Next() {
		var name string
		assert.NoError(t, slotRows.Scan(&name))
		slots = append(slots, name)
	}
	slotRows.Close()
	for _, name := range slots {
		_, _, _ = conn.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", name)
	}

	tableRows, err := conn.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE tablename LIKE 'noisia_slotbloat_%'")
	assert.NoError(t, err)
	var tables []string
	for tableRows.Next() {
		var name string
		assert.NoError(t, tableRows.Scan(&name))
		tables = append(tables, name)
	}
	tableRows.Close()
	for _, name := range tables {
		_, _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS \""+name+"\"")
	}
}

// swapUserPassword rewrites a URL-form DSN (as produced by testcontainers), replacing
// the userinfo so a test can reconnect to the same host/port/db as a different role.
func swapUserPassword(t *testing.T, dsn, user, password string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	assert.NoError(t, err)
	u.User = url.UserPassword(user, password)
	return u.String()
}
