package waitxacts

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"github.com/stretchr/testify/assert"
)

func TestConfig_validate(t *testing.T) {
	baseline := Config{
		LocktimeMin:    5 * time.Second,
		LocktimeMax:    10 * time.Second,
		Waiters:        10,
		WaitersRate:    1,
		ReportInterval: time.Second,
	}

	testcases := []struct {
		name   string
		valid  bool
		config Config
	}{
		{name: "valid baseline", valid: true, config: baseline},
		{name: "waiters zero is unlimited (valid)", valid: true, config: func() Config { c := baseline; c.Waiters = 0; return c }()},
		{name: "waiters-rate zero invalid", valid: false, config: func() Config { c := baseline; c.WaitersRate = 0; return c }()},
		{name: "waiters-rate negative invalid", valid: false, config: func() Config { c := baseline; c.WaitersRate = -1; return c }()},
		{name: "report-interval zero invalid", valid: false, config: func() Config { c := baseline; c.ReportInterval = 0; return c }()},
		{name: "report-interval negative invalid", valid: false, config: func() Config { c := baseline; c.ReportInterval = -time.Second; return c }()},
		{name: "locktime-min greater than max invalid", valid: false, config: func() Config { c := baseline; c.LocktimeMin = 20 * time.Second; return c }()},
		{name: "locktime-max zero invalid", valid: false, config: func() Config { c := baseline; c.LocktimeMax = 0; return c }()},
		{name: "locktime-min zero invalid", valid: false, config: func() Config { c := baseline; c.LocktimeMin = 0; return c }()},
		{name: "both locktimes zero invalid", valid: false, config: func() Config { c := baseline; c.LocktimeMin = 0; c.LocktimeMax = 0; return c }()},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.valid {
				assert.NoError(t, tc.config.validate())
			} else {
				assert.Error(t, tc.config.validate())
			}
		})
	}
}

func Test_sanitize_noDSNInLogs(t *testing.T) {
	// A connect error carrying a URL-form DSN (as pgx produces) must be redacted so
	// neither the password nor any DSN fragment reaches the log.
	err := fmt.Errorf("failed to connect to `postgres://appuser:hunter2@db.internal:5432/noisia?sslmode=disable`: too many clients")

	out := sanitize(err)
	assert.NotContains(t, out, "hunter2")
	assert.NotContains(t, out, "appuser")
	assert.NotContains(t, out, "db.internal")
	assert.NotContains(t, out, "://")
	assert.Equal(t, "connection error (details suppressed)", out)
}

func TestWorkload_Run_waitersHonored(t *testing.T) {
	// With Waiters=N, exactly up to N noisia backends block on the locked table
	// (wait_event_type='Lock'). Each waiter owns its own db.Connect, so the count is
	// independent of the host CPU / pool sizing. A slow rate=1/s exercises both the
	// accrual pacing and the hard cap.
	const waiters = 2
	config := Config{
		Conninfo:       db.TestConninfo,
		Fixture:        true,
		LocktimeMin:    10 * time.Second,
		LocktimeMax:    20 * time.Second,
		Waiters:        waiters,
		WaitersRate:    1,
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

	// Lower bound: poll until at least Waiters noisia backends are blocked on a Lock.
	// The observer conn does not set application_name, so it is not counted.
	var n int
	for i := 0; i < 250; i++ {
		n = countBlockedNoisiaBackends(t, obs)
		if n >= waiters {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, n, waiters, "at least Waiters backends must be blocked on the locked table, independent of host CPU")

	// Upper bound (hard cap): the manager must never spawn past Waiters. Were the cap
	// not enforced, rate=1/s would keep adding waiters every second — sample across a
	// >1s window to catch any overshoot.
	for i := 0; i < 60; i++ {
		assert.LessOrEqual(t, countBlockedNoisiaBackends(t, obs), waiters, "the manager must never exceed the Waiters cap")
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_gracefulRefused(t *testing.T) {
	// A role with CONNECTION LIMIT n makes the (n+1)-th connect fail with
	// "too many connections for role" — the expected too-many-clients path. The run
	// must not crash and the established pile must be preserved.
	const connLimit = 4

	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()

	_, _, _ = obs.Exec(context.Background(), "DROP ROLE IF EXISTS noisia_wx_limited")
	_, _, err = obs.Exec(context.Background(),
		fmt.Sprintf("CREATE ROLE noisia_wx_limited LOGIN PASSWORD 'SENTINEL_SECRET' CONNECTION LIMIT %d", connLimit))
	assert.NoError(t, err)
	_, _, _ = obs.Exec(context.Background(), "GRANT CREATE ON SCHEMA public TO noisia_wx_limited")
	defer func() { _, _, _ = obs.Exec(context.Background(), "DROP ROLE IF EXISTS noisia_wx_limited") }()

	// A superuser bypasses CONNECTION LIMIT, which would make this proof vacuous.
	assert.False(t, scalarBool(t, obs,
		"SELECT rolsuper FROM pg_roles WHERE rolname = 'noisia_wx_limited'"),
		"the limited role must NOT be superuser (else the refusal path is never driven)")

	conninfo := swapUserPassword(t, db.TestConninfo, "noisia_wx_limited", "SENTINEL_SECRET")

	config := Config{
		Conninfo:       conninfo,
		Fixture:        true,
		LocktimeMin:    10 * time.Second,
		LocktimeMax:    20 * time.Second,
		Waiters:        0, // unlimited: manager keeps trying to spawn past the connection limit
		WaitersRate:    50,
		ReportInterval: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Capture the reporter lines so we can assert the refused counter actually grows.
	logger := &captureLogger{}
	w, err := NewWorkload(config, logger)
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// The (n+1)-th connect is refused by the role's CONNECTION LIMIT: the reporter must
	// eventually print refused>0. This assertion fails if refused stops being counted.
	var refused int
	for i := 0; i < 150; i++ {
		refused = maxRefused(logger.lines())
		if refused > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Greater(t, refused, 0, "refused must increment once the role connection limit is hit")

	// The pile is capped by the role's CONNECTION LIMIT (holder takes one slot), so
	// with Waiters=0 the established count plateaus instead of growing unbounded — the
	// established waiters are preserved despite the refusals. Observe via the superuser
	// observer, which is not subject to the role's limit.
	var n int
	for i := 0; i < 150; i++ {
		n = countBlockedNoisiaBackends(t, obs)
		if n >= connLimit-2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, n, connLimit-2, "the pile must be preserved up to the role connection limit despite refusals")

	cancel()
	// No crash: refused connects are the expected observable, not a fatal error.
	assert.NoError(t, <-done)
}

func TestWorkload_Run_cleanupDrains(t *testing.T) {
	config := Config{
		Conninfo:       db.TestConninfo,
		Fixture:        true,
		LocktimeMin:    2 * time.Second,
		LocktimeMax:    4 * time.Second,
		Waiters:        3,
		WaitersRate:    20,
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

	// Let the pile build up before cancelling.
	for i := 0; i < 150; i++ {
		if countBlockedNoisiaBackends(t, obs) >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	assert.NoError(t, <-done)

	// After a clean drain the fixture table is gone and no noisia session lingers.
	assert.False(t, tableExists(t, obs, "_noisia_waitxacts_workload"), "fixture table must be dropped after cleanup")

	var lingering int
	for i := 0; i < 150; i++ {
		lingering = countNoisiaBackends(t, obs)
		if lingering == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Equal(t, 0, lingering, "no noisia sessions may linger after drain")
}

// countBlockedNoisiaBackends returns the number of noisia backends parked on a Lock.
func countBlockedNoisiaBackends(t *testing.T, conn db.Conn) int {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT count(*) FROM pg_stat_activity WHERE application_name = 'noisia' AND wait_event_type = 'Lock'")
	assert.NoError(t, err)
	defer rows.Close()
	var n int
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

// tableExists reports whether a regular table with the given name exists.
func tableExists(t *testing.T, conn db.Conn, name string) bool {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT count(*) FROM pg_class WHERE relname = $1 AND relkind = 'r'", name)
	assert.NoError(t, err)
	defer rows.Close()
	var n int
	for rows.Next() {
		assert.NoError(t, rows.Scan(&n))
	}
	assert.NoError(t, rows.Err())
	return n > 0
}

// scalarBool reads a single boolean scalar from the given query.
func scalarBool(t *testing.T, conn db.Conn, query string) bool {
	t.Helper()
	rows, err := conn.Query(context.Background(), query)
	assert.NoError(t, err)
	defer rows.Close()
	var b bool
	for rows.Next() {
		assert.NoError(t, rows.Scan(&b))
	}
	assert.NoError(t, rows.Err())
	return b
}

// swapUserPassword rewrites a URL-form DSN, replacing the userinfo so a test can
// reconnect to the same host/port/db as a different role.
func swapUserPassword(t *testing.T, dsn, user, password string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	assert.NoError(t, err)
	u.User = url.UserPassword(user, password)
	return u.String()
}

// captureLogger is a log.Logger test double that records every Infof line so a test
// can assert the reporter panel content (e.g. refused=<n>) without touching real output.
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

// maxRefused parses the reporter lines for the "refused=<n>" field and returns the
// largest value seen (the counter is monotonic, so this is its latest reported value).
func maxRefused(lines []string) int {
	const key = "refused="
	max := 0
	for _, l := range lines {
		idx := strings.Index(l, key)
		if idx < 0 {
			continue
		}
		rest := l[idx+len(key):]
		end := 0
		for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
			end++
		}
		if end == 0 {
			continue
		}
		if n, err := strconv.Atoi(rest[:end]); err == nil && n > max {
			max = n
		}
	}
	return max
}
