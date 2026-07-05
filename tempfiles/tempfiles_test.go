package tempfiles

import (
	"context"
	"strings"
	"sync"
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
		{valid: true, config: Config{Jobs: 1, Rate: 1}},
		{valid: false, config: Config{Jobs: 0, Rate: 1}},
		{valid: false, config: Config{Jobs: 1, Rate: 0}},
	}

	for _, tc := range testcases {
		if tc.valid {
			assert.NoError(t, tc.config.validate())
		} else {
			assert.Error(t, tc.config.validate())
		}
	}
}

func TestNewWorkload(t *testing.T) {
	testcases := []struct {
		valid bool
		cfg   Config
	}{
		{valid: true, cfg: Config{Jobs: 1, Rate: 1}},
		{valid: false, cfg: Config{Jobs: 1, Rate: 0}},
	}

	for _, tc := range testcases {
		w, err := NewWorkload(tc.cfg, log.NewDefaultLogger("error"))
		if tc.valid {
			assert.NoError(t, err)
			assert.NotNil(t, w)
		} else {
			assert.Error(t, err)
		}
	}
}

// fakeDB is a db.DB double that records the SQL passed to Exec. Only Exec is exercised by
// execQuery/startLoop; the other interface methods are inert. It is concurrency-safe because
// startLoop fires Exec from multiple goroutines.
type fakeDB struct {
	mu    sync.Mutex
	execs []string
}

func (f *fakeDB) Begin(ctx context.Context) (db.Tx, error) { return nil, nil }
func (f *fakeDB) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
	f.mu.Lock()
	f.execs = append(f.execs, sql)
	f.mu.Unlock()
	return 0, "", nil
}
func (f *fakeDB) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return nil, nil
}
func (f *fakeDB) Close() {}
func (f *fakeDB) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.execs) }
func (f *fakeDB) sqls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.execs...)
}

// Test_execQuery_issuesOnlyQueryNoWorkMem is the AC1 regression guard: execQuery must issue
// exactly the sort query and never a SET work_mem — that lowering is what made the book's
// remediation undemonstrable. A live DB cannot prove the negative (a statement was never sent),
// so a double is the right tool here.
func Test_execQuery_issuesOnlyQueryNoWorkMem(t *testing.T) {
	f := &fakeDB{}
	const q = `SELECT id, payload FROM "noisia_tempfiles_x" ORDER BY random()`

	err := execQuery(context.Background(), f, q)
	assert.NoError(t, err)

	assert.Equal(t, []string{q}, f.sqls(), "execQuery must issue exactly the given query")
	for _, s := range f.sqls() {
		assert.NotContains(t, strings.ToLower(s), "work_mem", "execQuery must never set work_mem")
	}
}

// Test_startLoop asserts behaviour, not just a clean return: the loop must actually issue the
// sort query (and nothing else) at the configured rate. It drives startLoop over the fakeDB
// double and cancels once a few queries have fired, so it never depends on wall-clock timing.
func Test_startLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	f := &fakeDB{}
	const q = `SELECT id, payload FROM "noisia_tempfiles_x" ORDER BY random()`

	// Stop the loop once it has fired a few queries — proving queries were issued rather than
	// waiting the full timeout for a possibly-empty loop.
	go func() {
		for f.count() < 3 {
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
	}()

	err := startLoop(ctx, f, log.NewDefaultLogger("error"), 50, q)
	assert.NoError(t, err)

	got := f.sqls()
	assert.GreaterOrEqual(t, len(got), 1, "startLoop must issue at least one query")
	for _, s := range got {
		assert.Equal(t, q, s, "startLoop must issue exactly the sort query")
	}
}

// Test_prepare_and_cleanup seeds a small dataset table, asserts it holds the rows, then proves
// cleanup drops it over its OWN fresh connection (self-sufficient, ADR-002-2).
func Test_prepare_and_cleanup(t *testing.T) {
	ctx := context.Background()

	conn, err := db.Connect(ctx, db.TestConninfo)
	assert.NoError(t, err)

	rawName := "noisia_tempfiles_test_" + randomSuffix(6)
	tableIdent := `"` + rawName + `"`

	err = prepare(ctx, conn, tableIdent, 100)
	assert.NoError(t, err)

	seeded := scalarInt(t, conn, "SELECT count(*) FROM "+tableIdent)
	assert.Equal(t, 100, seeded, "prepare must seed the requested number of rows")
	_ = conn.Close()

	// cleanup opens its own fresh connection; it must not depend on the (now closed) seed conn.
	w := &workload{config: Config{Conninfo: db.TestConninfo}, logger: log.NewDefaultLogger("error")}
	w.cleanup(tableIdent)

	conn2, err := db.Connect(ctx, db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = conn2.Close() }()

	remaining := scalarInt(t, conn2, "SELECT count(*) FROM pg_class WHERE relname = $1", rawName)
	assert.Equal(t, 0, remaining, "cleanup must drop the dataset table")
}

func Test_countTempBytes(t *testing.T) {
	bytes, err := countTempBytes(db.TestConninfo)
	assert.NoError(t, err)
	assert.Greater(t, bytes, -1)
}

// scalarInt runs a single-row single-column int query and returns its value, checking rows.Err()
// so a mid-iteration scan error is not silently swallowed.
func scalarInt(t *testing.T, conn db.Conn, sql string, args ...interface{}) int {
	t.Helper()
	rows, err := conn.Query(context.Background(), sql, args...)
	assert.NoError(t, err)
	defer rows.Close()

	var v int
	for rows.Next() {
		assert.NoError(t, rows.Scan(&v))
	}
	assert.NoError(t, rows.Err())
	return v
}
