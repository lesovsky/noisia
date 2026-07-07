package tempfiles

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testTableSize is a shrunk-for-speed dataset size shared by the integration tests. It is kept
// comfortably above the default 4MB work_mem so the sort still spills, and above the 16 MiB
// validate() floor so NewWorkload accepts it.
const testTableSize int64 = 24 << 20

// TestWorkload_Run_producesTempFiles runs the full workload against the container's
// DEFAULT-configured PostgreSQL and asserts two things:
//  1. it produces temp files (database temp_bytes grows), which proves the sort genuinely
//     spills to disk on the default work_mem — with no SET work_mem anywhere;
//  2. the per-run dataset table is dropped on exit (no noisia_tempfiles_% left behind).
//
// The seed is shrunk for speed (via Config.TableSize) but kept well above the default 4MB
// work_mem so the sort still spills.
func TestWorkload_Run_producesTempFiles(t *testing.T) {
	conn, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// The natural-spill contract depends on the sort exceeding work_mem. If the container were
	// tuned with a work_mem at/above the (shrunk) dataset, the sort would not spill and the
	// assertion below would fail confusingly — skip loudly instead.
	workMemKB := scalarInt(t, conn, "SELECT setting::bigint FROM pg_settings WHERE name = 'work_mem'")
	if int64(workMemKB)*1024 >= testTableSize {
		t.Skipf("work_mem (%d kB) is >= dataset (%d bytes); the sort would not spill", workMemKB, testTableSize)
	}

	// Clean slate independent of test order: drop any noisia_tempfiles_% tables an earlier
	// crashed/aborted run may have orphaned, so the leftover assertion cannot false-fail.
	purgeTempfiles(t, conn)

	before, err := countTempBytes(db.TestConninfo)
	assert.NoError(t, err)

	w, err := NewWorkload(
		Config{Conninfo: db.TestConninfo, Jobs: 2, Rate: 5, TableSize: testTableSize},
		log.NewDefaultLogger("error"),
	)
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	err = w.Run(ctx)
	assert.NoError(t, err)

	after, err := countTempBytes(db.TestConninfo)
	assert.NoError(t, err)
	assert.Greater(t, after, before, "the sort must spill temp bytes on a default-configured server")

	// The dataset table must be dropped on exit.
	leftovers := scalarInt(t, conn, "SELECT count(*) FROM pg_class WHERE relname LIKE 'noisia_tempfiles_%'")
	assert.Equal(t, 0, leftovers, "the dataset table must be dropped on exit")
}

// purgeTempfiles drops any leftover noisia_tempfiles_% tables so an integration test starts from
// a clean slate regardless of test order or prior crashed runs.
func purgeTempfiles(t *testing.T, conn db.Conn) {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT relname FROM pg_class WHERE relname LIKE 'noisia_tempfiles_%'")
	assert.NoError(t, err)

	var names []string
	for rows.Next() {
		var n string
		assert.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}
	assert.NoError(t, rows.Err())
	rows.Close()

	for _, n := range names {
		_, _, _ = conn.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, n))
	}
}

// TestWorkload_Run_seedSizeFaithful is the ADR-008-2 regression guard: it proves the configured
// --tempfiles.table-size actually materializes on disk. The tempfiles payload is inline (256B,
// below the ~2KB TOAST threshold) with no index, so the requested bytes land in the MAIN heap
// fork — pg_relation_size, not pg_total_relation_size, is the right metric. The 0.6× threshold is
// well below the real ~1.0-1.1× (page/fillfactor overhead) yet far above a decoupled-from-size
// regression (e.g. the old TOASTed-away bloatchurn heap at <0.02×).
func TestWorkload_Run_seedSizeFaithful(t *testing.T) {
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()

	// Clean slate so table discovery cannot latch onto an orphan from an earlier crashed run.
	purgeTempfiles(t, obs)

	w, err := NewWorkload(
		Config{Conninfo: db.TestConninfo, Jobs: 2, Rate: 5, TableSize: testTableSize},
		log.NewDefaultLogger("error"),
	)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// The whole seed (CREATE + INSERT) commits in one transaction, so the moment the table is
	// visible the full heap is already on disk.
	table := waitForTempfilesTable(t, obs)

	heapSize := relationSize(t, obs, table) // MAIN fork: the inline payload lives in the heap
	// 0.6× the target, computed in integer arithmetic (testTableSize is a constant, so a float
	// literal would not fold to an int64).
	minExpected := testTableSize * 6 / 10
	t.Logf("seed pg_relation_size = %d bytes (target %d, floor %d)", heapSize, testTableSize, minExpected)
	assert.GreaterOrEqual(t, heapSize, minExpected,
		"seed heap (%d bytes) must reach ~table-size, proving --tempfiles.table-size materializes on disk (target %d, floor %d)",
		heapSize, testTableSize, minExpected)

	cancel()
	assert.NoError(t, <-done)
}

// waitForTempfilesTable bounded-polls until the single seed table appears. The budget is generous
// (500 × 20ms = 10s): seeding can run slowly when the shared container is already under load from
// prior tests in the same package (the TD-005-1/TD-007-1 flakiness class).
func waitForTempfilesTable(t *testing.T, conn db.Conn) string {
	t.Helper()
	var table string
	for i := 0; i < 500; i++ {
		table = discoverTempfilesTable(t, conn)
		if table != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return table
}

// discoverTempfilesTable returns the single noisia_tempfiles_% table present in the target DB, or
// "" if none. The suffix is random, so integration tests discover the table rather than knowing
// its name.
func discoverTempfilesTable(t *testing.T, conn db.Conn) string {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE tablename LIKE 'noisia_tempfiles_%'")
	assert.NoError(t, err)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		assert.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	assert.NoError(t, rows.Err())
	// At most one tempfiles table should exist at a time; more than one means a prior test
	// orphaned its table, which would mask a real cleanup regression.
	assert.LessOrEqual(t, len(names), 1, "at most one tempfiles table should exist at a time")
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

// relationSize returns pg_relation_size (the MAIN fork only — heap, excluding TOAST and indexes)
// for the given relation. The table must be non-empty: a discovery timeout would otherwise pass
// "" to pg_relation_size, whose error leaves a nil rows handle and panics on Close — require fails
// the test cleanly instead. The UNQUOTED relname from pg_tables is the bind arg.
func relationSize(t *testing.T, conn db.Conn, table string) int64 {
	t.Helper()
	require.NotEmpty(t, table, "seed table must be discovered before reading its size")
	return scalarInt64(t, conn, "SELECT pg_relation_size($1)", table)
}

// scalarInt64 runs a single-int64 query (db.Conn has no QueryRow) and returns the first row's
// value, or 0 if there is no row.
func scalarInt64(t *testing.T, conn db.Conn, query string, args ...interface{}) int64 {
	t.Helper()
	rows, err := conn.Query(context.Background(), query, args...)
	assert.NoError(t, err)
	defer rows.Close()
	var n int64
	for rows.Next() {
		assert.NoError(t, rows.Scan(&n))
	}
	assert.NoError(t, rows.Err())
	return n
}
