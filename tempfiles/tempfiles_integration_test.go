package tempfiles

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"github.com/stretchr/testify/assert"
)

// TestWorkload_Run_producesTempFiles runs the full workload against the container's
// DEFAULT-configured PostgreSQL and asserts two things:
//  1. it produces temp files (database temp_bytes grows), which proves the sort genuinely
//     spills to disk on the default work_mem — with no SET work_mem anywhere;
//  2. the per-run dataset table is dropped on exit (no noisia_tempfiles_% left behind).
//
// The seed is shrunk for speed (via Config.TableSize) but kept well above the default 4MB
// work_mem so the sort still spills.
func TestWorkload_Run_producesTempFiles(t *testing.T) {
	// A shrunk-for-speed dataset, still comfortably above the default 4MB work_mem so the
	// sort spills. Reused in the Config below and the skip guard.
	const testTableSize int64 = 24 << 20

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
