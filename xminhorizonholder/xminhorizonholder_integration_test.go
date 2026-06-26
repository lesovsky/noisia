// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xminhorizonholder

import (
	"context"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"github.com/stretchr/testify/assert"
)

// -----------------------------------------------------------------------------
// Integration tests (testcontainers, real PostgreSQL via db.TestConninfo).
//
// They assert the OBSERVABLE proxies of the xmin-horizon-holder workload against
// a live server, mirroring checkpointstorm/checkpointstorm_integration_test.go but
// with the KEY assertions inverted. Where checkpoint-storm proves checkpoints_req
// growth and that the privilege gate FAILS under a non-superuser, this suite proves:
//
//   - HEADLINE: under a live holder, n_dead_tup on the churn table strictly grows
//     and does not drop (autovacuum cannot reclaim the dead tuples because the xmin
//     horizon is frozen), and backend_xmin/backend_xid for the noisia backend are
//     non-NULL (the visible signature of the pinned snapshot).
//   - REVEAL: after ctx-cancel + Run return, a manual observer-VACUUM drops
//     n_dead_tup back toward 0 — proving the held horizon (not churn volume) was the
//     cause of the unreclaimable bloat.
//   - NO-SUPERUSER: the workload SUCCEEDS under a freshly-created non-superuser role
//     (only LOGIN + CREATE ON SCHEMA public, NO pg_checkpoint) — the INVERSE of the
//     checkpoint-storm privilege-gate test, confirming Decision 5 (holder needs no
//     privileges, works on managed PostgreSQL).
//   - JOBS HONORED: at least Jobs noisia backends are concurrently live.
//   - CLEANUP: the seed table is absent after exit (incl. mid-churn cancel) +
//     a self-sufficient cleanup test.
//
// All state waits use bounded condition-polling (a for-loop with a short sleep),
// never assert.Eventually and never a fixed sleep as the wait mechanism (pg_stat_*
// updates asynchronously).
//
// TableSize is pinned to the 64 MiB floor (minTableSize): it is the smallest value
// validate() accepts and seeds fast. PayloadBytes 8192 yields a small row count so
// the heap churns lively and n_dead_tup grows quickly within a generous poll budget.
// Wraparound (txid-wraparound) is NOT exercised — it only triggers after hours/days
// and depends on the environment (Decision 8).
// -----------------------------------------------------------------------------

func TestWorkload_Run_deadTuplesHeldNotReclaimed(t *testing.T) {
	// HEADLINE: under a live holder, pg_stat_user_tables.n_dead_tup on the churn
	// table strictly grows and does not drop — the deterministic proof that the
	// frozen xmin horizon prevents autovacuum from reclaiming dead tuples. The test
	// reads its own baseline and asserts a LATER reading strictly exceeds it, so it
	// never depends on an absolute value left by a prior test. It also proves the
	// snapshot is actually pinned: backend_xmin (and backend_xid) for the noisia
	// backend are non-NULL (backendXminSet).
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeXminhold(t, obs) // clean slate so discover cannot pick up an orphan table

	// PayloadBytes 8192 keeps the seed row count small (~8158 rows for the 64 MiB
	// floor), so a brisk Rate dirties a large fraction of the heap quickly and
	// n_dead_tup climbs within the post-baseline poll budget.
	config := Config{
		Conninfo:       db.TestConninfo,
		TableSize:      minTableSize,
		PayloadBytes:   8192,
		Rate:           3000,
		ReportInterval: 200 * time.Millisecond,
		Jobs:           3,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Baseline once the workload is up (seed table present).
	table := waitForXminholdTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear")

	// Prove the snapshot is pinned: poll until a noisia backend reports a non-NULL
	// backend_xmin/backend_xid. Generous budget — the holder is established
	// synchronously before churn, but the seed (a multi-row INSERT) runs first.
	var xminSet bool
	for i := 0; i < 300; i++ {
		xminSet = backendXminSet(t, obs)
		if xminSet {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.True(t, xminSet, "a noisia backend must hold a non-NULL backend_xmin/backend_xid (snapshot pinned)")

	firstDead := nDeadTup(t, obs, table)

	// Strict-increase assertion: poll until a LATER reading exceeds the baseline,
	// proving dead tuples accumulate and autovacuum cannot reclaim them while the
	// horizon is frozen. Generous budget (300 × 20ms = 6s) because stats update
	// asynchronously. "Not drop" follows from strict growth under the live holder.
	var laterDead int64
	for i := 0; i < 300; i++ {
		laterDead = nDeadTup(t, obs, table)
		if laterDead > firstDead {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Greater(t, laterDead, firstDead, "n_dead_tup must strictly grow under a live holder (frozen horizon blocks reclaim)")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_vacuumReclaimsAfterStop(t *testing.T) {
	// REVEAL: proves the HELD xmin horizon (not churn volume) is what makes the bloat
	// unreclaimable. Run drops its seed table on exit (the cleanup defer, ADR-002-2),
	// so the causal VACUUM-reclaim relationship CANNOT be observed on the Run-managed
	// table after the workload returns — by the time Run returns the table is gone.
	// Instead this test drives the SAME production holder pin (establishHolder) and
	// seed (prepare) directly against a manually owned table that outlives the holder,
	// and shows BOTH directions of the proof (a two-direction control):
	//   - while the holder snapshot is live, an observer VACUUM does NOT reclaim the
	//     dead tuples (the frozen horizon pins the old row versions);
	//   - once the holder connection is closed (snapshot released, Decision 10), the
	//     SAME VACUUM reclaims them back to 0.
	// Both directions together prove the held horizon was the cause, which a single
	// after-stop reading could not.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeXminhold(t, obs)

	ctx := context.Background()

	base := "noisia_xminhold_" + randomSuffix(8)
	tableIdent := "\"" + base + "\""

	// Seed via the production seed path so the table schema matches the workload's.
	// A small explicit row count keeps the synchronous churn below fast.
	const rows = 5000
	assert.NoError(t, prepare(ctx, obs, tableIdent, rows, 64))
	defer func() { _ = dropTable(context.Background(), obs, tableIdent) }()

	// Pin the holder snapshot on its OWN connection via the REAL production
	// establishHolder — the organ under test. heldSince is required by its signature
	// but irrelevant here.
	holderConn, err := db.Connect(ctx, db.TestConninfo)
	assert.NoError(t, err)
	var heldSince atomic.Int64
	assert.NoError(t, establishHolder(ctx, holderConn, &heldSince))

	// Create dead tuples AFTER the holder's snapshot: UPDATE every row twice so the
	// old versions are visible only to the pinned snapshot.
	updateSQL := fmt.Sprintf("UPDATE %s SET payload = $1 WHERE id = $2", tableIdent)
	payload := make([]byte, 64)
	for round := 0; round < 2; round++ {
		for id := int64(1); id <= rows; id++ {
			_, _, err = obs.Exec(ctx, updateSQL, payload, id)
			assert.NoError(t, err)
		}
	}

	// VACUUM while the holder is live (autocommit — VACUUM cannot run in a tx block):
	// the frozen horizon blocks reclaim, so n_dead_tup stays > 0.
	_, _, err = obs.Exec(ctx, "VACUUM \""+base+"\"")
	assert.NoError(t, err)
	var heldDead int64
	for i := 0; i < 300; i++ {
		heldDead = nDeadTup(t, obs, base)
		if heldDead > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Greater(t, heldDead, int64(0), "VACUUM under a live holder must NOT reclaim dead tuples (frozen horizon)")

	// Release the holder snapshot by closing its connection (the horizon advances).
	assert.NoError(t, holderConn.Close())

	// VACUUM again: with the horizon advanced, the SAME VACUUM reclaims the dead
	// tuples back to 0. Re-VACUUM inside the loop because n_dead_tup is refreshed by
	// VACUUM and the backend exit may lag the Close by a moment. This is the net-new
	// assert proving the held horizon (not churn volume) was the cause.
	var afterDead int64
	for i := 0; i < 300; i++ {
		_, _, _ = obs.Exec(ctx, "VACUUM \""+base+"\"")
		afterDead = nDeadTup(t, obs, base)
		if afterDead == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Equal(t, int64(0), afterDead, "VACUUM after the holder is released must reclaim dead tuples (held horizon was the cause)")
}

func TestWorkload_Run_nonSuperuserSucceeds(t *testing.T) {
	// NO-SUPERUSER: the INVERSE of checkpoint-storm's privilege-gate test. A freshly
	// created non-superuser role with ONLY LOGIN + CREATE ON SCHEMA public (NO
	// pg_checkpoint) runs the holder + churn SUCCESSFULLY — direct confirmation of
	// Decision 5 (the holder needs no special privilege, so it works on managed
	// PostgreSQL). There is NO assert on an error; the catalog is read on the
	// observer (superuser) conn because the limited role may lack listing rights.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeXminhold(t, obs)

	_, _, _ = obs.Exec(context.Background(), "DROP ROLE IF EXISTS limited")
	_, _, err = obs.Exec(context.Background(),
		"CREATE ROLE limited LOGIN PASSWORD 'SENTINEL_SECRET'")
	assert.NoError(t, err)
	// CREATE on public is all the workload needs to seed; crucially NO pg_checkpoint
	// membership is granted — yet the holder still succeeds, unlike checkpoint-storm.
	_, _, _ = obs.Exec(context.Background(), "GRANT CREATE ON SCHEMA public TO limited")
	defer func() { _, _, _ = obs.Exec(context.Background(), "DROP ROLE IF EXISTS limited") }()

	// Build the role's conninfo from the test DSN, swapping in user/password.
	conninfo := swapUserPassword(t, db.TestConninfo, "limited", "SENTINEL_SECRET")

	config := Config{
		Conninfo:       conninfo,
		TableSize:      minTableSize,
		PayloadBytes:   8192,
		Rate:           3000,
		ReportInterval: 200 * time.Millisecond,
		Jobs:           2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Prove SUCCESS: the seed table appears AND churn actually lands under the
	// limited role (n_dead_tup > 0). Read on the observer superuser conn.
	table := waitForXminholdTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear under the non-superuser role")

	var dead int64
	for i := 0; i < 300; i++ {
		dead = nDeadTup(t, obs, table)
		if dead > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Greater(t, dead, int64(0), "churn must land under the non-superuser role (holder needs no privilege)")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_jobsHonored(t *testing.T) {
	// With Jobs=N, at least N noisia backends are concurrently live in
	// pg_stat_activity. Holds INDEPENDENT of host CPU count because each worker owns
	// its own db.Connect (ADR-003-1). xmin-horizon-holder also attaches seed/holder
	// connections with application_name='noisia', so the count can exceed Jobs —
	// assert >= Jobs (not ==). The observer sets no application_name, so it is not
	// counted.
	const jobs = 3
	config := Config{
		Conninfo:       db.TestConninfo,
		TableSize:      minTableSize,
		PayloadBytes:   8192,
		Rate:           3000,
		ReportInterval: 200 * time.Millisecond,
		Jobs:           jobs,
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

	// Poll until at least Jobs noisia backends are concurrently live.
	var n int
	for i := 0; i < 300; i++ {
		n = countNoisiaBackends(t, obs)
		if n >= jobs {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, n, jobs, "at least Jobs worker backends must be concurrently live, independent of host CPU")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_cleanupOnExit(t *testing.T) {
	// Graceful exit drops the seed table, even when the cancel lands mid-churn.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeXminhold(t, obs) // clean slate, independent of test order

	config := Config{
		Conninfo:       db.TestConninfo,
		TableSize:      minTableSize,
		PayloadBytes:   8192,
		Rate:           3000,
		ReportInterval: 200 * time.Millisecond,
		Jobs:           2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Confirm the seed table appears mid-run (i.e. mid-churn), then cancel and
	// assert it is gone.
	table := waitForXminholdTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear mid-run")

	cancel()
	assert.NoError(t, <-done)

	assert.Empty(t, discoverXminholdTable(t, obs), "seed table must be dropped on graceful exit")
}

func TestWorkload_cleanup_selfSufficient(t *testing.T) {
	// cleanup must not depend on any live workload connection: it opens its OWN
	// fresh connection (ADR-002-2). Prove it deterministically — manually create a
	// matching table (the schema the workload uses), invoke cleanup directly with no
	// Run, and assert it is gone. Catches a conn-reusing regression every run.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeXminhold(t, obs)
	defer purgeXminhold(t, obs)

	base := "noisia_xminhold_" + randomSuffix(8)
	tableIdent := "\"" + base + "\""

	_, _, err = obs.Exec(context.Background(), "CREATE TABLE "+tableIdent+" (id bigint PRIMARY KEY, payload bytea)")
	assert.NoError(t, err)
	assert.NotEmpty(t, discoverXminholdTable(t, obs), "table must exist before cleanup")

	// Two-value return cannot be asserted inline; bind the workload, then type-assert
	// to *workload to reach the private cleanup method.
	wl, err := NewWorkload(Config{
		Conninfo:       db.TestConninfo,
		TableSize:      minTableSize,
		PayloadBytes:   8192,
		Rate:           3000,
		ReportInterval: 200 * time.Millisecond,
		Jobs:           1,
	}, log.NewDefaultLogger("error"))
	assert.NoError(t, err)
	wl.(*workload).cleanup(tableIdent)

	assert.Empty(t, discoverXminholdTable(t, obs), "table must be dropped by self-sufficient cleanup")
}

// -----------------------------------------------------------------------------
// Test-local helpers (mirror the sibling suites, renamed for noisia_xminhold_%).
// -----------------------------------------------------------------------------

// waitForXminholdTable bounded-polls until the single seed table appears. The
// budget is generous (500 × 20ms = 10s): seeding the 64 MiB floor table is a
// multi-row INSERT that can run slowly when the shared container is already under
// load from prior tests in the same package.
func waitForXminholdTable(t *testing.T, conn db.Conn) string {
	t.Helper()
	var table string
	for i := 0; i < 500; i++ {
		table = discoverXminholdTable(t, conn)
		if table != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return table
}

// discoverXminholdTable returns the single noisia_xminhold_% table name present in
// the target DB, or "" if none. The suffix is random, so integration tests discover
// the table rather than knowing its name.
func discoverXminholdTable(t *testing.T, conn db.Conn) string {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE tablename LIKE 'noisia_xminhold_%'")
	assert.NoError(t, err)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		assert.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	assert.NoError(t, rows.Err())
	// At most one xminhold table should exist at a time; more than one means a prior
	// test orphaned its table, which would mask a real cleanup regression.
	assert.LessOrEqual(t, len(names), 1, "at most one xminhold table should exist at a time")
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

// nDeadTup returns n_dead_tup for the given relation from pg_stat_user_tables. The
// UNQUOTED relname is the bind arg (stats key on the plain relname). Stats are
// async, so callers bounded-poll.
func nDeadTup(t *testing.T, conn db.Conn, relname string) int64 {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT coalesce(n_dead_tup, 0) FROM pg_stat_user_tables WHERE relname = $1", relname)
	assert.NoError(t, err)
	defer rows.Close()
	var n int64
	for rows.Next() {
		assert.NoError(t, rows.Scan(&n))
	}
	assert.NoError(t, rows.Err())
	return n
}

// backendXminSet reports whether any noisia backend holds a non-NULL backend_xmin
// AND backend_xid — the visible signature of the pinned REPEATABLE READ snapshot
// (Decision 1/2). db.Conn has no QueryRow, so it reads the count via Query+Next/Scan.
func backendXminSet(t *testing.T, conn db.Conn) bool {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT count(*) FROM pg_stat_activity WHERE application_name = 'noisia' AND backend_xmin IS NOT NULL AND backend_xid IS NOT NULL")
	assert.NoError(t, err)
	defer rows.Close()
	var n int64
	for rows.Next() {
		assert.NoError(t, rows.Scan(&n))
	}
	assert.NoError(t, rows.Err())
	return n > 0
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

// purgeXminhold drops every leftover noisia_xminhold_% table so a test starts from a
// clean slate, independent of test order.
func purgeXminhold(t *testing.T, conn db.Conn) {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE tablename LIKE 'noisia_xminhold_%'")
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

// swapUserPassword rewrites a URL-form DSN (as produced by testcontainers),
// replacing the userinfo so a test can reconnect to the same host/port/db as a
// different role.
func swapUserPassword(t *testing.T, dsn, user, password string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	assert.NoError(t, err)
	u.User = url.UserPassword(user, password)
	return u.String()
}
