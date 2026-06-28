// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bloatchurn

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"github.com/stretchr/testify/assert"
)

// -----------------------------------------------------------------------------
// Integration tests (testcontainers, real PostgreSQL via db.TestConninfo).
//
// They assert the OBSERVABLE proxies of the bloat-churn workload against a live
// server, mirroring xminhorizonholder/xminhorizonholder_integration_test.go but
// proving the bloat MECHANISM with deterministic proxies rather than unbounded
// bloat (variant A is env-dependent: on a fast/aggressively-autovacuumed CI host
// n_dead_tup may stay low, so "n_dead_tup grows unbounded" CANNOT be a headline).
// The headline proofs hold regardless of autovacuum speed:
//
//   - HOT BROKEN (HEADLINE): indexing the churned updated_at column and setting it
//     to now() on every UPDATE forbids HOT — after churn n_tup_upd > 0 while
//     n_tup_hot_upd / n_tup_upd is ~0 (< 0.1).
//   - TAIL UNTOUCHED (HEADLINE): churn touches only the lower half [1, hotRows];
//     no tail row (id > hotRows) is ever updated past the seed instant, while some
//     prefix row is — the live tail heap pages are why VACUUM cannot truncate the
//     file and the bloat survives.
//   - n_tup_upd strictly grows (UPDATEs land); n_dead_tup is a SOFTER signal only.
//   - JOBS HONORED: at least Jobs noisia backends are concurrently live.
//   - NO-SUPERUSER: the workload SUCCEEDS under a freshly-created LOGIN+CREATE-only
//     role (no superuser) — seeding and churn need no special privilege.
//   - CLEANUP: the seed table is absent after exit (incl. mid-churn cancel); with
//     KeepTable it SURVIVES.
//
// All state waits use bounded condition-polling (a for-loop with a short sleep),
// never assert.Eventually and never a fixed sleep as the wait mechanism (pg_stat_*
// and the heap update asynchronously).
//
// TableSize is pinned to the 64 MiB floor (minTableSize): the smallest value
// validate() accepts, and it seeds fast. PayloadBytes 8192 yields a small row count
// so the heap churns lively and effects land quickly within a generous poll budget.
// The remediable-after-stop VACUUM reveal is a manual stand demo (horizon-specific
// in the sibling suite), NOT a CI test, so it is intentionally absent here.
// -----------------------------------------------------------------------------

func TestWorkload_Run_seedShape(t *testing.T) {
	// The seed table is exactly (id bigint PRIMARY KEY, payload bytea, updated_at
	// timestamptz) with the SINGLE bloat index on updated_at. Verified from the
	// catalog on the observer (superuser) conn: three columns of the right types and
	// exactly one (non-unique) index referencing updated_at.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeBloatchurn(t, obs)

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

	table := waitForBloatchurnTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear")

	// Exactly three columns of the expected types, in order.
	rows, err := obs.Query(context.Background(),
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position", table)
	assert.NoError(t, err)
	cols := map[string]string{}
	for rows.Next() {
		var name, typ string
		assert.NoError(t, rows.Scan(&name, &typ))
		cols[name] = typ
	}
	assert.NoError(t, rows.Err())
	rows.Close()

	assert.Len(t, cols, 3, "seed table must have exactly three columns")
	assert.Equal(t, "bigint", cols["id"], "id must be bigint")
	assert.Equal(t, "bytea", cols["payload"], "payload must be bytea")
	assert.Equal(t, "timestamp with time zone", cols["updated_at"], "updated_at must be timestamptz")

	// Exactly one (non-unique) index references updated_at — the bloat target. The
	// PRIMARY KEY's implicit index on id is the only other index and is excluded by
	// the updated_at filter.
	idxOnUpdatedAt := scalarInt64(t, obs,
		"SELECT count(*) FROM pg_indexes WHERE tablename = $1 AND indexdef LIKE '%(updated_at%'", table)
	assert.Equal(t, int64(1), idxOnUpdatedAt, "exactly one index must reference updated_at")
	uniqueOnUpdatedAt := scalarInt64(t, obs,
		"SELECT count(*) FROM pg_indexes WHERE tablename = $1 AND indexdef LIKE 'CREATE UNIQUE INDEX%(updated_at%'", table)
	assert.Equal(t, int64(0), uniqueOnUpdatedAt, "the updated_at index must be a plain (non-unique) index")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_hotBroken(t *testing.T) {
	// HEADLINE (bloat-churn specific): indexing updated_at and setting it on every
	// UPDATE forbids HOT. After churn n_tup_upd > 0 while n_tup_hot_upd / n_tup_upd
	// is ~0 (< 0.1) — deterministic regardless of autovacuum speed. The index is
	// created in prepare BEFORE churn, so the HOT share is expectedly ≈0; the 0.1
	// threshold leaves a little tolerance.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeBloatchurn(t, obs)

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

	table := waitForBloatchurnTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear")

	// Bounded-poll until UPDATEs are recorded (stats are async).
	var upd int64
	for i := 0; i < 500; i++ {
		upd = nTupUpd(t, obs, table)
		if upd > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Greater(t, upd, int64(0), "n_tup_upd must be > 0 after churn")

	// Read the HOT count alongside a fresh total. Any churn between the two reads
	// only adds more non-HOT updates, which keeps the ratio low — so the headline
	// assertion is conservative.
	hot := nTupHotUpd(t, obs, table)
	upd = nTupUpd(t, obs, table)
	assert.Greater(t, upd, int64(0), "n_tup_upd must be > 0 after churn")
	assert.Less(t, float64(hot)/float64(upd), 0.1,
		"n_tup_hot_upd/n_tup_upd must be ~0: indexing updated_at=now() forbids HOT")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_tailUntouched(t *testing.T) {
	// HEADLINE (truncate-block): churn touches only the hot prefix [1, hotRows]. No
	// tail row (id > hotRows) is ever updated past the seed instant, while some
	// prefix row is — proving the hot-prefix bound and why VACUUM cannot truncate
	// the file (the live tail heap pages remain). The seed instant is taken from the
	// table itself: the single INSERT stamps every seed row with the same now(), and
	// the tail is never touched, so any tail row's updated_at is the seed instant.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeBloatchurn(t, obs)

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

	table := waitForBloatchurnTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear")
	ident := "\"" + table + "\""

	// Row count and hot-prefix bound, computed exactly as the production worker does.
	rowCount := scalarInt64(t, obs, "SELECT count(*) FROM "+ident)
	assert.Greater(t, rowCount, int64(1), "the 64 MiB floor must yield many rows")
	hotRows := int64(float64(rowCount) * hotFraction)
	assert.GreaterOrEqual(t, hotRows, int64(1), "hot-prefix bound must be at least 1")

	// The tail seed instant: the tail is never churned, so its updated_at is the
	// single seed now(). The table is committed in one transaction, so it is present
	// the moment discover sees it.
	tailInstant := scalarTime(t, obs,
		fmt.Sprintf("SELECT max(updated_at) FROM %s WHERE id > $1", ident), hotRows)

	// Bounded-poll until churn lands in the prefix strictly after the seed instant.
	var prefixMax time.Time
	for i := 0; i < 500; i++ {
		prefixMax = scalarTime(t, obs,
			fmt.Sprintf("SELECT max(updated_at) FROM %s WHERE id <= $1", ident), hotRows)
		if prefixMax.After(tailInstant) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.True(t, prefixMax.After(tailInstant),
		"some prefix row must be churned after the seed instant")

	// HEADLINE: no tail row was updated past the seed instant.
	tailTouched := scalarInt64(t, obs,
		fmt.Sprintf("SELECT count(*) FROM %s WHERE id > $1 AND updated_at > $2", ident), hotRows, tailInstant)
	assert.Equal(t, int64(0), tailTouched,
		"no tail row may be updated past the seed instant (live tail blocks VACUUM truncation)")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_nTupUpdGrows(t *testing.T) {
	// n_tup_upd strictly grows (scattered UPDATEs land). n_dead_tup is read only as a
	// SOFTER signal (env-dependent: a fast autovacuum can keep it low), so it is NOT
	// asserted to grow — the strict-growth headline is n_tup_upd alone.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeBloatchurn(t, obs)

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

	table := waitForBloatchurnTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear")

	// Soft, non-racy confirmation UPDATEs landed: poll n_tup_upd > 0.
	var firstUpd int64
	for i := 0; i < 500; i++ {
		firstUpd = nTupUpd(t, obs, table)
		if firstUpd > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Greater(t, firstUpd, int64(0), "at least one UPDATE must be recorded under load")

	// Strict-increase assertion: poll until a LATER reading exceeds the first.
	var laterUpd int64
	for i := 0; i < 500; i++ {
		laterUpd = nTupUpd(t, obs, table)
		if laterUpd > firstUpd {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Greater(t, laterUpd, firstUpd, "n_tup_upd must strictly grow under load (scattered UPDATEs land)")

	// Softer signal only (not a headline): dead tuples are non-negative. On a slow
	// autovacuum they accumulate, but a fast one can reclaim them, so growth is not
	// asserted here.
	assert.GreaterOrEqual(t, nDeadTup(t, obs, table), int64(0), "n_dead_tup is a soft signal only")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_jobsHonored(t *testing.T) {
	// With Jobs=N, at least N noisia backends are concurrently live in
	// pg_stat_activity. Holds INDEPENDENT of host CPU count because each worker owns
	// its own db.Connect (ADR-003-1). The seed/cleanup connections also set
	// application_name='noisia', so the count can exceed Jobs — assert >= Jobs (not
	// ==). The observer sets no application_name, so it is not counted.
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
	for i := 0; i < 500; i++ {
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

func TestWorkload_Run_nonSuperuserSucceeds(t *testing.T) {
	// NO-SUPERUSER: a freshly created role with ONLY LOGIN + CREATE ON SCHEMA public
	// seeds and churns SUCCESSFULLY — bloat-churn needs no special privilege. There
	// is NO assert on an error; the catalog is read on the observer (superuser) conn
	// because the limited role may lack listing rights.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeBloatchurn(t, obs)

	_, _, _ = obs.Exec(context.Background(), "DROP ROLE IF EXISTS limited")
	_, _, err = obs.Exec(context.Background(),
		"CREATE ROLE limited LOGIN PASSWORD 'SENTINEL_SECRET'")
	assert.NoError(t, err)
	// CREATE on public is all the workload needs to seed.
	_, _, _ = obs.Exec(context.Background(), "GRANT CREATE ON SCHEMA public TO limited")
	defer func() { _, _, _ = obs.Exec(context.Background(), "DROP ROLE IF EXISTS limited") }()

	// Guard: prove the role genuinely lacks superuser, so the SUCCESS proof below is
	// load-bearing ("needs no privilege"), not true by construction.
	assert.False(t, scalarBool(t, obs,
		"SELECT rolsuper FROM pg_roles WHERE rolname = 'limited'"),
		"the limited role must NOT be superuser (else the no-privilege proof is vacuous)")

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
	// limited role (n_tup_upd > 0). Read on the observer superuser conn.
	table := waitForBloatchurnTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear under the non-superuser role")

	var upd int64
	for i := 0; i < 500; i++ {
		upd = nTupUpd(t, obs, table)
		if upd > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Greater(t, upd, int64(0), "churn must land under the non-superuser role (needs no privilege)")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_cleanupOnExit(t *testing.T) {
	// Graceful exit drops the seed table, even when the cancel lands mid-churn.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeBloatchurn(t, obs) // clean slate, independent of test order

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

	// Confirm the seed table appears mid-run (i.e. mid-churn), then cancel and assert
	// it is gone.
	table := waitForBloatchurnTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear mid-run")

	cancel()
	assert.NoError(t, <-done)

	assert.Empty(t, discoverBloatchurnTable(t, obs), "seed table must be dropped on graceful exit")
}

func TestWorkload_Run_keepTableSurvives(t *testing.T) {
	// With KeepTable=true the seed table SURVIVES graceful exit (Decision 5: keep for
	// the post-stop repair demo). It is dropped manually here via purgeBloatchurn so
	// it does not pollute the shared container for the next test in this package.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeBloatchurn(t, obs)
	defer purgeBloatchurn(t, obs)

	config := Config{
		Conninfo:       db.TestConninfo,
		TableSize:      minTableSize,
		PayloadBytes:   8192,
		Rate:           3000,
		ReportInterval: 200 * time.Millisecond,
		Jobs:           2,
		KeepTable:      true,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	table := waitForBloatchurnTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear mid-run")

	cancel()
	assert.NoError(t, <-done)

	assert.NotEmpty(t, discoverBloatchurnTable(t, obs), "seed table must survive with KeepTable=true")
}

// -----------------------------------------------------------------------------
// Test-local helpers (mirror the sibling suites, renamed for noisia_bloatchurn_%).
// -----------------------------------------------------------------------------

// waitForBloatchurnTable bounded-polls until the single seed table appears. The
// budget is generous (500 × 20ms = 10s): seeding the 64 MiB floor table is a
// multi-row INSERT that can run slowly when the shared container is already under
// load from prior tests in the same package.
func waitForBloatchurnTable(t *testing.T, conn db.Conn) string {
	t.Helper()
	var table string
	for i := 0; i < 500; i++ {
		table = discoverBloatchurnTable(t, conn)
		if table != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return table
}

// discoverBloatchurnTable returns the single noisia_bloatchurn_% table name present
// in the target DB, or "" if none. The suffix is random, so integration tests
// discover the table rather than knowing its name.
func discoverBloatchurnTable(t *testing.T, conn db.Conn) string {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE tablename LIKE 'noisia_bloatchurn_%'")
	assert.NoError(t, err)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		assert.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	assert.NoError(t, rows.Err())
	// At most one bloatchurn table should exist at a time; more than one means a
	// prior test orphaned its table, which would mask a real cleanup regression.
	assert.LessOrEqual(t, len(names), 1, "at most one bloatchurn table should exist at a time")
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

// nTupUpd returns n_tup_upd for the given relation from pg_stat_user_tables. The
// UNQUOTED relname is the bind arg (stats key on the plain relname). Stats are
// async, so callers bounded-poll.
func nTupUpd(t *testing.T, conn db.Conn, relname string) int64 {
	t.Helper()
	return scalarInt64(t, conn,
		"SELECT coalesce(n_tup_upd, 0) FROM pg_stat_user_tables WHERE relname = $1", relname)
}

// nTupHotUpd returns n_tup_hot_upd for the given relation from pg_stat_user_tables.
// The UNQUOTED relname is the bind arg. A ~0 HOT count under a non-zero n_tup_upd is
// the headline proof that indexing updated_at=now() forbids HOT.
func nTupHotUpd(t *testing.T, conn db.Conn, relname string) int64 {
	t.Helper()
	return scalarInt64(t, conn,
		"SELECT coalesce(n_tup_hot_upd, 0) FROM pg_stat_user_tables WHERE relname = $1", relname)
}

// nDeadTup returns n_dead_tup for the given relation from pg_stat_user_tables. It is
// a SOFTER signal only (env-dependent), never a strict-growth headline.
func nDeadTup(t *testing.T, conn db.Conn, relname string) int64 {
	t.Helper()
	return scalarInt64(t, conn,
		"SELECT coalesce(n_dead_tup, 0) FROM pg_stat_user_tables WHERE relname = $1", relname)
}

// countNoisiaBackends returns the number of backends with application_name='noisia'.
func countNoisiaBackends(t *testing.T, conn db.Conn) int {
	t.Helper()
	return int(scalarInt64(t, conn,
		"SELECT count(*) FROM pg_stat_activity WHERE application_name = 'noisia'"))
}

// scalarInt64 runs a single-int64 query (db.Conn has no QueryRow) and returns the
// first row's value, or 0 if there is no row. Stats/count reads go through here.
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

// scalarTime runs a single-timestamptz query (db.Conn has no QueryRow) and returns
// the first row's value. Used to compare the hot-prefix instant against the
// never-touched tail seed instant.
func scalarTime(t *testing.T, conn db.Conn, query string, args ...interface{}) time.Time {
	t.Helper()
	rows, err := conn.Query(context.Background(), query, args...)
	assert.NoError(t, err)
	defer rows.Close()
	var ts time.Time
	for rows.Next() {
		assert.NoError(t, rows.Scan(&ts))
	}
	assert.NoError(t, rows.Err())
	return ts
}

// scalarBool runs a single-bool query (db.Conn has no QueryRow) and returns the
// first row's bool, or false if there is no row. Used to make the privilege guard
// load-bearing.
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

// purgeBloatchurn drops every leftover noisia_bloatchurn_% table so a test starts
// from a clean slate, independent of test order.
func purgeBloatchurn(t *testing.T, conn db.Conn) {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE tablename LIKE 'noisia_bloatchurn_%'")
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
