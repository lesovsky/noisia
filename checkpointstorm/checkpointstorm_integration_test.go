// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package checkpointstorm

import (
	"context"
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
// They assert the OBSERVABLE proxies of the checkpoint-storm workload against a
// live server, mirroring seqscanstorm/seqscanstorm_integration_test.go. The
// pg_stat_bgwriter.checkpoints_req strict-increase proof (the forcer issues real
// CHECKPOINTs) and the non-superuser privilege-gate proof are NET-NEW for the
// repo. All state waits use bounded condition-polling (a for-loop with a short
// sleep), never assert.Eventually and never a fixed sleep as the wait mechanism
// (pg_stat_* updates asynchronously).
//
// TableSize is pinned to the 64 MiB floor (minTableSize): it is the smallest
// value validate() accepts, seeds fast, yet is large enough that the forced
// CHECKPOINT does meaningful I/O work. DirtyPct is pinned to the minDirtyPct
// floor (5) and Jobs raised so the rows-based trigger is crossed quickly and
// checkpoints_req grows within a generous poll budget.
// -----------------------------------------------------------------------------

func TestWorkload_Run_checkpointsGrow(t *testing.T) {
	// pg_stat_bgwriter.checkpoints_req strictly grows under load — the
	// deterministic proof that the serial forcer issues real, requested
	// CHECKPOINTs. This is a server-wide counter (not per-table): the test reads
	// its own baseline (firstReq) and asserts a LATER reading strictly exceeds it,
	// so it never depends on the absolute value left by a prior test.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeChkptstorm(t, obs) // clean slate so discover cannot pick up an orphan table

	config := Config{
		Conninfo:       db.TestConninfo,
		TableSize:      minTableSize,
		DirtyPct:       minDirtyPct,
		PayloadBytes:   8,
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
	table := waitForChkptstormTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear")

	firstReq := checkpointsReq(t, obs)

	// Strict-increase assertion: poll until a LATER reading exceeds the first,
	// proving the forcer keeps issuing CHECKPOINTs under load. Generous budget
	// (300 × 20ms = 6s) because the rows-based trigger can take a moment to cross.
	var laterReq int64
	for i := 0; i < 300; i++ {
		laterReq = checkpointsReq(t, obs)
		if laterReq > firstReq {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Greater(t, laterReq, firstReq, "checkpoints_req must strictly grow under load (forcer issues real CHECKPOINTs)")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_scatteredUpdatesLand(t *testing.T) {
	// pg_stat_user_tables.n_tup_upd for the seed table strictly grows under load —
	// the proof that the scattered random-id UPDATE workers actually reach the
	// heap (mirror of the seqscan seq_scan strict-increase, metric n_tup_upd).
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeChkptstorm(t, obs)

	config := Config{
		Conninfo:       db.TestConninfo,
		TableSize:      minTableSize,
		DirtyPct:       minDirtyPct,
		PayloadBytes:   8,
		ReportInterval: 200 * time.Millisecond,
		Jobs:           3,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	table := waitForChkptstormTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear")

	// Soft, non-racy confirmation UPDATEs landed: poll n_tup_upd > 0.
	var firstUpd int64
	for i := 0; i < 300; i++ {
		firstUpd = nTupUpd(t, obs, table)
		if firstUpd > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Greater(t, firstUpd, int64(0), "at least one UPDATE must be recorded under load")

	// Strict-increase assertion: poll until a LATER reading exceeds the first.
	var laterUpd int64
	for i := 0; i < 300; i++ {
		laterUpd = nTupUpd(t, obs, table)
		if laterUpd > firstUpd {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Greater(t, laterUpd, firstUpd, "n_tup_upd must strictly grow under load (scattered UPDATEs land)")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_privilegePreconditionFailsFast(t *testing.T) {
	// A net-new non-superuser role WITHOUT pg_checkpoint membership cannot issue
	// CHECKPOINT: Run returns the honest privilege error and NO seed table is
	// created. The role is granted CREATE ON SCHEMA public so the ONLY failure
	// cause is the missing CHECKPOINT privilege (not a seed permission error) —
	// otherwise the gate could be falsely "confirmed" by an unrelated error.
	// The catalog listing is read on the observer (superuser) conn because the
	// limited role may lack rights to list it.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeChkptstorm(t, obs) // clean slate so the "no table created" assert is meaningful

	_, _, _ = obs.Exec(context.Background(), "DROP ROLE IF EXISTS limited")
	_, _, err = obs.Exec(context.Background(),
		"CREATE ROLE limited LOGIN PASSWORD 'SENTINEL_SECRET'")
	assert.NoError(t, err)
	// Grant CREATE on public for the (never-reached) seed step so the only failure
	// is the missing CHECKPOINT privilege, not a seed permission error.
	_, _, _ = obs.Exec(context.Background(), "GRANT CREATE ON SCHEMA public TO limited")
	defer func() { _, _, _ = obs.Exec(context.Background(), "DROP ROLE IF EXISTS limited") }()

	// Build the role's conninfo from the test DSN, swapping in user/password.
	conninfo := swapUserPassword(t, db.TestConninfo, "limited", "SENTINEL_SECRET")

	config := Config{
		Conninfo:       conninfo,
		TableSize:      minTableSize,
		DirtyPct:       minDirtyPct,
		PayloadBytes:   8,
		ReportInterval: 200 * time.Millisecond,
		Jobs:           1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	err = w.Run(ctx)
	assert.Error(t, err, "Run under a role without CHECKPOINT privilege must fail")
	assert.Contains(t, err.Error(), "CHECKPOINT", "the error must name the missing CHECKPOINT privilege")
	assert.NotContains(t, err.Error(), "SENTINEL_SECRET", "the privilege error must not leak the DSN")

	// The gate fires BEFORE seeding: no table must have been created (read on the
	// observer superuser conn — the limited role may not list the catalog).
	assert.Empty(t, discoverChkptstormTable(t, obs), "no seed table must be created when the privilege gate fails")
}

func TestWorkload_Run_jobsHonored(t *testing.T) {
	// With Jobs=N, at least N noisia backends are concurrently live in
	// pg_stat_activity. Holds INDEPENDENT of host CPU count because each worker
	// owns its own db.Connect (ADR-003-1). checkpoint-storm also attaches
	// seed/forcer connections with application_name='noisia', so the count can
	// exceed Jobs — assert >= Jobs (not ==). The observer sets no application_name,
	// so it is not counted.
	const jobs = 3
	config := Config{
		Conninfo:       db.TestConninfo,
		TableSize:      minTableSize,
		DirtyPct:       minDirtyPct,
		PayloadBytes:   8,
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
	// Graceful exit drops the seed table.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeChkptstorm(t, obs) // clean slate, independent of test order

	config := Config{
		Conninfo:       db.TestConninfo,
		TableSize:      minTableSize,
		DirtyPct:       minDirtyPct,
		PayloadBytes:   8,
		ReportInterval: 200 * time.Millisecond,
		Jobs:           2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Confirm the seed table appears mid-run, then cancel and assert it is gone.
	table := waitForChkptstormTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear mid-run")

	cancel()
	assert.NoError(t, <-done)

	assert.Empty(t, discoverChkptstormTable(t, obs), "seed table must be dropped on graceful exit")
}

func TestWorkload_cleanup_selfSufficient(t *testing.T) {
	// cleanup must not depend on any live workload connection: it opens its OWN
	// fresh connection (ADR-002-2). Prove it deterministically — manually create a
	// matching table (the schema the workload uses), invoke cleanup directly with
	// no Run, and assert it is gone. Catches a conn-reusing regression every run.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeChkptstorm(t, obs)
	defer purgeChkptstorm(t, obs)

	base := "noisia_chkptstorm_" + randomSuffix(8)
	tableIdent := "\"" + base + "\""

	_, _, err = obs.Exec(context.Background(), "CREATE TABLE "+tableIdent+" (id bigint PRIMARY KEY, payload bytea)")
	assert.NoError(t, err)
	assert.NotEmpty(t, discoverChkptstormTable(t, obs), "table must exist before cleanup")

	gw, err := NewWorkload(Config{
		Conninfo:       db.TestConninfo,
		TableSize:      minTableSize,
		DirtyPct:       minDirtyPct,
		PayloadBytes:   8,
		ReportInterval: 200 * time.Millisecond,
		Jobs:           1,
	}, log.NewDefaultLogger("error"))
	assert.NoError(t, err)
	gw.(*workload).cleanup(tableIdent)

	assert.Empty(t, discoverChkptstormTable(t, obs), "table must be dropped by self-sufficient cleanup")
}

// -----------------------------------------------------------------------------
// Test-local helpers (mirror the sibling suites, renamed for noisia_chkptstorm_%).
// -----------------------------------------------------------------------------

// waitForChkptstormTable bounded-polls until the single seed table appears.
// The budget is generous (500 × 20ms = 10s): seeding the 64 MiB floor table is
// a multi-million-row INSERT that can run slowly when the shared container is
// already under load from prior tests in the same package.
func waitForChkptstormTable(t *testing.T, conn db.Conn) string {
	t.Helper()
	var table string
	for i := 0; i < 500; i++ {
		table = discoverChkptstormTable(t, conn)
		if table != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return table
}

// discoverChkptstormTable returns the single noisia_chkptstorm_% table name
// present in the target DB, or "" if none. The suffix is random, so integration
// tests discover the table rather than knowing its name.
func discoverChkptstormTable(t *testing.T, conn db.Conn) string {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE tablename LIKE 'noisia_chkptstorm_%'")
	assert.NoError(t, err)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		assert.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	assert.NoError(t, rows.Err())
	// At most one chkptstorm table should exist at a time; more than one means a
	// prior test orphaned its table, which would mask a real cleanup regression.
	assert.LessOrEqual(t, len(names), 1, "at most one chkptstorm table should exist at a time")
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

// checkpointsReq returns the server-wide pg_stat_bgwriter.checkpoints_req counter
// (PG15; on PG17+ this moved to pg_stat_checkpointer.num_requested, but CI is
// PG15 so no version branch is needed). It is a global counter (not per-table);
// callers read a baseline and bounded-poll for a strict increase.
func checkpointsReq(t *testing.T, conn db.Conn) int64 {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT checkpoints_req FROM pg_stat_bgwriter")
	assert.NoError(t, err)
	defer rows.Close()
	var n int64
	for rows.Next() {
		assert.NoError(t, rows.Scan(&n))
	}
	assert.NoError(t, rows.Err())
	return n
}

// nTupUpd returns n_tup_upd for the given relation from pg_stat_user_tables. The
// UNQUOTED relname is the bind arg (stats key on the plain relname). Stats are
// async, so callers bounded-poll.
func nTupUpd(t *testing.T, conn db.Conn, relname string) int64 {
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

// purgeChkptstorm drops every leftover noisia_chkptstorm_% table so a test starts
// from a clean slate, independent of test order.
func purgeChkptstorm(t *testing.T, conn db.Conn) {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE tablename LIKE 'noisia_chkptstorm_%'")
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
