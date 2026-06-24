// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package seqscanstorm

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"github.com/stretchr/testify/assert"
)

// -----------------------------------------------------------------------------
// Integration tests (testcontainers, real PostgreSQL via db.TestConninfo).
//
// They assert the OBSERVABLE proxies of the seqscan-storm workload against a
// live server, mirroring walflood/walflood_test.go and
// hotrowcontention/hotrowcontention_test.go. EXPLAIN + pg_stat_user_tables
// proofs are NET-NEW for the repo. All state waits use bounded condition-polling
// (a for-loop with a short sleep), never assert.Eventually and never a fixed
// sleep as the wait mechanism (pg_stat_* updates asynchronously).
//
// TableSize is pinned to the 64 MiB floor (minTableSize): it is the smallest
// value validate() accepts, seeds fast (~1.4M rows), yet is large enough that an
// un-indexed count(*) WHERE payload=0 still plans a real Seq Scan.
// -----------------------------------------------------------------------------

func TestWorkload_Run_seqScanProof(t *testing.T) {
	// The worker query plans as a Seq Scan and NEVER an Index Scan on payload —
	// the deterministic proof that the un-indexed-filter design forces a full
	// heap scan (an index on payload would flip this to an Index Scan and fail).
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeSeqscan(t, obs) // clean slate so discover cannot pick up an orphan table

	config := Config{
		Conninfo:  db.TestConninfo,
		TableSize: minTableSize,
		Jobs:      1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	table := waitForSeqscanTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear")

	tableIdent := "\"" + table + "\""
	plan := explainPlan(t, obs, "SELECT count(*) FROM "+tableIdent+" WHERE payload = 0")

	assert.Contains(t, plan, "Seq Scan", "the worker query must plan as a Seq Scan over the un-indexed table")
	assert.NotContains(t, plan, "Index Scan", "the worker query must NOT use an index on payload")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_scanGrowsAndWarmup(t *testing.T) {
	// pg_stat_user_tables.seq_scan / seq_tup_read grow under load (strict
	// increase between two readings). Warm-up is proven DETERMINISTICALLY off the
	// captured warm-up log line (which must carry no conninfo); the seq_scan>0
	// poll is only a soft, non-racy confirmation that a full scan happened.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeSeqscan(t, obs)

	logger := &captureLogger{}
	config := Config{
		Conninfo:  db.TestConninfo,
		TableSize: minTableSize,
		Jobs:      2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, logger)
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Warm-up proof (deterministic): bounded-poll the captured info lines until
	// the warm-up line appears. The literal mirrors the implementation's format
	// in warmup(): "seqscan-storm: warming up cache: full scan of ...".
	var warmLine string
	for i := 0; i < 200; i++ {
		for _, l := range logger.infos() {
			if strings.Contains(l, "warming up cache: full scan of") {
				warmLine = l
				break
			}
		}
		if warmLine != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.NotEmpty(t, warmLine, "the warm-up log line must be emitted before escalation")
	// The warm-up line must never leak the DSN.
	for _, tok := range []string{"password=", "host=", "user=", "dbname=", "sslmode=", "://"} {
		assert.NotContains(t, warmLine, tok, "the warm-up log line must not contain conninfo")
	}

	table := waitForSeqscanTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear")

	// Soft, non-racy confirmation a full scan happened (warm-up + workers): poll
	// seq_scan > 0. This is NOT a warm-up-before-workers ordering assertion.
	var firstScan, firstRead int64
	for i := 0; i < 200; i++ {
		firstScan, firstRead = seqScan(t, obs, table)
		if firstScan > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Greater(t, firstScan, int64(0), "at least one Seq Scan must be recorded under load")

	// Strict-increase assertion: poll until a LATER reading exceeds the first,
	// proving the workers keep scanning (not just the single warm-up pass).
	var laterScan, laterRead int64
	for i := 0; i < 200; i++ {
		laterScan, laterRead = seqScan(t, obs, table)
		if laterScan > firstScan && laterRead > firstRead {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Greater(t, laterScan, firstScan, "seq_scan must strictly grow under load")
	assert.Greater(t, laterRead, firstRead, "seq_tup_read must strictly grow under load")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_parallelOff(t *testing.T) {
	// Under the workload's determinism GUC the worker query plans single-process
	// (no Gather node). The worker sets max_parallel_workers_per_gather=0 on its
	// PRIVATE session; the observer is a different session whose postgres:15
	// default is 2, so the SAME GUC must be set on the observer BEFORE the EXPLAIN
	// (otherwise a bare count(*) over an un-indexed >=64 MiB table could plan a
	// Gather and false-fail). The SET makes the observer's planner match the
	// worker's; only then is the no-Gather assertion meaningful.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeSeqscan(t, obs)

	config := Config{
		Conninfo:  db.TestConninfo,
		TableSize: minTableSize,
		Jobs:      1,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	table := waitForSeqscanTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear")
	tableIdent := "\"" + table + "\""

	// Mirror the worker's determinism GUC on the observer conn BEFORE the EXPLAIN.
	_, _, err = obs.Exec(context.Background(), "SET max_parallel_workers_per_gather = 0")
	assert.NoError(t, err)

	plan := explainPlan(t, obs, "SELECT count(*) FROM "+tableIdent+" WHERE payload = 0")
	assert.Contains(t, plan, "Seq Scan", "the determinism plan must still be a Seq Scan")
	assert.NotContains(t, plan, "Gather", "under max_parallel_workers_per_gather=0 the scan must plan single-process (no Gather)")

	cancel()
	assert.NoError(t, <-done)
}

func TestWorkload_Run_jobsHonored(t *testing.T) {
	// With Jobs=N, at least N noisia backends are concurrently live in
	// pg_stat_activity. Holds INDEPENDENT of host CPU count because each worker
	// owns its own db.Connect (ADR-003-1) — a shared pool's default max_conns
	// would cap this below Jobs. The observer sets no application_name, so it is
	// not counted.
	const jobs = 3
	config := Config{
		Conninfo:  db.TestConninfo,
		TableSize: minTableSize,
		Jobs:      jobs,
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

	// Poll until at least Jobs noisia backends are concurrently live. The seeding
	// backend also sets application_name='noisia', so this is reached once the
	// workers are up.
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
	purgeSeqscan(t, obs) // clean slate, independent of test order

	config := Config{
		Conninfo:  db.TestConninfo,
		TableSize: minTableSize,
		Jobs:      2,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Confirm the seed table appears mid-run, then cancel and assert it is gone.
	table := waitForSeqscanTable(t, obs)
	assert.NotEmpty(t, table, "seed table must appear mid-run")

	cancel()
	assert.NoError(t, <-done)

	assert.Empty(t, discoverSeqscanTable(t, obs), "seed table must be dropped on graceful exit")
}

func TestWorkload_cleanup_selfSufficient(t *testing.T) {
	// cleanup must not depend on any live workload connection: it opens its OWN
	// fresh connection (ADR-002-2). Prove it deterministically — manually create a
	// matching table (the name the workload uses), invoke cleanup directly with no
	// Run, and assert it is gone. Catches a conn-reusing regression every run.
	obs, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)
	defer func() { _ = obs.Close() }()
	purgeSeqscan(t, obs)
	defer purgeSeqscan(t, obs)

	base := "noisia_seqscan_" + randomSuffix(8)
	tableIdent := "\"" + base + "\""

	_, _, err = obs.Exec(context.Background(), "CREATE TABLE "+tableIdent+" (id bigint PRIMARY KEY, payload bigint)")
	assert.NoError(t, err)
	assert.NotEmpty(t, discoverSeqscanTable(t, obs), "table must exist before cleanup")

	gw, err := NewWorkload(Config{
		Conninfo:  db.TestConninfo,
		TableSize: minTableSize,
		Jobs:      1,
	}, log.NewDefaultLogger("error"))
	assert.NoError(t, err)
	gw.(*workload).cleanup(tableIdent)

	assert.Empty(t, discoverSeqscanTable(t, obs), "table must be dropped by self-sufficient cleanup")
}

// -----------------------------------------------------------------------------
// Test-local helpers (mirror the sibling suites).
// -----------------------------------------------------------------------------

// waitForSeqscanTable bounded-polls until the single seed table appears.
func waitForSeqscanTable(t *testing.T, conn db.Conn) string {
	t.Helper()
	var table string
	for i := 0; i < 150; i++ {
		table = discoverSeqscanTable(t, conn)
		if table != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return table
}

// discoverSeqscanTable returns the single noisia_seqscan_% table name present in
// the target DB, or "" if none. The suffix is random, so integration tests
// discover the table rather than knowing its name.
func discoverSeqscanTable(t *testing.T, conn db.Conn) string {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE tablename LIKE 'noisia_seqscan_%'")
	assert.NoError(t, err)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		assert.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	assert.NoError(t, rows.Err())
	// At most one seqscan table should exist at a time; more than one means a
	// prior test orphaned its table, which would mask a real cleanup regression.
	assert.LessOrEqual(t, len(names), 1, "at most one seqscan table should exist at a time")
	if len(names) == 0 {
		return ""
	}
	return names[0]
}

// seqScan returns (seq_scan, seq_tup_read) for the given relation from
// pg_stat_user_tables. The UNQUOTED relname is the bind arg (stats keys on the
// plain relname). Stats are async, so callers bounded-poll.
func seqScan(t *testing.T, conn db.Conn, relname string) (int64, int64) {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT coalesce(seq_scan, 0), coalesce(seq_tup_read, 0) FROM pg_stat_user_tables WHERE relname = $1", relname)
	assert.NoError(t, err)
	defer rows.Close()
	var scan, read int64
	for rows.Next() {
		assert.NoError(t, rows.Scan(&scan, &read))
	}
	assert.NoError(t, rows.Err())
	return scan, read
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

// explainPlan runs EXPLAIN over the given SQL and returns the full plan text,
// accumulating every plan row (db.Conn has no QueryRow). Built as a plain
// Query + Next/Scan loop (the readRelationSize template).
func explainPlan(t *testing.T, conn db.Conn, sql string) string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "EXPLAIN "+sql)
	assert.NoError(t, err)
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		assert.NoError(t, rows.Scan(&line))
		lines = append(lines, line)
	}
	assert.NoError(t, rows.Err())
	return strings.Join(lines, "\n")
}

// purgeSeqscan drops every leftover noisia_seqscan_% table so a test starts from
// a clean slate, independent of test order.
func purgeSeqscan(t *testing.T, conn db.Conn) {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT tablename FROM pg_tables WHERE tablename LIKE 'noisia_seqscan_%'")
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
