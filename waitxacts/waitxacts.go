// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package waitxacts defines implementation of workload which locks tables and
// blocks queries. Blocked queries have to wait until lock is released.
//
// Before starting the workload looking for the tables with the most UPDATE and
// DELETE operations. Suppose there is a concurrent workload is running on those
// tables. Start goroutines in a loop (where number of goroutines depends on
// Config.Jobs). Each goroutine select random table from the list and set EXCLUSIVE
// lock. During the time the table is locked, all activity related to this table
// is stuck in waiting until lock is released. Goroutine release the lock after
// random time between Config.LocktimeMin and Config.LocktimeMax.
//
// There is also fixture mode exists, for scenarios with no concurrent activity, or
// when no tables found. In this mode, special working table is created, which is
// used for locks. Worker use two goroutines, first used for locking the table, the
// second used for issuing query to locked table.
package waitxacts

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"github.com/lesovsky/noisia/targeting"
	"math/rand"
	"sync"
	"time"
)

// Config defines configuration settings for waiting transactions workload
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	Conninfo string
	// Jobs defines how many workers should be created for producing waiting transactions.
	Jobs uint16
	// Fixture defines to run fixture test which is not affect already running workload.
	Fixture bool
	// LocktimeMin defines a lower threshold of locking interval for blocking transactions.
	LocktimeMin time.Duration
	// LocktimeMax defines an upper threshold of locking interval for blocking transactions.
	LocktimeMax time.Duration
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be greater than 0")
	}

	if c.LocktimeMin == 0 || c.LocktimeMax == 0 {
		return fmt.Errorf("min and max lock time must be greater than zero")
	}

	if c.LocktimeMin > c.LocktimeMax {
		return fmt.Errorf("min lock time must be less or equal to max lock time")
	}

	return nil
}

// workload implements noisia.Workload interface.
type workload struct {
	config Config
	logger log.Logger
	pool   db.DB
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config Config, logger log.Logger) (noisia.Workload, error) {
	err := config.validate()
	if err != nil {
		return nil, err
	}

	return &workload{config, logger, nil}, nil
}

// Run connects to Postgres and starts the workload.
func (w *workload) Run(ctx context.Context) error {
	// maxAffectedTables defines max number of tables which will be affected by blocking transactions.
	maxAffectedTables := 3

	pool, err := db.NewPostgresDB(ctx, w.config.Conninfo)
	if err != nil {
		return err
	}
	w.pool = pool
	defer w.pool.Close()

	// Calculate the number of tables which will be used in workload.
	tables, err := targeting.TopWriteTables(pool, maxAffectedTables)
	if err != nil {
		return err
	}

	// Enable fixture mode, if no tables found.
	if len(tables) == 0 {
		w.config.Fixture = true
	}

	// Prepare stuff for fixture mode if enabled.
	if w.config.Fixture {
		// Prepare working table.
		err = w.prepare(ctx)
		if err != nil {
			return err
		}

		tables = []string{"_noisia_waitxacts_workload"}

		// Cleanup in the end.
		defer func() {
			err = w.cleanup()
			if err != nil {
				w.logger.Warnf("waiting transactions cleanup failed: %s", err)
			}
		}()
	}

	return startLoop(ctx, w.logger, pool, tables, w.config)
}

// prepare method creates fixture table for workload.
func (w *workload) prepare(ctx context.Context) error {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, _, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS _noisia_waitxacts_workload (payload bigint)")
	if err != nil {
		return err
	}

	_, _, err = tx.Exec(ctx, "INSERT INTO _noisia_waitxacts_workload (payload) VALUES (0)")
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// cleanup perform fixtures cleanup after workload has been done.
func (w *workload) cleanup() error {
	_, _, err := w.pool.Exec(context.Background(), "DROP TABLE IF EXISTS _noisia_waitxacts_workload")
	if err != nil {
		return err
	}

	return nil
}

// startLoop start workload loop until context timeout exceeded.
func startLoop(ctx context.Context, log log.Logger, pool db.DB, tables []string, config Config) error {
	// Initialize random, used for calculating lock duration.
	rand.Seed(time.Now().UnixNano())

	// Increment maxTime up to 1 second due to rand.Int63n() never return max value.
	minTime, maxTime := config.LocktimeMin, config.LocktimeMax+1

	// guardCh defines worker queue - run new workers only there is any free slot
	guardCh := make(chan struct{}, config.Jobs)

	// lockedCh defines notification channel which tells when table is locked
	lockedCh := make(chan struct{})

	for {
		select {
		// run workers only when it's possible to write into channel (channel is limited by number of jobs)
		case guardCh <- struct{}{}:
			var wg sync.WaitGroup
			table := selectRandomTable(tables)
			naptime := time.Duration(rand.Int63n(maxTime.Nanoseconds()-minTime.Nanoseconds()) + minTime.Nanoseconds())

			// Start goroutine which locks target for calculated nap time.
			wg.Add(1)
			go func() {
				err := lockTable(ctx, pool, table, naptime, lockedCh)
				if err != nil && ctx.Err() == nil {
					log.Warnf("lock table failed: %s", err)
				}
				wg.Done()
			}()

			// Waiting for signal when table is locked (needed only in fixtures mode).
			<-lockedCh

			// If fixture mode is enabled, issue our own query which becomes blocked.
			if config.Fixture {
				wg.Add(1)
				go func() {
					_, _, err := pool.Exec(ctx, fmt.Sprintf("SELECT * FROM %s", table))
					if err != nil && ctx.Err() == nil {
						log.Warnf("query failed: %s", err)
					}
					wg.Done()
				}()
			}

			// When work is finished, read from the channel to allow starting another iteration of work.
			wg.Wait()
			<-guardCh
		case <-ctx.Done():
			close(guardCh)
			close(lockedCh)
			return nil
		}
	}
}

// lockTable tries to lock specified table for 'idle' amount of time. In case of errors
// send notify to lockedCh to avoid stuck of reading goroutine.
func lockTable(ctx context.Context, pool db.DB, table string, idle time.Duration, lockedCh chan struct{}) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		lockedCh <- struct{}{}
		return fmt.Errorf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := fmt.Sprintf("LOCK TABLE %s IN ACCESS EXCLUSIVE MODE", table)
	_, _, err = tx.Exec(ctx, q)
	if err != nil {
		lockedCh <- struct{}{}
		return fmt.Errorf("lock: %v", err)
	}

	// Table is locked, send a signal to query channel to allow make a query to locked table.
	lockedCh <- struct{}{}

	// Stop execution only if context has been done or idle interval is timed out
	timer := time.NewTimer(idle)
	select {
	case <-ctx.Done():
		return nil
	case <-timer.C:
		return nil
	}
}

// selectRandomTable returns random table from passed list. Empty value returned if empty list.
func selectRandomTable(tables []string) string {
	if len(tables) == 0 {
		return ""
	}

	rand.Seed(time.Now().UnixNano())
	return tables[rand.Intn(len(tables))]
}
