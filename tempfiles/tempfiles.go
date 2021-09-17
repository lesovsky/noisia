// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package tempfiles defines implementation of workload which executes queries
// which create on-disk temporary files due to lack of work_mem.
//
// Before the starting workload, necessary number of workers is started. Each worker
// connects to the database, creates connection pool and starts working loop. In the
// loop, worker executes queries in a dedicated goroutine (to avoid awaiting when query
// is finished). Before start query, reduce work_mem to guarantee creation of temp
// file. Next query is executed accordingly to rate specified in Config.Rate.
// Workload duration is controlled by context created outside and passed to Run method.
// Context is passed to each worker and used in the worker's loop. When context expires
// loop is stopped.
package tempfiles

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"golang.org/x/time/rate"
	"sync"
)

// Config defines configuration settings for temp files workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	Conninfo string
	// Jobs defines how many workers should be created for producing temp files.
	Jobs uint16
	// Rate defines rate interval for queries executing.
	Rate float64
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be greater than zero")
	}

	if c.Rate <= 0 {
		return fmt.Errorf("temp files queries rate must be positive")
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

// Run creates necessary number of workers and waiting for until the are finish.
// Also collect stats about temp files before and after workload. This is not the
// perfect, but there is no way to know how many temp bytes generated inside the
// session or even transaction.
func (w *workload) Run(ctx context.Context) error {
	workers := int(w.config.Jobs)

	var wg sync.WaitGroup

	bytesBefore, err := countTempBytes(w.config.Conninfo)
	if err != nil {
		return err
	}

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			err := runWorker(ctx, w.logger, w.config)
			if err != nil {
				w.logger.Warnf("start tempfiles worker failed: %s, continue", err)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	bytesAfter, err := countTempBytes(w.config.Conninfo)
	if err != nil {
		return err
	}
	w.logger.Infof("generated %d temp bytes (might include temp bytes produced by concurrent workload)", bytesAfter-bytesBefore)

	return nil
}

// runWorker connects to the database and starts tempfiles loop.
func runWorker(ctx context.Context, log log.Logger, config Config) error {
	log.Info("start tempfiles worker")

	// Use pool because single connection is not enough here. Working loop executes
	// queries asynchronously and several queries might be executed concurrently.
	pool, err := db.NewPostgresDB(ctx, config.Conninfo)
	if err != nil {
		return err
	}

	defer pool.Close()

	err = startLoop(ctx, pool, log, config.Rate)
	if err != nil {
		return err
	}

	log.Infof("tempfiles worker finished")
	return nil
}

// startLoop start executing queries in a loop with required rate until context timeout exceeded.
func startLoop(ctx context.Context, pool db.DB, log log.Logger, r float64) error {
	var wg sync.WaitGroup

	limiter := rate.NewLimiter(rate.Limit(r), 1)
	for {
		if limiter.Allow() {
			wg.Add(1)

			// Due to produced temp files, queries could be executed too long. At the same time
			// we would like to preserve required rate of queries. Don't wait when query is
			// finished and execute them asynchronously.
			go func() {
				// Ignore errors related to context expiration.
				err := execQuery(ctx, pool)
				if err != nil && ctx.Err() == nil {
					log.Warnf("executing tempfiles query failed: %v, continue", err)
				}

				wg.Done()
			}()
		}

		select {
		case <-ctx.Done():
			wg.Wait()
			return nil
		default:
		}
	}
}

// execQuery executes query which should create a temp file. Before execute query,
// set work_mem value to minimum possible value to guarantee creation of temp file.
func execQuery(ctx context.Context, pool db.DB) error {
	_, _, err := pool.Exec(ctx, "SET work_mem TO '64kB'")
	if err != nil {
		return err
	}

	// Even on empty database this query might produce ~50MB temp file.
	_, _, err = pool.Exec(ctx, "SELECT * FROM pg_class a, pg_class b ORDER BY random()")
	if err != nil {
		return err
	}

	return nil
}

// countTempBytes queries current database statistics about temp bytes written.
// Private context is used here, because this is auxiliary routine and is not related to
// main workload.
func countTempBytes(conninfo string) (int, error) {
	bytes := -1 // zero could be returned from database and it is valid value

	conn, err := db.Connect(context.Background(), conninfo)
	if err != nil {
		return bytes, err
	}

	defer func() { _ = conn.Close() }()

	rows, err := conn.Query(context.Background(), "SELECT pg_stat_get_db_temp_bytes(oid) from pg_database where datname = current_database()")
	if err != nil {
		return bytes, err
	}

	for rows.Next() {
		err = rows.Scan(&bytes)
		if err != nil {
			return bytes, err
		}
	}

	return bytes, nil
}
