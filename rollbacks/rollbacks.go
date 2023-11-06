// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package rollbacks defines implementation of workload which issues definitely
// invalid queries which are doomed to fail and as a result pg_stat_database.xact_rollback
// counter is incremented.
//
// For creating the workload, start required number of workers (number of goroutines
// depends on Config.Jobs). Each worker creates a temporary table. The table is used
// in queries to bypass parser errors related to querying non-existent table. Next,
// rollbacks loop is started. In the loop, a random query is selected and issued.
// The query obviously fails. Next query is executed accordingly to rate specified
// in Config.Rate.
// Workload duration is controlled by context created outside and passed to Run method.
// Context is passed to each worker and used in the worker's loop. When context expires
// loop is stopped.
package rollbacks

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"golang.org/x/time/rate"
	"math/rand"
	"sync"
	"time"
)

// Config defines configuration settings for rollbacks workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	Conninfo string
	// Jobs defines how many workers should be created for producing rollbacks.
	Jobs uint16
	// Rate defines rollbacks rate produced per second (per single worker).
	Rate float64
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be greater than zero")
	}

	if c.Rate <= 0 {
		return fmt.Errorf("rate must be positive")
	}

	return nil
}

// workload implements noisia.Workload interface.
type workload struct {
	config Config
	logger log.Logger
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config Config, logger log.Logger) (noisia.Workload, error) {
	err := config.validate()
	if err != nil {
		return nil, err
	}

	return &workload{config, logger}, nil
}

// Run method starts necessary number of workers and waiting until they finish.
func (w *workload) Run(ctx context.Context) error {
	workers := int(w.config.Jobs)

	var wg sync.WaitGroup

	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			err := runWorker(ctx, w.logger, w.config)
			if err != nil {
				w.logger.Warnf("start rollbacks worker failed: %s, continue", err)
			}
			wg.Done()
		}()
	}

	wg.Wait()
	return nil
}

// runWorker connects to the database and start rollback loop.
func runWorker(ctx context.Context, log log.Logger, config Config) error {
	log.Info("start rollback worker")

	conn, err := db.Connect(ctx, config.Conninfo)
	if err != nil {
		return err
	}

	commits, rollbacks, err := startLoop(ctx, conn, config.Rate)
	if err != nil {
		log.Warnf("rollbacks worker failed: %s", err)
	}

	log.Infof("rollbacks worker finished: %d rollbacks, %d commits", rollbacks, commits)
	return nil
}

// startLoop start rollbacks in a loop with required rate until context timeout exceeded.
func startLoop(ctx context.Context, conn db.Conn, r float64) (int, int, error) {
	table, err := createTempTable(ctx, conn)
	if err != nil {
		return 0, 0, err
	}

	var commits, rollbacks int

	limiter := rate.NewLimiter(rate.Limit(r), 1)
	for {
		if limiter.Allow() {
			// Select random query with arguments.
			q, args := newErrQuery(table)

			// Execute query. Suppress errors, it is designed all generated queries produce errors.
			// Consider the error related to context expiration lead to rollback.
			_, _, err = conn.Exec(ctx, q, args...)
			if err != nil {
				rollbacks++
			} else {
				commits++
			}
		}

		select {
		case <-ctx.Done():
			return commits, rollbacks, nil
		default:
		}
	}
}

// createTempTable creates temporary table for session.
func createTempTable(ctx context.Context, conn db.Conn) (string, error) {
	t := fmt.Sprintf("noisia_%d", time.Now().Unix())
	q := fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s (entity_id INT, name TEXT, size_b BIGINT, created_at TIMESTAMPTZ)", t)

	_, _, err := conn.Exec(ctx, q)
	if err != nil {
		return "", err
	}

	return t, nil
}

// newErrQuery returns random invalid query with arguments.
func newErrQuery(table string) (string, []interface{}) {
	// Total number of available erroneous queries.
	const total = 15

	rand.Seed(time.Now().UnixNano())
	idx := rand.Intn(total)

	var (
		num1, num2 = rand.Intn(1000), rand.Intn(10000)
		str1       = fmt.Sprintf("AUX-%d-%d-%d", rand.Intn(1000), rand.Intn(1000), rand.Intn(1000))
		str2       = fmt.Sprintf("AUX-%d-%d-%d", rand.Intn(1000), rand.Intn(1000), rand.Intn(1000))

		q    string
		args []interface{}
	)

	switch idx {
	case 0:
		// ERROR:  INSERT has more expressions than target columns
		q = fmt.Sprintf("INSERT INTO %s (entity_id, name, size_b) VALUES ($1, $2, $3, $4)", table)
		args = []interface{}{num1, str1, num2, time.Now().String()}
	case 1:
		// ERROR:  invalid input syntax for type integer: "???"
		q = fmt.Sprintf("INSERT INTO %s (entity_id, name, size_b) VALUES ($1, $2, $3)", table)
		args = []interface{}{num1, str1, str2}
	case 2:
		// ERROR:  date/time field value out of range: "???" at character ???
		q = fmt.Sprintf("INSERT INTO %s (entity_id, name, size_b, created_at) VALUES ($1, $2, $3, $4)", table)
		args = []interface{}{num1, str1, num2, "30/02/2021"}
	case 3:
		// ERROR:  could not open file "???" for writing: No such file or directory
		q = fmt.Sprintf("COPY %s FROM '/mnt/vol9/raw/data/%d/noisia.in.csv'", table, num1)
	case 4:
		// ERROR:  syntax error at or near "???" at character ???
		q = fmt.Sprintf("INSERT SELECT entity_id, name, size_b, created_at FROM %s WHERE entity_id = $1", table)
		args = []interface{}{num1}
	case 5:
		// ERROR:  column "???" does not exist at character ???
		q = fmt.Sprintf("SELECT id, name, size_b, created_at FROM %s WHERE id = $1", table)
		args = []interface{}{num1}
	case 6:
		// ERROR:  relation "???" does not exist at character ???
		q = fmt.Sprintf("SELECT entity_id, name, size_b, created_at FROM %s_1 WHERE entity_id = $1", table)
		args = []interface{}{num1}
	case 7:
		// ERROR:  function string_agg(integer, unknown) does not exist at character ???
		q = fmt.Sprintf("SELECT string_agg(name, 10) FROM %s WHERE entity_id >= $1 and entity_id < $2", table)
		args = []interface{}{num1, num2}
	case 8:
		// ERROR:  column "???" must appear in the GROUP BY clause or be used in an aggregate function at character ???
		q = fmt.Sprintf("SELECT name, created_at::date, count(size_b) FROM %s WHERE created_at > to_timestamp($1) GROUP BY name ORDER BY 3 DESC", table)
		args = []interface{}{num1 * 999999}
	case 9:
		// ERROR:  aggregate functions are not allowed in GROUP BY at character ???
		q = fmt.Sprintf("SELECT name, created_at::date, count(size_b) FROM %s WHERE created_at > to_timestamp($1) GROUP BY 1,2,3 ORDER BY 3 DESC", table)
		args = []interface{}{num1 * 999999}
	case 10:
		// ERROR:  ORDER BY position 4 is not in select list
		q = fmt.Sprintf("SELECT name, created_at::date, count(size_b) FROM %s WHERE created_at > to_timestamp($1) GROUP BY 1,2,3 ORDER BY 4 DESC", table)
		args = []interface{}{num1 * 999999}
	case 11:
		// ERROR:  more than one row returned by a subquery used as an expression
		q = "SELECT relname, reltuples FROM pg_class WHERE relname = (SELECT relname FROM pg_stat_sys_indexes WHERE relname = 'pg_constraint')"
	case 12:
		// ERROR:  missing FROM-clause entry for table "???" at character ???
		q = fmt.Sprintf("SELECT st.entity_id, s.name, s.size_b, s.created_at FROM %s s WHERE entity_id = $1", table)
		args = []interface{}{num1}
	case 13:
		// ERROR:  NUMERIC scale 2 must be between 0 and precision 1 at character ???
		q = fmt.Sprintf("SELECT entity_id, name, (size_b / 8192)::numeric(1,2) AS size_t, created_at FROM %s WHERE entity_id = $1", table)
		args = []interface{}{num1}
	case 14:
		// ERROR:  COALESCE types date and bigint cannot be matched at character ???
		q = fmt.Sprintf("SELECT entity_id, name, size_b, coalesce(created_at, 0) FROM %s WHERE entity_id = $1", table)
		args = []interface{}{num1}
	}

	return q, args
}
