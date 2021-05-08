package waitxacts

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/targeting"
	"math/rand"
	"time"
)

const (
	// defaultJobsNum defines minimum number of workers necessary for generating the workload.
	defaultJobsNum = 2
	// defaultWaitXactsLocktimeMin defines default lower threshold of locking period.
	defaultWaitXactsLocktimeMin = 5
	// defaultWaitXactsLocktimeMax defines default upper threshold of locking period.
	defaultWaitXactsLocktimeMax = 20
	// maxAffectedTables defines max number of tables which will be affected by blocking transactions.
	maxAffectedTables = 3
)

// Config defines configuration settings for waiting transactions workload
type Config struct {
	// PostgresConninfo defines connections string used for connecting to Postgres.
	PostgresConninfo string
	// Jobs defines how many workers should be created for producing waiting transactions.
	Jobs uint16
	// Fixture defines to run fixture test which is not affect already running workload.
	Fixture bool
	// waitXactsLocktimeMin defines a lower threshold of locking interval for blocking transactions.
	WaitXactsLocktimeMin int
	// waitXactsLocktimeMax defines an upper threshold of locking interval for blocking transactions.
	WaitXactsLocktimeMax int
}

func (c *Config) defaults() {
	if c.Jobs < 2 {
		c.Jobs = defaultJobsNum
	}
	if c.WaitXactsLocktimeMin == 0 {
		c.WaitXactsLocktimeMin = defaultWaitXactsLocktimeMin
	}
	if c.WaitXactsLocktimeMax == 0 {
		c.WaitXactsLocktimeMax = defaultWaitXactsLocktimeMax
	}
}

type workload struct {
	config *Config
	pool   db.DB
}

func NewWorkload(config *Config) noisia.Workload {
	config.defaults()
	return &workload{config, nil}
}

func (w *workload) Run(ctx context.Context) error {
	pool, err := db.NewPostgresDB(ctx, w.config.PostgresConninfo)
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

	// If there are no tables, or user requests fixture test, then prepare stuff for fixture test.
	if w.config.Fixture || len(tables) == 0 {
		// Prepare temp tables and fixtures for workload.
		if err := w.prepare(ctx); err != nil {
			return err
		}

		tables = []string{"_noisia_waitxacts_workload"}

		// Cleanup in the end.
		defer func() { _ = w.cleanup(ctx) }()
	}

	return startLoop(ctx, pool, tables, w.config.Jobs, w.config.WaitXactsLocktimeMin, w.config.WaitXactsLocktimeMax)
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

	_ = tx.Commit(ctx)
	return nil
}

// cleanup perform fixtures cleanup after workload has been done.
func (w *workload) cleanup(ctx context.Context) error {
	_, _, err := w.pool.Exec(ctx, "DROP TABLE IF EXISTS _noisia_waitxacts_workload")
	if err != nil {
		return err
	}
	return nil
}

// startLoop start workload loop until context timeout exceeded.
func startLoop(ctx context.Context, pool db.DB, tables []string, jobs uint16, minTime, maxTime int) error {
	// Keep specified number of workers using channel - run new workers until there is any free slot
	guard := make(chan struct{}, jobs)
	for {
		select {
		// run workers only when it's possible to write into channel (channel is limited by number of jobs)
		case guard <- struct{}{}:
			go func() {
				table := selectRandomTable(tables)
				naptime := time.Duration(rand.Intn(maxTime-minTime)+minTime) * time.Second

				lockTable(ctx, pool, table, naptime)

				// when worker finished, read from the channel to allow starting another workers
				<-guard
			}()
		case <-ctx.Done():
			return nil
		}
	}
}

// lockTable tries to lock specified table for 'idle' amount of time.
func lockTable(ctx context.Context, pool db.DB, table string, idle time.Duration) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := fmt.Sprintf("LOCK TABLE %s IN ACCESS EXCLUSIVE MODE", table)
	_, _, err = tx.Exec(ctx, q)
	if err != nil {
		return
	}

	// Stop execution only if context has been done or idle interval is timed out
	timer := time.NewTimer(idle)
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		return
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
