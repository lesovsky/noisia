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

// Config defines configuration settings for waiting transactions workload
type Config struct {
	// Conninfo defines connections string used for connecting to Postgres.
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
	if c.Jobs < 2 {
		return fmt.Errorf("jobs must be greater than 1")
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
	pool   db.DB
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config Config) (noisia.Workload, error) {
	err := config.validate()
	if err != nil {
		return nil, err
	}

	return &workload{config, nil}, nil
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

	// If there are no tables, or user requests fixture test, then prepare stuff for fixture test.
	if w.config.Fixture || len(tables) == 0 {
		// Prepare temp tables and fixtures for workload.
		if err := w.prepare(ctx); err != nil {
			return err
		}

		tables = []string{"_noisia_waitxacts_workload"}

		// Cleanup in the end.
		defer func() { _ = w.cleanup() }()
	}

	return startLoop(ctx, pool, tables, w.config.Jobs, w.config.LocktimeMin, w.config.LocktimeMax)
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
func (w *workload) cleanup() error {
	_, _, err := w.pool.Exec(context.Background(), "DROP TABLE IF EXISTS _noisia_waitxacts_workload")
	if err != nil {
		return err
	}

	return nil
}

// startLoop start workload loop until context timeout exceeded.
func startLoop(ctx context.Context, pool db.DB, tables []string, jobs uint16, minTime, maxTime time.Duration) error {
	rand.Seed(time.Now().UnixNano())

	// Increment maxTime up to 1 second due to rand.Int63n() never return max value.
	maxTime++

	// Keep specified number of workers using channel - run new workers until there is any free slot
	guard := make(chan struct{}, jobs)
	for {
		select {
		// run workers only when it's possible to write into channel (channel is limited by number of jobs)
		case guard <- struct{}{}:
			go func() {
				table := selectRandomTable(tables)
				naptime := time.Duration(rand.Int63n(maxTime.Nanoseconds()-minTime.Nanoseconds()) + minTime.Nanoseconds())

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
