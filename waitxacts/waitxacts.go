package waitxacts

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lesovsky/noisia"
	"math/rand"
	"time"
)

const (
	// defaultWaitXactsLocktimeMin defines default lower threshold of locking interval for blocking transactions.
	defaultWaitXactsLocktimeMin = 5
	// defaultWaitXactsLocktimeMax defines default upper threshold of locking interval for blocking transactions.
	defaultWaitXactsLocktimeMax = 20
)

// Config defines configuration settings for waiting transactions workload
type Config struct {
	// PostgresConninfo defines connections string used for connecting to Postgres.
	PostgresConninfo string
	// Jobs defines how many workers should be created for producing waiting transactions.
	Jobs uint16
	// waitXactsLocktimeMin defines a lower threshold of locking interval for blocking transactions.
	WaitXactsLocktimeMin int
	// waitXactsLocktimeMax defines an upper threshold of locking interval for blocking transactions.
	WaitXactsLocktimeMax int
}

func (c *Config) defaults() {
	if c.WaitXactsLocktimeMin == 0 {
		c.WaitXactsLocktimeMin = defaultWaitXactsLocktimeMin
	}
	if c.WaitXactsLocktimeMax == 0 {
		c.WaitXactsLocktimeMax = defaultWaitXactsLocktimeMax
	}
}

type workload struct {
	config *Config
	pool   *pgxpool.Pool
}

func NewWorkload(config *Config) noisia.Workload {
	config.defaults()
	return &workload{config, &pgxpool.Pool{}}
}

func (w *workload) Run(ctx context.Context) error {
	pool, err := pgxpool.Connect(ctx, w.config.PostgresConninfo)
	if err != nil {
		return err
	}
	w.pool = pool
	defer w.pool.Close()

	// Prepare temp tables and fixtures for workload.
	if err := w.prepare(ctx); err != nil {
		return err
	}

	// Cleanup in the end.
	defer func() { _ = w.cleanup(ctx) }()

	// Keep specified number of workers using channel - run new workers until there is any free slot
	guard := make(chan struct{}, w.config.Jobs)
	min := w.config.WaitXactsLocktimeMin
	max := w.config.WaitXactsLocktimeMax
	for {
		select {
		// run workers only when it's possible to write into channel (channel is limited by number of jobs)
		case guard <- struct{}{}:
			go func() {
				naptime := time.Duration(rand.Intn(max-min)+min) * time.Second
				startConcurrentXact(ctx, pool, naptime)

				// when worker finished, read from the channel to allow starting another workers
				<-guard
			}()
		case <-ctx.Done():
			return nil
		}
	}
}

func (w *workload) prepare(ctx context.Context) error {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS _noisia_waitxacts_workload (payload bigint)")
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, "INSERT INTO _noisia_waitxacts_workload (payload) VALUES (0)")
	if err != nil {
		return err
	}

	_ = tx.Commit(ctx)
	return nil
}

func (w *workload) cleanup(ctx context.Context) error {
	_, err := w.pool.Exec(ctx, "DROP TABLE IF EXISTS _noisia_waitxacts_workload")
	if err != nil {
		return err
	}
	return nil
}

func startConcurrentXact(ctx context.Context, pool *pgxpool.Pool, idle time.Duration) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = tx.Exec(ctx, "UPDATE _noisia_waitxacts_workload SET payload = $1", rand.Int())
	if err != nil {
		return
	}

	// Stop execution only if context has been done or idle interval is timed out
	timer := time.NewTimer(idle)
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		// Don't care about errors.
		_ = tx.Commit(ctx)
		return
	}
}
