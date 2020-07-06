package rollbacks

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lesovsky/noisia"
	"time"
)

const (
	// defaultRollbacksRate defines default rate of produced rollbacks per second.
	defaultRollbacksRate = 10
)

// Config defines configuration settings for rollbacks workload.
type Config struct {
	// PostgresConninfo defines connections string used for connecting to Postgres.
	PostgresConninfo string
	// Jobs defines how many workers should be created for producing rollbacks.
	Jobs uint16
	// RollbacksRate defines target rate for produced rollbacks per second (per single worker).
	RollbacksRate int
}

func (c *Config) defaults() {
	if c.RollbacksRate == 0 {
		c.RollbacksRate = defaultRollbacksRate
	}
}

type workload struct {
	config *Config
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config *Config) noisia.Workload {
	config.defaults()
	return &workload{config}
}

// Run method connects to Postgres and starts the workload.
func (w *workload) Run(ctx context.Context) error {
	pool, err := pgxpool.Connect(ctx, w.config.PostgresConninfo)
	if err != nil {
		return err
	}
	defer pool.Close()

	// calculate inter-query interval for rate throttling
	interval := 1000000000 / int64(w.config.RollbacksRate)

	// keep specified number of workers using channel - run new workers until there is any free slot
	guard := make(chan struct{}, w.config.Jobs)
	for {
		select {
		// run workers only when it's possible to write into channel (channel is limited by number of jobs)
		case guard <- struct{}{}:
			go func() {
				startXactRollback(ctx, pool)
				time.Sleep(time.Duration(interval) * time.Nanosecond)

				<-guard
			}()
		case <-ctx.Done():
			//log.Info("exit signaled, stop rollbacks workload")
			return nil
		}
	}
}

func startXactRollback(ctx context.Context, pool *pgxpool.Pool) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, "SELECT * FROM pg_stat_replication")
	if err != nil {
		return
	}
	rows.Close()
}
