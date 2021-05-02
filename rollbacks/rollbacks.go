package rollbacks

import (
	"context"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"math/rand"
	"time"
)

const (
	// defaultRate defines default rate of produced rollbacks per second.
	defaultRate = 10
)

// Config defines configuration settings for rollbacks workload.
type Config struct {
	// PostgresConninfo defines connections string used for connecting to Postgres.
	PostgresConninfo string
	// Jobs defines how many workers should be created for producing rollbacks.
	Jobs uint16
	// MinRate defines minimum approximate target rate for produced rollbacks per second (per single worker).
	MinRate int
	// MaxRate defines maximum approximate target rate for produced rollbacks per second (per single worker).
	MaxRate int
}

func (c *Config) defaults() {
	if c.MinRate == 0 && c.MaxRate == 0 {
		c.MinRate, c.MaxRate = defaultRate, defaultRate
	}

	// Min rate cannot be higher than max rate.
	if c.MinRate > c.MaxRate {
		c.MinRate = c.MaxRate
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
	pool, err := db.NewPostgresDB(ctx, w.config.PostgresConninfo)
	if err != nil {
		return err
	}
	defer pool.Close()

	// Calculate interval for rate throttling. Use 900ms as working time and take 100ms as calculation overhead.
	interval := 900000000 / int64(w.config.MinRate)

	// keep specified number of workers using channel - run new workers until there is any free slot
	guard := make(chan struct{}, w.config.Jobs)
	for {
		if w.config.MinRate != w.config.MaxRate {
			interval = 900000000 / int64(rand.Intn(w.config.MaxRate-w.config.MinRate)+w.config.MinRate)
		}

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

func startXactRollback(ctx context.Context, pool db.DB) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return
	}

	_, err = tx.Query(ctx, "SELECT * FROM pg_stat_replication")
	if err != nil {
		return
	}

	_ = tx.Rollback(ctx)
}
