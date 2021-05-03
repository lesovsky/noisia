package rollbacks

import (
	"context"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"math/rand"
	"sync"
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

	var wg sync.WaitGroup
	for i := 0; i < int(w.config.Jobs); i++ {
		wg.Add(1)
		go func() {
			startWorker(ctx, pool, w.config.MinRate, w.config.MaxRate)
			wg.Done()
		}()
	}

	wg.Wait()
	return nil
}

// startLoop start workload loop until context timeout exceeded.
func startWorker(ctx context.Context, pool db.DB, minRate, maxRate int) {
	for {
		startXactRollback(ctx, pool)

		// Calculate interval for rate throttling. Use 900ms as working time and take 100ms as calculation/executing overhead.
		var interval int64
		if minRate != maxRate {
			interval = 900000000 / int64(rand.Intn(maxRate-minRate)+minRate)
		} else {
			interval = 900000000 / int64(minRate)
		}

		timer := time.NewTimer(time.Duration(interval) * time.Nanosecond)

		select {
		case <-timer.C:
			continue
		case <-ctx.Done():
			//log.Info("exit signaled, stop rollbacks workload")
			return
		}
	}
}

// startXactRollback executes transaction with rollback.
func startXactRollback(ctx context.Context, pool db.DB) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return
	}

	_, _, err = tx.Exec(ctx, "SELECT * FROM pg_stat_replication")
	if err != nil {
		return
	}

	_ = tx.Rollback(ctx)
}
