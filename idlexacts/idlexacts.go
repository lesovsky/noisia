package idlexacts

import (
	"context"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"math/rand"
	"time"
)

const (
	// defaultIdleXactsNaptimeMin defines default lower threshold for idle interval.
	defaultIdleXactsNaptimeMin = 5
	// defaultIdleXactsNaptimeMax defines default upper threshold for idle interval.
	defaultIdleXactsNaptimeMax = 20
)

// Config defines configuration settings for idle transactions workload.
type Config struct {
	// PostgresConninfo defines connections string used for connecting to Postgres.
	PostgresConninfo string
	// Jobs defines how many concurrent idle transactions should be running during workload.
	Jobs uint16
	// IdleXactsNaptimeMin defines lower threshold when transactions can idle.
	IdleXactsNaptimeMin int
	// IdleXactsNaptimeMax defines upper threshold when transactions can idle.
	IdleXactsNaptimeMax int
}

func (c *Config) defaults() {
	if c.IdleXactsNaptimeMin == 0 {
		c.IdleXactsNaptimeMin = defaultIdleXactsNaptimeMin
	}

	if c.IdleXactsNaptimeMax == 0 {
		c.IdleXactsNaptimeMax = defaultIdleXactsNaptimeMax
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

// Run connects to Postgres and starts the workload.
func (w *workload) Run(ctx context.Context) error {
	pool, err := db.NewPostgresDB(w.config.PostgresConninfo)
	if err != nil {
		return err
	}
	defer pool.Close()

	return startLoop(ctx, pool, w.config.Jobs, w.config.IdleXactsNaptimeMin, w.config.IdleXactsNaptimeMax)
}

// startLoop starts workload using passed settings and database connection.
func startLoop(ctx context.Context, pool db.DB, jobs uint16, minTime, maxTime int) error {
	// While running, keep required number of workers using channel.
	// Run new workers only until there is any free slot.

	guard := make(chan struct{}, jobs)
	for {
		select {
		// Run workers only when it's possible to write into channel (channel is limited by number of jobs).
		case guard <- struct{}{}:
			go func() {
				naptime := time.Duration(rand.Intn(maxTime-minTime)+minTime) * time.Second
				startSingleIdleXact(ctx, pool, naptime)

				// When worker finishes, read from the channel to allow starting another worker.
				<-guard
			}()
		case <-ctx.Done():
			//log.Info("exit signaled, stop idle transaction workload")
			return nil
		}
	}
}

// startSingleIdleXact starts transaction and goes sleeping for specified amount of time.
func startSingleIdleXact(ctx context.Context, pool db.DB, naptime time.Duration) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, _, err = tx.Exec(ctx, "SELECT * FROM pg_stat_database")
	if err != nil {
		return
	}

	// Stop execution only if context has been done or naptime interval is timed out
	timer := time.NewTimer(naptime)
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		// Don't care about errors.
		_ = tx.Commit(ctx)
		return
	}
}
