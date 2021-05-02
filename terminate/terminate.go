package terminate

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"time"
)

const (
	// defaultTerminateInterval defines default interval during which the number of backends should be terminated (or canceled).
	defaultTerminateInterval = 1
	// defaultTerminateRate defines default number of backend should be terminated per interval.
	defaultTerminateRate = 1
)

// Config defines configuration settings for backends terminate workload.
type Config struct {
	// PostgresConninfo defines connections string used for connecting to Postgres.
	PostgresConninfo string
	// TerminateInterval defines a single round in seconds during which the number of backends/queries should be terminated (accordingly to rate).
	TerminateInterval int
	// TerminateRate defines a rate of how many backends should be terminated (or queries canceled) per interval.
	TerminateRate int
	// SoftMode defines to use pg_cancel_backend() instead of pg_terminate_backend().
	SoftMode bool
	// IgnoreSystemBackends controls whether system background process should be terminated or not.
	IgnoreSystemBackends bool
}

func (c *Config) defaults() {
	if c.TerminateInterval == 0 {
		c.TerminateInterval = defaultTerminateInterval
	}
	if c.TerminateRate == 0 {
		c.TerminateRate = defaultTerminateRate
	}
}

type workload struct {
	config *Config
	pool   db.DB
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config *Config) noisia.Workload {
	config.defaults()
	return &workload{config, nil}
}

// Run method connects to Postgres and starts the workload.
func (w *workload) Run(ctx context.Context) error {
	pool, err := db.NewPostgresDB(w.config.PostgresConninfo)
	if err != nil {
		return err
	}
	w.pool = pool
	defer w.pool.Close()

	// calculate inter-query interval for rate throttling
	interval := time.Duration(1000000000*w.config.TerminateInterval/w.config.TerminateRate) * time.Nanosecond
	timer := time.NewTimer(interval)

	for {
		w.signalProcess(ctx)
		select {
		case <-timer.C:
			timer.Reset(interval)
			continue
		case <-ctx.Done():
			return nil
		}
	}
}

func (w *workload) signalProcess(ctx context.Context) {
	// Don't care about errors
	var signalFuncname, signalClientBackendsOnly string

	if w.config.SoftMode {
		signalFuncname = "pg_cancel_backend(pid)"
	} else {
		signalFuncname = "pg_terminate_backend(pid)"
	}

	if w.config.IgnoreSystemBackends {
		signalClientBackendsOnly = "AND backend_type = 'client backend'"
	}

	_, _, _ = w.pool.Exec(
		ctx,
		fmt.Sprintf("SELECT %s FROM pg_stat_activity WHERE pid <> pg_backend_pid() %s ORDER BY random() LIMIT 1", signalFuncname, signalClientBackendsOnly),
	)
}
