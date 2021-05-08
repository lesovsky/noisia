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
	// ClientAddr defines pattern applied to pg_stat_activity.client_addr
	ClientAddr string
	// User defines pattern applied to pg_stat_activity.usename
	User string
	// Database defines pattern applied to pg_stat_activity.datname
	Database string
	// ApplicationName defines patter applied to pg_stat_activity.application_name
	ApplicationName string
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
	pool, err := db.NewPostgresDB(ctx, w.config.PostgresConninfo)
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

// signalProcess sends cancel/terminate query to Postgres.
func (w *workload) signalProcess(ctx context.Context) {
	q := buildQuery(w.config)

	// Don't care about errors
	_, _, _ = w.pool.Exec(ctx, q)
}

// buildQuery creates cancel/terminate query depending on passed config.
func buildQuery(c *Config) string {
	var signalFuncname, signalClientBackendsOnly, signalClientAddr, signalUser, signalDatabase, signalAppName string

	if c.SoftMode {
		signalFuncname = "pg_cancel_backend(pid)"
	} else {
		signalFuncname = "pg_terminate_backend(pid)"
	}

	if c.IgnoreSystemBackends {
		signalClientBackendsOnly = "AND backend_type = 'client backend' "
	}

	if c.ClientAddr != "" {
		signalClientAddr = fmt.Sprintf("AND client_addr::text ~ '%s' ", c.ClientAddr)
	}

	if c.User != "" {
		signalUser = fmt.Sprintf("AND usename ~ '%s' ", c.User)
	}

	if c.Database != "" {
		signalDatabase = fmt.Sprintf("AND datname ~ '%s' ", c.Database)
	}

	if c.ApplicationName != "" {
		signalAppName = fmt.Sprintf("AND application_name ~ '%s' ", c.ApplicationName)
	}

	return fmt.Sprintf(
		"SELECT %s FROM pg_stat_activity WHERE pid <> pg_backend_pid() %s%s%s%s%sORDER BY random() LIMIT 1",
		signalFuncname,
		signalClientBackendsOnly,
		signalClientAddr,
		signalUser,
		signalDatabase,
		signalAppName,
	)
}
