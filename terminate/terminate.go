// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package terminate defines implementation of workload which randomly cancel queries
// or terminate backends. Application that executes canceled queries or hold terminated
// connections receive errors.
//
// The workload is implemented as single worker which sends cancel/terminate commands
// with interval based on Config.Rate and Config.Interval. Exact command to be sent is
// based on Config.SoftMode, depending on it pg_cancel_backend() or pg_terminate_backend()
// is used.  The workload could be additionally tuned for cancel/terminate processes
// of exact users, from specific client address, connected to specific databases or
// which has specific application name.
package terminate

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"time"
)

// Config defines configuration settings for backends terminate workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	Conninfo string
	// Interval defines an interval of single round during which the number of backends/queries should be signalled (accordingly to rate).
	Interval time.Duration
	// Rate defines a rate of how many backends should be terminated (or queries canceled) per interval.
	Rate uint16
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

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Interval < 10*time.Millisecond {
		return fmt.Errorf("terminate interval must be greater than 10ms")
	}

	if c.Rate < 1 {
		return fmt.Errorf("terminate rate must be greater than zero")
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

// Run method connects to Postgres and starts the workload.
func (w *workload) Run(ctx context.Context) error {
	pool, err := db.NewPostgresDB(ctx, w.config.Conninfo)
	if err != nil {
		return err
	}
	defer pool.Close()

	// calculate inter-query interval for per-second rate throttling
	naptime := w.config.Interval / time.Duration(w.config.Rate)
	timer := time.NewTimer(naptime)

	for {
		err = signalProcess(ctx, pool, w.config)
		if err != nil {
			w.logger.Warnf("failed terminate: %s", err)
		}

		select {
		case <-timer.C:
			timer.Reset(naptime)
			continue
		case <-ctx.Done():
			return nil
		}
	}
}

// signalProcess sends cancel/terminate query to Postgres.
func signalProcess(ctx context.Context, pool db.DB, c Config) error {
	q := buildQuery(c)

	// Don't care about errors
	_, _, err := pool.Exec(ctx, q)
	if err != nil {
		return err
	}

	return nil
}

// buildQuery creates cancel/terminate query depending on passed config.
func buildQuery(c Config) string {
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
