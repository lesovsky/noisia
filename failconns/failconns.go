// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package failconns implements a workload which creates many database connections
// forcing Postgres to fork children processes until the value of max_connections
// is reached. When max_connections is reached all connection slots are exhausted
// and no one client can connect to Postgres.
//
// Implementation of the workload is quite simple - create new connections in a
// loop until Postgres starts respond with error. By default, an array with 1000
// slots is used, so it possible to set max_connections to higher value and
// pass the workload with no errors.
package failconns

import (
	"context"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"time"
)

// Config defines configuration settings for failconns workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	Conninfo string
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	// nothing to validate

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
	// defaultConnInterval defines default interval between making new connection to Postgres
	defaultConnInterval := 50 * time.Millisecond

	conns := make([]db.Conn, 0, 1000)
	interval := defaultConnInterval
	timer := time.NewTimer(interval)

	for {
		// Wait until timer has been expired or context has been done.
		select {
		case <-timer.C:
			c, err := db.Connect(ctx, w.config.Conninfo)
			if err != nil {
				w.logger.Info(err.Error())

				// if connect has failed, increase interval between connects
				interval = interval * 2
			} else {
				// append connection into slice
				conns = append(conns, c)

				// if attempt was successful reduce interval, but no less than default
				if interval > defaultConnInterval {
					interval = interval / 2
				}
			}

			timer.Reset(interval)
		case <-ctx.Done():
			w.cleanup(conns)
			return nil
		}
	}
}

// cleanup gracefully closes all database connections
func (w *workload) cleanup(conns []db.Conn) {
	for i := range conns {
		_ = conns[i].Close()
	}
}
