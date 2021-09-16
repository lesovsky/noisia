// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package forkconns defines implementation of workload which creates many
// short-lived database connections which forces Postgres to fork children
// processes. Postgres is based on fork-based process model and creating a
// lot of children processes is not cheap - it requires CPU and memory
// resources, initializing and allocating internal resources, etc.
//
// For creating a workload, workers are spawned. Each worker in a loop, with
// defined interval makes a connection to Postgres and perform simple query
// to pg_class relation and then close the connection. The number of workers
// depends on Config.Jobs. Interval between creating connections is based on
// Config.Rate and calculated on per-second manner.
package forkconns

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"sync"
	"time"
)

// Config defines configuration settings for 'forkconns' workload.
type Config struct {
	// Conninfo defines connection string used for connecting to Postgres.
	Conninfo string
	// Rate defines a rate of how many connections should be established per interval.
	Rate uint16
	// Jobs defines how many workers should be created for producing connections.
	Jobs uint16
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Rate < 1 {
		return fmt.Errorf("terminate rate must be greater than zero")
	}

	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be greater than zero")
	}

	return nil
}

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

// Run method creates worker goroutines which produces the workload.
func (w *workload) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	wg.Add(int(w.config.Jobs))

	for i := uint16(0); i < w.config.Jobs; i++ {
		go func() {
			err := makeConnectionLoop(ctx, w.config.Conninfo, w.config.Rate)
			if err != nil {
				w.logger.Warnf("worker failed: %s, continue", err)
			}
			wg.Done()
		}()
	}

	w.logger.Infof("all workers started, waiting for finish")
	wg.Wait()

	return nil
}

// makeConnectionLoop establishes database connections in a loop, executes query and closes connection.
func makeConnectionLoop(ctx context.Context, conninfo string, rate uint16) error {
	// calculate naptime interval between establishing connections
	naptime := time.Second / time.Duration(rate)
	timer := time.NewTimer(naptime)

	for {
		conn, err := db.Connect(ctx, conninfo)
		if err != nil {
			return err
		}

		_, _, err = conn.Exec(ctx, "SELECT count(*) FROM pg_class LIMIT 1")
		if err != nil {
			return err
		}

		err = conn.Close()
		if err != nil {
			return err
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
