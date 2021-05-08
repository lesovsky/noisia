package failconns

import (
	"context"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"time"
)

// Config defines configuration settings for idle transactions workload.
type Config struct {
	// Conninfo defines connections string used for connecting to Postgres.
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
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config Config) (noisia.Workload, error) {
	err := config.validate()
	if err != nil {
		return nil, err
	}

	return &workload{config}, nil
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

func (w *workload) cleanup(conns []db.Conn) {
	for i := range conns {
		_ = conns[i].Close()
	}
}
