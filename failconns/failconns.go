package failconns

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/lesovsky/noisia"
	"time"
)

const (
	// defaultConnInterval defines default interval between making new connection to Postgres
	defaultConnInterval = 50 * time.Millisecond
)

// Config defines configuration settings for idle transactions workload.
type Config struct {
	// PostgresConninfo defines connections string used for connecting to Postgres.
	PostgresConninfo string
}

type workload struct {
	config *Config
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config *Config) noisia.Workload {
	return &workload{config}
}

// Run method connects to Postgres and starts the workload.
func (w *workload) Run(ctx context.Context) error {
	conns := make([]*pgx.Conn, 0, 1000)
	interval := defaultConnInterval
	timer := time.NewTimer(interval)

	for {
		// Wait until timer has been expired or context has been done.
		select {
		case <-timer.C:
			c, err := pgx.Connect(ctx, w.config.PostgresConninfo)
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

func (w *workload) cleanup(conns []*pgx.Conn) {
	for i := range conns {
		_ = conns[i].Close(context.Background())
	}
}
