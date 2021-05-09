package tempfiles

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"time"
)

const (
	queryCreateTable = `CREATE TABLE IF NOT EXISTS _noisia_tempfiles_workload (a text, b text, c text, d text, e text, f text, g text, h text, i text, j text, k text, l text, m text, n text, o text, p text, q text, r text, s text, t text, u text, v text, w text, x text, y text, z text)`
	queryLoadData    = `INSERT INTO _noisia_tempfiles_workload (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z) SELECT random()::text,random()::text,
random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,
random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,
random()::text,random()::text,random()::text,random()::text,random()::text,random()::text from generate_series(1,$1)`
	querySelectData = `SELECT a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z FROM _noisia_tempfiles_workload GROUP BY z,y,x,w,v,u,t,s,r,q,p,o,n,m,l,k,j,i,h,g,f,e,d,c,b,a ORDER BY a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z DESC`
)

// Config defines configuration settings for temp files workload
type Config struct {
	// Conninfo defines connections string used for connecting to Postgres.
	Conninfo string
	// Jobs defines how many workers should be created for producing temp files.
	Jobs uint16
	// Rate defines rate interval for queries executing.
	Rate uint16
	// ScaleFactor defines multiplier for amount of fixtures in temporary table.
	ScaleFactor uint16
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be greater than zero")
	}

	if c.Rate == 0 {
		return fmt.Errorf("temp files rate must be greater than zero")
	}

	if c.ScaleFactor == 0 {
		return fmt.Errorf("temp files rate must be greater than zero")
	}

	return nil
}

// workload implements noisia.Workload interface.
type workload struct {
	config Config
	logger log.Logger
	pool   db.DB
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config Config, logger log.Logger) (noisia.Workload, error) {
	err := config.validate()
	if err != nil {
		return nil, err
	}

	return &workload{config, logger, nil}, nil
}

// Run connects to Postgres and starts the workload.
func (w *workload) Run(ctx context.Context) error {
	pool, err := db.NewPostgresDB(ctx, w.config.Conninfo)
	if err != nil {
		return err
	}
	w.pool = pool
	defer w.pool.Close()

	// Prepare temp tables and fixtures for workload.
	err = w.prepare(ctx)
	if err != nil {
		return err
	}

	// Cleanup in the end.
	defer func() {
		err = w.cleanup()
		if err != nil {
			w.logger.Warnf("temp files cleanup failed: %s", err)
		}
	}()

	// calculate inter-query interval for rate throttling
	interval := 1000000000 / int64(w.config.Rate)

	// keep specified number of workers using channel - run new workers until there is any free slot
	guard := make(chan struct{}, w.config.Jobs)
	for {
		select {
		// run workers only when it's possible to write into channel (channel is limited by number of jobs)
		case guard <- struct{}{}:
			go func() {
				// Don't care about errors.
				_, _, workerErr := pool.Exec(ctx, querySelectData)
				if workerErr != nil {
					w.logger.Warnf("query failed: %s", err)
				}

				time.Sleep(time.Duration(interval) * time.Nanosecond)

				<-guard
			}()
		case <-ctx.Done():
			return nil
		}
	}
}

func (w *workload) prepare(ctx context.Context) error {
	_, _, err := w.pool.Exec(ctx, queryCreateTable)
	if err != nil {
		return err
	}
	_, _, err = w.pool.Exec(ctx, queryLoadData, 1000*w.config.ScaleFactor)
	if err != nil {
		return err
	}
	return nil
}

func (w *workload) cleanup() error {
	_, _, err := w.pool.Exec(context.Background(), "DROP TABLE IF EXISTS _noisia_tempfiles_workload")
	if err != nil {
		return err
	}
	return nil
}
