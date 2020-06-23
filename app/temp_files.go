package app

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lesovsky/noisia/app/internal/log"
	"time"
)

const (
	queryCreateTable = `CREATE TABLE IF NOT EXISTS noisia_temp_files_workload (a text, b text, c text, d text, e text, f text, g text, h text, i text, j text, k text, l text, m text, n text, o text, p text, q text, r text, s text, t text, u text, v text, w text, x text, y text, z text)`
	queryLoadData    = `INSERT INTO noisia_temp_files_workload (a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z) SELECT random()::text,random()::text,
random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,
random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,random()::text,
random()::text,random()::text,random()::text,random()::text,random()::text,random()::text from generate_series(1,$1)`
	querySelectData = `SELECT a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z FROM noisia_temp_files_workload GROUP BY z,y,x,w,v,u,t,s,r,q,p,o,n,m,l,k,j,i,h,g,f,e,d,c,b,a ORDER BY a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z DESC`
)

func runTempFilesWorkload(ctx context.Context, config *Config) error {
	log.Infoln("Starting temporary files workload")

	pool, err := pgxpool.Connect(context.Background(), config.PostgresConninfo)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := prepareTempFilesWorkload(ctx, pool, config.TempFilesScaleFactor); err != nil {
		return err
	}

	// calculate inter-query interval for rate throttling
	interval := 1000000000 / int64(config.TempFilesRate)

	// keep specified number of workers using channel - run new workers until there is any free slot
	guard := make(chan struct{}, config.Jobs)
	for {
		select {
		// run workers only when it's possible to write into channel (channel is limited by number of jobs)
		case guard <- struct{}{}:
			go func() {
				log.Debugln("starting temp files produced query")

				rows, err := pool.Query(context.Background(), querySelectData)
				if err != nil {
					log.Errorln(err)
				}
				rows.Close()
				time.Sleep(time.Duration(interval) * time.Nanosecond)

				<-guard
			}()
		case <-ctx.Done():
			log.Info("exit signaled, stop temp files workload")
			// TODO: cleanup is not working - workload table still exists in the database (no suspicious logs)
			return cleanupTempFilesWorkload(context.Background(), pool)
		}
	}
}

func prepareTempFilesWorkload(ctx context.Context, pool *pgxpool.Pool, scaleFactor int) error {
	_, err := pool.Exec(ctx, queryCreateTable)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, queryLoadData, 1000*scaleFactor)
	if err != nil {
		return err
	}
	return nil
}

func cleanupTempFilesWorkload(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, "DROP TABLE IF EXISTS noisia_temp_files_workload")
	if err != nil {
		return err
	}
	return nil
}
