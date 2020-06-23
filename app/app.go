package app

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/lesovsky/noisia/app/internal/log"
	"sync"
)

func Start(ctx context.Context, c *Config) error {
	if c.DoCleanup {
		return runCleanup(c.PostgresConninfo)
	}

	var wg sync.WaitGroup

	if c.IdleXacts {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := runIdleXactsWorkload(ctx, c)
			if err != nil {
				log.Errorf("idle transactions workload failed: %s", err)
			}
		}()
	}

	if c.Rollbacks {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := runRollbacksWorkload(ctx, c)
			if err != nil {
				log.Errorf("rollbacks workload failed: %s", err)
			}
		}()
	}

	if c.WaitXacts {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := runWaitXactsWorkload(ctx, c)
			if err != nil {
				log.Errorf("wait xacts workload failed: %s", err)
			}
		}()
	}

	if c.Deadlocks {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := runDeadlocksWorkload(ctx, c)
			if err != nil {
				log.Errorf("deadlocks workload failed: %s", err)
			}
		}()
	}

	if c.TempFiles {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := runTempFilesWorkload(ctx, c)
			if err != nil {
				log.Errorf("temporary files workload failed: %s", err)
			}
		}()
	}

	wg.Wait()

	return nil
}

func runCleanup(dsn string) error {
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		return err
	}

	var tables = []string{"noisia_deadlocks_workload", "noisia_wait_xact_workload", "noisia_temp_files_workload"}
	for _, t := range tables {
		log.Infof("cleanup: drop table %s", t)
		_, err := conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", t))
		if err != nil {
			return err
		}
	}
	return nil
}
