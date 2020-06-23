package app

import (
	"context"
	"github.com/lesovsky/noisia/app/internal/log"
	"sync"
)

func Start(ctx context.Context, c *Config) error {
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
