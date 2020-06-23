package app

import (
	"context"
	"github.com/lesovsky/noisia/app/internal/log"
	"sync"
)

func Start(ctx context.Context, c *Config) error {
	var wg sync.WaitGroup

	if c.IdleXact {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := runIdleXactWorkload(ctx, c)
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

	wg.Wait()

	return nil
}
