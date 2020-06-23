package app

import (
	"context"
	"github.com/lesovsky/noisia/app/internal/log"
)

func Start(ctx context.Context, c *Config) error {
	if c.IdleXact {
		err := runIdleXactWorkload(ctx, c)
		if err != nil {
			log.Errorf("idle transactions workload failed: %s", err)
		}
	}

	return nil
}
