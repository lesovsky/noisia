package app

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lesovsky/noisia/app/internal/log"
	"time"
)

func runRollbacksWorkload(ctx context.Context, config *Config) error {
	log.Infoln("Starting rollbacks workload")

	pool, err := pgxpool.Connect(context.Background(), config.PostgresConninfo)
	if err != nil {
		return err
	}
	defer pool.Close()

	// keep specified number of workers using channel - run new workers until there is any free slot
	guard := make(chan struct{}, config.Jobs)
	for {
		select {
		// run workers only when it's possible to write into channel (channel is limited by number of jobs)
		case guard <- struct{}{}:
			go func() {
				log.Debugln("starting xact with rollback")
				var interval = 1000000000 / int64(config.RollbacksRate)

				err := startXactRollback(context.Background(), pool)
				if err != nil {
					log.Errorln(err)
				}
				time.Sleep(time.Duration(interval) * time.Nanosecond)

				<-guard
			}()
		case <-ctx.Done():
			log.Info("exit signaled, stop rollbacks workload")
			return nil
		}
	}
}

func startXactRollback(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}

	rows, err := tx.Query(ctx, "SELECT * FROM pg_stat_replication")
	if err != nil {
		return err
	}
	rows.Close()

	if err := tx.Rollback(ctx); err != nil {
		log.Warnln(err)
	}
	return nil
}
