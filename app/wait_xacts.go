package app

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lesovsky/noisia/app/internal/log"
	"math/rand"
	"time"
)

func runWaitXactsWorkload(ctx context.Context, config *Config) error {
	log.Infoln("Starting idle transactions workload")

	// connect to postgres
	pool, err := pgxpool.Connect(context.Background(), config.PostgresConninfo)
	if err != nil {
		return err
	}
	defer pool.Close()

	// prepare workload
	if err := prepareWaitXactWorkload(ctx, pool); err != nil {
		return err
	}

	// keep specified number of workers using channel - run new workers until there is any free slot
	guard := make(chan struct{}, config.Jobs)
	for {
		select {
		// run workers only when it's possible to write into channel (channel is limited by number of jobs)
		case guard <- struct{}{}:
			go func() {
				naptime := time.Duration(rand.Intn(config.WaitXactsLocktimeMax-config.WaitXactsLocktimeMin)+config.WaitXactsLocktimeMin) * time.Second

				log.Debugf("starting wait transactions with post-update naptime %s", naptime)
				err := startConcurrentXact(context.Background(), pool, naptime)
				if err != nil {
					log.Errorln(err)
				}

				// when worker finished, read from the channel to allow starting another workers
				<-guard
			}()
		case <-ctx.Done():
			log.Info("exit signaled, stop waiting transaction workload")
			return cleanupWaitXactWorkload(context.Background(), pool)
		}
	}
}

func prepareWaitXactWorkload(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Commit(ctx); err != nil {
			log.Warnln(err)
		}
	}()

	_, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS noisia_wait_xact_workload (payload bigint)")
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, "INSERT INTO noisia_wait_xact_workload (payload) VALUES (0)")
	if err != nil {
		return err
	}

	// return with no explicit commit, transaction will be committed using 'defer' construction
	return nil
}

func cleanupWaitXactWorkload(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, "DROP TABLE noisia_wait_xact_workload")
	if err != nil {
		return err
	}
	return nil
}

func startConcurrentXact(ctx context.Context, pool *pgxpool.Pool, idle time.Duration) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Commit(ctx); err != nil {
			log.Warnln(err)
		}
	}()

	_, err = tx.Exec(ctx, "UPDATE noisia_wait_xact_workload SET payload = $1", rand.Int())
	if err != nil {
		return err
	}

	time.Sleep(idle)

	return nil
}
