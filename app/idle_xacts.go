package app

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lesovsky/noisia/app/internal/log"
	"math/rand"
	"time"
)

func runIdleXactWorkload(ctx context.Context, config *Config) error {
	log.Infoln("Starting idle transactions workload")

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
				naptime := time.Duration(rand.Intn(config.IdleXactNaptimeMax-config.IdleXactNaptimeMin)+config.IdleXactNaptimeMin) * time.Second

				log.Infof("starting xact with naptime %s", naptime)
				err := startSingleIdleXact(context.Background(), pool, naptime)
				if err != nil {
					log.Errorln(err)
				}

				// when worker finished, read from the channel to allow starting another workers
				<-guard
			}()
		case <-ctx.Done():
			log.Info("exit signaled, stop idle transaction workload")
			return nil
		}
	}
}

func startSingleIdleXact(ctx context.Context, pool *pgxpool.Pool, naptime time.Duration) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}

	rows, err := tx.Query(ctx, "SELECT * FROM pg_stat_database")
	if err != nil {
		return err
	}
	rows.Close()
	time.Sleep(naptime)

	if err := tx.Commit(ctx); err != nil {
		log.Warnln(err)
	}
	return nil
}
