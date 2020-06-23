package app

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/lesovsky/noisia/app/internal/log"
	"math/rand"
	"sync"
	"time"
)

func runDeadlocksWorkload(ctx context.Context, config *Config) error {
	log.Infoln("Starting idle transactions workload")

	// connect to postgres
	pool, err := pgxpool.Connect(context.Background(), config.PostgresConninfo)
	if err != nil {
		return err
	}
	defer pool.Close()

	// prepare workload
	if err := prepareDeadlocksWorkload(ctx, pool); err != nil {
		return err
	}

	// keep specified number of workers using channel - run new workers until there is any free slot
	guard := make(chan struct{}, config.Jobs)
	for {
		select {
		// run workers only when it's possible to write into channel (channel is limited by number of jobs)
		case guard <- struct{}{}:
			go func() {
				log.Debugln("starting a pair of deadlock-producing transactions")
				err := executeDeadlock(context.Background(), pool)
				if err != nil {
					log.Errorln(err)
				}

				// when worker finished, read from the channel to allow starting another workers
				<-guard
			}()
		case <-ctx.Done():
			log.Info("exit signaled, stop waiting transaction workload")
			// TODO: cleanup is not working - workload table still exists in the database (no suspicious logs)
			return cleanupDeadlocksWorkload(ctx, pool)
		}
	}
}

func prepareDeadlocksWorkload(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Commit(ctx); err != nil {
			log.Warnln(err)
		}
	}()

	_, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS noisia_deadlocks_workload (id bigint, payload text)")
	if err != nil {
		return err
	}

	// return with no explicit commit, transaction will be committed using 'defer' construction
	return nil
}

func cleanupDeadlocksWorkload(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, "DROP TABLE noisia_deadlocks_workload")
	if err != nil {
		return err
	}
	return nil
}

func executeDeadlock(ctx context.Context, pool *pgxpool.Pool) error {
	// insert two rows
	id1, id2 := rand.Int(), rand.Int()
	_, err := pool.Exec(ctx, "INSERT INTO noisia_deadlocks_workload (id, payload) VALUES ($1, md5(random()::text)), ($2, md5(random()::text))", id1, id2)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		if err := runUpdateXact(ctx, pool, id1, id2); err != nil {
			log.Debugln(err)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		if err := runUpdateXact(ctx, pool, id2, id1); err != nil {
			log.Debugln(err)
		}
		wg.Done()
	}()

	wg.Wait()
	return nil
}

func runUpdateXact(ctx context.Context, pool *pgxpool.Pool, id1 int, id2 int) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil
	}
	defer func() {
		if err := tx.Commit(ctx); err != nil {
			log.Debugln(err)
		}
	}()

	// update row 1
	_, err = tx.Exec(ctx, "UPDATE noisia_deadlocks_workload SET payload = md5(random()::text) WHERE id = $1", id1)
	if err != nil {
		return err
	}

	time.Sleep(10 * time.Millisecond)

	// update row 2 by tx 1
	_, err = tx.Exec(ctx, "UPDATE noisia_deadlocks_workload SET payload = md5(random()::text) WHERE id = $1", id2)
	if err != nil {
		return err
	}

	return nil
}
