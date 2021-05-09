package deadlocks

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"math/rand"
	"sync"
	"time"
)

// Config defines configuration settings for deadlocks workload.
type Config struct {
	// Conninfo defines connections string used for connecting to Postgres.
	Conninfo string
	// Jobs defines how many workers should be created for producing deadlocks.
	Jobs uint16
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be greater than zero")
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

// Run method connects to Postgres and starts the workload.
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
			w.logger.Warnf("deadlocks cleanup failed: %s")
		}
	}()

	// Keep specified number of workers using channel - run new workers until there is any free slot.
	guard := make(chan struct{}, w.config.Jobs)
	for {
		select {
		// run workers only when it's possible to write into channel (channel is limited by number of jobs).
		case guard <- struct{}{}:
			go func() {
				err := executeDeadlock(ctx, w.logger, w.config.Conninfo)
				if err != nil {
					w.logger.Warnf("reproduce deadlock failed: %s", err)
				}

				// when worker finished, read from the channel to allow starting another workers
				<-guard
			}()
		case <-ctx.Done():
			return nil
		}
	}
}

func (w *workload) prepare(ctx context.Context) error {
	_, _, err := w.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS _noisia_deadlocks_workload (id bigint, payload text)")
	if err != nil {
		return err
	}
	return nil
}

func (w *workload) cleanup() error {
	_, _, err := w.pool.Exec(context.Background(), "DROP TABLE IF EXISTS _noisia_deadlocks_workload")
	if err != nil {
		return err
	}
	return nil
}

func executeDeadlock(ctx context.Context, log log.Logger, conninfo string) error {
	conn1, err := db.Connect(ctx, conninfo)
	if err != nil {
		return err
	}

	conn2, err := db.Connect(ctx, conninfo)
	if err != nil {
		return err
	}

	// insert two rows
	rand.Seed(time.Now().UnixNano())
	id1, id2 := rand.Int(), rand.Int()
	_, _, err = conn1.Exec(ctx, "INSERT INTO _noisia_deadlocks_workload (id, payload) VALUES ($1, md5(random()::text)), ($2, md5(random()::text))", id1, id2)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		err := runUpdateXact(context.Background(), conn1, id1, id2)
		if err != nil {
			if err.Error() == "ERROR: deadlock detected (SQLSTATE 40P01)" {
				log.Info("deadlock detected")
			} else {
				log.Warnf("update failed: %s", err)
			}
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		err := runUpdateXact(context.Background(), conn2, id2, id1)
		if err != nil {
			if err.Error() == "ERROR: deadlock detected (SQLSTATE 40P01)" {
				log.Info("deadlock detected")
			} else {
				log.Warnf("update failed: %s", err)
			}
		}
		wg.Done()
	}()

	wg.Wait()
	return nil
}

func runUpdateXact(ctx context.Context, conn db.Conn, id1 int, id2 int) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Update row #1
	_, _, err = tx.Exec(ctx, "UPDATE _noisia_deadlocks_workload SET payload = md5(random()::text) WHERE id = $1", id1)
	if err != nil {
		return err
	}

	// This time is sufficient to allow capturing locks in concurrent transaction.
	time.Sleep(10 * time.Millisecond)

	// Update row #2
	_, _, err = tx.Exec(ctx, "UPDATE _noisia_deadlocks_workload SET payload = md5(random()::text) WHERE id = $1", id2)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}
