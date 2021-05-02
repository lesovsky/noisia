package deadlocks

import (
	"context"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"math/rand"
	"sync"
	"time"
)

// Config defines configuration settings for deadlocks workload.
type Config struct {
	// PostgresConninfo defines connections string used for connecting to Postgres.
	PostgresConninfo string
	// Jobs defines how many workers should be created for producing deadlocks.
	Jobs uint16
}

type workload struct {
	config *Config
	pool   db.DB
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config *Config) noisia.Workload {
	return &workload{config, nil}
}

// Run method connects to Postgres and starts the workload.
func (w *workload) Run(ctx context.Context) error {
	pool, err := db.NewPostgresDB(ctx, w.config.PostgresConninfo)
	if err != nil {
		return err
	}
	w.pool = pool
	defer w.pool.Close()

	// Prepare temp tables and fixtures for workload.
	if err := w.prepare(ctx); err != nil {
		return err
	}

	// Cleanup in the end.
	defer func() { _ = w.cleanup(ctx) }()

	// Keep specified number of workers using channel - run new workers until there is any free slot.
	guard := make(chan struct{}, w.config.Jobs)
	for {
		select {
		// run workers only when it's possible to write into channel (channel is limited by number of jobs).
		case guard <- struct{}{}:
			go func() {
				w.executeDeadlock(ctx)

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

func (w *workload) cleanup(ctx context.Context) error {
	_, _, err := w.pool.Exec(ctx, "DROP TABLE IF EXISTS _noisia_deadlocks_workload")
	if err != nil {
		return err
	}
	return nil
}

func (w *workload) executeDeadlock(ctx context.Context) {
	// insert two rows
	id1, id2 := rand.Int(), rand.Int()
	_, _, err := w.pool.Exec(ctx, "INSERT INTO _noisia_deadlocks_workload (id, payload) VALUES ($1, md5(random()::text)), ($2, md5(random()::text))", id1, id2)
	if err != nil {
		return
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		w.runUpdateXact(ctx, id1, id2)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		w.runUpdateXact(ctx, id2, id1)
		wg.Done()
	}()

	wg.Wait()
}

func (w *workload) runUpdateXact(ctx context.Context, id1 int, id2 int) {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Update row #1
	_, _, err = tx.Exec(ctx, "UPDATE _noisia_deadlocks_workload SET payload = md5(random()::text) WHERE id = $1", id1)
	if err != nil {
		return
	}

	// This time is sufficient to allow capturing locks in concurrent xacts.
	time.Sleep(10 * time.Millisecond)

	// Update row #2
	_, _, err = tx.Exec(ctx, "UPDATE _noisia_deadlocks_workload SET payload = md5(random()::text) WHERE id = $1", id2)
	if err != nil {
		return
	}

	// Don't care about errors.
	_ = tx.Commit(ctx)
}
