package idlexacts

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/targeting"
	"math/rand"
	"time"
)

const (
	// defaultIdleXactsNaptimeMin defines default lower threshold for idle interval.
	defaultIdleXactsNaptimeMin = 5
	// defaultIdleXactsNaptimeMax defines default upper threshold for idle interval.
	defaultIdleXactsNaptimeMax = 20
	// maxAffectedTables defines max number of tables which will be affected by idle transactions.
	maxAffectedTables = 3
)

// Config defines configuration settings for idle transactions workload.
type Config struct {
	// PostgresConninfo defines connections string used for connecting to Postgres.
	PostgresConninfo string
	// Jobs defines how many concurrent idle transactions should be running during workload.
	Jobs uint16
	// IdleXactsNaptimeMin defines lower threshold when transactions can idle.
	IdleXactsNaptimeMin int
	// IdleXactsNaptimeMax defines upper threshold when transactions can idle.
	IdleXactsNaptimeMax int
}

func (c *Config) defaults() {
	if c.IdleXactsNaptimeMin == 0 {
		c.IdleXactsNaptimeMin = defaultIdleXactsNaptimeMin
	}

	if c.IdleXactsNaptimeMax == 0 {
		c.IdleXactsNaptimeMax = defaultIdleXactsNaptimeMax
	}
}

type workload struct {
	config *Config
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config *Config) noisia.Workload {
	config.defaults()
	return &workload{config}
}

// Run connects to Postgres and starts the workload.
func (w *workload) Run(ctx context.Context) error {
	pool, err := db.NewPostgresDB(ctx, w.config.PostgresConninfo)
	if err != nil {
		return err
	}
	defer pool.Close()

	// Looking for the top-N most writable (delete/update) tables.
	// Each idle transaction will produce a write operation (which will rolled back
	// at the end). As a result, write operation and idle transaction will lead to
	// keep dead rows versions and affect overall performance.
	tables, err := targeting.TopWriteTables(pool, maxAffectedTables)
	if err != nil {
		return err
	}

	// TODO: что если база пустая и нет таблиц?

	return startLoop(ctx, pool, tables, w.config.Jobs, w.config.IdleXactsNaptimeMin, w.config.IdleXactsNaptimeMax)
}

// startLoop starts workload using passed settings and database connection.
func startLoop(ctx context.Context, pool db.DB, tables []string, jobs uint16, minTime, maxTime int) error {
	// While running, keep required number of workers using channel.
	// Run new workers only until there is any free slot.

	guard := make(chan struct{}, jobs)
	for {
		select {
		// Run workers only when it's possible to write into channel (channel is limited by number of jobs).
		case guard <- struct{}{}:
			go func() {
				table := selectRandomTable(tables)
				naptime := time.Duration(rand.Intn(maxTime-minTime)+minTime) * time.Second

				startSingleIdleXact(ctx, pool, table, naptime)

				// When worker finishes, read from the channel to allow starting another worker.
				<-guard
			}()
		case <-ctx.Done():
			//log.Info("exit signaled, stop idle transaction workload")
			return nil
		}
	}
}

// startSingleIdleXact starts transaction and goes sleeping for specified amount of time.
func startSingleIdleXact(ctx context.Context, pool db.DB, table string, naptime time.Duration) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// When table is specified, create a temp table using single row from target table. Later,
	// transaction will be rolled back and temp table will be dropped. Also, any errors could
	// be ignored, because in this case transaction (aborted) also stay idle.
	if table != "" {
		_ = createTempTable(tx, table)
	}

	// Stop execution only if context has been done or naptime interval is timed out.
	timer := time.NewTimer(naptime)
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		// Don't care about errors.
		_ = tx.Rollback(ctx)
		return
	}
}

// selectRandomTable returns random table from passed list. Empty value returned if empty list.
func selectRandomTable(tables []string) string {
	if len(tables) == 0 {
		return ""
	}

	return tables[rand.Intn(len(tables))]
}

// createTempTable creates a temporary table within a transaction using single row from passed table.
func createTempTable(tx db.Tx, table string) error {
	q := fmt.Sprintf("CREATE TEMP TABLE noisia_%d ON COMMIT DROP AS SELECT * FROM %s LIMIT 1", time.Now().Unix(), table)
	_, _, err := tx.Exec(context.Background(), q)
	if err != nil {
		return err
	}

	return nil
}
