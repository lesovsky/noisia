package idlexacts

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"github.com/lesovsky/noisia/targeting"
	"math/rand"
	"time"
)

// Config defines configuration settings for idle transactions workload.
type Config struct {
	// Conninfo defines connections string used for connecting to Postgres.
	Conninfo string
	// Jobs defines how many concurrent idle transactions should be running during workload.
	Jobs uint16
	// NaptimeMin defines lower threshold when transactions can idle.
	NaptimeMin time.Duration
	// NaptimeMax defines upper threshold when transactions can idle.
	NaptimeMax time.Duration
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be greater than zero")
	}

	if c.NaptimeMin == 0 || c.NaptimeMax == 0 {
		return fmt.Errorf("min and max idle time must be greater than zero")
	}

	if c.NaptimeMin > c.NaptimeMax {
		return fmt.Errorf("min naptime must be less or equal to naptime max")
	}

	return nil
}

// workload implements noisia.Workload interface.
type workload struct {
	config Config
	logger log.Logger
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config Config, logger log.Logger) (noisia.Workload, error) {
	err := config.validate()
	if err != nil {
		return nil, err
	}
	return &workload{config, logger}, nil
}

// Run connects to Postgres and starts the workload.
func (w *workload) Run(ctx context.Context) error {
	// maxAffectedTables defines max number of tables which will be affected by idle transactions.
	maxAffectedTables := 3

	pool, err := db.NewPostgresDB(ctx, w.config.Conninfo)
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

	return startLoop(ctx, w.logger, pool, tables, w.config.Jobs, w.config.NaptimeMin, w.config.NaptimeMax)
}

// startLoop starts workload using passed settings and database connection.
func startLoop(ctx context.Context, log log.Logger, pool db.DB, tables []string, jobs uint16, minTime, maxTime time.Duration) error {
	rand.Seed(time.Now().UnixNano())

	// Increment maxTime up to 1 due to rand.Int63n() never return max value.
	maxTime++

	// While running, keep required number of workers using channel.
	// Run new workers only until there is any free slot.
	guard := make(chan struct{}, jobs)
	for {
		select {
		// Run workers only when it's possible to write into channel (channel is limited by number of jobs).
		case guard <- struct{}{}:
			go func() {
				table := selectRandomTable(tables)
				naptime := time.Duration(rand.Int63n(maxTime.Nanoseconds()-minTime.Nanoseconds()) + minTime.Nanoseconds())

				err := startSingleIdleXact(ctx, pool, table, naptime)
				if err != nil {
					log.Warnf("start idle xact failed: %s", err)
				}

				// When worker finishes, read from the channel to allow starting another worker.
				<-guard
			}()
		case <-ctx.Done():

			return nil
		}
	}
}

// startSingleIdleXact starts transaction and goes sleeping for specified amount of time.
func startSingleIdleXact(ctx context.Context, pool db.DB, table string, naptime time.Duration) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// When table is specified, create a temp table using single row from target table. Later,
	// transaction will be rolled back and temp table will be dropped. Also, any errors could
	// be ignored, because in this case transaction (aborted) also stay idle.
	if table != "" {
		err = createTempTable(tx, table)
		if err != nil {
			return err
		}
	}

	// Stop execution only if context has been done or naptime interval is timed out.
	timer := time.NewTimer(naptime)
	select {
	case <-ctx.Done():
		return nil
	case <-timer.C:
		return nil
	}
}

// selectRandomTable returns random table from passed list. Empty value returned if empty list.
func selectRandomTable(tables []string) string {
	if len(tables) == 0 {
		return ""
	}

	rand.Seed(time.Now().UnixNano())
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
