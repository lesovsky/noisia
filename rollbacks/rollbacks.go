package rollbacks

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"math/rand"
	"sync"
	"time"
)

// Config defines configuration settings for rollbacks workload.
type Config struct {
	// PostgresConninfo defines connections string used for connecting to Postgres.
	PostgresConninfo string
	// Jobs defines how many workers should be created for producing rollbacks.
	Jobs uint16
	// MinRate defines minimum approximate target rate for produced rollbacks per second (per single worker).
	MinRate int
	// MaxRate defines maximum approximate target rate for produced rollbacks per second (per single worker).
	MaxRate int
}

// validate method checks workload configuration settings.
func (c Config) validate() error {
	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be greater than zero")
	}

	if c.MinRate == 0 && c.MaxRate == 0 {
		return fmt.Errorf("min and max rate must be greater than zero")
	}

	if c.MinRate > c.MaxRate {
		return fmt.Errorf("min rate must be less or equal to max rate")
	}

	return nil
}

// workload implements noisia.Workload interface.
type workload struct {
	config Config
}

// NewWorkload creates a new workload with specified config.
func NewWorkload(config Config) (noisia.Workload, error) {
	err := config.validate()
	if err != nil {
		return nil, err
	}

	return &workload{config}, nil
}

// Run method connects to Postgres and starts the workload.
func (w *workload) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	for i := 0; i < int(w.config.Jobs); i++ {
		// Create connection for each worker. Pool is not used because each worker
		// creates a temporary table used during workload.
		conn, err := db.Connect(ctx, w.config.PostgresConninfo)
		if err != nil {
			// TODO: expose errors outside
			continue
		}

		// Start working loop.
		wg.Add(1)
		go func() {
			// Increment MaxRate up to 1 due to rand.Intn() never return max value.
			_, _, _ = startLoop(ctx, conn, w.config.MinRate, w.config.MaxRate+1)
			// TODO: expose errors outside if any

			_ = conn.Close()
			wg.Done()
		}()
	}

	wg.Wait()
	return nil
}

// startLoop start workload loop until context timeout exceeded.
func startLoop(ctx context.Context, conn db.Conn, minRate, maxRate int) (int, int, error) {
	table, err := createTempTable(ctx, conn)
	if err != nil {
		return 0, 0, err
	}

	// Initialize random source.
	rand.Seed(time.Now().UnixNano())

	var successful, failed int

	for {
		// Select random query with arguments.
		q, args := newErrQuery(-1, table)

		// Execute query. Suppress errors, it is designed all generated queries produce errors.
		_, _, err = conn.Exec(ctx, q, args...)
		if err != nil {
			failed++
		} else {
			successful++
		}

		// Calculate interval for rate throttling. Use 900ms as working time and take 100ms
		// as calculation/executing overhead.
		var interval int64
		if minRate != maxRate {
			interval = 900000000 / int64(rand.Intn(maxRate-minRate)+minRate)
		} else {
			interval = 900000000 / int64(minRate)
		}

		timer := time.NewTimer(time.Duration(interval) * time.Nanosecond)

		select {
		case <-timer.C:
			continue
		case <-ctx.Done():
			//log.Info("exit signaled, stop rollbacks workload")
			return successful, failed, nil
		}
	}
}

// createTempTable creates temporary table for session.
func createTempTable(ctx context.Context, conn db.Conn) (string, error) {
	t := fmt.Sprintf("noisia_%d", time.Now().Unix())
	q := fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s (entity_id INT, name TEXT, size_b BIGINT, created_at TIMESTAMPTZ)", t)

	_, _, err := conn.Exec(ctx, q)
	if err != nil {
		return "", err
	}

	return t, nil
}

// newErrQuery returns random erroneous query with arguments.
func newErrQuery(n int, table string) (string, []interface{}) {
	// Total number of available erroneous queries.
	total := 15

	rand.Seed(time.Now().UnixNano())

	var index = n
	if index < 0 || index >= 15 {
		index = rand.Intn(total)
	}

	var (
		num1, num2 = rand.Intn(1000), rand.Intn(10000)
		str1       = fmt.Sprintf("AUX-%d-%d-%d", rand.Intn(1000), rand.Intn(1000), rand.Intn(1000))
		str2       = fmt.Sprintf("AUX-%d-%d-%d", rand.Intn(1000), rand.Intn(1000), rand.Intn(1000))

		q    string
		args []interface{}
	)

	switch index {
	case 0:
		// ERROR:  INSERT has more expressions than target columns
		q = fmt.Sprintf("INSERT INTO %s (entity_id, name, size_b) VALUES ($1, $2, $3, $4)", table)
		args = []interface{}{num1, str1, num2, time.Now().String()}
	case 1:
		// ERROR:  invalid input syntax for type integer: "???"
		q = fmt.Sprintf("INSERT INTO %s (entity_id, name, size_b) VALUES ($1, $2, $3)", table)
		args = []interface{}{num1, str1, str2}
	case 2:
		// ERROR:  date/time field value out of range: "???" at character ???
		q = fmt.Sprintf("INSERT INTO %s (entity_id, name, size_b, created_at) VALUES ($1, $2, $3, $4)", table)
		args = []interface{}{num1, str1, num2, "30/02/2021"}
	case 3:
		// ERROR:  could not open file "???" for writing: No such file or directory
		q = fmt.Sprintf("COPY %s FROM '/mnt/vol9/raw/data/%d/noisia.in.csv'", table, num1)
	case 4:
		// ERROR:  syntax error at or near "???" at character ???
		q = fmt.Sprintf("INSERT SELECT entity_id, name, size_b, created_at FROM %s WHERE entity_id = $1", table)
		args = []interface{}{num1}
	case 5:
		// ERROR:  column "???" does not exist at character ???
		q = fmt.Sprintf("SELECT id, name, size_b, created_at FROM %s WHERE id = $1", table)
		args = []interface{}{num1}
	case 6:
		// ERROR:  relation "???" does not exist at character ???
		q = fmt.Sprintf("SELECT entity_id, name, size_b, created_at FROM %s_1 WHERE entity_id = $1", table)
		args = []interface{}{num1}
	case 7:
		// ERROR:  function string_agg(integer, unknown) does not exist at character ???
		q = fmt.Sprintf("SELECT string_agg(name, 10) FROM %s WHERE entity_id >= $1 and entity_id < $2", table)
		args = []interface{}{num1, num2}
	case 8:
		// ERROR:  column "???" must appear in the GROUP BY clause or be used in an aggregate function at character ???
		q = fmt.Sprintf("SELECT name, created_at::date, count(size_b) FROM %s WHERE created_at > to_timestamp($1) GROUP BY name ORDER BY 3 DESC", table)
		args = []interface{}{num1 * 999999}
	case 9:
		// ERROR:  aggregate functions are not allowed in GROUP BY at character ???
		q = fmt.Sprintf("SELECT name, created_at::date, count(size_b) FROM %s WHERE created_at > to_timestamp($1) GROUP BY 1,2,3 ORDER BY 3 DESC", table)
		args = []interface{}{num1 * 999999}
	case 10:
		// ERROR:  ORDER BY position 4 is not in select list
		q = fmt.Sprintf("SELECT name, created_at::date, count(size_b) FROM %s WHERE created_at > to_timestamp($1) GROUP BY 1,2,3 ORDER BY 4 DESC", table)
		args = []interface{}{num1 * 999999}
	case 11:
		// ERROR:  more than one row returned by a subquery used as an expression
		q = "SELECT relname, reltuples FROM pg_class WHERE relname = (SELECT relname FROM pg_stat_sys_indexes WHERE relname = 'pg_constraint')"
	case 12:
		// ERROR:  missing FROM-clause entry for table "???" at character ???
		q = fmt.Sprintf("SELECT st.entity_id, s.name, s.size_b, s.created_at FROM %s s WHERE entity_id = $1", table)
		args = []interface{}{num1}
	case 13:
		// ERROR:  NUMERIC scale 2 must be between 0 and precision 1 at character ???
		q = fmt.Sprintf("SELECT entity_id, name, (size_b / 8192)::numeric(1,2) AS size_t, created_at FROM %s WHERE entity_id = $1", table)
		args = []interface{}{num1}
	case 14:
		// ERROR:  COALESCE types date and bigint cannot be matched at character ???
		q = fmt.Sprintf("SELECT entity_id, name, size_b, coalesce(created_at, 0) FROM %s WHERE entity_id = $1", table)
		args = []interface{}{num1}
	}

	return q, args
}
