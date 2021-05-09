package idlexacts

import (
	"context"
	"github.com/lesovsky/noisia/db"
	"github.com/lesovsky/noisia/log"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestConfig_validate(t *testing.T) {
	testcases := []struct {
		valid  bool
		config Config
	}{
		{valid: true, config: Config{Jobs: 1, NaptimeMin: 5 * time.Second, NaptimeMax: 10 * time.Second}},
		{valid: true, config: Config{Jobs: 1, NaptimeMin: 5 * time.Second, NaptimeMax: 5 * time.Second}},
		{valid: false, config: Config{Jobs: 0}},
		{valid: false, config: Config{Jobs: 1, NaptimeMin: 5 * time.Second, NaptimeMax: 4 * time.Second}},
		{valid: false, config: Config{Jobs: 1, NaptimeMin: 5 * time.Second, NaptimeMax: 0}},
		{valid: false, config: Config{Jobs: 1, NaptimeMin: 0, NaptimeMax: 5 * time.Second}},
		{valid: false, config: Config{Jobs: 1, NaptimeMin: 0, NaptimeMax: 0}},
	}

	for _, tc := range testcases {
		if tc.valid {
			assert.NoError(t, tc.config.validate())
		} else {
			assert.Error(t, tc.config.validate())
		}
	}
}

func TestWorkload_Run(t *testing.T) {
	config := Config{
		Conninfo:   db.TestConninfo,
		Jobs:       2,
		NaptimeMin: 1 * time.Second,
		NaptimeMax: 2 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger())
	assert.NoError(t, err)
	err = w.Run(ctx)
	assert.NoError(t, err)

	// Connect to invalid DB
	config.Conninfo = "database=noisia_invalid"
	err = w.Run(ctx)
	assert.Error(t, err)
}

func Test_startLoop(t *testing.T) {
	pool, err := db.NewTestDB()
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	assert.NoError(t, startLoop(ctx, log.NewDefaultLogger(), pool, []string{""}, 2, 1, 2))
}

func Test_startSingleIdleXact(t *testing.T) {
	pool, err := db.NewTestDB()
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	assert.NoError(t, startSingleIdleXact(ctx, pool, "pg_class", 10*time.Millisecond))
	assert.NoError(t, startSingleIdleXact(ctx, pool, "", 10*time.Millisecond))
}

func Test_selectRandomTable(t *testing.T) {
	testcases := []struct {
		tables []string
		want   int
	}{
		{tables: []string{"test.test1", "test.test2", "test.test3"}, want: 10},
		{tables: []string{}, want: 0},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, len(selectRandomTable(tc.tables)))
	}
}

func Test_createTempTable(t *testing.T) {
	pool, err := db.NewTestDB()
	assert.NoError(t, err)

	tx, err := pool.Begin(context.Background())
	assert.NoError(t, err)

	assert.NoError(t, createTempTable(tx, "pg_class"))

	assert.NoError(t, tx.Rollback(context.Background()))
}
