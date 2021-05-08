package idlexacts

import (
	"context"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestConfig_validate(t *testing.T) {
	testcases := []struct {
		valid  bool
		config Config
	}{
		{valid: true, config: Config{Jobs: 1, IdleXactsNaptimeMin: 5, IdleXactsNaptimeMax: 10}},
		{valid: false, config: Config{Jobs: 0}},
		{valid: false, config: Config{Jobs: 1, IdleXactsNaptimeMin: 5, IdleXactsNaptimeMax: 4}},
		{valid: false, config: Config{Jobs: 1, IdleXactsNaptimeMin: 0, IdleXactsNaptimeMax: 0}},
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
	config := &Config{
		PostgresConninfo:    db.TestConninfo,
		Jobs:                2,
		IdleXactsNaptimeMin: 1,
		IdleXactsNaptimeMax: 2,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w, err := NewWorkload(config)
	assert.NoError(t, err)
	err = w.Run(ctx)
	assert.NoError(t, err)

	assert.NoError(t, noisia.Cleanup(context.Background(), config.PostgresConninfo))

	// Connect to invalid DB
	config.PostgresConninfo = "database=noisia_invalid"
	err = w.Run(ctx)
	assert.Error(t, err)
}

func Test_startLoop(t *testing.T) {
	pool, err := db.NewTestDB()
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	assert.NoError(t, startLoop(ctx, pool, []string{""}, 2, 1, 2))
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

func Test_startSingleIdleXact(t *testing.T) {
	pool, err := db.NewTestDB()
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	startSingleIdleXact(ctx, pool, "", 10*time.Millisecond)
}
