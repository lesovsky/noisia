package waitxacts

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
		{valid: true, config: Config{Jobs: 2, LocktimeMin: 5 * time.Second, LocktimeMax: 10 * time.Second}},
		{valid: false, config: Config{Jobs: 1}},
		{valid: false, config: Config{Jobs: 2, LocktimeMin: 5 * time.Second, LocktimeMax: 4 * time.Second}},
		{valid: false, config: Config{Jobs: 2, LocktimeMin: 5 * time.Second, LocktimeMax: 0}},
		{valid: false, config: Config{Jobs: 2, LocktimeMin: 0, LocktimeMax: 5 * time.Second}},
		{valid: false, config: Config{Jobs: 2, LocktimeMin: 0, LocktimeMax: 0}},
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
		Conninfo:    db.TestConninfo,
		Fixture:     true,
		Jobs:        2,
		LocktimeMin: 100 * time.Millisecond,
		LocktimeMax: 200 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger())
	assert.NoError(t, err)
	err = w.Run(ctx)
	assert.Nil(t, err)
}

func Test_startLoop(t *testing.T) {
	pool, err := db.NewTestDB()
	assert.NoError(t, err)
	defer pool.Close()

	_, _, err = pool.Exec(context.Background(), "CREATE TABLE noisia_test_1 (a int)")
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	assert.NoError(t, startLoop(ctx, log.NewDefaultLogger(), pool, []string{"noisia_test_1"}, 2, 10*time.Millisecond, 50*time.Millisecond))

	_, _, err = pool.Exec(context.Background(), "DROP TABLE noisia_test_1")
	assert.NoError(t, err)
}

func Test_lockTable(t *testing.T) {
	pool, err := db.NewTestDB()
	assert.NoError(t, err)

	_, _, err = pool.Exec(context.Background(), "CREATE TABLE noisia_test_2 (a int)")
	assert.NoError(t, err)

	assert.NoError(t, lockTable(context.Background(), pool, "noisia_test_2", 10*time.Millisecond))

	_, _, err = pool.Exec(context.Background(), "DROP TABLE noisia_test_2")
	assert.NoError(t, err)
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
