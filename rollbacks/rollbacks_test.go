package rollbacks

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
		{valid: true, config: Config{Jobs: 1, Rate: 1}},
		{valid: false, config: Config{Jobs: 0, Rate: 1}},
		{valid: false, config: Config{Jobs: 1, Rate: 0}},
	}

	for _, tc := range testcases {
		if tc.valid {
			assert.NoError(t, tc.config.validate())
		} else {
			assert.Error(t, tc.config.validate())
		}
	}
}

func TestNewWorkload(t *testing.T) {
	testcases := []struct {
		valid bool
		cfg   Config
	}{
		{valid: true, cfg: Config{Jobs: 1, Rate: 1}},
		{valid: false, cfg: Config{Jobs: 1, Rate: 0}},
	}

	for _, tc := range testcases {
		w, err := NewWorkload(tc.cfg, log.NewDefaultLogger("info"))
		if tc.valid {
			assert.NoError(t, err)
			assert.NotNil(t, w)
		} else {
			assert.Error(t, err)
		}
	}
}

func TestWorkload_Run(t *testing.T) {
	config := Config{Conninfo: db.TestConninfo, Jobs: 2, Rate: 2}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("info"))
	assert.NoError(t, err)
	err = w.Run(ctx)
	assert.Nil(t, err)
}

func Test_runWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	assert.NoError(t, runWorker(ctx, log.NewDefaultLogger("error"), Config{Rate: 2, Conninfo: db.TestConninfo}))
}

func Test_startLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	conn, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)

	c, r, err := startLoop(ctx, conn, 2)
	assert.NoError(t, err)
	assert.Equal(t, 0, c) // expecting no commits
	assert.Equal(t, 2, r) // expecting 2 rollbacks (rate 2, duration 1 second)
}

func Test_createTempTable(t *testing.T) {
	conn, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)

	tbl, err := createTempTable(context.Background(), conn)
	assert.NoError(t, err)
	assert.Greater(t, len(tbl), 0)

	assert.NoError(t, conn.Close())
}

func Test_newErrQuery(t *testing.T) {
	for i := 0; i < 1000; i++ {
		q, _ := newErrQuery("test")
		assert.Greater(t, len(q), 0)
	}
}
