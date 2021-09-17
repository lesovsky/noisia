package tempfiles

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
		w, err := NewWorkload(tc.cfg, log.NewDefaultLogger("error"))
		if tc.valid {
			assert.NoError(t, err)
			assert.NotNil(t, w)
		} else {
			assert.Error(t, err)
		}
	}
}

func TestWorkload_Run(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w, err := NewWorkload(
		Config{Conninfo: db.TestConninfo, Jobs: 2, Rate: 2},
		log.NewDefaultLogger("error"),
	)
	assert.NoError(t, err)
	err = w.Run(ctx)
	assert.Nil(t, err)
}

func Test_runWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := runWorker(ctx, log.NewDefaultLogger("error"), Config{Rate: 1, Conninfo: db.TestConninfo})
	assert.NoError(t, err)
}

func Test_startLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pool, err := db.NewTestDB()
	assert.NoError(t, err)

	err = startLoop(ctx, pool, log.NewDefaultLogger("error"), 2)
	assert.NoError(t, err)
}

func Test_execQuery(t *testing.T) {
	pool, err := db.NewTestDB()
	assert.NoError(t, err)

	err = execQuery(context.Background(), pool)
	assert.NoError(t, err)
}

func Test_countTempBytes(t *testing.T) {
	bytes, err := countTempBytes(db.TestConninfo)
	assert.NoError(t, err)
	assert.Greater(t, bytes, -1)
}
