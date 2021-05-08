package rollbacks

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
		{valid: true, config: Config{Jobs: 1, MinRate: 5, MaxRate: 10}},
		{valid: false, config: Config{Jobs: 0}},
		{valid: false, config: Config{Jobs: 1, MinRate: 5, MaxRate: 4}},
		{valid: false, config: Config{Jobs: 1, MinRate: 0, MaxRate: 0}},
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
	config := &Config{PostgresConninfo: db.TestConninfo, Jobs: 2, MinRate: 5, MaxRate: 10}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w, err := NewWorkload(config)
	assert.NoError(t, err)
	err = w.Run(ctx)
	assert.Nil(t, err)

	assert.NoError(t, noisia.Cleanup(context.Background(), config.PostgresConninfo))
}

func Test_createTempTable(t *testing.T) {
	conn, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)

	tbl, err := createTempTable(context.Background(), conn)
	assert.NoError(t, err)
	assert.Greater(t, len(tbl), 0)
}

func Test_newErrQuery(t *testing.T) {
	for i := -1; i < 16; i++ {
		q, _ := newErrQuery(i, "test")
		assert.Greater(t, len(q), 0)
	}
}
