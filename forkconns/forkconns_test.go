package forkconns

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
		{valid: true, config: Config{Rate: 1, Jobs: 1}},
		{valid: false, config: Config{Rate: 0, Jobs: 1}},
		{valid: false, config: Config{Rate: 1, Jobs: 0}},
		{valid: false, config: Config{}},
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
		Conninfo: db.TestConninfo,
		Rate:     2,
		Jobs:     2,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("error"))
	assert.NoError(t, err)
	err = w.Run(ctx)
	assert.Nil(t, err)
}

func Test_makeConnectionLoop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := makeConnectionLoop(ctx, db.TestConninfo, 2)
	assert.NoError(t, err)
}
