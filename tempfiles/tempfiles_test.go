package tempfiles

import (
	"context"
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
		{valid: true, config: Config{Jobs: 1, Rate: 5, ScaleFactor: 5}},
		{valid: false, config: Config{Jobs: 0}},
		{valid: false, config: Config{Jobs: 1, Rate: 0, ScaleFactor: 5}},
		{valid: false, config: Config{Jobs: 1, Rate: 5, ScaleFactor: 0}},
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
		Jobs:        2,
		Rate:        2,
		ScaleFactor: 10,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w, err := NewWorkload(config)
	assert.NoError(t, err)
	err = w.Run(ctx)
	assert.Nil(t, err)
}
