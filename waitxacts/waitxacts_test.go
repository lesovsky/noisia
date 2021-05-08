package waitxacts

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
		Jobs:        2,
		LocktimeMin: 1,
		LocktimeMax: 2,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w, err := NewWorkload(config)
	assert.NoError(t, err)
	err = w.Run(ctx)
	assert.Nil(t, err)

	assert.NoError(t, noisia.Cleanup(context.Background(), config.Conninfo))
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
