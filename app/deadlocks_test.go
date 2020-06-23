package app

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_runDeadlocksWorkload(t *testing.T) {
	config := &Config{
		PostgresConninfo: "host=127.0.0.1",
		Jobs:             2,
		Deadlocks:        true,
	}
	assert.NoError(t, config.Validate())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := runDeadlocksWorkload(ctx, config)
	assert.NoError(t, err)
}
