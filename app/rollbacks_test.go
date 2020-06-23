package app

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_runRollbacksWorkload(t *testing.T) {
	config := &Config{
		PostgresConninfo: "host=127.0.0.1",
		Jobs:             2,
		Rollbacks:        true,
		RollbacksRate:    2,
	}
	assert.NoError(t, config.Validate())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := runRollbacksWorkload(ctx, config)
	assert.Nil(t, err)
}
