package app

import (
	"context"
	"github.com/lesovsky/noisia/app/internal/log"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_runWaitXactsWorkload(t *testing.T) {
	config := &Config{
		PostgresConninfo:     "host=127.0.0.1",
		Jobs:                 2,
		WaitXacts:            true,
		WaitXactsLocktimeMin: 1,
		WaitXactsLocktimeMax: 2,
	}
	assert.NoError(t, config.Validate())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := runWaitXactsWorkload(ctx, config)
	log.Errorln(err)
	assert.Nil(t, err)
}
