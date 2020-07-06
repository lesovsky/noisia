package tempfiles

import (
	"context"
	"github.com/lesovsky/noisia"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestWorkload_Run(t *testing.T) {
	config := &Config{
		PostgresConninfo:     "host=127.0.0.1",
		Jobs:                 2,
		TempFilesRate:        2,
		TempFilesScaleFactor: 10,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w := NewWorkload(config)
	err := w.Run(ctx)
	assert.Nil(t, err)

	assert.NoError(t, noisia.Cleanup(context.Background(), config.PostgresConninfo))
}
