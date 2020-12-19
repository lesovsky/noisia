package waitxacts

import (
	"context"
	"github.com/lesovsky/noisia"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestWorkload_Run(t *testing.T) {
	config := &Config{
		PostgresConninfo:     "host=postgres",
		Jobs:                 2,
		WaitXactsLocktimeMin: 1,
		WaitXactsLocktimeMax: 2,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w := NewWorkload(config)
	err := w.Run(ctx)
	assert.Nil(t, err)

	assert.NoError(t, noisia.Cleanup(context.Background(), config.PostgresConninfo))
}
