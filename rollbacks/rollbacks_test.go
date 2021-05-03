package rollbacks

import (
	"context"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestWorkload_Run(t *testing.T) {
	config := &Config{PostgresConninfo: db.TestConninfo, Jobs: 2, MinRate: 5, MaxRate: 10}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w := NewWorkload(config)
	err := w.Run(ctx)
	assert.Nil(t, err)

	assert.NoError(t, noisia.Cleanup(context.Background(), config.PostgresConninfo))
}
