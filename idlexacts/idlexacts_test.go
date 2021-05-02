package idlexacts

import (
	"context"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestWorkload_Run(t *testing.T) {
	config := &Config{
		PostgresConninfo:    db.TestConninfo,
		Jobs:                2,
		IdleXactsNaptimeMin: 1,
		IdleXactsNaptimeMax: 2,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w := NewWorkload(config)
	err := w.Run(ctx)
	assert.NoError(t, err)

	assert.NoError(t, noisia.Cleanup(context.Background(), config.PostgresConninfo))

	// Connect to invalid DB
	config.PostgresConninfo = "database=noisia_invalid"
	err = w.Run(ctx)
	assert.Error(t, err)
}

func Test_startLoop(t *testing.T) {
	pool, err := db.NewTestDB()
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	assert.NoError(t, startLoop(ctx, pool, 2, 1, 2))
}

func Test_startSingleIdleXact(t *testing.T) {
	pool, err := db.NewTestDB()
	assert.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	startSingleIdleXact(ctx, pool, 10*time.Millisecond)
}
