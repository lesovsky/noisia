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

func Test_createTempTable(t *testing.T) {
	conn, err := db.Connect(context.Background(), db.TestConninfo)
	assert.NoError(t, err)

	tbl, err := createTempTable(context.Background(), conn)
	assert.NoError(t, err)
	assert.Greater(t, len(tbl), 0)
}

func Test_newErrQuery(t *testing.T) {
	for i := -1; i < 16; i++ {
		q, _ := newErrQuery(i, "test")
		assert.Greater(t, len(q), 0)
	}
}
