package targeting

import (
	"github.com/lesovsky/noisia/db"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTopWriteTables(t *testing.T) {
	pool, err := db.NewTestDB()
	assert.NoError(t, err)

	got, err := TopWriteTables(pool, 5)
	assert.NoError(t, err)
	assert.NotNil(t, got)
}
