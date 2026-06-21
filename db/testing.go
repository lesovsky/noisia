package db

import (
	"context"
)

// TestConninfo defines connection string to the test database. It is populated
// at test time by internal/dbtest.RunMain (from each package's TestMain) and
// points to a throwaway PostgreSQL container.
var TestConninfo string

// NewTestDB creates connection for test database.
func NewTestDB() (DB, error) {
	return NewPostgresDB(context.Background(), TestConninfo)
}
