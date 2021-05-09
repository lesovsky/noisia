package db

import (
	"context"
)

// TestConninfo defines connection string to test database.
const TestConninfo = "host=postgres user=noisia database=noisia_fixtures"

// NewTestDB creates connection for test database.
func NewTestDB() (DB, error) {
	return NewPostgresDB(context.Background(), TestConninfo)
}
