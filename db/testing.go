package db

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
)

const TestConninfo = "host=postgres user=noisia database=noisia_fixtures"

func NewTestDB() (DB, error) {
	pool, err := pgxpool.Connect(context.Background(), TestConninfo)
	if err != nil {
		return nil, err
	}

	return &PostgresDB{
		pool: pool,
	}, nil
}
