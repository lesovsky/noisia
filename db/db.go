package db

import (
	"context"
	"github.com/jackc/pgx/v4"
)

/* Database connection wrapper */

type DB interface {
	Begin(ctx context.Context) (Tx, error)
	Exec(ctx context.Context, sql string, arguments ...interface{}) (int64, string, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	Close()
}
