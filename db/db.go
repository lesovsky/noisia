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

type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...interface{}) (int64, string, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
}

type Conn interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (int64, string, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	Close() error
}
