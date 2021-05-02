package db

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// PostgresDB implements pgxpool.Pool as DB interface.
type PostgresDB struct {
	pool *pgxpool.Pool
}

// NewPostgresDB creates new database connections pool.
func NewPostgresDB(conninfo string) (DB, error) {
	pool, err := pgxpool.Connect(context.TODO(), conninfo)
	if err != nil {
		return nil, err
	}

	return &PostgresDB{
		pool: pool,
	}, nil
}

// Begin opens transaction in database and returns transaction object.
func (db *PostgresDB) Begin(ctx context.Context) (Tx, error) {
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &PostgresTx{
		tx: tx,
	}, nil
}

// Exec executes query expression and returns resulting tag.
func (db *PostgresDB) Exec(ctx context.Context, sql string, arguments ...interface{}) (int64, string, error) {
	tag, err := db.pool.Exec(ctx, sql, arguments...)
	if err != nil {
		return 0, "", err
	}

	return tag.RowsAffected(), tag.String(), nil
}

// Query executes query expression and returns resulting Rows.
func (db *PostgresDB) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return db.pool.Query(ctx, sql, args)
}

// Close closes database connections pool.
func (db *PostgresDB) Close() {
	return
}

/* Transaction wrapper */

type Tx interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...interface{}) (int64, string, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
}

// PostgresTx implements PostgreSQL transaction object.
type PostgresTx struct {
	tx pgx.Tx
}

// Commit does transaction commit.
func (tx *PostgresTx) Commit(ctx context.Context) error {
	return tx.tx.Commit(ctx)
}

// Rollback does transaction rollback.
func (tx *PostgresTx) Rollback(ctx context.Context) error {
	return tx.tx.Rollback(ctx)
}

// Exec executes query expression inside the transaction and returns resulting tag.
func (tx *PostgresTx) Exec(ctx context.Context, sql string, arguments ...interface{}) (int64, string, error) {
	tag, err := tx.tx.Exec(ctx, sql, arguments...)
	if err != nil {
		return 0, "", err
	}

	return tag.RowsAffected(), tag.String(), nil
}

// Query executes query expression inside the transaction and returns resulting Rows.
func (tx *PostgresTx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return tx.tx.Query(ctx, sql, args)
}