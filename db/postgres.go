package db

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

/* Database connections pool implementation */

// PostgresDB implements pgxpool.Pool as DB interface.
type PostgresDB struct {
	pool *pgxpool.Pool
}

// NewPostgresDB creates new database connections pool.
func NewPostgresDB(ctx context.Context, conninfo string) (DB, error) {
	pool, err := pgxpool.Connect(ctx, conninfo)
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
func (db *PostgresDB) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
	tag, err := db.pool.Exec(ctx, sql, args...)
	if err != nil {
		return 0, "", err
	}

	return tag.RowsAffected(), tag.String(), nil
}

// Query executes query expression and returns resulting Rows.
func (db *PostgresDB) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return db.pool.Query(ctx, sql, args...)
}

// Close closes database connections pool.
func (db *PostgresDB) Close() {
	return
}

/* Transaction implementation */

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
func (tx *PostgresTx) Exec(ctx context.Context, sql string, args ...interface{}) (int64, string, error) {
	tag, err := tx.tx.Exec(ctx, sql, args...)
	if err != nil {
		return 0, "", err
	}

	return tag.RowsAffected(), tag.String(), nil
}

// Query executes query expression inside the transaction and returns resulting Rows.
func (tx *PostgresTx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return tx.tx.Query(ctx, sql, args...)
}

/* Connection implementation */

// PostgresConn wraps *pgx.Conn.
type PostgresConn struct {
	conn *pgx.Conn
}

// Connect accepts connection string and create new connection.
func Connect(ctx context.Context, connString string) (Conn, error) {
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, err
	}

	return &PostgresConn{
		conn: conn,
	}, nil
}

func (c *PostgresConn) Close() error {
	return c.conn.Close(context.Background())
}
