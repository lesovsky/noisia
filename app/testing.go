package app

//import (
//  "context"
//  "fmt"
//  "github.com/jackc/pgx/v4/pgxpool"
//  "github.com/stretchr/testify/assert"
//  "testing"
//)
//
//func TestDBPool(t *testing.T) (*pgxpool.Pool, func()) {
//  pool, err := pgxpool.Connect(context.Background(), "host=127.0.0.1")
//  assert.NoError(t, err)
//  assert.NotNil(t, pool)
//
//  return pool, func() {
//    tables := "noisia_deadlocks_workload, noisia_wait_xact_workload, noisia_temp_files_workload"
//    if _, err := pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", tables)); err != nil {
//      t.Fatal(err)
//    }
//    pool.Close()
//  }
//}
