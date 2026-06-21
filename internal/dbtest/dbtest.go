// Package dbtest provides a throwaway PostgreSQL container for integration tests.
// It is imported only from *_test.go files (via each package's TestMain), so its
// heavy testcontainers/docker dependencies never end up in the noisia binary.
package dbtest

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lesovsky/noisia/db"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// RunMain starts a throwaway PostgreSQL container, points db.TestConninfo at it,
// runs the package tests and removes the container afterwards. Each test package
// calls it from TestMain, so every package gets its own isolated database — which
// matters because the workloads mutate server-wide state (pg_stat_*, temp files).
//
// Usage in a package's TestMain:
//
//	func TestMain(m *testing.M) { os.Exit(dbtest.RunMain(m)) }
func RunMain(m *testing.M) int {
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:15-alpine",
		postgres.WithDatabase("noisia_fixtures"),
		postgres.WithUsername("noisia"),
		postgres.WithPassword("noisia"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dbtest: failed to start postgres container: %s\n", err)
		return 1
	}
	defer func() {
		if err := container.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "dbtest: failed to terminate postgres container: %s\n", err)
		}
	}()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintf(os.Stderr, "dbtest: failed to get connection string: %s\n", err)
		return 1
	}
	db.TestConninfo = dsn

	return m.Run()
}
