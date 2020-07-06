package noisia

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
)

type Workload interface {
	Run(context.Context) error
}

func Cleanup(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}

	var tables = []string{"_noisia_deadlocks_workload", "_noisia_waitxacts_workload", "_noisia_tempfiles_workload"}
	for _, t := range tables {
		_, err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", t))
		if err != nil {
			return err
		}
	}
	return nil
}
