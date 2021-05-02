package targeting

import (
	"context"
	"github.com/lesovsky/noisia/db"
)

// TopWriteTables returns tables with the most of tuples updated/deleted.
func TopWriteTables(db db.DB, n int) ([]string, error) {
	q := "SELECT schemaname ||'.'|| relname FROM pg_stat_user_tables " +
		"WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') " +
		"ORDER BY (n_tup_upd + n_tup_del) DESC LIMIT $1"
	rows, err := db.Query(context.Background(), q, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]string, 0, n)
	for rows.Next() {
		var t string

		err = rows.Scan(&t)
		if err != nil {
			return nil, err
		}

		tables = append(tables, t)
	}

	return tables, nil
}
