package terminate

import (
	"context"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/db"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestWorkload_Run(t *testing.T) {
	config := &Config{
		PostgresConninfo:     db.TestConninfo,
		TerminateRate:        4,
		IgnoreSystemBackends: true,
		SoftMode:             false,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w := NewWorkload(config)
	err := w.Run(ctx)
	assert.Nil(t, err)

	assert.NoError(t, noisia.Cleanup(context.Background(), config.PostgresConninfo))
}

func Test_buildQuery(t *testing.T) {
	testcases := []struct {
		config *Config
		want   string
	}{
		{config: &Config{SoftMode: false}, want: "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() ORDER BY random() LIMIT 1"},
		{config: &Config{SoftMode: true}, want: "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() ORDER BY random() LIMIT 1"},
		{config: &Config{SoftMode: true, IgnoreSystemBackends: true}, want: "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND backend_type = 'client backend' ORDER BY random() LIMIT 1"},
		{config: &Config{SoftMode: true, ClientAddr: "192.168"}, want: "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND client_addr::text ~ '192.168' ORDER BY random() LIMIT 1"},
		{config: &Config{SoftMode: true, User: "example"}, want: "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND usename ~ 'example' ORDER BY random() LIMIT 1"},
		{config: &Config{SoftMode: true, Database: "example"}, want: "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname ~ 'example' ORDER BY random() LIMIT 1"},
		{config: &Config{SoftMode: true, ApplicationName: "example"}, want: "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND application_name ~ 'example' ORDER BY random() LIMIT 1"},
		{config: &Config{SoftMode: true, ClientAddr: "192.168", User: "example", Database: "example", ApplicationName: "example"}, want: "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND client_addr::text ~ '192.168' AND usename ~ 'example' AND datname ~ 'example' AND application_name ~ 'example' ORDER BY random() LIMIT 1"},
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, buildQuery(tc.config))
	}
}
