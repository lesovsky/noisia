// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package walflood

import (
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfig_validate(t *testing.T) {
	base := Config{
		Conninfo:       "host=127.0.0.1 dbname=postgres",
		Rate:           0,
		Rows:           1000,
		PayloadBytes:   8192,
		ReportInterval: time.Second,
		Jobs:           4,
	}

	assert.NoError(t, base.validate(), "valid baseline must pass")

	tests := []struct {
		name string
		mut  func(c *Config)
	}{
		{"empty conninfo", func(c *Config) { c.Conninfo = "" }},
		{"negative rate", func(c *Config) { c.Rate = -1 }},
		{"zero rows", func(c *Config) { c.Rows = 0 }},
		{"zero payload bytes", func(c *Config) { c.PayloadBytes = 0 }},
		{"zero report interval", func(c *Config) { c.ReportInterval = 0 }},
		{"negative report interval", func(c *Config) { c.ReportInterval = -time.Second }},
		{"zero jobs", func(c *Config) { c.Jobs = 0 }},
		{"rows less than jobs", func(c *Config) { c.Rows = 2; c.Jobs = 3 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := base
			tt.mut(&c)
			assert.Error(t, c.validate(), "invalid config must be rejected")
		})
	}
}

func Test_partition(t *testing.T) {
	cases := []struct {
		rows int
		jobs uint16
	}{
		{1000, 4}, // even split
		{10, 3},   // rows % jobs != 0 (remainder spread into tail ranges)
		{5, 5},    // rows == jobs (one id per worker)
		{100, 1},  // single worker owns the whole range
		{7, 2},    // odd remainder
	}

	for _, c := range cases {
		ranges := partition(c.rows, c.jobs)

		assert.Len(t, ranges, int(c.jobs), "must return exactly Jobs ranges")
		assert.Equal(t, 1, ranges[0][0], "coverage must start at id 1")
		assert.Equal(t, c.rows, ranges[len(ranges)-1][1], "coverage must end at id Rows")

		for i, r := range ranges {
			assert.LessOrEqual(t, r[0], r[1], "range must be non-empty (lo <= hi)")
			if i > 0 {
				assert.Equal(t, ranges[i-1][1]+1, r[0],
					"ranges must be contiguous with no gap or overlap")
			}
		}
	}
}

func Test_randomSuffix(t *testing.T) {
	re := regexp.MustCompile(`^[a-z0-9]+$`)
	s := randomSuffix(8)

	assert.Len(t, s, 8, "suffix must have the requested length")
	assert.Regexp(t, re, s, "suffix must be an injection-safe identifier")
}

func Test_sanitize(t *testing.T) {
	testcases := []struct {
		name       string
		err        error
		suppressed bool
		want       string // expected output for the non-suppressed (pass-through) cases
	}{
		{name: "password token", err: fmt.Errorf("failed: password=SENTINEL_SECRET host=db"), suppressed: true},
		{name: "host token", err: fmt.Errorf("dial error host=SENTINEL_SECRET"), suppressed: true},
		{name: "user token", err: fmt.Errorf("auth failed user=SENTINEL_SECRET"), suppressed: true},
		{name: "dbname token", err: fmt.Errorf("connect failed dbname=SENTINEL_SECRET"), suppressed: true},
		{name: "database token", err: fmt.Errorf("dial error database=SENTINEL_SECRET"), suppressed: true},
		{name: "sslmode token", err: fmt.Errorf("tls error sslmode=SENTINEL_SECRET"), suppressed: true},
		{name: "url form", err: fmt.Errorf("failed: postgres://user:SENTINEL_SECRET@db/noisia"), suppressed: true},
		// seed/cleanup error paths: a wrapped DSN fragment must still collapse.
		{name: "seed error", err: fmt.Errorf("seed table failed: dial tcp host=SENTINEL_SECRET refused"), suppressed: true},
		{name: "cleanup drop error", err: fmt.Errorf("DROP TABLE failed: password=SENTINEL_SECRET timeout"), suppressed: true},
		{name: "benign", err: fmt.Errorf("server closed the connection unexpectedly"), suppressed: false, want: "server closed the connection unexpectedly"},
		{name: "nil", err: nil, suppressed: false, want: ""},
	}

	for _, tc := range testcases {
		got := sanitize(tc.err)
		assert.NotContains(t, got, "SENTINEL_SECRET", tc.name)
		if tc.suppressed {
			assert.Equal(t, "connection error (details suppressed)", got, tc.name)
		} else {
			assert.Equal(t, tc.want, got, tc.name)
		}
	}
}

func Test_formatBytes(t *testing.T) {
	testcases := []struct {
		n    int64
		want string
	}{
		{n: 0, want: "0B"},                      // zero
		{n: 42, want: "42B"},                    // within B range
		{n: 1023, want: "1023B"},                // just below the KB boundary
		{n: 1024, want: "1.0KB"},                // exactly the KB boundary
		{n: 512 * 1024, want: "512.0KB"},        // within KB range
		{n: 1024 * 1024, want: "1.0MB"},         // exactly the MB boundary
		{n: 180 * 1024 * 1024, want: "180.0MB"}, // within MB range
		{n: 1024 * 1024 * 1024, want: "1.0GB"},  // exactly the GB boundary
		{n: 4509715660, want: "4.2GB"},          // within GB range
	}

	for _, tc := range testcases {
		assert.Equal(t, tc.want, formatBytes(tc.n))
	}
}
