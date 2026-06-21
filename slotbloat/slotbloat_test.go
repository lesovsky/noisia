// Copyright 2021 The Noisia Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package slotbloat

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfig_validate(t *testing.T) {
	testcases := []struct {
		valid  bool
		config Config
	}{
		// valid cases
		{valid: true, config: Config{Conninfo: "host=127.0.0.1", Rate: 0, Rows: 1, PayloadBytes: 1, ReportInterval: time.Second}},
		{valid: true, config: Config{Conninfo: "host=127.0.0.1", Rate: 5, Rows: 1000, PayloadBytes: 8192, ReportInterval: time.Second}},
		// invalid cases
		{valid: false, config: Config{Conninfo: "", Rate: 1, Rows: 1, PayloadBytes: 1, ReportInterval: time.Second}},                    // empty conninfo
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: -1, Rows: 1, PayloadBytes: 1, ReportInterval: time.Second}},     // negative rate
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: 1, Rows: 0, PayloadBytes: 1, ReportInterval: time.Second}},      // rows < 1
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: 1, Rows: 1, PayloadBytes: 0, ReportInterval: time.Second}},      // payload-bytes < 1
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: 1, Rows: 1, PayloadBytes: 1, ReportInterval: 0}},                // report-interval == 0
		{valid: false, config: Config{Conninfo: "host=127.0.0.1", Rate: 1, Rows: 1, PayloadBytes: 1, ReportInterval: -1 * time.Second}}, // report-interval < 0
	}

	for _, tc := range testcases {
		if tc.valid {
			assert.NoError(t, tc.config.validate())
		} else {
			assert.Error(t, tc.config.validate())
		}
	}
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
