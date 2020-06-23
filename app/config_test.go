package app

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConfig_Validate(t *testing.T) {
	testcases := []struct {
		config Config
		valid  bool
	}{
		{config: Config{PostgresConninfo: "127.0.0.1", IdleXactNaptimeMin: 10, IdleXactNaptimeMax: 20}, valid: true},
		{config: Config{}, valid: false},
		{config: Config{PostgresConninfo: "127.0.0.1", IdleXactNaptimeMin: 0, IdleXactNaptimeMax: 10}, valid: false},
		{config: Config{PostgresConninfo: "127.0.0.1", IdleXactNaptimeMin: 10, IdleXactNaptimeMax: 5}, valid: false},
	}
	for _, tc := range testcases {
		if tc.valid {
			assert.NoError(t, tc.config.Validate())
		} else {
			assert.Error(t, tc.config.Validate())
		}
	}
}
