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
		{config: Config{PostgresConninfo: "127.0.0.1", IdleXacts: true, IdleXactsNaptimeMin: 10, IdleXactsNaptimeMax: 20}, valid: true},
		{config: Config{PostgresConninfo: "127.0.0.1", WaitXacts: true, Jobs: 2, WaitXactsLocktimeMin: 10, WaitXactsLocktimeMax: 20}, valid: true},
		{config: Config{}, valid: false},
		{config: Config{PostgresConninfo: "127.0.0.1", IdleXacts: true, IdleXactsNaptimeMin: 0, IdleXactsNaptimeMax: 10}, valid: false},
		{config: Config{PostgresConninfo: "127.0.0.1", IdleXacts: true, IdleXactsNaptimeMin: 10, IdleXactsNaptimeMax: 5}, valid: false},
		{config: Config{PostgresConninfo: "127.0.0.1", WaitXacts: true, WaitXactsLocktimeMin: 0, WaitXactsLocktimeMax: 10}, valid: false},
		{config: Config{PostgresConninfo: "127.0.0.1", WaitXacts: true, WaitXactsLocktimeMin: 10, WaitXactsLocktimeMax: 5}, valid: false},
		{config: Config{PostgresConninfo: "127.0.0.1", WaitXacts: true, WaitXactsLocktimeMin: 10, WaitXactsLocktimeMax: 5}, valid: false},
		{config: Config{PostgresConninfo: "127.0.0.1", WaitXacts: true, Jobs: 1, WaitXactsLocktimeMin: 10, WaitXactsLocktimeMax: 20}, valid: false},
	}
	for _, tc := range testcases {
		if tc.valid {
			assert.NoError(t, tc.config.Validate())
		} else {
			assert.Error(t, tc.config.Validate())
		}
	}
}
