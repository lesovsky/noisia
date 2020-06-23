package app

import (
	"errors"
)

type Config struct {
	PostgresConninfo   string
	Jobs               uint16 // max 65535
	IdleXact           bool
	IdleXactNaptimeMin int
	IdleXactNaptimeMax int
	Rollbacks          bool
	RollbacksRate      int
}

func (c *Config) Validate() error {
	if c.PostgresConninfo == "" {
		return errors.New("'conninfo' is not specified")
	}

	if c.IdleXactNaptimeMin < 1 || c.IdleXactNaptimeMin > c.IdleXactNaptimeMax {
		return errors.New("wrong 'idle-xact.naptime-min' or 'idle-xact.naptime-max' specified")
	}

	return nil
}
