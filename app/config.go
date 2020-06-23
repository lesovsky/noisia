package app

import (
	"errors"
)

type Config struct {
	PostgresConninfo    string
	Jobs                uint16 // max 65535
	IdleXacts           bool
	IdleXactsNaptimeMin int
	IdleXactsNaptimeMax int
	Rollbacks           bool
	RollbacksRate       int
}

func (c *Config) Validate() error {
	if c.PostgresConninfo == "" {
		return errors.New("'conninfo' is not specified")
	}

	if c.IdleXactsNaptimeMin < 1 || c.IdleXactsNaptimeMin > c.IdleXactsNaptimeMax {
		return errors.New("wrong 'idle-xact.naptime-min' or 'idle-xact.naptime-max' specified")
	}

	return nil
}
