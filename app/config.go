package app

import (
	"errors"
)

type Config struct {
	PostgresConninfo     string
	Jobs                 uint16 // max 65535
	IdleXacts            bool
	IdleXactsNaptimeMin  int
	IdleXactsNaptimeMax  int
	Rollbacks            bool
	RollbacksRate        int
	WaitXacts            bool
	WaitXactsLocktimeMin int
	WaitXactsLocktimeMax int
	Deadlocks            bool
}

func (c *Config) Validate() error {
	if c.PostgresConninfo == "" {
		return errors.New("'conninfo' is not specified")
	}

	if c.IdleXacts && (c.IdleXactsNaptimeMin < 1 || c.IdleXactsNaptimeMin > c.IdleXactsNaptimeMax) {
		return errors.New("wrong 'idle-xact.naptime-min' or 'idle-xact.naptime-max' specified")
	}

	if c.WaitXacts && (c.WaitXactsLocktimeMin < 1 || c.WaitXactsLocktimeMin > c.WaitXactsLocktimeMax) {
		return errors.New("wrong 'wait-xact.locktime-min' or 'wait-xact.locktime-max' specified")
	}

	if (c.WaitXacts || c.Deadlocks) && c.Jobs < 2 {
		return errors.New("insufficient 'jobs' for this workload")
	}

	return nil
}
