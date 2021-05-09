package main

import (
	"context"
	"github.com/lesovsky/noisia/deadlocks"
	"github.com/lesovsky/noisia/failconns"
	"github.com/lesovsky/noisia/idlexacts"
	"github.com/lesovsky/noisia/log"
	"github.com/lesovsky/noisia/rollbacks"
	"github.com/lesovsky/noisia/tempfiles"
	"github.com/lesovsky/noisia/terminate"
	"github.com/lesovsky/noisia/waitxacts"
	"sync"
	"time"
)

type config struct {
	logger                log.Logger
	postgresConninfo      string
	jobs                  uint16 // max 65535
	duration              time.Duration
	idleXacts             bool
	idleXactsNaptimeMin   time.Duration
	idleXactsNaptimeMax   time.Duration
	rollbacks             bool
	rollbacksMinRate      uint16
	rollbacksMaxRate      uint16
	waitXacts             bool
	waitXactsFixture      bool
	waitXactsLocktimeMin  time.Duration
	waitXactsLocktimeMax  time.Duration
	deadlocks             bool
	tempFiles             bool
	tempFilesRate         uint16
	tempFilesScaleFactor  uint16
	terminate             bool
	terminateInterval     time.Duration
	terminateRate         uint16
	terminateSoftMode     bool
	terminateIgnoreSystem bool
	terminateClientAddr   string
	terminateUser         string
	terminateDatabase     string
	terminateAppName      string
	failconns             bool
}

func runApplication(ctx context.Context, c config, log log.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, c.duration)
	defer cancel()

	var wg sync.WaitGroup

	if c.idleXacts {
		log.Info("start idle transactions workload")
		wg.Add(1)
		go func() {
			err := startIdleXactsWorkload(ctx, &wg, c, log)
			if err != nil {
				log.Errorf("idle transactions workload failed: %s", err)
			}
		}()
	}

	if c.rollbacks {
		log.Info("start rollbacks workload")
		wg.Add(1)
		go func() {
			err := startRollbacksWorkload(ctx, &wg, c, log)
			if err != nil {
				log.Errorf("rollbacks workload failed: %s", err)
			}
		}()
	}

	if c.waitXacts {
		log.Info("start wait xacts workload")
		wg.Add(1)
		go func() {
			err := startWaitxactsWorkload(ctx, &wg, c, log)
			if err != nil {
				log.Errorf("wait xacts workload failed: %s", err)
			}
		}()
	}

	if c.deadlocks {
		log.Info("start deadlocks workload")
		wg.Add(1)
		go func() {
			err := startDeadlocksWorkload(ctx, &wg, c, log)
			if err != nil {
				log.Errorf("deadlocks workload failed: %s", err)
			}
		}()
	}

	if c.tempFiles {
		log.Info("start temp files workload")
		wg.Add(1)
		go func() {
			err := startTempFilesWorkload(ctx, &wg, c, log)
			if err != nil {
				log.Errorf("temp files workload failed: %s", err)
			}
		}()
	}

	if c.terminate {
		log.Info("start terminate backends workload")
		wg.Add(1)
		go func() {
			err := startTerminateWorkload(ctx, &wg, c, log)
			if err != nil {
				log.Errorf("terminate backends workload failed: %s", err)
			}
		}()
	}

	if c.failconns {
		log.Info("start failconns backends workload")
		wg.Add(1)
		go func() {
			err := startFailconnsWorkload(ctx, &wg, c, log)
			if err != nil {
				log.Errorf("failconns backends workload failed: %s", err)
			}
		}()
	}

	wg.Wait()

	return nil
}

// startIdleXactsWorkload start generating workload with idle transactions.
func startIdleXactsWorkload(ctx context.Context, wg *sync.WaitGroup, c config, logger log.Logger) error {
	defer wg.Done()

	workload, err := idlexacts.NewWorkload(
		idlexacts.Config{
			Conninfo:   c.postgresConninfo,
			Jobs:       c.jobs,
			NaptimeMin: c.idleXactsNaptimeMin,
			NaptimeMax: c.idleXactsNaptimeMax,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startRollbacksWorkload(ctx context.Context, wg *sync.WaitGroup, c config, logger log.Logger) error {
	defer wg.Done()

	workload, err := rollbacks.NewWorkload(
		rollbacks.Config{
			Conninfo: c.postgresConninfo,
			Jobs:     c.jobs,
			MinRate:  c.rollbacksMinRate,
			MaxRate:  c.rollbacksMaxRate,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startWaitxactsWorkload(ctx context.Context, wg *sync.WaitGroup, c config, logger log.Logger) error {
	defer wg.Done()

	workload, err := waitxacts.NewWorkload(
		waitxacts.Config{
			Conninfo:    c.postgresConninfo,
			Jobs:        c.jobs,
			Fixture:     c.waitXactsFixture,
			LocktimeMin: c.waitXactsLocktimeMin,
			LocktimeMax: c.waitXactsLocktimeMax,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startDeadlocksWorkload(ctx context.Context, wg *sync.WaitGroup, c config, logger log.Logger) error {
	defer wg.Done()

	workload, err := deadlocks.NewWorkload(
		deadlocks.Config{
			Conninfo: c.postgresConninfo,
			Jobs:     c.jobs,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startTempFilesWorkload(ctx context.Context, wg *sync.WaitGroup, c config, logger log.Logger) error {
	defer wg.Done()

	workload, err := tempfiles.NewWorkload(
		tempfiles.Config{
			Conninfo:    c.postgresConninfo,
			Jobs:        c.jobs,
			Rate:        c.tempFilesRate,
			ScaleFactor: c.tempFilesScaleFactor,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startTerminateWorkload(ctx context.Context, wg *sync.WaitGroup, c config, logger log.Logger) error {
	defer wg.Done()

	workload, err := terminate.NewWorkload(
		terminate.Config{
			Conninfo:             c.postgresConninfo,
			Interval:             c.terminateInterval,
			Rate:                 c.terminateRate,
			SoftMode:             c.terminateSoftMode,
			IgnoreSystemBackends: c.terminateIgnoreSystem,
			ClientAddr:           c.terminateClientAddr,
			User:                 c.terminateUser,
			Database:             c.terminateDatabase,
			ApplicationName:      c.terminateAppName,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startFailconnsWorkload(ctx context.Context, wg *sync.WaitGroup, c config, logger log.Logger) error {
	defer wg.Done()

	workload, err := failconns.NewWorkload(
		failconns.Config{
			Conninfo: c.postgresConninfo,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}
