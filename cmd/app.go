package main

import (
	"context"
	"fmt"
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
	terminateInterval     uint16
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
		go startRollbacksWorkload(ctx, &wg, c)
	}

	if c.waitXacts {
		log.Info("start wait xacts workload")
		wg.Add(1)
		go startWaitxactsWorkload(ctx, &wg, c)
	}

	if c.deadlocks {
		log.Info("start deadlocks workload")
		wg.Add(1)
		go startDeadlocksWorkload(ctx, &wg, c)
	}

	if c.tempFiles {
		log.Info("start temp files workload")
		wg.Add(1)
		go startTempFilesWorkload(ctx, &wg, c)
	}

	if c.terminate {
		log.Info("start terminate backends workload")
		wg.Add(1)
		go startTerminateWorkload(ctx, &wg, c)
	}

	if c.failconns {
		log.Info("start failconns backends workload")
		wg.Add(1)
		go startFailconnsWorkload(ctx, &wg, c)
	}

	wg.Wait()

	return nil
}

// startIdleXactsWorkload start generating workload with idle transactions.
func startIdleXactsWorkload(ctx context.Context, wg *sync.WaitGroup, c config, log log.Logger) error {
	defer wg.Done()

	workload, err := idlexacts.NewWorkload(
		idlexacts.Config{
			Conninfo:   c.postgresConninfo,
			Jobs:       c.jobs,
			NaptimeMin: c.idleXactsNaptimeMin,
			NaptimeMax: c.idleXactsNaptimeMax,
		}, log,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startRollbacksWorkload(ctx context.Context, wg *sync.WaitGroup, c config) {
	defer wg.Done()

	workload, err := rollbacks.NewWorkload(rollbacks.Config{
		Conninfo: c.postgresConninfo,
		Jobs:     c.jobs,
		MinRate:  c.rollbacksMinRate,
		MaxRate:  c.rollbacksMaxRate,
	})
	if err != nil {
		fmt.Printf("rollbacks workload failed: %s\n", err)
		return
	}

	err = workload.Run(ctx)
	if err != nil {
		fmt.Printf("rollbacks workload failed: %s\n", err)
	}
}

func startWaitxactsWorkload(ctx context.Context, wg *sync.WaitGroup, c config) {
	defer wg.Done()

	workload, err := waitxacts.NewWorkload(waitxacts.Config{
		Conninfo:    c.postgresConninfo,
		Jobs:        c.jobs,
		Fixture:     c.waitXactsFixture,
		LocktimeMin: c.waitXactsLocktimeMin,
		LocktimeMax: c.waitXactsLocktimeMax,
	})
	if err != nil {
		fmt.Printf("waiting xacts workload failed: %s\n", err)
		return
	}

	err = workload.Run(ctx)
	if err != nil {
		fmt.Printf("waiting xacts workload failed: %s\n", err)
	}
}

func startDeadlocksWorkload(ctx context.Context, wg *sync.WaitGroup, c config) {
	defer wg.Done()

	workload, err := deadlocks.NewWorkload(deadlocks.Config{
		Conninfo: c.postgresConninfo,
		Jobs:     c.jobs,
	})
	if err != nil {
		fmt.Printf("deadlocks workload failed: %s\n", err)
		return
	}

	err = workload.Run(ctx)
	if err != nil {
		fmt.Printf("deadlocks workload failed: %s\n", err)
	}
}

func startTempFilesWorkload(ctx context.Context, wg *sync.WaitGroup, c config) {
	defer wg.Done()

	workload, err := tempfiles.NewWorkload(tempfiles.Config{
		Conninfo:    c.postgresConninfo,
		Jobs:        c.jobs,
		Rate:        c.tempFilesRate,
		ScaleFactor: c.tempFilesScaleFactor,
	})
	if err != nil {
		fmt.Printf("temp files workload failed: %s\n", err)
		return
	}

	err = workload.Run(ctx)
	if err != nil {
		fmt.Printf("temp files workload failed: %s\n", err)
	}
}

func startTerminateWorkload(ctx context.Context, wg *sync.WaitGroup, c config) {
	defer wg.Done()

	workload, err := terminate.NewWorkload(terminate.Config{
		Conninfo:             c.postgresConninfo,
		Interval:             c.terminateInterval,
		Rate:                 c.terminateRate,
		SoftMode:             c.terminateSoftMode,
		IgnoreSystemBackends: c.terminateIgnoreSystem,
		ClientAddr:           c.terminateClientAddr,
		User:                 c.terminateUser,
		Database:             c.terminateDatabase,
		ApplicationName:      c.terminateAppName,
	})
	if err != nil {
		fmt.Printf("terminate backends workload failed: %s\n", err)
		return
	}

	err = workload.Run(ctx)
	if err != nil {
		fmt.Printf("terminate backends workload failed: %s\n", err)
	}
}

func startFailconnsWorkload(ctx context.Context, wg *sync.WaitGroup, c config) {
	defer wg.Done()

	workload, err := failconns.NewWorkload(failconns.Config{
		Conninfo: c.postgresConninfo,
	})
	if err != nil {
		fmt.Printf("failconns workload failed: %s\n", err)
		return
	}

	err = workload.Run(ctx)
	if err != nil {
		fmt.Printf("failconns workload failed: %s\n", err)
	}
}
