package main

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/deadlocks"
	"github.com/lesovsky/noisia/failconns"
	"github.com/lesovsky/noisia/idlexacts"
	"github.com/lesovsky/noisia/rollbacks"
	"github.com/lesovsky/noisia/tempfiles"
	"github.com/lesovsky/noisia/terminate"
	"github.com/lesovsky/noisia/waitxacts"
	"github.com/rs/zerolog"
	"sync"
	"time"
)

type config struct {
	logger                zerolog.Logger
	doCleanup             bool
	postgresConninfo      string
	jobs                  uint16 // max 65535
	duration              int
	idleXacts             bool
	idleXactsNaptimeMin   int
	idleXactsNaptimeMax   int
	rollbacks             bool
	rollbacksMinRate      int
	rollbacksMaxRate      int
	waitXacts             bool
	waitXactsFixture      bool
	waitXactsLocktimeMin  int
	waitXactsLocktimeMax  int
	deadlocks             bool
	tempFiles             bool
	tempFilesRate         int
	tempFilesScaleFactor  int
	terminate             bool
	terminateInterval     int
	terminateRate         int
	terminateSoftMode     bool
	terminateIgnoreSystem bool
	terminateClientAddr   string
	terminateUser         string
	terminateDatabase     string
	terminateAppName      string
	failconns             bool
}

func runApplication(ctx context.Context, c *config, log zerolog.Logger) error {
	if c.doCleanup {
		log.Info().Msg("do cleanup")
		return noisia.Cleanup(ctx, c.postgresConninfo)
	}

	timeout := time.Duration(c.duration) * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var wg sync.WaitGroup

	if c.idleXacts {
		log.Info().Msg("start idle transactions workload")
		wg.Add(1)
		go startIdleXactsWorkload(ctx, &wg, c)
	}

	if c.rollbacks {
		log.Info().Msg("start rollbacks workload")
		wg.Add(1)
		go startRollbacksWorkload(ctx, &wg, c)
	}

	if c.waitXacts {
		log.Info().Msg("start wait xacts workload")
		wg.Add(1)
		go startWaitxactsWorkload(ctx, &wg, c)
	}

	if c.deadlocks {
		log.Info().Msg("start deadlocks workload")
		wg.Add(1)
		go startDeadlocksWorkload(ctx, &wg, c)
	}

	if c.tempFiles {
		log.Info().Msg("start temp files workload")
		wg.Add(1)
		go startTempFilesWorkload(ctx, &wg, c)
	}

	if c.terminate {
		log.Info().Msg("start terminate backends workload")
		wg.Add(1)
		go startTerminateWorkload(ctx, &wg, c)
	}

	if c.failconns {
		log.Info().Msg("start failconns backends workload")
		wg.Add(1)
		go startFailconnsWorkload(ctx, &wg, c)
	}

	wg.Wait()

	return nil
}

// startIdleXactsWorkload start generating workload with idle transactions.
func startIdleXactsWorkload(ctx context.Context, wg *sync.WaitGroup, c *config) {
	defer wg.Done()

	workload, err := idlexacts.NewWorkload(idlexacts.Config{
		PostgresConninfo:    c.postgresConninfo,
		Jobs:                c.jobs,
		IdleXactsNaptimeMin: c.idleXactsNaptimeMin,
		IdleXactsNaptimeMax: c.idleXactsNaptimeMax,
	})
	if err != nil {
		fmt.Printf("idle transactions workload failed: %s\n", err)
		return
	}

	err = workload.Run(ctx)
	if err != nil {
		fmt.Printf("idle transactions workload failed: %s\n", err)
	}
}

func startRollbacksWorkload(ctx context.Context, wg *sync.WaitGroup, c *config) {
	defer wg.Done()

	workload, err := rollbacks.NewWorkload(rollbacks.Config{
		PostgresConninfo: c.postgresConninfo,
		Jobs:             c.jobs,
		MinRate:          c.rollbacksMinRate,
		MaxRate:          c.rollbacksMaxRate,
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

func startWaitxactsWorkload(ctx context.Context, wg *sync.WaitGroup, c *config) {
	defer wg.Done()

	workload, err := waitxacts.NewWorkload(waitxacts.Config{
		PostgresConninfo:     c.postgresConninfo,
		Jobs:                 c.jobs,
		Fixture:              c.waitXactsFixture,
		WaitXactsLocktimeMin: c.waitXactsLocktimeMin,
		WaitXactsLocktimeMax: c.waitXactsLocktimeMax,
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

func startDeadlocksWorkload(ctx context.Context, wg *sync.WaitGroup, c *config) {
	defer wg.Done()

	workload, err := deadlocks.NewWorkload(deadlocks.Config{
		PostgresConninfo: c.postgresConninfo,
		Jobs:             c.jobs,
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

func startTempFilesWorkload(ctx context.Context, wg *sync.WaitGroup, c *config) {
	defer wg.Done()

	workload, err := tempfiles.NewWorkload(tempfiles.Config{
		PostgresConninfo:     c.postgresConninfo,
		Jobs:                 c.jobs,
		TempFilesRate:        c.tempFilesRate,
		TempFilesScaleFactor: c.tempFilesScaleFactor,
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

func startTerminateWorkload(ctx context.Context, wg *sync.WaitGroup, c *config) {
	defer wg.Done()

	workload, err := terminate.NewWorkload(terminate.Config{
		PostgresConninfo:     c.postgresConninfo,
		TerminateInterval:    c.terminateInterval,
		TerminateRate:        c.terminateRate,
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

func startFailconnsWorkload(ctx context.Context, wg *sync.WaitGroup, c *config) {
	defer wg.Done()

	workload, err := failconns.NewWorkload(failconns.Config{
		PostgresConninfo: c.postgresConninfo,
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
