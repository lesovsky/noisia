package main

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia"
	"github.com/lesovsky/noisia/deadlocks"
	"github.com/lesovsky/noisia/idlexacts"
	"github.com/lesovsky/noisia/rollbacks"
	"github.com/lesovsky/noisia/tempfiles"
	"github.com/lesovsky/noisia/waitxacts"
	"github.com/rs/zerolog"
	"sync"
)

type config struct {
	logger               zerolog.Logger
	doCleanup            bool
	postgresConninfo     string
	jobs                 uint16 // max 65535
	idleXacts            bool
	idleXactsNaptimeMin  int
	idleXactsNaptimeMax  int
	rollbacks            bool
	rollbacksRate        int
	waitXacts            bool
	waitXactsLocktimeMin int
	waitXactsLocktimeMax int
	deadlocks            bool
	tempFiles            bool
	tempFilesRate        int
	tempFilesScaleFactor int
}

func runApplication(ctx context.Context, c *config, log zerolog.Logger) error {

	if c.doCleanup {
		log.Info().Msg("do cleanup")
		return noisia.Cleanup(ctx, c.postgresConninfo)
	}

	var wg sync.WaitGroup

	if c.idleXacts {
		log.Info().Msg("start idle xacts workload")
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

	wg.Wait()

	return nil
}

func startIdleXactsWorkload(ctx context.Context, wg *sync.WaitGroup, c *config) {
	defer wg.Done()

	workload := idlexacts.NewWorkload(&idlexacts.Config{
		PostgresConninfo:    c.postgresConninfo,
		Jobs:                c.jobs,
		IdleXactsNaptimeMin: c.idleXactsNaptimeMin,
		IdleXactsNaptimeMax: c.idleXactsNaptimeMax,
	})

	err := workload.Run(ctx)
	if err != nil {
		fmt.Printf("idle transactions workload failed: %s", err)
	}
}

func startRollbacksWorkload(ctx context.Context, wg *sync.WaitGroup, c *config) {
	defer wg.Done()

	workload := rollbacks.NewWorkload(&rollbacks.Config{
		PostgresConninfo: c.postgresConninfo,
		Jobs:             c.jobs,
		RollbacksRate:    c.rollbacksRate,
	})

	err := workload.Run(ctx)
	if err != nil {
		fmt.Printf("rollbacks workload failed: %s", err)
	}
}

func startWaitxactsWorkload(ctx context.Context, wg *sync.WaitGroup, c *config) {
	defer wg.Done()

	workload := waitxacts.NewWorkload(&waitxacts.Config{
		PostgresConninfo:     c.postgresConninfo,
		Jobs:                 c.jobs,
		WaitXactsLocktimeMin: c.waitXactsLocktimeMin,
		WaitXactsLocktimeMax: c.waitXactsLocktimeMax,
	})

	err := workload.Run(ctx)
	if err != nil {
		fmt.Printf("waiting xacts workload failed: %s", err)
	}
}

func startDeadlocksWorkload(ctx context.Context, wg *sync.WaitGroup, c *config) {
	defer wg.Done()

	workload := deadlocks.NewWorkload(&deadlocks.Config{
		PostgresConninfo: c.postgresConninfo,
		Jobs:             c.jobs,
	})

	err := workload.Run(ctx)
	if err != nil {
		fmt.Printf("deadlocks workload failed: %s", err)
	}
}

func startTempFilesWorkload(ctx context.Context, wg *sync.WaitGroup, c *config) {
	defer wg.Done()

	workload := tempfiles.NewWorkload(&tempfiles.Config{
		PostgresConninfo:     c.postgresConninfo,
		Jobs:                 c.jobs,
		TempFilesRate:        c.tempFilesRate,
		TempFilesScaleFactor: c.tempFilesScaleFactor,
	})

	err := workload.Run(ctx)
	if err != nil {
		fmt.Printf("temp files workload failed: %s", err)
	}
}
