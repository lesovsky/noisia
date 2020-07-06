package main

import (
	"context"
	"fmt"
	"github.com/rs/zerolog"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	appName, gitCommit, gitBranch string
)

func main() {
	var (
		showVersion          = kingpin.Flag("version", "show version and exit").Default().Bool()
		doCleanup            = kingpin.Flag("cleanup", "do cleanup of workloads related tables from database").Default("false").Envar("NOISIA_CLEANUP").Bool()
		postgresConninfo     = kingpin.Flag("conninfo", "Postgres connection string (DSN or URL), must be specified explicitly").Default("").Envar("NOISIA_POSTGRES_CONNINFO").String()
		jobs                 = kingpin.Flag("jobs", "Run workload with specified number of workers").Default("1").Envar("NOISIA_JOBS").Uint16()
		idleXacts            = kingpin.Flag("idle-xacts", "Run idle transactions workload").Default("false").Envar("NOISIA_IDLE_XACTS").Bool()
		idleXactsNaptimeMin  = kingpin.Flag("idle-xacts.naptime-min", "Min transactions naptime, in seconds").Default("5").Envar("NOISIA_IDLE_XACTS_NAPTIME_MIN").Int()
		idleXactsNaptimeMax  = kingpin.Flag("idle-xacts.naptime-max", "Max transactions naptime, in seconds").Default("20").Envar("NOISIA_IDLE_XACTS_NAPTIME_MAX").Int()
		rollbacks            = kingpin.Flag("rollbacks", "Run rollbacks workload").Default("false").Envar("NOISIA_ROLLBACKS").Bool()
		rollbacksRate        = kingpin.Flag("rollbacks.rate", "Number of transactions per second (per worker)").Default("10").Envar("NOISIA_ROLLBACKS_RATE").Int()
		waitXacts            = kingpin.Flag("wait-xacts", "Run idle transactions workload").Default("false").Envar("NOISIA_IDLE_XACTS").Bool()
		waitXactsLocktimeMin = kingpin.Flag("wait-xacts.locktime-min", "Min transactions locking time, in seconds").Default("5").Envar("NOISIA_WAIT_XACTS_LOCKTIME_MIN").Int()
		waitXactsLocktimeMax = kingpin.Flag("wait-xacts.locktime-max", "Max transactions locking time, in seconds").Default("20").Envar("NOISIA_WAIT_XACTS_LOCKTIME_MAX").Int()
		deadlocks            = kingpin.Flag("deadlocks", "Run deadlocks workload").Default("false").Envar("NOISIA_DEADLOCKS").Bool()
		tempFiles            = kingpin.Flag("temp-files", "Run temp-files workload").Default("false").Envar("NOISIA_TEMP_FILES").Bool()
		tempFilesRate        = kingpin.Flag("temp-files.rate", "Number of queries per second (per worker)").Default("10").Envar("NOISIA_TEMP_FILES_RATE").Int()
		tempFilesScaleFactor = kingpin.Flag("temp-files.scale-factor", "Test data multiplier, 1 = 1000 rows").Default("10").Envar("NOISIA_TEMP_FILES_SCALE_FACTOR").Int()
	)
	kingpin.Parse()

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	if *showVersion {
		fmt.Printf("%s %s-%s\n", appName, gitCommit, gitBranch)
		os.Exit(0)
	}

	config := &config{
		logger:               logger,
		doCleanup:            *doCleanup,
		postgresConninfo:     *postgresConninfo,
		jobs:                 *jobs,
		idleXacts:            *idleXacts,
		idleXactsNaptimeMin:  *idleXactsNaptimeMin,
		idleXactsNaptimeMax:  *idleXactsNaptimeMax,
		rollbacks:            *rollbacks,
		rollbacksRate:        *rollbacksRate,
		waitXacts:            *waitXacts,
		waitXactsLocktimeMin: *waitXactsLocktimeMin,
		waitXactsLocktimeMax: *waitXactsLocktimeMax,
		deadlocks:            *deadlocks,
		tempFiles:            *tempFiles,
		tempFilesRate:        *tempFilesRate,
		tempFilesScaleFactor: *tempFilesScaleFactor,
	}

	ctx, cancel := context.WithCancel(context.Background())

	var doExit = make(chan error, 2)
	go func() {
		doExit <- listenSignals()
		cancel()
	}()

	go func() {
		doExit <- runApplication(ctx, config, logger)
		cancel()
	}()

	logger.Info().Msgf("shutdown: %s", <-doExit)
}

func listenSignals() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("got %s", <-c)
}
