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
	appName, gitTag, gitCommit, gitBranch string
)

func main() {
	var (
		showVersion           = kingpin.Flag("version", "show version and exit").Default().Bool()
		doCleanup             = kingpin.Flag("cleanup", "do cleanup of workloads related tables from database").Default("false").Envar("NOISIA_CLEANUP").Bool()
		postgresConninfo      = kingpin.Flag("conninfo", "Postgres connection string (DSN or URL), must be specified explicitly").Default("").Envar("NOISIA_POSTGRES_CONNINFO").String()
		jobs                  = kingpin.Flag("jobs", "Run workload with specified number of workers").Default("1").Envar("NOISIA_JOBS").Uint16()
		duration              = kingpin.Flag("duration", "Duration of tests in seconds").Default("10").Envar("NOISIA_DURATION").Int()
		idleXacts             = kingpin.Flag("idle-xacts", "Run idle transactions workload").Default("false").Envar("NOISIA_IDLE_XACTS").Bool()
		idleXactsNaptimeMin   = kingpin.Flag("idle-xacts.naptime-min", "Min transactions naptime, in seconds").Default("5").Envar("NOISIA_IDLE_XACTS_NAPTIME_MIN").Int()
		idleXactsNaptimeMax   = kingpin.Flag("idle-xacts.naptime-max", "Max transactions naptime, in seconds").Default("20").Envar("NOISIA_IDLE_XACTS_NAPTIME_MAX").Int()
		rollbacks             = kingpin.Flag("rollbacks", "Run rollbacks workload").Default("false").Envar("NOISIA_ROLLBACKS").Bool()
		rollbacksMinRate      = kingpin.Flag("rollbacks.min-rate", "Approximate minimum number of rollbacks per second (per worker)").Default("10").Envar("NOISIA_ROLLBACKS_MIN_RATE").Int()
		rollbacksMaxRate      = kingpin.Flag("rollbacks.max-rate", "Approximate maximum number of rollbacks per second (per worker)").Default("10").Envar("NOISIA_ROLLBACKS_MAX_RATE").Int()
		waitXacts             = kingpin.Flag("wait-xacts", "Run idle transactions workload").Default("false").Envar("NOISIA_IDLE_XACTS").Bool()
		waitXactsFixture      = kingpin.Flag("wait-xacts.fixture", "Run workload using fixture table").Default("false").Envar("NOISIA_WAIT_XACTS_FIXTURE").Bool()
		waitXactsLocktimeMin  = kingpin.Flag("wait-xacts.locktime-min", "Min transactions locking time, in seconds").Default("5").Envar("NOISIA_WAIT_XACTS_LOCKTIME_MIN").Int()
		waitXactsLocktimeMax  = kingpin.Flag("wait-xacts.locktime-max", "Max transactions locking time, in seconds").Default("20").Envar("NOISIA_WAIT_XACTS_LOCKTIME_MAX").Int()
		deadlocks             = kingpin.Flag("deadlocks", "Run deadlocks workload").Default("false").Envar("NOISIA_DEADLOCKS").Bool()
		tempFiles             = kingpin.Flag("temp-files", "Run temp-files workload").Default("false").Envar("NOISIA_TEMP_FILES").Bool()
		tempFilesRate         = kingpin.Flag("temp-files.rate", "Number of queries per second (per worker)").Default("10").Envar("NOISIA_TEMP_FILES_RATE").Int()
		tempFilesScaleFactor  = kingpin.Flag("temp-files.scale-factor", "Test data multiplier, 1 = 1000 rows").Default("10").Envar("NOISIA_TEMP_FILES_SCALE_FACTOR").Int()
		terminate             = kingpin.Flag("terminate", "Run terminate workload").Default("false").Envar("NOISIA_TERMINATE").Bool()
		terminateRate         = kingpin.Flag("terminate.rate", "Number of backends/queries terminate per interval").Default("1").Envar("NOISIA_TERMINATE_RATE").Int()
		terminateInterval     = kingpin.Flag("terminate.interval", "Time interval of single round, in seconds").Default("1").Envar("NOISIA_TERMINATE_INTERVAL").Int()
		terminateSoftMode     = kingpin.Flag("terminate.soft-mode", "Use queries cancel mode").Default("false").Envar("NOISIA_TERMINATE_SOFT_MODE").Bool()
		terminateIgnoreSystem = kingpin.Flag("terminate.ignore-system", "Ignore postgres system processes").Default("false").Envar("NOISIA_TERMINATE_IGNORE_SYSTEM").Bool()
		terminateClientAddr   = kingpin.Flag("terminate.client-addr", "Terminate backends created from specific client addresses").Default("").Envar("NOISIA_TERMINATE_CLIENT_ADDR").String()
		terminateUser         = kingpin.Flag("terminate.user", "Terminate backends handled by specific user").Default("").Envar("NOISIA_TERMINATE_USER").String()
		terminateDatabase     = kingpin.Flag("terminate.database", "Terminate backends connected to specific database").Default("").Envar("NOISIA_TERMINATE_DATABASE").String()
		terminateAppName      = kingpin.Flag("terminate.appname", "Terminate backends created from specific applications").Default("").Envar("NOISIA_TERMINATE_APPNAME").String()
		failconns             = kingpin.Flag("failconns", "Run connections exhaustion workload").Default("false").Envar("NOISIA_FAILCONNS").Bool()
	)
	kingpin.Parse()

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	if *showVersion {
		fmt.Printf("%s %s %s-%s\n", appName, gitTag, gitCommit, gitBranch)
		os.Exit(0)
	}

	config := &config{
		logger:                logger,
		doCleanup:             *doCleanup,
		postgresConninfo:      *postgresConninfo,
		jobs:                  *jobs,
		duration:              *duration,
		idleXacts:             *idleXacts,
		idleXactsNaptimeMin:   *idleXactsNaptimeMin,
		idleXactsNaptimeMax:   *idleXactsNaptimeMax,
		rollbacks:             *rollbacks,
		rollbacksMinRate:      *rollbacksMinRate,
		rollbacksMaxRate:      *rollbacksMaxRate,
		waitXacts:             *waitXacts,
		waitXactsFixture:      *waitXactsFixture,
		waitXactsLocktimeMin:  *waitXactsLocktimeMin,
		waitXactsLocktimeMax:  *waitXactsLocktimeMax,
		deadlocks:             *deadlocks,
		tempFiles:             *tempFiles,
		tempFilesRate:         *tempFilesRate,
		tempFilesScaleFactor:  *tempFilesScaleFactor,
		terminate:             *terminate,
		terminateRate:         *terminateRate,
		terminateInterval:     *terminateInterval,
		terminateSoftMode:     *terminateSoftMode,
		terminateIgnoreSystem: *terminateIgnoreSystem,
		terminateClientAddr:   *terminateClientAddr,
		terminateUser:         *terminateUser,
		terminateDatabase:     *terminateDatabase,
		terminateAppName:      *terminateAppName,
		failconns:             *failconns,
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

	rc := <-doExit
	if rc != nil {
		logger.Info().Msgf("shutdown: %s", rc)
	} else {
		logger.Info().Msgf("shutdown: done")
	}
}

func listenSignals() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("got %s", <-c)
}
