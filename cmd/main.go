package main

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia/log"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	appName, gitTag, gitCommit, gitBranch string
)

func main() {
	var (
		showVersion           = kingpin.Flag("version", "show version and exit").Default().Bool()
		logLevel              = kingpin.Flag("log-level", "Log level: info, warn, error").Default("info").Envar("NOISIA_LOG_LEVEL").Enum("info", "warn", "error")
		postgresConninfo      = kingpin.Flag("conninfo", "Postgres connection string (DSN or URL), must be specified explicitly").Default("").Envar("NOISIA_POSTGRES_CONNINFO").String()
		jobs                  = kingpin.Flag("jobs", "Run workload with specified number of workers").Default("1").Envar("NOISIA_JOBS").Uint16()
		duration              = kingpin.Flag("duration", "Duration of tests").Default("10s").Envar("NOISIA_DURATION").Duration()
		idleXacts             = kingpin.Flag("idle-xacts", "Run idle transactions workload").Default("false").Envar("NOISIA_IDLE_XACTS").Bool()
		idleXactsNaptimeMin   = kingpin.Flag("idle-xacts.naptime-min", "Min transactions naptime").Default("5s").Envar("NOISIA_IDLE_XACTS_NAPTIME_MIN").Duration()
		idleXactsNaptimeMax   = kingpin.Flag("idle-xacts.naptime-max", "Max transactions naptime").Default("20s").Envar("NOISIA_IDLE_XACTS_NAPTIME_MAX").Duration()
		rollbacks             = kingpin.Flag("rollbacks", "Run rollbacks workload").Default("false").Envar("NOISIA_ROLLBACKS").Bool()
		rollbacksRate         = kingpin.Flag("rollbacks.rate", "Rollbacks rate per second (per worker)").Default("1").Envar("NOISIA_ROLLBACKS_RATE").Float64()
		waitXacts             = kingpin.Flag("wait-xacts", "Run waiting transactions workload").Default("false").Envar("NOISIA_IDLE_XACTS").Bool()
		waitXactsFixture      = kingpin.Flag("wait-xacts.fixture", "Run workload using fixture table").Default("false").Envar("NOISIA_WAIT_XACTS_FIXTURE").Bool()
		waitXactsLocktimeMin  = kingpin.Flag("wait-xacts.locktime-min", "Min transactions locking time").Default("5s").Envar("NOISIA_WAIT_XACTS_LOCKTIME_MIN").Duration()
		waitXactsLocktimeMax  = kingpin.Flag("wait-xacts.locktime-max", "Max transactions locking time").Default("20s").Envar("NOISIA_WAIT_XACTS_LOCKTIME_MAX").Duration()
		deadlocks             = kingpin.Flag("deadlocks", "Run deadlocks workload").Default("false").Envar("NOISIA_DEADLOCKS").Bool()
		tempFiles             = kingpin.Flag("tempfiles", "Run temporary files workload").Default("false").Envar("NOISIA_TEMP_FILES").Bool()
		tempFilesRate         = kingpin.Flag("tempfiles.rate", "Number of queries per second (per worker)").Default("1").Envar("NOISIA_TEMP_FILES_RATE").Float64()
		terminate             = kingpin.Flag("terminate", "Run terminate workload").Default("false").Envar("NOISIA_TERMINATE").Bool()
		terminateRate         = kingpin.Flag("terminate.rate", "Number of backends/queries terminate per interval").Default("1").Envar("NOISIA_TERMINATE_RATE").Uint16()
		terminateInterval     = kingpin.Flag("terminate.interval", "Time interval of single round of termination").Default("1s").Envar("NOISIA_TERMINATE_INTERVAL").Duration()
		terminateSoftMode     = kingpin.Flag("terminate.soft-mode", "Use queries cancel mode").Default("false").Envar("NOISIA_TERMINATE_SOFT_MODE").Bool()
		terminateIgnoreSystem = kingpin.Flag("terminate.ignore-system", "Don't terminate postgres system processes").Default("false").Envar("NOISIA_TERMINATE_IGNORE_SYSTEM").Bool()
		terminateClientAddr   = kingpin.Flag("terminate.client-addr", "Terminate backends created from specific client addresses").Default("").Envar("NOISIA_TERMINATE_CLIENT_ADDR").String()
		terminateUser         = kingpin.Flag("terminate.user", "Terminate backends handled by specific user").Default("").Envar("NOISIA_TERMINATE_USER").String()
		terminateDatabase     = kingpin.Flag("terminate.database", "Terminate backends connected to specific database").Default("").Envar("NOISIA_TERMINATE_DATABASE").String()
		terminateAppName      = kingpin.Flag("terminate.appname", "Terminate backends created from specific applications").Default("").Envar("NOISIA_TERMINATE_APPNAME").String()
		failconns             = kingpin.Flag("failconns", "Run connections exhaustion workload").Default("false").Envar("NOISIA_FAILCONNS").Bool()
		forkconns             = kingpin.Flag("forkconns", "Run queries in dedicated connections").Default("false").Envar("NOISIA_FORKCONNS").Bool()
		forkconnsRate         = kingpin.Flag("forkconns.rate", "Number of connections made per second").Default("1").Envar("NOISIA_FORKCONNS_RATE").Uint16()
	)
	kingpin.Parse()

	if *showVersion {
		fmt.Printf("%s %s %s-%s\n", appName, gitTag, gitCommit, gitBranch)
		os.Exit(0)
	}

	logger := log.NewDefaultLogger(*logLevel)

	config := config{
		logger:                logger,
		postgresConninfo:      *postgresConninfo,
		jobs:                  *jobs,
		duration:              *duration,
		idleXacts:             *idleXacts,
		idleXactsNaptimeMin:   *idleXactsNaptimeMin,
		idleXactsNaptimeMax:   *idleXactsNaptimeMax,
		rollbacks:             *rollbacks,
		rollbacksRate:         *rollbacksRate,
		waitXacts:             *waitXacts,
		waitXactsFixture:      *waitXactsFixture,
		waitXactsLocktimeMin:  *waitXactsLocktimeMin,
		waitXactsLocktimeMax:  *waitXactsLocktimeMax,
		deadlocks:             *deadlocks,
		tempFiles:             *tempFiles,
		tempFilesRate:         *tempFilesRate,
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
		forkconns:             *forkconns,
		forkconnsRate:         *forkconnsRate,
	}

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	doExit := make(chan error, 2)

	// Run signal listener.
	go func() {
		doExit <- listenSignals()
		cancel()
	}()

	// Run application.
	wg.Add(1)
	go func() {
		doExit <- runApplication(ctx, config, logger)
		cancel()
		wg.Done()
	}()

	// Waiting for signal or application done.
	rc := <-doExit

	// Waiting until goroutines finish.
	wg.Wait()

	// Print last message and return.
	if rc != nil {
		logger.Infof("shutdown: %s", rc)
	} else {
		logger.Info("shutdown: done")
	}
}

func listenSignals() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("got %s", <-c)
}
