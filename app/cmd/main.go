package main

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia/app"
	"github.com/lesovsky/noisia/app/internal/log"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"syscall"
)

var (
	appName, gitCommit, gitBranch string
)

func main() {
	var (
		showVersion         = kingpin.Flag("version", "show version and exit").Default().Bool()
		logLevel            = kingpin.Flag("log-level", "set log level: debug, info, warn, error").Default("info").Envar("NOISIA_LOG_LEVEL").String()
		postgresConninfo    = kingpin.Flag("conninfo", "Postgres connection string (DSN or URL), must be specified explicitly").Default("").Envar("NOISIA_POSTGRES_CONNINFO").String()
		jobs                = kingpin.Flag("jobs", "Run workload with specified number of workers").Default("1").Envar("NOISIA_JOBS").Uint16()
		idleXacts           = kingpin.Flag("idle-xacts", "Run idle transactions workload").Default("false").Envar("NOISIA_IDLE_XACTS").Bool()
		idleXactsNaptimeMin = kingpin.Flag("idle-xacts.naptime-min", "Min transactions naptime, in seconds").Default("5").Envar("NOISIA_IDLE_XACTS_NAPTIME_MIN").Int()
		idleXactsNaptimeMax = kingpin.Flag("idle-xacts.naptime-max", "Max transactions naptime, in seconds").Default("20").Envar("NOISIA_IDLE_XACTS_NAPTIME_MAX").Int()
		rollbacks           = kingpin.Flag("rollbacks", "Run rollbacks workload").Default("false").Envar("NOISIA_ROLLBACKS").Bool()
		rollbacksRate       = kingpin.Flag("rollbacks.rate", "Number of transactions per second (per worker)").Default("10").Envar("NOISIA_ROLLBACKS_RATE").Int()
	)
	kingpin.Parse()
	log.SetLevel(*logLevel)

	if *showVersion {
		fmt.Printf("%s %s-%s\n", appName, gitCommit, gitBranch)
		os.Exit(0)
	}

	config := &app.Config{
		PostgresConninfo:    *postgresConninfo,
		Jobs:                *jobs,
		IdleXacts:           *idleXacts,
		IdleXactsNaptimeMin: *idleXactsNaptimeMin,
		IdleXactsNaptimeMax: *idleXactsNaptimeMax,
		Rollbacks:           *rollbacks,
		RollbacksRate:       *rollbacksRate,
	}

	if err := config.Validate(); err != nil {
		log.Errorln(err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var doExit = make(chan error, 2)
	go func() {
		doExit <- listenSignals()
		cancel()
	}()

	go func() {
		doExit <- app.Start(ctx, config)
		cancel()
	}()

	log.Warnf("shutdown: %s", <-doExit)
}

func listenSignals() error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	return fmt.Errorf("got %s", <-c)
}
