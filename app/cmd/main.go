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
		showVersion        = kingpin.Flag("version", "show version and exit").Default().Bool()
		logLevel           = kingpin.Flag("log-level", "set log level: debug, info, warn, error").Default("warn").Envar("NOISIA_LOG_LEVEL").String()
		postgresConninfo   = kingpin.Flag("conninfo", "Postgres connection string (DSN or URL), must be specified explicitly").Default("").Envar("NOISIA_POSTGRES_CONNINFO").String()
		jobs               = kingpin.Flag("jobs", "Run workload with specified number of workers").Default("1").Envar("NOISIA_JOBS").Uint16()
		idleXact           = kingpin.Flag("idle-xact", "Run idle transactions workload").Default("false").Envar("NOISIA_IDLE_XACT").Bool()
		idleXactNaptimeMin = kingpin.Flag("idle-xact.naptime-min", "Min transactions naptime, in seconds").Default("5").Envar("NOISIA_IDLE_XACT_NAPTIME_MIN").Int()
		idleXactNaptimeMax = kingpin.Flag("idle-xact.naptime-max", "Max transactions naptime, in seconds").Default("20").Envar("NOISIA_IDLE_XACT_NAPTIME_MAX").Int()
	)
	kingpin.Parse()
	log.SetLevel(*logLevel)

	if *showVersion {
		fmt.Printf("%s %s-%s\n", appName, gitCommit, gitBranch)
		os.Exit(0)
	}

	config := &app.Config{
		PostgresConninfo:   *postgresConninfo,
		Jobs:               *jobs,
		IdleXact:           *idleXact,
		IdleXactNaptimeMin: *idleXactNaptimeMin,
		IdleXactNaptimeMax: *idleXactNaptimeMax,
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
