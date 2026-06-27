package main

import (
	"context"
	"github.com/lesovsky/noisia/backendkiller"
	"github.com/lesovsky/noisia/checkpointstorm"
	"github.com/lesovsky/noisia/deadlocks"
	"github.com/lesovsky/noisia/failconns"
	"github.com/lesovsky/noisia/forkconns"
	"github.com/lesovsky/noisia/hotrowcontention"
	"github.com/lesovsky/noisia/idlexacts"
	"github.com/lesovsky/noisia/log"
	"github.com/lesovsky/noisia/rollbacks"
	"github.com/lesovsky/noisia/seqscanstorm"
	"github.com/lesovsky/noisia/slotbloat"
	"github.com/lesovsky/noisia/tempfiles"
	"github.com/lesovsky/noisia/terminate"
	"github.com/lesovsky/noisia/waitxacts"
	"github.com/lesovsky/noisia/walflood"
	"github.com/lesovsky/noisia/xminhorizonholder"
	"sync"
	"time"
)

type config struct {
	logger                          log.Logger
	postgresConninfo                string
	jobs                            uint16 // max 65535
	duration                        time.Duration
	idleXacts                       bool
	idleXactsNaptimeMin             time.Duration
	idleXactsNaptimeMax             time.Duration
	rollbacks                       bool
	rollbacksRate                   float64
	waitXacts                       bool
	waitXactsFixture                bool
	waitXactsLocktimeMin            time.Duration
	waitXactsLocktimeMax            time.Duration
	deadlocks                       bool
	tempFiles                       bool
	tempFilesRate                   float64
	terminate                       bool
	terminateInterval               time.Duration
	terminateRate                   uint16
	terminateSoftMode               bool
	terminateIgnoreSystem           bool
	terminateClientAddr             string
	terminateUser                   string
	terminateDatabase               string
	terminateAppName                string
	failconns                       bool
	forkconns                       bool
	forkconnsRate                   uint16
	backendKiller                   bool
	backendKillerRate               float64
	backendKillerPlanSize           int
	backendKillerShowMemory         bool
	backendKillerReportInterval     time.Duration
	slotBloat                       bool
	slotBloatRate                   float64
	slotBloatRows                   int
	slotBloatPayloadBytes           int
	slotBloatReportInterval         time.Duration
	slotBloatKeepSlot               bool
	walFlood                        bool
	walFloodRate                    float64
	walFloodRows                    int
	walFloodPayloadBytes            int
	walFloodReportInterval          time.Duration
	hotRowContention                bool
	hotRows                         uint
	hotRowContentionReportInterval  time.Duration
	seqscanStorm                    bool
	seqscanStormTableSize           int64
	checkpointStorm                 bool
	checkpointStormTableSize        int64
	checkpointStormDirtyPct         int
	checkpointStormPayloadBytes     int
	checkpointStormRate             float64
	checkpointStormReportInterval   time.Duration
	xminHorizonHolder               bool
	xminHorizonHolderTableSize      int64
	xminHorizonHolderPayloadBytes   int
	xminHorizonHolderRate           float64
	xminHorizonHolderReportInterval time.Duration
}

func runApplication(ctx context.Context, c config, log log.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, c.duration)
	defer cancel()

	var wg sync.WaitGroup

	if c.idleXacts {
		log.Info("start idle transactions workload")
		wg.Add(1)
		go func() {
			err := startIdleXactsWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("idle transactions workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.rollbacks {
		log.Infof("start rollbacks workload for %s", c.duration)
		wg.Add(1)
		go func() {
			err := startRollbacksWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("rollbacks workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.waitXacts {
		log.Info("start wait xacts workload")
		wg.Add(1)
		go func() {
			err := startWaitxactsWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("wait xacts workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.deadlocks {
		log.Info("start deadlocks workload")
		wg.Add(1)
		go func() {
			err := startDeadlocksWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("deadlocks workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.tempFiles {
		log.Info("start temp files workload")
		wg.Add(1)
		go func() {
			err := startTempFilesWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("temp files workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.terminate {
		log.Info("start terminate backends workload")
		wg.Add(1)
		go func() {
			err := startTerminateWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("terminate backends workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.failconns {
		log.Info("start failconns backends workload")
		wg.Add(1)
		go func() {
			err := startFailconnsWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("failconns backends workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.forkconns {
		log.Info("start fork connections workload")
		wg.Add(1)
		go func() {
			err := startForkconnsWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("fork connections workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.backendKiller {
		log.Info("start backend-killer workload")
		wg.Add(1)
		go func() {
			err := startBackendKillerWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("backend-killer workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.slotBloat {
		log.Info("start slot-bloat workload")
		wg.Add(1)
		go func() {
			err := startSlotBloatWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("slot-bloat workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.walFlood {
		log.Info("start wal-flood workload")
		wg.Add(1)
		go func() {
			err := startWalFloodWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("wal-flood workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.hotRowContention {
		log.Info("start hot-row-contention workload")
		wg.Add(1)
		go func() {
			err := startHotRowContentionWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("hot-row-contention workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.seqscanStorm {
		log.Info("start seqscan-storm workload")
		wg.Add(1)
		go func() {
			err := startSeqscanStormWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("seqscan-storm workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.checkpointStorm {
		log.Info("start checkpoint-storm workload")
		wg.Add(1)
		go func() {
			err := startCheckpointStormWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("checkpoint-storm workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	if c.xminHorizonHolder {
		log.Info("start xmin-horizon-holder workload")
		wg.Add(1)
		go func() {
			err := startXminHorizonHolderWorkload(ctx, c, log)
			if err != nil {
				log.Errorf("xmin-horizon-holder workload failed: %s", err)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return nil
}

// startIdleXactsWorkload start generating workload with idle transactions.
func startIdleXactsWorkload(ctx context.Context, c config, logger log.Logger) error {
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

func startRollbacksWorkload(ctx context.Context, c config, logger log.Logger) error {
	workload, err := rollbacks.NewWorkload(
		rollbacks.Config{
			Conninfo: c.postgresConninfo,
			Jobs:     c.jobs,
			Rate:     c.rollbacksRate,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startWaitxactsWorkload(ctx context.Context, c config, logger log.Logger) error {
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

func startDeadlocksWorkload(ctx context.Context, c config, logger log.Logger) error {
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

func startTempFilesWorkload(ctx context.Context, c config, logger log.Logger) error {
	workload, err := tempfiles.NewWorkload(
		tempfiles.Config{
			Conninfo: c.postgresConninfo,
			Jobs:     c.jobs,
			Rate:     c.tempFilesRate,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startTerminateWorkload(ctx context.Context, c config, logger log.Logger) error {
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

func startFailconnsWorkload(ctx context.Context, c config, logger log.Logger) error {
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

func startForkconnsWorkload(ctx context.Context, c config, logger log.Logger) error {
	workload, err := forkconns.NewWorkload(
		forkconns.Config{
			Conninfo: c.postgresConninfo,
			Rate:     c.forkconnsRate,
			Jobs:     c.jobs,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startSlotBloatWorkload(ctx context.Context, c config, logger log.Logger) error {
	workload, err := slotbloat.NewWorkload(
		slotbloat.Config{
			Conninfo:       c.postgresConninfo,
			Rate:           c.slotBloatRate,
			Rows:           c.slotBloatRows,
			PayloadBytes:   c.slotBloatPayloadBytes,
			ReportInterval: c.slotBloatReportInterval,
			KeepSlot:       c.slotBloatKeepSlot,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startWalFloodWorkload(ctx context.Context, c config, logger log.Logger) error {
	workload, err := walflood.NewWorkload(
		walflood.Config{
			Conninfo:       c.postgresConninfo,
			Rate:           c.walFloodRate,
			Rows:           c.walFloodRows,
			PayloadBytes:   c.walFloodPayloadBytes,
			ReportInterval: c.walFloodReportInterval,
			Jobs:           c.jobs,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

// resolveHotRows resolves the hot-rows default. The --hot-rows flag uses a sentinel
// default of 0 meaning "not set": kingpin cannot express a default derived from --jobs,
// so when hotRows is 0 the default max(1, jobs/10) is computed here; otherwise the
// explicit value is passed through. Validation (HotRows >= 1, Jobs >= 2*HotRows) is the
// hotrowcontention package's responsibility.
func resolveHotRows(hotRows uint, jobs uint16) int {
	if hotRows != 0 {
		return int(hotRows)
	}

	if d := int(jobs) / 10; d > 1 {
		return d
	}
	return 1
}

func startHotRowContentionWorkload(ctx context.Context, c config, logger log.Logger) error {
	workload, err := hotrowcontention.NewWorkload(
		hotrowcontention.Config{
			Conninfo:       c.postgresConninfo,
			Jobs:           c.jobs,
			HotRows:        resolveHotRows(c.hotRows, c.jobs),
			ReportInterval: c.hotRowContentionReportInterval,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startSeqscanStormWorkload(ctx context.Context, c config, logger log.Logger) error {
	workload, err := seqscanstorm.NewWorkload(
		seqscanstorm.Config{
			Conninfo:  c.postgresConninfo,
			TableSize: c.seqscanStormTableSize,
			Jobs:      c.jobs,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startCheckpointStormWorkload(ctx context.Context, c config, logger log.Logger) error {
	workload, err := checkpointstorm.NewWorkload(
		checkpointstorm.Config{
			Conninfo:       c.postgresConninfo,
			TableSize:      c.checkpointStormTableSize,
			DirtyPct:       c.checkpointStormDirtyPct,
			PayloadBytes:   c.checkpointStormPayloadBytes,
			Rate:           c.checkpointStormRate,
			ReportInterval: c.checkpointStormReportInterval,
			Jobs:           c.jobs,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startXminHorizonHolderWorkload(ctx context.Context, c config, logger log.Logger) error {
	workload, err := xminhorizonholder.NewWorkload(
		xminhorizonholder.Config{
			Conninfo:       c.postgresConninfo,
			TableSize:      c.xminHorizonHolderTableSize,
			PayloadBytes:   c.xminHorizonHolderPayloadBytes,
			Rate:           c.xminHorizonHolderRate,
			ReportInterval: c.xminHorizonHolderReportInterval,
			Jobs:           c.jobs,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}

func startBackendKillerWorkload(ctx context.Context, c config, logger log.Logger) error {
	workload, err := backendkiller.NewWorkload(
		backendkiller.Config{
			Conninfo:       c.postgresConninfo,
			Rate:           c.backendKillerRate,
			PlanSize:       c.backendKillerPlanSize,
			ShowMemory:     c.backendKillerShowMemory,
			ReportInterval: c.backendKillerReportInterval,
		}, logger,
	)
	if err != nil {
		return err
	}

	return workload.Run(ctx)
}
