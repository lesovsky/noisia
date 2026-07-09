# Noisia

**Harmful workload generator for PostgreSQL.**

---

#### Supported workloads:
- `idle transactions` - active transactions on hot-write tables that do nothing during their lifetime.
- `rollbacks` - fake invalid queries that generate errors and increase rollbacks counter.
- `waiting transactions` - one holder cycles an `ACCESS EXCLUSIVE` lock on a single table while a rate-limited manager opens N separate connections (`--wait-xacts.waiters`) that each block on it — a flickering staircase of blocked sessions that, unbounded (`--wait-xacts.waiters=0`), climbs until `max_connections` is exhausted and new clients get `FATAL: sorry, too many clients already`.
- `deadlocks` - simultaneous transactions where each holds locks that the other transactions want.
- `temporary files` - queries that sort a seeded dataset larger than `work_mem`, spilling to on-disk temporary files on a default-configured server; raising `work_mem` (per-role/-database) makes the same query run in memory — the spill disappears (the remediation demo).
- `terminate backends` - terminate random backends (or queries) using `pg_terminate_backend()`, `pg_cancel_backend()`.
- `failed connections` - exhaust all available connections (other clients unable to connect to Postgres).
- `fork connections` - execute single, short query in a dedicated connection (lead to excessive forking of Postgres backends).
- `backend-killer` - single session leaks prepared statements (plan-cache growth) inflating its backend's memory until OOM-kill restarts the whole instance; a very large `--backend-killer.plan-size` makes each `PREPARE` heavy/slow.
- `slot-bloat` - a single un-consumed physical replication slot pins WAL so `pg_wal` grows without bound → disk full → instance PANIC; the data never grows and checkpoints keep running, yet the disk still fills.
- `wal-flood` - many parallel `UPDATE`-churn workers (`--jobs`) flood WAL on the primary by raw write rate, driving replication lag and — when recycle/archiving can't keep up — `pg_wal` growth toward disk-full; the visible-activity counterpart of `slot-bloat` (disk-full here is environment-dependent, not guaranteed).
- `bloat-churn` - many parallel `UPDATE`-churn workers (`--jobs`) outrun a still-enabled autovacuum by raw rate, breaking HOT with an indexed `updated_at = now()` so heap **and** index bloat while the untouched table tail keeps `VACUUM` from truncating the file; the **remediable** rate-attack counterpart to `xmin-horizon-holder` — same symptom, but after stopping noisia the table is repairable with `VACUUM FULL` / `pg_repack` / `REINDEX CONCURRENTLY`.
- ...see built-in help for more runtime options.

#### Disclaimer

ATTENTION: USE ONLY FOR TESTING PURPOSES, DO NOT EXECUTE NOISIA WITHOUT COMPLETE UNDERSTANDING WHAT YOU REALLY DO, RECKLESS USAGE WILL CAUSE PROBLEMS.

DISCLAIMER: THIS SOFTWARE PROVIDED AS-IS WITH NO CARES AND GUARANTEES RELATED TO YOUR DATABASES. USE AT YOUR OWN RISK.


#### Installation and usage
Check out [releases](https://github.com/lesovsky/noisia/releases) page.
 
#### Using Docker
```shell script
docker pull lesovsky/noisia:latest
docker run --rm -ti lesovsky/noisia:latest noisia --help
```

#### Using in your own code
You can import `noisia` and use necessary workloads in your code. Always use contexts to avoid infinite run. See tiny example below:

```go
package main

import (
	"context"
	"fmt"
	"github.com/lesovsky/noisia/waitxacts"
	"github.com/rs/zerolog"
	"log"
	"os"
	"time"
)

func main() {
	config := waitxacts.Config{
		Conninfo:       "host=127.0.0.1",
		Waiters:        10,
		WaitersRate:    1,
		ReportInterval: 1*time.Second,
		LocktimeMin:    5*time.Second,
		LocktimeMax:    20*time.Second,
	}

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	w, err := waitxacts.NewWorkload(config, logger)
	if err != nil {
		log.Panicln(err)
	}
	
	err = w.Run(ctx)
	if err != nil {
		fmt.Println(err)
	}
}
```

#### Workload impact

Running workloads could impact already running workloads produced by other applications. This impact might be expressed as performance degradation, transactions getting stuck, cancelled queries, disconnected clients, etc.

| Workload  | Impact? |
| :---         |     :---:      |
| backendkiller  | **Yes**: a single session grows backend RSS until OOM-kill and full instance restart; a very large `plan-size` makes each `PREPARE` heavy/slow  |
| deadlocks  | No  |
| failconns  | **Yes**: exhaust `max_connections` limit; this leads to other clients are unable to connect to Postgres |
| forkconns  | **Yes**: excessive creation of Postgres child processes; potentially might lead to `max_connections` exhaustion |
| idlexacts  | **Yes**: might lead to tables and indexes bloat |
| rollbacks  | No  |
| slotbloat  | **Yes**: an un-consumed replication slot pins WAL; `pg_wal` grows until the filesystem is full, the instance can no longer write and PANICs  |
| tempfiles  | **Yes**: might increase storage utilization and degrade storage performance  |
| terminate  | **Yes**: already established database connections could be terminated accidentally  |
| waitxacts  | **Yes**: locks heavy-write tables; this leads to blocking concurrently executed queries  |
| walflood  | **Yes**: high-rate WAL generation drives replication lag and IO pressure; in a constrained environment `pg_wal` grows toward disk-full and the instance PANICs  |

#### Demo & tuning guides

The escalating workloads each have a dedicated demo and tuning guide covering how to build a reliable stand, tune the pressure, read the self-report, and recover afterwards:

- [`docs/workloads/backend-killer.md`](docs/workloads/backend-killer.md) — drive a single backend to OOM and an instance restart; cap memory, disable swap, and turn up the plan pressure.
- [`docs/workloads/slot-bloat.md`](docs/workloads/slot-bloat.md) — fill `pg_wal` with one forgotten replication slot until the disk is full; CLI flags, two stand recipes, and slot-crash recovery.
- [`docs/workloads/wal-flood.md`](docs/workloads/wal-flood.md) — flood WAL with parallel `UPDATE`-churn workers; the honest env-dependent contract, the disk-full conditions, demo parameters, and how it differs from `slot-bloat`.
- [`docs/workloads/bloat-churn.md`](docs/workloads/bloat-churn.md) — outrun autovacuum by raw rate to grow remediable heap+index bloat; the trio that builds it, the post-stop repair reveal (`VACUUM FULL` / `pg_repack` / `REINDEX CONCURRENTLY`), what to watch via pgstattuple, and how a rate attack differs from `xmin-horizon-holder`'s horizon attack.
- [`docs/workloads/tempfiles.md`](docs/workloads/tempfiles.md) — spill on-disk temp files by sorting a seeded dataset larger than `work_mem` on a default-configured server; how to watch `temp_files`/`temp_bytes` and `log_temp_files`, and the headline remediation — raise `work_mem` and the spill disappears.
- [`docs/workloads/waitxacts.md`](docs/workloads/waitxacts.md) — build a pile of blocked waiter sessions behind one held table lock; the observation demo (a flickering staircase of `wait_event_type='Lock'`) and the cascade demo (unbounded waiters until `max_connections` is exhausted), plus the `superuser_reserved_connections` role trick that keeps a `postgres` monitor alive while `app` clients are refused.

#### Contribution
- PR's are welcome.
- Ideas could be proposed [here](https://github.com/lesovsky/noisia/discussions)
- About grammar issues or typos let me know [here](https://github.com/lesovsky/noisia/discussions/8).

#### License
BSD-3. See [LICENSE](LICENSE) for more details.
