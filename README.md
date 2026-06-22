# Noisia

**Harmful workload generator for PostgreSQL.**

---

#### Supported workloads:
- `idle transactions` - active transactions on hot-write tables that do nothing during their lifetime.
- `rollbacks` - fake invalid queries that generate errors and increase rollbacks counter.
- `waiting transactions` - transactions that lock hot-write tables and then idle, leading to other transactions getting stuck
- `deadlocks` - simultaneous transactions where each holds locks that the other transactions want.
- `temporary files` - queries that produce on-disk temporary files due to lack of `work_mem`.
- `terminate backends` - terminate random backends (or queries) using `pg_terminate_backend()`, `pg_cancel_backend()`.
- `failed connections` - exhaust all available connections (other clients unable to connect to Postgres).
- `fork connections` - execute single, short query in a dedicated connection (lead to excessive forking of Postgres backends).
- `backend-killer` - single session leaks prepared statements (plan-cache growth) inflating its backend's memory until OOM-kill restarts the whole instance; a very large `--backend-killer.plan-size` makes each `PREPARE` heavy/slow.
- `slot-bloat` - a single un-consumed physical replication slot pins WAL so `pg_wal` grows without bound → disk full → instance PANIC; the data never grows and checkpoints keep running, yet the disk still fills.
- `wal-flood` - many parallel `UPDATE`-churn workers (`--jobs`) flood WAL on the primary by raw write rate, driving replication lag and — when recycle/archiving can't keep up — `pg_wal` growth toward disk-full; the visible-activity counterpart of `slot-bloat` (disk-full here is environment-dependent, not guaranteed).
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
		Conninfo:    "host=127.0.0.1",
		Jobs:        2,
		LocktimeMin: 5*time.Second,
		LocktimeMax: 20*time.Second,
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

#### Contribution
- PR's are welcome.
- Ideas could be proposed [here](https://github.com/lesovsky/noisia/discussions)
- About grammar issues or typos let me know [here](https://github.com/lesovsky/noisia/discussions/8).

#### License
BSD-3. See [LICENSE](LICENSE) for more details.
