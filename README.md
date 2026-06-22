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

#### Tips for a reliable `backend-killer` demo

`backend-killer` drives a single backend toward OOM by leaking prepared statements. How fast you reach
the OOM kill (and the instance restart) depends on the stand:

- **Cap the backend's memory** so OOM is reached in minutes, not hours: a systemd `MemoryMax=` on the
  PostgreSQL service (you then get a cgroup/`CONSTRAINT_MEMCG` OOM), a container `--memory=...` limit, or
  run on a small-RAM host.
- **Disable swap** on the stand (`swapoff -a`): with swap the system slowly thrashes (the panel rate
  drops toward `0/s` — memory pressure, not a hang) instead of OOM-killing promptly.
- **Turn up the pressure** with `--backend-killer.plan-size` (heavier plans per `PREPARE`); raise
  `--backend-killer.rate` is unbounded by default.

When OOM fires, the backend is killed, the instance restarts, noisia's connection drops, and it logs a
`connection lost … target likely OOM-restarted` line — its self-report survives the crash.

#### Tips for a reliable `slot-bloat` demo

`slot-bloat` creates a single un-consumed physical replication slot and drives `UPDATE` churn over a
pre-seeded table. The slot freezes `restart_lsn`, so checkpoints can never recycle WAL: `pg_wal` grows
without bound until the filesystem is full and the instance can no longer write — at which point
PostgreSQL PANICs and becomes unavailable over SQL. The counter-intuitive lesson (versus `wal-flood`,
which fills the disk with visible raw transaction volume): there is **no obvious user activity** here —
the data never grows, checkpoints keep running — and a single forgotten slot still kills the instance.

##### CLI flags

| Flag | Envar | Type | Default | Purpose |
|------|-------|------|---------|---------|
| `--slot-bloat` | `NOISIA_SLOT_BLOAT` | bool | `false` | Enable the workload |
| `--slot-bloat.rate` | `NOISIA_SLOT_BLOAT_RATE` | float64 | `0` | UPDATEs/sec; `0` = unbounded (`rate.Inf`) |
| `--slot-bloat.rows` | `NOISIA_SLOT_BLOAT_ROWS` | int | `1000` | Number of rows seeded into the table |
| `--slot-bloat.payload-bytes` | `NOISIA_SLOT_BLOAT_PAYLOAD_BYTES` | int | `8192` | Payload size (bytes) per row / per UPDATE |
| `--slot-bloat.report-interval` | `NOISIA_SLOT_BLOAT_REPORT_INTERVAL` | duration | `1s` | How often the escalation panel is printed |
| `--slot-bloat.keep-slot` | `NOISIA_SLOT_BLOAT_KEEP_SLOT` | bool | `false` | Keep the slot and table on graceful exit (do not drop) |

`--slot-bloat.rows` sizes the churn row-set, not the WAL volume — a modest value is enough, since the
WAL written per `UPDATE` is governed by `--slot-bloat.payload-bytes`. The initial seed table is roughly
`rows × payload-bytes`, so very large `rows` create a large seed table/heap and a correspondingly slow
startup (the workload logs `slot-bloat: seeding …` while it seeds, then `seeding done, starting churn`).

The workload needs PostgreSQL 10+, the `REPLICATION` privilege (or superuser), `wal_level >= replica`
(the default), and `CREATE` on a schema to seed its table. It opens one dedicated connection, creates a
slot named `noisia_slotbloat_<random>` with `immediately_reserve := true`, seeds a table of the same name,
and loops `UPDATE`s over the fixed row set so the heap stays flat — the disk fill is attributable to WAL
alone, not to table growth. On graceful exit it best-effort drops the slot and table (logs
`slot dropped`); with `--keep-slot`, or when the drop fails (e.g. the target is already dead), it logs the
slot/table names so you can clean up manually.

##### Honest self-report caveat

Every `report-interval` the escalation panel prints a line such as:

```
slot-bloat: payload-written=4.2GB rate=180MB/s elapsed=3m12s
```

`payload-written` is labelled honestly: it is the **application bytes the workload itself pushed**
(successful UPDATEs × `payload-bytes`), **not** the size of `pg_wal` on disk. The real `pg_wal` is
larger — full-page writes (FPI), WAL-record alignment, and autovacuum-generated WAL all add to it. The
label is `payload-written` rather than `written` precisely so you are not surprised when the on-disk WAL
volume exceeds the reported number. The self-report never polls server state, so the panel stays truthful
even when the instance is already down at the moment of the catastrophe.

##### Demo recipe 1 — whole PGDATA on a small filesystem (quick start)

The fastest way to reach a real disk-full:

- **Put PGDATA on a small volume.** Run `initdb` against a directory on a small filesystem (a small LVM
  volume, a loopback ext4 image, or a dedicated small partition of a few GB), then start the cluster.
- **Run the workload unthrottled** so WAL piles up as fast as possible:
  `noisia --slot-bloat --conninfo="host=127.0.0.1 user=postgres dbname=postgres" --duration=30m`
  (leave `--slot-bloat.rate` at its `0` default).
- **Watch the slot and the disk.** In `pg_replication_slots` you will see one slot named
  `noisia_slotbloat_*` with `active = f` and a frozen `restart_lsn`; in pgcenter / `df -h` you will see
  `pg_wal` (inside PGDATA) grow while the row count in the seeded table stays constant and checkpoints
  keep firing.
- **Sign of the catastrophe:** the filesystem hits 100%, WAL writes fail, PostgreSQL PANICs, the instance
  becomes unavailable over SQL, and noisia logs
  `slot-bloat: connection lost — target likely disk-full/restarted`.

##### Demo recipe 2 — `pg_wal` on a separate small volume (the illustrative stand)

This is the teaching stand (Scenario 1 in the spec): it makes the lesson visually obvious because the
heap volume stays flat while a separate WAL volume fills up.

- **Move `pg_wal` onto its own small volume.** With the server stopped, mount a small filesystem (a few
  GB), move the existing `pg_wal` contents onto it, and replace `PGDATA/pg_wal` with a symlink to the new
  mount (equivalently, use `initdb --waldir=/mnt/small-wal`). Start the cluster.
- **Run the workload unthrottled** exactly as in recipe 1.
- **Watch two volumes side by side.** `df -h` shows the WAL volume climbing toward 100% while the PGDATA
  (heap) volume barely moves — the seeded table holds a fixed number of rows, so it never grows. In
  `pg_replication_slots` the `noisia_slotbloat_*` slot is inactive and pinning WAL.
- **Sign of the catastrophe:** the WAL volume fills, the instance PANICs and goes unavailable over SQL —
  driven entirely by one forgotten slot, not by data growth. That contrast is the whole point of the
  exercise.

> A real disk-full cannot be reproduced in CI — both recipes are stand demos verified by hand.
> Managed PostgreSQL (RDS / Cloud SQL / Supabase) is out of scope: recovery needs filesystem access
> (`rm pg_replslot`, `tune2fs`) that managed providers do not grant.

##### Recovery — bringing back a slot-crashed PostgreSQL

Once the disk is full the connection is dead, so noisia can no longer auto-drop the slot — the orphaned
slot keeps pinning the WAL it already accumulated. Recover the instance step by step:

1. **Free space outside PGDATA.** Delete unrelated junk on the same filesystem so the server has room to
   start; never delete WAL segments by hand from inside `pg_wal`.
2. **Reclaim ext-filesystem root-reserved blocks.** On ext2/3/4 a percentage of the volume is reserved
   for root and is invisible to a non-root process; reclaiming it often frees just enough to start the
   instance: `tune2fs -m 1 /dev/<wal-or-pgdata-device>` (lowers the reserve to 1%; `-m 0` removes it
   entirely). Restore a sane reserve (e.g. `-m 5`) once recovery is done — leaving it at `0` permanently
   risks fragmentation and a future no-space deadlock.
3. **Drop the orphaned slot.** The clean way is `SELECT pg_drop_replication_slot('noisia_slotbloat_xxx');`
   once the server is back up (the slot name is in noisia's log under `--keep-slot` or when the auto-drop
   failed). If the instance still cannot start because of the pinned WAL, remove the slot physically —
   **only while the server is stopped** — by deleting its directory:
   `rm -rf "$PGDATA/pg_replslot/noisia_slotbloat_xxx"`. Doing this on a running server corrupts slot
   state; the server must be down.
4. **Start the instance and let WAL drain.** With space freed and the slot gone, start PostgreSQL and wait
   for a checkpoint to recycle the now-unpinned WAL — `pg_wal` shrinks back to normal.

**Prevention:** on PostgreSQL 13+ set `max_slot_wal_keep_size` to cap how much WAL a slot may pin before
it is invalidated, so a forgotten slot can no longer fill the disk. Monitor `pg_replication_slots` for
inactive slots (`active = f`) with a growing retained-WAL size and alert on them.

#### Contribution
- PR's are welcome.
- Ideas could be proposed [here](https://github.com/lesovsky/noisia/discussions)
- About grammar issues or typos let me know [here](https://github.com/lesovsky/noisia/discussions/8).

#### License
BSD-3. See [LICENSE](LICENSE) for more details.
