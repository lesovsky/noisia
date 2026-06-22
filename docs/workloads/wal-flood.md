# wal-flood — demo & tuning guide

How to drive a `wal-flood` demo: CLI flags, the honest self-report caveat, the environment conditions that turn pressure into disk-full, recommended demo parameters, and how it differs from `slot-bloat`.

`wal-flood` seeds one fixed-size table and fans out `--jobs` long-lived `UPDATE`-churn workers over
**disjoint** id ranges of it, with **no replication slot**. The churn generates WAL as fast as the
workers (and the server) allow. Unlike `slot-bloat`, which pins WAL with a forgotten slot while the
instance is idle, `wal-flood` produces WAL by raw write rate — the visible-activity counterpart of the
same disk-full lesson. Each worker owns its own connection, so the aggregate WAL rate scales with
`--jobs` and does not collapse on row-lock contention.

## The honest contract — what is and isn't guaranteed

`wal-flood` **reliably** produces sustained WAL pressure, and — when a standby is attached — **replication
lag**, because the primary generates WAL faster than the replica can apply it. Escalation all the way to
a disk-full PANIC is **environment-dependent, not a guarantee**: unlike `slot-bloat` (where a pinned slot
makes `pg_wal` growth deterministic), a pure generation-rate flood **plateaus** once checkpoints recycle
WAL as fast as it is written. On capable I/O you will see high WAL throughput and replica lag, but the
disk will not fill. That is expected behaviour, not a bug.

### Conditions for a real disk-full demo

To make the flood actually fill `pg_wal`, the environment must prevent WAL from being recycled or drained
as fast as it is generated. Arrange at least one of:

- **Slow storage / constrained I/O** — the checkpointer cannot flush dirty buffers fast enough, so WAL
  cannot be recycled and `pg_wal` grows.
- **A small `pg_wal` volume** — put `pg_wal` on its own small filesystem (a few GB) so it fills quickly.
- **A lagging or broken `archive_command`** — with `archive_mode=on` and an `archive_command` that is slow
  or failing, WAL segments are retained until archived; `.ready` files pile up and `pg_wal` grows.
- **A lagging or disconnected replica with WAL retention** (`wal_keep_size` / a slot held elsewhere) — the
  primary retains WAL the standby has not consumed.

If none of these hold, the workload demonstrates WAL throughput and replica lag but will not reach
disk-full — read the panel and replica lag instead.

## CLI flags

| Flag | Envar | Type | Default | Purpose |
|------|-------|------|---------|---------|
| `--wal-flood` | `NOISIA_WAL_FLOOD` | bool | `false` | Enable the workload |
| `--wal-flood.rate` | `NOISIA_WAL_FLOOD_RATE` | float64 | `0` | UPDATEs/sec **per worker**; `0` = unbounded (`rate.Inf`) |
| `--wal-flood.rows` | `NOISIA_WAL_FLOOD_ROWS` | int | `1000` | Total rows seeded into the table, split into `--jobs` disjoint id ranges |
| `--wal-flood.payload-bytes` | `NOISIA_WAL_FLOOD_PAYLOAD_BYTES` | int | `8192` | Payload size (bytes) per UPDATE |
| `--wal-flood.report-interval` | `NOISIA_WAL_FLOOD_REPORT_INTERVAL` | duration | `1s` | How often the escalation panel is printed |

The number of churn workers comes from the **global** `--jobs` flag (there is no per-workload jobs flag).
`--wal-flood.rows` must be `>= --jobs` so every worker owns at least one row; the aggregate rate is roughly
`--jobs × --wal-flood.rate` (or unbounded when `rate=0`). The workload logs `wal-flood: seeding …` while it
seeds, then `seeding done, starting N churn workers`.

### Connections and privileges

The workload uses **`--jobs` + 2** connections: one per worker, plus one for seeding and one for cleanup.
At high `--jobs` make sure the target's `max_connections` (and any pooler limit) leaves room for them.

It needs only ordinary write access: `CREATE` on a schema to seed its table and `UPDATE` on it.
**No `REPLICATION` privilege is required** (unlike `slot-bloat`) — `wal-flood` creates no slot.

## Honest self-report caveat

Every `report-interval` the escalation panel prints a line such as:

```
wal-flood: payload-written=4.2GB rate=180MB/s elapsed=3m12s
```

`payload-written` is labelled honestly: it is the **application bytes the workers themselves pushed**
(successful UPDATEs × `payload-bytes`), **not** the size of `pg_wal` on disk. The real `pg_wal` is larger —
full-page writes (FPI) after each checkpoint, WAL-record alignment, and autovacuum-generated WAL all add to
it. The self-report never polls server state, so the panel stays truthful even when the instance is already
down at the moment of a catastrophe. Replication lag is **not** reported by noisia — observe it on the
server with pgcenter / pgSCV (`pg_stat_replication`).

## Recommended parameters for a visible demo

The defaults (`rows=1000`, `payload-bytes=8192` ≈ an 8 MB seed table, `jobs=1`) are deliberately modest and
may not produce a visible effect on fast hardware. For a demonstration, turn up the pressure:

- **Parallelism:** `--jobs=8` (or as many as the host and `max_connections` allow) so multiple backends
  write WAL concurrently.
- **Payload:** `--wal-flood.payload-bytes=65536` (64 KB) so each UPDATE writes more WAL.
- **Rows:** `--wal-flood.rows=10000` so writes spread across many heap pages (and `rows >= jobs`).
- **Rate:** leave `--wal-flood.rate=0` (unbounded) for maximum pressure.

```
noisia --wal-flood --jobs=8 \
  --wal-flood.payload-bytes=65536 --wal-flood.rows=10000 \
  --conninfo="host=127.0.0.1 user=postgres dbname=postgres" --duration=10m
```

Watch the WAL rate and replica lag in pgcenter / pgSCV (`pg_stat_replication`), and `pg_wal` size via
`df -h` on a constrained stand. Note the full-page-write spike right after each checkpoint — that
amplification is a natural side effect of churning a heap table, visible as a jump in the WAL rate.

> A real disk-full cannot be reproduced in CI — it is a stand demo verified by hand, and only on a
> standalone/on-prem instance. Managed PostgreSQL (RDS / Cloud SQL / Supabase) is out of scope: recovery
> needs filesystem access that managed providers do not grant.

## wal-flood vs slot-bloat

Both fill `pg_wal`, but the mechanism — and the lesson — differ:

- **`slot-bloat` — retention, deterministic.** A forgotten replication slot pins WAL so checkpoints can
  never recycle it; `pg_wal` grows without bound regardless of write rate, while the data stays flat. The
  disk fills with **no visible activity** — the counter-intuitive case.
- **`wal-flood` — generation rate, environment-dependent.** Many fast writes generate WAL faster than it
  can be recycled/archived/replicated; the symptom is a WAL-rate spike, replication lag and archiver
  pressure, and disk-full only when the environment cannot keep up. The cause is **obvious** — the server
  is writing a lot.

## Remediation discussion

Topics this demo sets up for a junior DBA: WAL tuning (`max_wal_size`, `checkpoint_completion_target`,
`min_wal_size`), checkpoint configuration and the cost of frequent checkpoints (FPI amplification),
batching writes to reduce WAL volume, sizing the `pg_wal` volume and storage throughput, monitoring
replication lag (`pg_stat_replication`) and archiver health, and capping retention with
`max_slot_wal_keep_size` / `wal_keep_size` where slots or standbys are involved.
