# checkpoint-storm — demo & tuning guide

How to drive a `checkpoint-storm` demo: the privilege/managed-PostgreSQL gate, the reproducibility guard, the
`--dirty-pct` cadence dial, external observation, the honest immediate-vs-scheduled caveat, remediation framing,
and how it differs from `wal-flood`.

`checkpoint-storm` seeds one large table and fans out `--jobs` workers that loop a **scattered random-id**
`UPDATE` — dirtying many **distinct** heap pages — while a single serial **forcer** goroutine issues a
synchronous `CHECKPOINT` every time the accumulated dirt crosses `--dirty-pct` of the seeded rows. The forcer's
cadence (driven by us, **not** by `max_wal_size`) plus the scattered dirty set are the two load-bearing halves:
together they make the buffer **flush** the bottleneck, producing a correlated **sawtooth** of IO stalls and
latency spikes. The instance survives — this is degradation, not catastrophe — and the climax is visible
**outside** noisia (pgcenter / iostat), never in noisia's own log.

## Privilege & managed PostgreSQL — read this first

`checkpoint-storm` requires the **`CHECKPOINT` privilege**: the role must be a **superuser** *or* a member of
the built-in **`pg_checkpoint`** role (PostgreSQL **15+**). noisia checks this at startup with a catalog query
(not a probe `CHECKPOINT`); without the privilege it **fails honestly at startup and does not run** — no table
is created.

This means the workload **does NOT work on managed PostgreSQL** — **RDS, Cloud SQL, Supabase, Neon** — where
the customer role is neither a superuser nor a `pg_checkpoint` member, and the privilege is not grantable
without provider cooperation. On those platforms the workload simply will not start. This is the one gate that
sets `checkpoint-storm` apart from `wal-flood` / `seqscan-storm`, which need no special privilege. Run it on a
**standalone / on-prem** stand where you control the role.

**`max_connections` sizing.** The workload opens up to `--jobs` worker connections **+ 1 seed + 1 forcer +
1 cleanup** — one more than `wal-flood`. At high `--jobs` make sure the target's `max_connections` (and any
pooler limit) leaves headroom; a worker that cannot connect is logged and skipped (degraded run), and the run
fails only if **zero** workers ever connect.

## Reproducibility guard — keep the dirty set under `shared_buffers`

The sawtooth only appears when the dirt accumulated between checkpoints is **flushed at checkpoint time**. That
dirt is `≈ --dirty-pct × --table-size`. Keep it **at or under `shared_buffers`**:

- **Under `shared_buffers`** → the dirty pages sit in the buffer cache until the forced `CHECKPOINT` flushes
  them in one burst → a crisp, correlated sawtooth with recovery valleys between spikes. This is the teachable
  signal.
- **Over `shared_buffers`** → the backends and the bgwriter evict (and flush) dirty pages **ahead** of the
  forced checkpoint, so the checkpoint finds less to do; the valleys fill in, the correlation smears, and there
  is nothing to teach.

**The defaults are deliberately "dirty out of the box."** `--table-size=1GB × --dirty-pct=25 = 256MB` of dirt
vs a stock `shared_buffers=128MB` — the defaults **overshoot on purpose**, so the operator must tune the stand.
Either raise `shared_buffers` on the stand (e.g. to 512MB+ so 256MB fits), or lower `--table-size` /
`--dirty-pct` until the dirty set lands under `shared_buffers`. Do not assume the defaults give a clean
sawtooth on an untuned instance.

## The `--dirty-pct` cadence dial

`--dirty-pct` is the symptom→treatment dial the DBA turns in front of the junior:

- **Higher `--dirty-pct`** (e.g. `50`) → the forcer waits for more rows to be dirtied before each
  `CHECKPOINT` → **rarer, larger checkpoints**: a clean sawtooth with visible **recovery valleys** between
  spikes.
- **Lower `--dirty-pct`** (e.g. `5`) **+ higher `--checkpoint-storm.rate`** → dirt accumulates fast and the
  trigger fires constantly → checkpoints land **back-to-back** (the forcer is serial, so the next one starts as
  soon as the last returns) → **near-sustained saturation**, the valleys disappear.

The forcer is a single serial goroutine, so this is natural backpressure, not a queue: when a flush takes
longer than the inter-trigger time, checkpoints simply run nose-to-tail.

## External observation — the sawtooth lives outside noisia

The climax is visible **outside** noisia. As the panel's `checkpoints=N` rises, watch the server:

- **pgcenter** — `buffers_checkpoint` (buffers written by the checkpointer) spikes in lockstep with each forced
  checkpoint; application latency dips in those windows.
- **iostat** — `%util` on the data device spikes during each flush: `iostat -x 1` and watch the `%util` column.

noisia's own panel reports **only its own counters** — it never polls DB state:

```
checkpoint-storm: dirtied=4.2GB checkpoints=37 (12/min) flush=180ms elapsed=3m12s
```

- `checkpoints=N (M/min)` — the **headline**: noisia's own forced-checkpoint count and rate, viscerally above a
  sane production rate of ~0.2/min.
- `flush=T` — the **client-side** duration of the **last** `CHECKPOINT` call (`time.Since` around the call,
  incl. queue wait). It is **not** the server's checkpoint IO time (`pg_stat_bgwriter.checkpoint_write_time`) —
  label it "CHECKPOINT call duration", not "flush time". Because the `--dirty-pct` trigger keeps the dirty set
  roughly constant, `flush=T` cleanly tracks storage speed.
- `dirtied=<bytes>` — noisia's own application payload (successful `UPDATE`s × `payload-bytes`). It is labelled
  **"dirtied"**, never "written" or "flushed": re-dirtying the same random row **recounts**, so this is **not**
  distinct on-disk bytes (the same honest caveat as wal-flood's `payload-written`).

**Hardware caveat.** On fast NVMe a small flush may not produce a visible `%util` spike — the storage drains it
before it registers. If the sawtooth does not show, increase `--checkpoint-storm.table-size` and/or
`--checkpoint-storm.dirty-pct` (while keeping the dirty set under `shared_buffers`) so each checkpoint flushes
more. The sawtooth is **env-dependent** (storage speed, `shared_buffers`, layout) — it is not guaranteed, which
is why this is a hand-verified stand demo.

## immediate vs scheduled CHECKPOINT — the honesty of the lesson

This demo forces an **immediate** `CHECKPOINT`, and an immediate checkpoint **bypasses
`checkpoint_completion_target`** — it flushes as fast as it can. So the spike you see here is **sharper and
shorter** than a real production checkpoint storm.

A real production storm is a **scheduled** checkpoint firing too often (small `max_wal_size` / short
`checkpoint_timeout`): each one is spread out over `checkpoint_completion_target`, so the symptom is a **broad,
rhythmic latency dip** correlated with checkpoint activity — not the razor spike this demo produces.

Spell this out to the junior, or they carry away the wrong model. **What to recognize on production** is a
**periodic latency dip correlated with `buffers_checkpoint`** (and the checkpoint cadence), not a sharp spike.
This demo teaches the *correlation* (latency ↔ checkpoint activity); it deliberately exaggerates the *shape*.

## Remediation framing

Frame the cure around **checkpoint frequency** and **storage throughput**:

- **Frequency** — fewer, better-spaced checkpoints: raise `max_wal_size` and `checkpoint_timeout` so scheduled
  checkpoints fire less often.
- **Throughput** — faster storage (or less dirt per checkpoint) so each flush costs less.

**Do NOT teach "raise `checkpoint_completion_target`" as the fix for what this demo shows.**
`checkpoint_completion_target` smooths **scheduled** checkpoints, **not immediate** ones — it would not touch
the immediate `CHECKPOINT` this workload forces at all. (It is a legitimate lever for a *real* scheduled-storm,
which is exactly why the immediate-vs-scheduled distinction above must come first.)

## This is NOT wal-flood

Both workloads write to the DB and load the disk, so juniors confuse them. The difference is the **bottleneck**:

- **`checkpoint-storm` — fsync/flush pressure.** The cost is **flushing dirty buffers** at checkpoint time over
  many scattered pages. To see it, watch **`buffers_checkpoint` / checkpoint activity** and the **iostat
  `%util`** sawtooth correlated with checkpoints.
- **`wal-flood` — WAL generation volume.** The cost is the **rate of WAL written** by churn over **disjoint
  id ranges** (cheap in-place flush, but high WAL volume). To see it, watch the **WAL rate** and, with a
  standby attached, **`pg_stat_replication`** lag.

In pgcenter: if the pain tracks **checkpoint buffers and the checkpoint cadence**, it is `checkpoint-storm`; if
it tracks **WAL throughput and replica lag**, it is `wal-flood`. (And `slot-bloat` is a third axis again —
**retention**: a forgotten slot pinning WAL with no visible activity.)

## CLI flags

| Flag | Envar | Type | Default | Purpose |
|------|-------|------|---------|---------|
| `--checkpoint-storm` | `NOISIA_CHECKPOINT_STORM` | bool | `false` | Enable the workload |
| `--checkpoint-storm.table-size` | `NOISIA_CHECKPOINT_STORM_TABLE_SIZE` | bytes (base-2) | `1GB` | Target seed-table size; floor `64MiB`. Big enough that random UPDATEs scatter across many pages |
| `--checkpoint-storm.dirty-pct` | `NOISIA_CHECKPOINT_STORM_DIRTY_PCT` | int | `25` | Fraction of seeded rows dirtied before the forcer fires a `CHECKPOINT` (cadence dial); floor `5` |
| `--checkpoint-storm.payload-bytes` | `NOISIA_CHECKPOINT_STORM_PAYLOAD_BYTES` | int | `8192` | Payload size (bytes) per UPDATE; floor `1`. Least load-bearing knob — scatter and the guard do not depend on it |
| `--checkpoint-storm.rate` | `NOISIA_CHECKPOINT_STORM_RATE` | float64 | `0` | UPDATEs/sec **per worker**; `0` = unbounded (`rate.Inf`) |
| `--checkpoint-storm.report-interval` | `NOISIA_CHECKPOINT_STORM_REPORT_INTERVAL` | duration | `1s` | How often the self-report panel is printed |

The worker count comes from the **global** `--jobs` flag (there is no per-workload jobs flag); a bare
`noisia --checkpoint-storm` (`--jobs=1`) is valid — one worker dirties, the forcer still storms. The workload
runs under the shared `--duration` timeout like every other noisia workload.

## Recommended parameters & example invocation

Tune the stand first (reproducibility guard), then drive it. For a stand with `shared_buffers=512MB`, the
defaults' 256MB dirty set fits and gives a clean sawtooth:

```
noisia --checkpoint-storm --jobs=8 \
  --checkpoint-storm.table-size=1GB --checkpoint-storm.dirty-pct=25 \
  --conninfo="host=127.0.0.1 user=postgres dbname=postgres" --duration=10m
```

Watch `buffers_checkpoint` in pgcenter and `%util` in `iostat -x 1` rise in lockstep with the panel's
`checkpoints=N`. To show the cadence spectrum, rerun with `--checkpoint-storm.dirty-pct=50` (rarer checkpoints,
clean valleys) and then `--checkpoint-storm.dirty-pct=5 --checkpoint-storm.rate=200` (back-to-back checkpoints,
near-sustained saturation).

> A real correlated sawtooth cannot be reproduced in CI — it is a stand demo verified by hand, and only on a
> standalone/on-prem instance with the `CHECKPOINT` privilege. Managed PostgreSQL (RDS / Cloud SQL / Supabase /
> Neon) is out of scope: without superuser or `pg_checkpoint` the workload does not start.
