# bloat-churn — demo & tuning guide

How to drive a `bloat-churn` demo: the rate-attack mechanism that builds **remediable** table-and-index
bloat, the headline **repair reveal** that proves the bloat is curable once noisia stops, what to watch
**outside** noisia, the rate-attack-vs-horizon-attack distinction, the managed-PostgreSQL (no-superuser)
note, and the honest environment-dependent contract.

`bloat-churn` seeds one large `(id bigint PK, payload bytea, updated_at timestamptz)` table with the single
index on `updated_at`, then fans out `--jobs` workers looping a **scattered random-id** `UPDATE` that sets
`updated_at = now()` over the **lower half** of the table. Autovacuum stays **enabled** — the bloat comes
from churning faster than autovacuum can reclaim, a pure **rate attack**. It is the sanctioned, **remediable**
twin of `xmin-horizon-holder` (ADR-007-4): the same visible symptom (rising `n_dead_tup`, a swelling table and
index), but the opposite cause and the opposite conclusion — stop noisia and ordinary tooling repairs the
table. The symptom lives entirely **outside** noisia (`pg_stat_user_tables`, `pgstattuple`, the table/index
size), never in noisia's own panel.

## Mechanism — the trio

Three moves together turn ordinary churn into durable, observable bloat:

- **Break HOT with an indexed `updated_at = now()`.** The seed table's only index is on `updated_at`, and
  every churn `UPDATE` sets `updated_at = now()`. Updating an **indexed** column forbids a Heap-Only Tuple
  update — so each new row version also needs a fresh **index entry**, and the **heap bloats** with dead
  tuples. Because `now()` is **monotonic**, the new index keys always sort to the **right edge** of the
  btree, bloating the index there too (the textbook `REINDEX CONCURRENTLY` lesson).
- **Churn the lower half, never the tail.** Workers only `UPDATE` ids in `[1, floor(0.5·rows)]` (the
  `hotFraction = 0.5` is a constant, not a flag). The trailing heap pages are **never touched**, so they keep
  **live tuples** — and a live tuple on the last pages stops `VACUUM` from **truncating** the relation. The
  bloat therefore **survives** the workload's exit, which is exactly what the post-stop repair demo needs.
- **Leave autovacuum on and win by rate.** Unlike `xmin-horizon-holder`, autovacuum is **not** disabled and
  nothing pins the xmin horizon. The bloat accumulates simply because multi-worker unbounded churn
  (`--jobs` workers, default `rate=0`) generates dead tuples **faster than autovacuum can reclaim them**. That
  is the whole attack: raw rate, not a handcuffed vacuum.

## The remediable reveal — read this first

The headline of this demo is **not** "watch bloat grow"; it is the proof that the bloat is **remediable**
once the rate attack stops. Run it in two acts in front of the junior:

1. **Act one — bloat outpaces autovacuum.** Start `noisia --bloat-churn --jobs 4 …`. In a separate session,
   watch the seed table and its index grow (`pg_relation_size`, `\di+`) and `n_dead_tup`/`dead_tuple_percent`
   climb while autovacuum runs and falls behind.
2. **Act two — stop noisia and repair.** Stop noisia with **`--bloat-churn.keep-table`** so the bloated table
   survives the exit (otherwise it is dropped). Now the dead tuples are immediately reclaimable — autovacuum
   was never forbidden, only outrun:
   - **`VACUUM`** frees the dead space **for reuse** (the file does not shrink, but new writes fill the
     freed slots instead of extending the relation).
   - **`VACUUM FULL` / `pg_repack` / `pgcompacttable`** rewrite the table and **shrink the file** on disk,
     returning space to the filesystem.
   - **`REINDEX CONCURRENTLY`** rebuilds the right-edge-bloated index into a compact one.

**Offline vs online — the lock tradeoff is the teaching point.** `VACUUM FULL` takes an
`AccessExclusiveLock` for the whole rewrite — it is **offline**, blocking all reads and writes, so it is for
maintenance windows. `pg_repack` and `REINDEX CONCURRENTLY` do the equivalent rewrite **online**, taking only
brief strong locks at the boundaries, at the cost of extra disk for the shadow copy and a longer wall-clock
run. Plain `VACUUM` is fully online but only enables **reuse**, it does not shrink the file.

## External observation — noisia never sees the bloat

The climax is entirely **outside** noisia. noisia self-reports only its own atomics and **never polls DB
state** (ADR-002-3) — it cannot and does not print server-side bloat. Watch the server yourself:

- **`pgstattuple`** — `pgstattuple('<seed table>')` shows the physical `dead_tuple_percent` and `free_percent`
  climb, the on-disk bloat itself.
- **`pg_stat_user_tables.n_dead_tup`** — on the seed table, grows while churn outruns autovacuum.
- **`n_tup_hot_upd` vs `n_tup_upd`** — the signature signal of this workload: `n_tup_upd` climbs while
  `n_tup_hot_upd` stays **≈ 0**. A ratio near zero means **HOT is broken** — every update is a non-HOT update
  writing a new heap tuple *and* a new index entry. This is what distinguishes deliberate index-bloating
  churn from benign in-place updates.
- **`pg_relation_size` / `\di+`** — the raw table and index byte sizes growing over time; `\di+` makes the
  right-edge index bloat visible as the index size outpacing the live row count.

noisia's own panel reports **only its own counters**:

```
bloat-churn: churned=<N> dirtied=<bytes> (<rate>/min) elapsed=<Z>
```

- `churned=<N>` — successful `UPDATE`s noisia issued. Re-updating the same random row recounts; this is churn
  volume, not distinct dead rows.
- `dirtied=<bytes>` — the **application bytes** noisia dirtied (`churned × --bloat-churn.payload-bytes`,
  ADR-002-3). This is **logical write pressure, NOT the server-side table size** — the real on-disk size is
  always larger (tuple/index overhead, fragmentation) and is observed externally via `pgstattuple` /
  `pg_relation_size`.
- `(<rate>/min)` — average churn rate since the workload started (`churned ÷ elapsed minutes`), not a windowed
  instantaneous rate: it converges on the steady-state rate but reads low during the startup ramp.
- `elapsed=<Z>` — total workload uptime.

There are **no holder fields** here (`held`, `holder-restarts`) — those belong to `xmin-horizon-holder`;
`bloat-churn` has no holder.

## Rate attack vs horizon attack

`bloat-churn` and `xmin-horizon-holder` produce the **same visible symptom** (rising `n_dead_tup`, swelling
table and index) from **opposite causes** (ADR-007-4):

- **`bloat-churn` — a rate attack.** The default `--bloat-churn.rate 0` (unbounded, per worker) is deliberate:
  churn **faster than autovacuum can keep up**, so dead tuples pile up because autovacuum falls **behind**.
  Here autovacuum is **too slow**. Raising the rate (or `--jobs`) *is* the attack.
- **`xmin-horizon-holder` — a horizon attack.** Its default `--xmin-horizon-holder.rate 3000` is modest by
  design: the bloat does not come from outrunning autovacuum but from a **held snapshot** that makes
  autovacuum **unable to reclaim anything at all**. There autovacuum is **handcuffed**, not slow.

## This is NOT xmin-horizon-holder

Juniors confuse the two neighbours; the distinction is the lesson. The single sharpest test:

> With `bloat-churn`, `pg_repack` (or `VACUUM`) **works** after you stop noisia. With `xmin-horizon-holder`,
> it does **not** while the holder is alive — nothing is reclaimable until the held snapshot is released.

A **rate-attack** backlog drains **gradually**: stop the churn and vacuum catches up over time, because the
dead rows were always collectable — autovacuum was merely behind. A **held-horizon** backlog drains the
**instant** the snapshot is released, because the rows were collectable all along but **forbidden** until then.
Same symptom, opposite remediation timing.

## Managed PostgreSQL — no superuser required

`bloat-churn` needs **no elevated privilege**: just `connect` and `CREATE` (to seed the table) plus `UPDATE`
on it — there is no startup privilege gate. So, unlike `checkpoint-storm` (which needs superuser or
`pg_checkpoint`), `bloat-churn` **runs on RDS, Cloud SQL, Supabase, and Neon** under an ordinary customer role.

**Mind the repair tooling, though.** The *workload* runs anywhere, but the *repair half* of the demo depends
on what the provider exposes: `pg_repack` (an extension) and `pgcompacttable` (an external binary) may be
**unavailable** on managed PostgreSQL, whereas `VACUUM FULL` and `REINDEX CONCURRENTLY` are **available
everywhere**. On a managed stand, plan the repair act around `VACUUM FULL` / `REINDEX CONCURRENTLY`.

## CLI flags

| Flag | Envar | Type | Default | Purpose |
|------|-------|------|---------|---------|
| `--bloat-churn` | `NOISIA_BLOAT_CHURN` | bool | `false` | Enable the workload |
| `--bloat-churn.table-size` | `NOISIA_BLOAT_CHURN_TABLE_SIZE` | bytes (base-2) | `1GB` | Target seed-table size; floor `64MiB`. Bigger surfaces observable bloat sooner as the rate attack outruns autovacuum |
| `--bloat-churn.payload-bytes` | `NOISIA_BLOAT_CHURN_PAYLOAD_BYTES` | int | `8192` | Payload size (bytes) written per UPDATE; floor `1` |
| `--bloat-churn.rate` | `NOISIA_BLOAT_CHURN_RATE` | float64 | `0` | UPDATEs/sec **per worker**; `0` = unbounded (`rate.Inf`). Unbounded by design — this is a rate attack (contrast `xmin-horizon-holder`'s `3000`) |
| `--bloat-churn.report-interval` | `NOISIA_BLOAT_CHURN_REPORT_INTERVAL` | duration | `1s` | How often the self-report panel is printed |
| `--bloat-churn.keep-table` | `NOISIA_BLOAT_CHURN_KEEP_TABLE` | bool | `false` | Keep the bloated table on graceful exit for the post-stop repair demo (otherwise it is dropped) |

The churn worker count comes from the **global** `--jobs` flag — there is **no per-workload jobs flag** — and
the workload runs under the shared `--duration` timeout like every other noisia workload. The hot fraction is
fixed at `0.5` (the lower half of the table is churned, the tail never) as a constant; **there is no
`hot-fraction` flag**.

## Recommended parameters & example invocation

For the **repair reveal**, a few minutes of multi-worker unbounded churn is plenty — then stop and repair.
Use `--bloat-churn.keep-table` so the bloated table survives the exit:

```
noisia --bloat-churn --jobs 4 --bloat-churn.keep-table \
  --bloat-churn.table-size=1GB \
  --conninfo="host=127.0.0.1 user=postgres dbname=postgres" --duration=5m
```

While it runs, in a second session watch the bloat climb and confirm HOT is broken:

- `SELECT pg_size_pretty(pg_relation_size(relid)), n_dead_tup, n_tup_upd, n_tup_hot_upd FROM pg_stat_user_tables WHERE relname LIKE 'noisia_bloatchurn_%';`
- `SELECT * FROM pgstattuple('<seed table>');`

Then stop noisia and repair: `VACUUM FULL <table>` (offline) or `pg_repack`/`REINDEX CONCURRENTLY` (online) —
the file shrinks. That is the reveal. (The kept table is logged by name on exit; drop it manually when done.)

> Real, physically-observable bloat (`pgstattuple`, `n_dead_tup` over time) and the shrink-after-stop repair
> are **hand-verified stand demos** — environment-dependent and destructive, never CI (like wal-flood's
> non-guaranteed disk-full, ADR-003-2). Bloat is env-dependent: on a very capable host, or one whose
> autovacuum is aggressively tuned, the rate attack may stay **flat**. Calibration: on default PostgreSQL
> autovacuum, with `--bloat-churn.rate 0` and `--jobs >= 4`, expect visible bloat in **2–3 minutes**; if the
> host's autovacuum is tuned aggressively, relax it for the duration of the demo.
