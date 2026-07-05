# tempfiles — demo & tuning guide

How to drive a `tempfiles` demo: the workload spills on-disk temporary files by running a sort
that genuinely does not fit in `work_mem` — **not** by artificially lowering `work_mem`. That
distinction is the whole point: because the spill is caused by a real memory shortfall, the
textbook remediation — **raise `work_mem`** (per-role / per-database) — actually makes the spill
disappear. This guide covers the mechanism, the headline remediation reveal, what to watch
**outside** noisia, the CLI, and the honest environment-dependent contract.

`tempfiles` seeds its own per-run dataset table `noisia_tempfiles_<random8>`
(`id bigint, payload bytea`, a wide **incompressible** heap of tens of MiB), then fans out
`--jobs` workers that loop a big `SELECT id, payload FROM <t> ORDER BY random()`. On a
**default-configured** server (`work_mem = 4MB`) that sort is far larger than `work_mem`, so
PostgreSQL performs an **external merge sort** and writes a temporary file for every execution.
The dataset table is dropped on exit. The symptom lives entirely **outside** noisia
(`pg_stat_database.temp_files`/`temp_bytes`, the server log, `EXPLAIN`), never in noisia's own
output.

## Mechanism

- **A dataset bigger than `work_mem`.** The seed table is sized (hardcoded, ~64 MiB) so a full
  sort of it comfortably exceeds the default 4MB `work_mem`. The payload is filled with random
  (incompressible) bytes and kept inline, so the on-disk size — and therefore the sort volume —
  is honest and does not compress away.
- **A sort that must spill.** `ORDER BY random()` carries the whole `(id, payload)` row set
  through the sort. When the sort's working set exceeds `work_mem`, PostgreSQL switches from an
  in-memory `quicksort` to an on-disk `external merge` and writes temp files.
- **No `SET work_mem`.** The workload issues only the sort query. It does **not** lower
  `work_mem` at the session level — which is exactly why raising `work_mem` can fix it (a
  session-level `SET` would override any global / per-role / per-database value and make the
  remediation impossible to demonstrate).

## The remediation reveal — read this first

The headline of this demo is the proof that **sizing `work_mem` correctly removes the spill**.
Run it in two acts:

1. **Act one — spills on a default server.** Start a default-configured PostgreSQL
   (`work_mem = 4MB`, `log_temp_files = 0`) and run:

   ```
   noisia --tempfiles --tempfiles.rate=1 --jobs=2 -C "postgres://user@host/db"
   ```

   In a separate `psql`, watch the spill grow:

   ```sql
   SELECT temp_files, pg_size_pretty(temp_bytes) FROM pg_stat_database WHERE datname = current_database();
   ```

   `temp_files` / `temp_bytes` climb, and the server log shows `temporary file` entries. An
   `EXPLAIN (ANALYZE, BUFFERS)` of the same sort shows `Sort Method: external merge  Disk: <n>kB`.

2. **Act two — raise `work_mem`, the spill disappears.** Give the workload's role more sort
   memory (takes effect for new sessions), then rerun the same command:

   ```sql
   ALTER ROLE <workload role> SET work_mem = '256MB';
   ```

   The same query now sorts **in memory** (`Sort Method: quicksort`), and `temp_files` /
   `temp_bytes` stop growing — **zero** new temp files. That is the remediation the demo exists
   to prove.

## What to watch (outside noisia)

- `pg_stat_database.temp_files` / `temp_bytes` — the primary signal (rising in act one, flat in
  act two).
- Server log with `log_temp_files = 0` — one `temporary file` line per spilling query.
- `EXPLAIN (ANALYZE, BUFFERS)` of `SELECT id, payload FROM <t> ORDER BY random()` —
  `external merge` on a default server, `quicksort` after raising `work_mem`.

noisia itself prints only a before/after `generated N temp bytes` summary; because
`pg_stat_get_db_temp_bytes` is database-wide and cumulative, that delta may include temp bytes
from other concurrent activity in the same database.

## CLI

- `--tempfiles` — enable the workload.
- `--tempfiles.rate` — queries per second, **per worker** (default `1`).
- `--jobs` — number of workers.

The dataset size is not configurable — it is fixed above the default `work_mem` on purpose.

## Tuning & the honest contract

- **Scaling the load.** Raise `--jobs` and/or `--tempfiles.rate` to increase the temp-file
  production rate. Note a ceiling: each worker uses its own connection pool
  (`max(4, NumCPU)` connections by default), so on a long, high-rate run the number of
  concurrently in-flight spilling queries — and thus the temp-file rate — saturates at the pool
  size rather than at `--tempfiles.rate`. For the low-rate teaching demo (`--jobs 2`,
  `--tempfiles.rate 1`) this ceiling is never reached; if you need more, add `pool_max_conns=N`
  to the connection string.
- **Degradation only.** The instance does not die — the effect is increased storage utilization
  and degraded storage performance while queries spill. The climax is observed externally, never
  as an instance crash.
- **Reruns are idempotent.** Each run creates a uniquely-named dataset table and drops it on
  exit. If a run is killed before cleanup, the table is left behind and its name is logged so it
  can be dropped manually.
- **No superuser required.** The workload only needs `CONNECT` + `CREATE` on the target
  database, so it runs on managed PostgreSQL (RDS / Cloud SQL / Supabase / Neon) as well as
  self-hosted.
