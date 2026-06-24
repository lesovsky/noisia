# Noisia — Workload Backlog

Backlog of new "slow / escalating" workload generators for noisia.

These differ from the existing acute workloads (`deadlocks`, `failconns`, `forkconns`,
`idlexacts`, `rollbacks`, `tempfiles`, `terminate`, `waitxacts`) in both purpose and UX:
they are meant as **teaching material** for senior DBAs training juniors — build a stand,
generate load, watch the system march toward an extreme limit, discuss symptoms and fixes.

## Design principles for slow generators

- **Escalate to a hard limit, not to a tidy target.** noisia's character is to keep
  applying pressure until something breaks: OOM, disk full, 100% CPU / IO. There is no
  "goal reached, stop" — the only finish line is the system's failure point.
- **Self-report only, no system-state polling.** noisia reports what *it* produced
  (volume generated, rate, load uptime), not the database's internal state. Rationale:
  at the moment of catastrophe the DB is often unreachable (backend killed, instance
  restarting, disk full) and any state monitor goes blind — but noisia's own log
  survives the crash and remains the only witness. System monitoring is the job of
  pgcenter / pgSCV, not noisia.
- **Escalation panel, not a percent bar.** The limit is set by host hardware, which the
  self-report model does not know, so there is no meaningful "X%". Instead show an
  escalation dashboard: accumulated volume + current rate + load uptime.
  Example: `WAL: 4.2 GB ▲ 180 MB/s · 3m12s`.
- **Pre-seeding where needed.** Some generators need a pre-populated dataset before the
  escalation phase; reuse the existing seeding approach.
- **Each generator ships via ai-sdlc:** user-spec → tech-spec → decomposition → implementation.

Each entry below is sized to be expanded into a user-spec.

---

## Priority 1 — Memory → OOM

### backend-killer
- **Limit pushed:** server memory → OOM killer.
- **Mechanism:** a single session accumulates a giant result in backend-local memory
  (e.g. `array_agg` / `string_agg` over a large table, or a huge in-memory hash) so the
  backend RSS climbs without bound.
- **Self-report:** rows/bytes accumulated by the workload, growth rate, load uptime.
- **Symptoms to teach:** one backend's RSS skyrockets, OOM killer reaps it, and because
  the postmaster treats an OOM-killed child as a crash, the **whole instance restarts** —
  "one query took down the database". Hard to predict from the query text alone.
- **Remediation discussion:** memory accounting per backend, `work_mem` sizing,
  cgroup / `vm.overcommit_memory`, query review, statement limits.
- **Notes:** flagship of the slow line; technically the most interesting and the best
  proving ground for the self-report mechanism.
- **Status:** implemented (prepared-statement / plan-cache leak variant). Verified end-to-end on a
  stand: cgroup `CONSTRAINT_MEMCG` OOM of the backend → instance restart → climax line logged.
- **Follow-up ideas:**
  - Optionally `EXECUTE` each prepared statement once: `PREPARE` caches only the query tree, while the
    generic plan is built/cached on first `EXECUTE` — executing would grow backend memory faster and
    reach OOM sooner without needing a tightly capped stand.
  - Document/observe that with swap enabled the approach degrades (panel rate → `0/s`) instead of a
    prompt OOM (already captured in the README demo tips).

## Priority 2 — Disk → full

### slot-bloat (stuck replication slot)
- **Limit pushed:** disk → `pg_wal` fills the filesystem.
- **Mechanism:** create a replication slot and never consume it, while driving write
  traffic; WAL the slot pins cannot be recycled and accumulates without bound.
- **Self-report:** WAL bytes generated since the slot was created, generation rate, uptime.
- **Symptoms to teach:** `pg_wal` grows relentlessly, checkpoints don't free space,
  eventually disk full → instance cannot write → crash. Cause is non-obvious to juniors.
- **Remediation discussion:** inactive slot detection, `max_slot_wal_keep_size`,
  dropping orphaned slots, monitoring `pg_replication_slots`.

### wal-flood
- **Limit pushed:** disk / IO via raw WAL volume.
- **Mechanism:** many small, fast write transactions; optionally amplify via
  full-page-write conditions (writes right after a checkpoint).
- **Self-report:** WAL bytes generated, transactions/s, uptime.
- **Symptoms to teach:** WAL volume spikes, archiver/replication pressure, IO saturation.
- **Remediation discussion:** WAL tuning, batching, checkpoint configuration.
- **Notes:** kept separate from `checkpoint-storm` (different mechanism: fsync pressure vs raw WAL
  volume); FPI/checkpoint-storm amplification is shown only as an observable side effect, not a mode.
- **Status:** implemented (003-feat-wal-flood). `--jobs` parallel UPDATE-churn over disjoint ranges,
  per-worker connection, no slot; self-reports `payload-written`. Verified by unit + testcontainers
  integration tests; real disk-full is a manual stand demo (env-dependent), not CI.

## Priority 3 — CPU → 100%

### hot-row-contention
- **Limit pushed:** CPU via lock-manager / LWLock contention.
- **Mechanism:** many concurrent sessions hammer `UPDATE` on the *same* row.
- **Self-report:** update attempts/s, sessions, uptime.
- **Symptoms to teach:** CPU pegged, TPS collapses, yet no classic blocking is visible —
  the cost is contention, not waiting. Counterintuitive for juniors.
- **Remediation discussion:** access pattern redesign, sharding hot rows, queue/batch.
- **Status:** implemented (004-feat-hot-row-contention). `--jobs` workers SHARE `--hot-rows` foci
  (worker `i` → row `(i mod hot_rows)+1`, default `hot_rows = max(1, jobs/10)`); autocommit UPDATE
  serializes on row-lock / `LWLock:BufferContent`. The deliberate inverse of `wal-flood` (shares rows
  instead of partitioning); `validate()` guards `jobs >= 2*hot_rows` (anti-self-defeat). Honest
  contract: degradation (CPU saturation / TPS collapse observed externally), **not** instance death —
  no PANIC/restart climax in the log. Verified by unit + testcontainers integration (incl. a
  shared-contention test proving rows are shared under `>= jobs` live backends); real CPU saturation is
  a manual stand demo, not CI.

### seqscan-storm
- **Limit pushed:** CPU / IO via repeated full scans.
- **Mechanism:** queries that bypass indexes (or force exploding nested loops) over large
  tables.
- **Self-report:** queries issued, rows scanned, uptime.
- **Symptoms to teach:** CPU/IO saturation, buffer cache churn, plan-driven meltdown.
- **Remediation discussion:** EXPLAIN, pg_stat_statements, indexing, `statement_timeout`.
- **Status:** implemented (005-feat-seqscan-storm). Pure seq-scan variant: `--jobs` workers loop
  `count(*) WHERE payload = 0` over an un-indexed `(id PK, payload)` table; `max_parallel_workers_per_gather=0`
  per worker (skip-on-failure); best-effort cache warm-up; one-time `pg_relation_size`; self-reports logical
  bytes scanned. CPU-bound contract (IO is an optional consequence of a large `--table-size`); degradation,
  not instance death. Verified by unit + testcontainers integration (EXPLAIN proves Seq Scan, `seq_scan`
  growth, parallel-off, warm-up, jobs, cleanup); real CPU saturation is a manual stand demo, not CI.

## Priority 4 — IO → 100%

### checkpoint-storm
- **Limit pushed:** IO via constant fsync pressure.
- **Mechanism:** bursty write load that forces the checkpointer into continuous flushing.
- **Self-report:** dirty volume generated, write rate, uptime.
- **Symptoms to teach:** periodic IO stalls, latency spikes correlated with checkpoints.
- **Remediation discussion:** checkpoint tuning, `checkpoint_completion_target`,
  storage throughput.
- **Notes:** evaluate merging with `wal-flood`.

---

## Optional — slow degradation (not a hard limit, but high teaching value)

These don't drive to an instant failure point; they demonstrate creeping degradation.
Keep separate from the escalate-to-failure line; include only if the teaching value
justifies the slower payoff.

### bloat-churn
- UPDATE/DELETE churn that outpaces autovacuum → measurable table/index bloat.
- Teaches pgstattuple, pg_repack, `VACUUM FULL` and why it locks.

### xmin-horizon-holder (long-lived transaction)
- One transaction lives for minutes holding an old snapshot, so vacuum cannot remove
  dead tuples database-wide. Distinct from `idlexacts` (those are short-lived).
- Teaches `backend_xmin`, oldest-xmin horizon, why "autovacuum runs but doesn't help".

### subxact-overflow (SLRU) — advanced bonus
- Savepoint abuse → SubtransSLRU contention and suboverflowed snapshots → mysterious
  global slowdown with no visible blocking.
- Obscure but impressive demo for advanced sessions.
