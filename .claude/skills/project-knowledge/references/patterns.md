# Patterns & Conventions

Coding conventions, development workflow, and project-specific practices.
For universal coding standards, see `~/.claude/skills/code-writing/references/universal-patterns.md`.

---

## Project-Specific Code Patterns

- **One workload = one package.** Each package exposes a `Config`, a `NewWorkload(config, logger) (Workload, error)`, a `Workload` implementing `Run(ctx)`, and a private `validate()` on `Config`.
- **Config validation is explicit and table-tested.** Each `Config` has a `validate()` covering valid/invalid combinations (see `*_test.go` `TestConfig_validate`).
- **Driver access goes through the `db` package interfaces** (`DB`/`Tx`/`Conn`), not pgx directly — this is what keeps the pgx version migration contained.
- **Context is the only stop mechanism.** Workloads loop until `ctx` is cancelled/timed out; never add a bespoke shutdown path.
- **Connection strings are secrets** — never log `Conninfo`.

---

## Git Workflow

### Branch Structure

- **`master`** — the single long-lived branch. CI (`default.yml`) runs lint + tests on push and PR to `master`. Tags `v*` on `master` trigger releases.
- **feature branches** — created from `master` for non-trivial work, merged back via PR.

> Note: this is a small single-maintainer utility; there is no `dev`/staging branch. Keep `master` releasable.

### Testing Requirements

- **On PR/push:** `make lint` + `make test` (unit + integration). Integration tests require a live PostgreSQL.
- **Before tagging a release:** full green CI on `master`.

### Security & Quality Gates

- **Secrets:** never commit connection strings / credentials. A gitleaks pre-commit hook is recommended (see global setup).
- **Lint:** `make lint` (golangci-lint) must pass.

---

## Testing & Verification

### Test Infrastructure

- **Type:** integration-heavy. Most workload tests connect to a real PostgreSQL and assert observable effects via server-side state (`pg_stat_activity`, `pg_stat_database`, temp-file counters).
- **Harness:** **testcontainers-go** (`modules/postgres`). `db.TestConninfo` is a `var` populated at test time; the container is started in `internal/dbtest.RunMain`, which each package calls from its `TestMain`. `internal/dbtest` is imported only from `*_test.go`, so testcontainers/docker deps never enter the noisia binary.
- **Isolation:** one PostgreSQL container **per test package** (automatic cross-package isolation). Must run **serially** (`-p 1`) — workloads mutate server-wide state, and parallel containers also destabilize tight per-test timings.
- **Requirement:** a working Docker daemon. No manual DB setup — same command locally and in CI.
- **Run:** `make test` → `go test -race -timeout 300s -p 1 -coverprofile=... ./...`.

### Verifying a workload manually

Run the workload against a scratch PostgreSQL and confirm the expected symptom appears:
- idle transactions → `idle in transaction` rows in `pg_stat_activity`
- deadlocks → rising `pg_stat_database.deadlocks`
- temp files → rising `pg_stat_database.temp_files`
- failconns → new client connections rejected (`max_connections` exhausted)

### Running tests

With Docker available, just:
```bash
make test          # or: go test -race -timeout 300s -p 1 ./...
```
testcontainers starts/stops PostgreSQL automatically; nothing else to set up.

---

## Reference Implementations

### Driver abstraction
**File:** `db/db.go`, `db/postgres.go`
**Shows:** how workloads stay decoupled from pgx via `DB`/`Tx`/`Conn` interfaces. Primary touch point for the pgx v4→v5 migration.

### Workload package shape
**File:** `idlexacts/` (incl. `idlexacts_test.go`)
**Shows:** the canonical `Config` + `validate()` + `NewWorkload` + `Run` structure and its test layout (config table tests + DB-backed tests).

### Single-connection, self-reporting workload
**File:** `backendkiller/` (incl. `backendkiller_test.go`)
**Shows:** the "slow / escalating" workload style — one dedicated `db.Connect` connection (not the pool), a single-threaded rate-limited loop (`rate.Inf` for unlimited), a self-report escalation panel emitted from a separate ticker goroutine reading only atomics, and connection-loss climax detection. Also the `sanitize` helper that keeps `Conninfo` out of every log/error line, and setting `application_name=noisia` on a raw `db.Connect` connection (which, unlike the pool, does not set it). First entry of the new-workloads backlog (`docs/BACKLOG.md`).

### Multi-worker fan-out over disjoint ranges (per-worker connection)
**File:** `walflood/` (incl. `walflood_test.go`)
**Shows:** the "slow/escalating + self-report" pattern scaled to N concurrent workers driven by the
global `--jobs` flag. Key lessons: spawn long-lived workers with the **rollbacks** `sync.WaitGroup` +
goroutine-per-job shape (not the idlexacts/deadlocks guard-channel, which is spawn-on-completion); give
each worker its **own** `db.Connect` rather than sharing a pool, because `db.NewPostgresDB` never sets
`max_conns` and the pgxpool default `max(4, NumCPU)` would silently cap real parallelism below `--jobs`
(and make a "--jobs honored" test flaky on low-CPU hosts) — the per-worker connection also needs a manual
`SET application_name='noisia'` since `db.Connect` (unlike the pool) does not set it; partition the work
into disjoint id sub-ranges so workers never serialize on row locks (validate `rows >= jobs` to drop the
empty-tail edge); share ONE `*atomic.Int64` and judge init-vs-climax on it so one slow worker's early
error cannot masquerade as a setup failure once any worker has written. The integration test asserts
`--jobs` is honored via `>= N` `application_name='noisia'` backends in `pg_stat_activity`, independent of
host CPU.

### Cleanup of persistent server objects (fresh connection)
**File:** `slotbloat/` (incl. `slotbloat_test.go`)
**Shows:** the "slow / escalating" pattern extended to a workload that creates **persistent** server objects (a physical replication slot + a regular table) which must be dropped on exit. Key lesson learned on the stand: a workload's own `db.Connect` connection is **dead at cleanup time** — when the run is stopped via ctx-cancel, pgx closes any connection whose in-flight query was interrupted, so the deferred cleanup must open a **fresh** `db.Connect` on a timeout-bounded `context.Background()` to run the drops (reusing the workload conn fails with `failed to deallocate cached statement(s): conn closed`). Pool-based workloads (e.g. `waitxacts`) get a healthy connection for free; single-`db.Connect` workloads must open one explicitly. Also: seed large tables with one set-based `INSERT ... SELECT generate_series(...)` (not a per-row loop) and bracket a potentially long seed with explicit progress log lines so it doesn't look hung.

### Shared-row contention (deliberate inverse of disjoint fan-out)
**File:** `hotrowcontention/` (incl. `hotrowcontention_test.go`)
**Shows:** the `--jobs` fan-out pattern (per-worker `db.Connect`, ADR-003-1) turned **inside-out** vs `walflood`. Where walflood partitions ids into disjoint ranges (and validates `rows >= jobs`) precisely to **avoid** row-lock serialization, hotrowcontention makes workers **share** `HotRows` rows (worker `i` → row `(i mod HotRows)+1`) to **cause** it — autocommit `UPDATE` (no tx, distinguishing it from the held-locks of `waitxacts`) on shared rows burns CPU on `LWLock:BufferContent`. Key lessons: (1) the anti-self-defeat guard is inverted — `validate()` requires `jobs >= 2*HotRows` (≥2 sessions per focus, else no contention); (2) the headline integration test must prove rows are **shared** (counter on a single row grows while `>= jobs` live `application_name='noisia'` backends are observed), not merely that a counter grows — a single worker or a disjoint partition would pass a naive version; (3) extract the churn loop into a standalone `runChurn(ctx, conn db.Conn, ...)` taking the interface so unit tests drive it with a fake `Conn`; (4) honest contract — the climax (CPU 100% / TPS collapse) is observed **externally** (pgcenter/top), never in noisia's own log, because the instance does not die.

### Forced sequential scan (brute-force CPU, deliberate inverse of contention)
**File:** `seqscanstorm/` (incl. `seqscanstorm_test.go`, `seqscanstorm_integration_test.go`)
**Shows:** the CPU-saturation counterpart to `hotrowcontention` — where hotrowcontention burns CPU on *contention* (little real work), seqscan-storm burns it on *brute-force work*: `--jobs` per-worker `db.Connect` sessions loop `SELECT count(*) WHERE payload = $1` over a table whose **filter column has no index**, forcing a full `Seq Scan` every query. Key lessons: (1) force the Seq Scan realistically — seed `(id bigint PK, payload bigint)` with **no index on payload** and bind an always-empty predicate (`payload = 0`, seeded `payload ∈ [1,N]`), never `enable_indexscan=off`; `count(*)` keeps the result one row so cost burns inside the scan, not on result transfer (avoids drifting into `backend-killer`). (2) Determinism: each worker sets `max_parallel_workers_per_gather = 0` so one worker = one scan = one core; this SET is **functional, not cosmetic** — if it fails the worker is **skipped** (not run without it), and `sessions` counts only live (post-SET) workers (Decision 5, ADR-004-1 sizing-from-flags reaffirmed). (3) `--table-size` is a base-2 byte flag (kingpin `.Bytes()`, default `500MB`); seed rows = `size / bytesPerRowEstimate`, but the self-report panel uses the real `pg_relation_size` read **once** at seed time (ADR-002-3 — not per-tick) and reports **logical** bytes scanned (`queries × size`), never physical IO. (4) Mandatory but **best-effort** warm-up — one full `count(*)` pass (no `pg_prewarm`) before escalation so the CPU profile is honest from second one; warm-up failure warns and proceeds, ctx-cancel exits cleanly. (5) Integration must prove the plan, not just activity — `EXPLAIN` asserts `Seq Scan` and no `Index Scan`, with `SET max_parallel_workers_per_gather=0` on the **observer** connection before EXPLAIN (else PG15's default-2 could plan a `Gather` and false-fail); warm-up is proven via the deterministic captured log line, not a racy `seq_scan`-ordering check. Honest contract — degradation (CPU saturation / TPS collapse) observed **externally**, never instance death in noisia's log.

### Singleton serial driver alongside the `--jobs` worker fan-out
**File:** `checkpointstorm/` (incl. `checkpointstorm_test.go`, `checkpointstorm_integration_test.go`)
**Shows:** three net-new patterns layered on the established `--jobs` per-worker `db.Connect` fan-out. (1) A **singleton serial "forcer" driver goroutine** runs *alongside* the worker pool on its own dedicated connection (first of its kind — siblings only fan out symmetric workers): it issues a synchronous `CHECKPOINT` whenever a shared atomic crosses a rows-based threshold, is extracted into a named `runForcer` taking the `db.Conn` interface (unit-testable over a conn double, like `runScan`/`runChurn`), and its death is **degradation, not run failure** — unlike the worker pool's "zero live = error" rule, a lone singleton must not gate `Run` (best-effort one reconnect, else a loud warning + exit). The threshold reset uses `Add(-threshold)`, not `Store(0)`, to avoid dropping worker increments that land in the read→reset window. (2) A **functional startup precondition gate via a PG<15-safe catalog query** — checking the `CHECKPOINT` privilege (`rolsuper OR pg_checkpoint` membership) with `to_regrole('pg_checkpoint') IS NOT NULL` short-circuiting `pg_has_role` (a single portable expression, no `server_version_num` branch), read with `Query`+`Next/Scan` (no `QueryRow` on `db.Conn`); the query is a static literal (no `fmt.Sprintf` of role/version), asserted by a unit test that checks for absent format verbs since CI is PG15-only. The precondition fails the workload honestly at startup (functional gate, ADR-005-1 generalized), not warn-and-continue. (3) **Client-side command-latency self-report** — `flush=T` times the forcer's own `CHECKPOINT` call (`time.Since` incl. queue wait), the first client-timed latency signal in the slow line, instead of polling `pg_stat_bgwriter.checkpoint_write_time` (ADR-002-3, ADR-006-2). Connection budget gains one over the siblings: `jobs` workers + 1 seed + **1 forcer** + 1 cleanup.
