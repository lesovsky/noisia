---
created: 2026-06-21
status: approved
branch: 001-feat-backend-killer
size: M
---

# Tech Spec: backend-killer

## Solution

Add a new noisia workload package `backendkiller` that mirrors the canonical workload shape
(`Config` + `validate()` + `NewWorkload` + `Run(ctx)`). It opens ONE dedicated connection via
`db.Connect` (not the pool) and, in a single-threaded rate-limited loop, issues unique literal
server-side `PREPARE noisia_bk_<i> AS SELECT 0 AS c0, 1 AS c1, …` statements through
`Conn.Exec`. The backend's plan cache grows without bound → RSS climbs → OOM killer reaps the
backend → postmaster restarts the instance. The loop maintains an in-process counter and emits
a self-report "escalation panel" via `logger.Infof` every `report-interval`; on the next `Exec`
error while the context is still active, it logs the climax line and returns `nil`. The workload
is wired into the CLI as a new boolean flag plus tuning flags, launched like the other workloads
under the shared `--duration` context.

## Architecture

### What we're building/modifying

- **`backendkiller/backendkiller.go`** (new) — the workload: `Config`, `validate()`,
  `NewWorkload`, the `workload` struct, `Run(ctx)`, the heavy-`PREPARE` builder, the escalation
  panel, the optional own-backend memory read, and climax detection.
- **`backendkiller/backendkiller_test.go`** + **`backendkiller/main_test.go`** (new) — config
  table test and a DB-backed mechanics test (counter growth + clean ctx stop), via testcontainers.
- **`cmd/main.go`** (modify) — declare 5 flags (`--backend-killer` + `.rate`, `.plan-size`,
  `.show-memory`, `.report-interval`) with `.Default()`/`.Envar()`, and add entries to the
  `config{}` literal.
- **`cmd/app.go`** (modify) — add the package import, the `config` struct fields, the
  `if c.backendKiller { … }` launch block in `runApplication`, and the `startBackendKillerWorkload`
  helper.
- **`README.md`** (modify) — add the workload to the bullet list and the impact table (impact = Yes).

### How it works

1. CLI flags → `cmd/app.go` builds `backendkiller.Config` → `NewWorkload` (validates) → launched
   as a goroutine under the shared `--duration` context + `WaitGroup`, exactly like other workloads.
2. `Run(ctx)` calls `db.Connect(ctx, conninfo)` for a single dedicated `Conn`. A connect failure
   is returned as an init error.
3. Loop (mirrors `rollbacks.startLoop`): a `rate.Limiter` gates each iteration; on `Allow()` the
   workload sends one unique `PREPARE` and increments the counter. `select { case <-ctx.Done(): return nil; default: }`
   is the only stop path.
4. A ticker every `report-interval` logs `backend-killer: prepared stmts=N rate=R/s elapsed=T`
   (rate computed from the in-process counter delta). The report ticker runs in a SEPARATE goroutine
   from the `PREPARE` loop and reads ONLY the atomic counter (no DB call) — so the panel keeps firing on
   cadence even if an `Exec` blocks under memory pressure (when timely output matters most). The pgx
   `Conn` is single-use (not concurrency-safe), so the optional `--show-memory` read
   (`SELECT sum(used_bytes) FROM pg_backend_memory_contexts`) is performed by the LOOP goroutine (the
   Conn owner) on its report cadence and published via an atomic for the ticker to print — never issued
   from the ticker goroutine.
5. When the backend is OOM-killed, the next `Conn.Exec` returns an error. If `ctx.Err()==nil`,
   the workload logs `connection lost after T, N statements issued — target likely OOM-restarted`
   and returns `nil` (success in a real run). If the error is a non-connection query error, it is
   a per-tick recoverable error: `Warnf` and continue.

## Decisions

### Decision 1: Package name `backendkiller`
**Decision:** Go package/directory is `backendkiller` (no hyphen); the CLI flag stays `--backend-killer`.
**Rationale:** Go identifiers/import paths can't contain hyphens; matches existing one-word packages
(`failconns`, `forkconns`, `idlexacts`).
**Alternatives considered:** `backend_killer` (un-idiomatic for Go packages) — rejected.

### Decision 2: `rate=0` means unlimited via `rate.Inf`
**Decision:** `Config.Rate` accepts `0` (= unlimited); implemented with `rate.NewLimiter(rate.Inf, 1)`
when `Rate==0`, otherwise `rate.NewLimiter(rate.Limit(Rate), 1)`. `validate()` allows `Rate>=0`.
**Rationale:** The user-spec requires `0=unlimited`. With `rate.Inf`, `Allow()` is always true, so the
SAME loop serves both throttled and unlimited modes — no separate code path. Note: unlimited mode is a
deliberate max-rate send loop (it issues a `PREPARE` on every iteration — real work, not an idle spin),
so it will use roughly one CPU core; this is intended for a "killer" workload, not the pathological
empty busy-spin the user-spec warned against. Operators throttle with `--backend-killer.rate`.
**Alternatives considered:** Mirror `rollbacks`/`tempfiles` which reject `Rate<=0` — rejected (would
forbid the required unlimited mode). A dedicated busy-loop branch for unlimited — rejected (duplicate path).

### Decision 3: Heavy plan = wide literal target-list
**Decision:** Build `PREPARE noisia_bk_<i> AS SELECT 0 AS c0, 1 AS c1, … , (N-1) AS c{N-1}` (no bind
args), N = `plan-size`; `<i>` is a monotonic counter for uniqueness. Executed via `Conn.Exec`.
**Rationale:** No existing helper; this is the one genuinely new piece. Width of the target-list scales
per-statement plan memory; literal constants keep it arg-free and dependency-free, like
`tempfiles.createTempTable`.
**Alternatives considered:** pgx-level prepared statements — rejected (we need server-side plan-cache
growth on one backend, not driver-side). A table/seed-based query — rejected (no pre-seeding by design).

### Decision 4: `plan-size` default
**Decision:** Default `plan-size = 1000` (target-list expressions); tunable via the flag.
**Rationale:** An empirical starting point giving meaningful per-statement memory while keeping each
`PREPARE` fast enough to sustain a high creation rate. Operators raise/lower it per stand.
**Alternatives considered:** A larger default (slower per-parse, fewer stmts/s) or tiny default (slow
memory growth) — rejected as defaults; left to the operator via the flag.

### Decision 5: Connection-loss = climax, no error typing — but only after first success
**Decision:** A `Conn.Exec` error while `ctx.Err()==nil` is treated as the climax (log the climax line,
return `nil`) ONLY if at least one `PREPARE` has already succeeded (counter > 0). If the FIRST `Exec`
fails (counter == 0), return it as an error instead — a builder/setup defect must not masquerade as a
successful OOM event. No pgx error-type classification.
**Rationale:** The `db` wrapper (`db/postgres.go`) returns an untyped `err`; the repo convention is
"any Exec error under a live context = failure/culmination". noisia cannot tell OOM from a network blip
or external terminate — hence the user-spec's "likely" wording. Guarding on counter>0 surfaces a broken
heavy-`PREPARE` builder (which would otherwise exit silently as `stmts=0` "climax").
**Alternatives considered:** Inspect pgx error types to confirm OOM — rejected (wrapper loses the type;
unreliable; over-engineered). Treat the first-Exec failure as a climax too — rejected (masks builder bugs).

### Decision 6: `--show-memory` is best-effort, same session
**Decision:** When enabled, a periodic `SELECT sum(used_bytes) FROM pg_backend_memory_contexts` runs on
the SAME (single) connection, issued by the LOOP goroutine that owns the `Conn` (the pgx `Conn` is not
concurrency-safe, so the report-ticker goroutine never queries it — it only prints the last value
published via an atomic). A read failure is a per-tick recoverable error (skip the field, `Warnf` once,
continue); only a broken `PREPARE` connection triggers the climax. On PG<14 (relation absent) degrade
gracefully: warn once, drop the memory field, keep the counter.
**Timing:** the loop goroutine has no ticker of its own (it is driven by `rate.Limiter.Allow()`), so it
gates the memory read by elapsed time — after a `Conn.Exec` it checks `time.Since(lastMemoryRead) >= ReportInterval`,
and if due, runs the memory `SELECT`, publishes the value to the atomic, and resets the timestamp.
**Rationale:** Reads the workload's OWN backend (not external monitoring); preserves the self-report
principle with the counter as the crash-surviving primary.
**Alternatives considered:** Polling external memory views / OS — rejected (external monitoring, against
the self-report principle).

### Decision 7: Single-threaded, no `--jobs`, no pre-seeding
**Decision:** One connection, one loop; do not wire `--jobs`; generate load in-session.
**Rationale:** OOM must hit a single backend — multiple backends would spread the plan cache. Matches
the user-spec.
**Alternatives considered:** Multi-connection via `--jobs` like `idlexacts` — rejected (spreads load
across backends, none reaches OOM). Pre-seeding a table to query — rejected (load is self-generated by design).

### Decision 8: Set `application_name=noisia` on the dedicated connection
**Decision:** Ensure the dedicated connection reports `application_name=noisia` (append it to the conninfo
if absent, or `SET application_name` after connect), so the offending backend is attributable in
`pg_stat_activity`.
**Rationale:** `db.NewPostgresDB` (pool path) sets `application_name=noisia`, but `db.Connect` does not.
For a deliberately destructive workload, attribution is valuable for incident response — and it is also a
teaching asset: juniors can locate the culprit backend in `pg_stat_activity` during the demo.
**Alternatives considered:** Leave it unset like raw `db.Connect` — rejected (anonymous backend, harder to
attribute). Modify `db.Connect` itself — rejected (out of scope; do it locally in the workload).

### Decision 9: No `Conninfo` in any log line, including wrapped errors
**Decision:** No log statement interpolates `Conninfo`. The two error paths that log a driver error
(connect failure at start, per-tick `Exec` error) must not echo a raw error that could embed the DSN:
wrap/sanitize so the password fragment cannot leak (e.g. log a fixed message + sanitized cause, never the
conninfo string).
**Rationale:** `db.Connect` wraps `pgx.Connect` without scrubbing; a raw error could carry DSN fragments
(`password=…`) into logs (CWE-532). The user-spec mandates `Conninfo` is a secret.
**Alternatives considered:** Trust pgx not to echo the password — rejected (not guaranteed; cheap to be safe).

## Data Models

`backendkiller.Config`:
```go
type Config struct {
    Conninfo       string        // target PostgreSQL conninfo (secret, never logged)
    Rate           float64       // PREPAREs/sec; 0 = unlimited (rate.Inf)
    PlanSize       int           // target-list width per PREPARE (plan heaviness); default 1000
    ShowMemory     bool          // append own-backend memory estimate (pg_backend_memory_contexts)
    ReportInterval time.Duration // escalation panel cadence; default 1s
}
```
`validate()` rules: `Conninfo` non-empty (per existing convention); `Rate >= 0`; `PlanSize >= 1`;
`ReportInterval > 0`.

`application_name=noisia` is not a config field — it is a fixed value the workload ensures on its
dedicated connection (Decision 8). The escalation-panel value (own-backend memory) is held in an atomic
so the report-ticker goroutine can read it without touching the `Conn`.

## Dependencies

### New packages
- None.

### Using existing (from project)
- `github.com/lesovsky/noisia/db` — `db.Connect(ctx, conninfo) (Conn, error)`, `Conn.Exec`.
- `golang.org/x/time/rate` — `rate.NewLimiter`, `rate.Inf`, `rate.Limit`, `Allow()`.
- `github.com/lesovsky/noisia/log` — `Logger` (`Infof`/`Warnf`).
- `github.com/alecthomas/kingpin/v2` — flag declarations in `cmd/`.
- `internal/dbtest` + `db.TestConninfo` — integration test harness (testcontainers).

## Testing Strategy

**Feature size:** M

### Unit tests
- `TestConfig_validate` (table test): valid (`Rate=0`, `Rate>0`, `PlanSize>=1`, `ReportInterval>0`);
  invalid (`PlanSize<1`, `ReportInterval<=0`, empty `Conninfo`, negative `Rate`).
- Heavy-`PREPARE` builder: a given `PlanSize` yields a well-formed statement with the right number of
  target-list expressions and a unique name.

### Integration tests
- `TestWorkload_Run` (testcontainers, `postgres:15-alpine` via `dbtest.RunMain`): with a short
  context timeout and a throttled `Rate`, the in-process prepared-statement counter grows monotonically
  over N iterations and `Run` returns `nil` on ctx cancel/timeout. Does NOT drive a real OOM.
- Invalid-conninfo case: `Run`/`NewWorkload` returns an init error.
- `--show-memory` happy path on PG 15 (≥14): the memory read returns a value and the panel includes it.
  (Graceful PG<14 degradation is not container-tested — noted as a manual/known limitation.)

### E2E tests
- None — CLI utility; integration + manual stand run suffice.

## Agent Verification Plan

**Source:** user-spec "Как проверить" section.

### Verification approach
Beyond automated tests, the agent builds the binary, confirms the new flags are registered, and runs
lint + the full test suite serially.

### Per-task verification
| Task | verify: | What to check |
|------|---------|--------------|
| 1 | bash | `go test -race -p 1 ./backendkiller/` green (config table test + mechanics test) |
| 2 | bash | `go build ./...` ok; `noisia --help` lists `--backend-killer[.rate/.plan-size/.show-memory/.report-interval]` |
| 3 | bash | `grep -n backend-killer README.md` shows the bullet + impact-table entry |
| 4 | bash | `make lint` (0 issues) and `make test` green across all packages |

### Tools required
bash only. No Playwright/MCP.

## Backward Compatibility

N/A — adding new code only. The CLI gains new opt-in flags; no existing flag, public function, or
behavior changes. **Breaking changes:** no.

## Risks

| Risk | Mitigation |
|------|-----------|
| Destructive: can crash a real instance | README disclaimer (existing project stance) + impact-table entry (Yes); user responsible |
| Real OOM would break CI | Tests verify mechanics only (counter growth + clean ctx stop); no real OOM in CI |
| `rate=0` unlimited mode spins a tight loop (pegs ~1 CPU core sending PREPAREs as fast as possible) and competes with other co-running noisia workloads | Intended for a "killer" workload; documented. Operators throttle with `--backend-killer.rate` |
| `plan-size` default may be too slow/fast on a given stand | Tunable via `--backend-killer.plan-size`; default documented as empirical |
| Very large `plan-size` builds a huge statement string → can exhaust noisia's OWN client memory | Documented as operator responsibility (project's no-guardrails stance); `plan-size` is the operator's dial |
| `Conninfo` leakage in logs, incl. indirectly via wrapped connect/`Exec` errors (CWE-532) | Decision 9: no log line interpolates `Conninfo`; error paths log a fixed message + sanitized cause, never the DSN |
| Offending backend not attributable in `pg_stat_activity` | Decision 8: set `application_name=noisia` on the dedicated connection |
| First-`Exec` failure (builder bug) masquerading as a successful OOM | Decision 5: an `Exec` error with counter==0 is returned as an init error, not a climax |
| `--show-memory` on PG<14 | Graceful degradation: warn once, drop memory field, keep counter |

## Acceptance Criteria

- [ ] `backendkiller` package implements `Config`/`validate()`/`NewWorkload`/`Run(ctx)` per the canonical shape.
- [ ] Uses a single `db.Connect` connection (not the pool); single-threaded; no pre-seeding.
- [ ] Issues unique literal server-side `PREPARE` via `Conn.Exec`; in-process counter grows monotonically.
- [ ] `Rate=0` ⇒ unlimited (`rate.Inf`); `Rate>0` ⇒ throttled; `validate()` enforces `Rate>=0`, `PlanSize>=1`, `ReportInterval>0`.
- [ ] Escalation panel logged every `report-interval` via `logger.Infof`; `--show-memory` appends own-backend memory (PG14+, graceful below).
- [ ] Connect failure at start ⇒ init error; mid-run `Exec` error under live ctx with counter>0 ⇒ climax line + return `nil`; first-`Exec` error with counter==0 ⇒ returned as init error; non-connection query error ⇒ `Warnf` + continue.
- [ ] Report ticker runs in its own goroutine reading the atomic counter only; the `--show-memory` query is issued by the loop goroutine (Conn owner), never concurrently on the shared `Conn`.
- [ ] `application_name=noisia` is set on the dedicated connection (attributable in `pg_stat_activity`).
- [ ] No log line interpolates `Conninfo`, including wrapped connect/`Exec` errors (fixed message + sanitized cause).
- [ ] Heavy-`PREPARE` builder consumes only validated ints (`PlanSize>=1`, `%d`-formatted name/expressions) — covered by a unit test (no injection surface).
- [ ] Clean stop on ctx cancel/timeout (returns `nil`).
- [ ] 5 CLI flags wired in `cmd/main.go` + `cmd/app.go` with `.Default()`/`.Envar()`; no `--jobs`.
- [ ] README updated (bullet list + impact table = Yes).
- [ ] All tests pass (`make test`, `-p 1`); `make lint` clean; no regressions in existing packages.

## Implementation Tasks

### Wave 1 (независимые)

#### Task 1: Implement `backendkiller` workload package ✅ done
- **Description:** Create the `backendkiller` package implementing the canonical workload shape and the
  backend-killer behavior: single dedicated `db.Connect`, single-threaded rate-limited loop issuing
  unique heavy literal `PREPARE` statements, the escalation panel, optional own-backend memory line, and
  connection-loss climax detection. Honor all Decisions, including `application_name=noisia` on the
  connection (D8), no `Conninfo` in any log/error (D9), the separate report-ticker goroutine reading an
  atomic (D6), and the first-`Exec` guard that returns an init error when counter==0 (D5). Include the
  config table test and a testcontainers mechanics test (counter growth + clean ctx stop), per the
  Decisions and Testing Strategy.
- **Skill:** code-writing
- **Reviewers:** dev-code-reviewer, dev-security-auditor, dev-test-reviewer
- **Verify:** bash — `go test -race -p 1 ./backendkiller/`
- **Files to modify:** `backendkiller/backendkiller.go`, `backendkiller/backendkiller_test.go`, `backendkiller/main_test.go`
- **Files to read:** `rollbacks/rollbacks.go`, `tempfiles/tempfiles.go`, `rollbacks/rollbacks_test.go`, `db/db.go`, `db/postgres.go`, `noisia.go`, `log/log.go`

### Wave 2 (зависит от Wave 1)

#### Task 2: Wire backend-killer into the CLI ✅ done
- **Description:** Register the workload in the CLI: declare the 5 flags with defaults/envars and the
  `config{}` literal entries in `cmd/main.go`, and add the package import, `config` struct fields, the
  launch `if`-block under the shared `--duration` context, and the start helper in `cmd/app.go`. No
  `--jobs`.
- **Skill:** code-writing
- **Reviewers:** dev-code-reviewer, dev-security-auditor, dev-test-reviewer
- **Verify:** bash — `go build ./...` and `noisia --help` lists the new flags
- **Files to modify:** `cmd/main.go`, `cmd/app.go`
- **Files to read:** `cmd/main.go`, `cmd/app.go`, `backendkiller/backendkiller.go`

#### Task 3: Update README ✅ done
- **Description:** Document the new workload in `README.md`: add it to the supported-workloads bullet
  list and add a row to the impact table with impact = Yes (can crash the instance via OOM). Note the
  operational caveat that a very large `plan-size` makes each `PREPARE` heavy/slow.
- **Skill:** documentation-writing
- **Reviewers:** dev-code-reviewer
- **Verify:** bash — `grep -n backend-killer README.md`
- **Files to modify:** `README.md`
- **Files to read:** `README.md`, `docs/features/001-feat-backend-killer/001-feat-backend-killer.md`

### Final Wave

#### Task 4: Pre-deploy QA
- **Description:** Acceptance testing: run `make lint` + full `make test` (serial), and verify the
  acceptance criteria from the user-spec and this tech-spec (flags present, mechanics test green, no
  regressions). No deploy (releases are tag-driven and out of scope for this feature).
- **Skill:** pre-deploy-qa
- **Reviewers:** none
