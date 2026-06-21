# Code Research: backend-killer workload

Research date: 2026-06-21. Target: add a NEW noisia workload package `backendkiller/`
(single-threaded, no pre-seeding, self-report escalation panel; mechanism = plan-cache /
prepared-statement leak in one session).

Note: `docs/decisions-log.md` and `docs/tech-debt.md` do not exist in this repo, so no ADRs
or tracked debt items apply.

---

## 1. Canonical workload package shape (`idlexacts/`)

A workload package is a single Go file `<name>.go` (package doc comment describing the
mechanism) plus a test file. Reference: `idlexacts/idlexacts.go`.

### Public surface (the only contract `cmd/` depends on)

- `type Config struct { Conninfo string; ... tuning fields }` — `idlexacts/idlexacts.go:34-43`.
  First field is always `Conninfo string`; other workloads add `Jobs uint16`, rate/time fields.
- `func (c Config) validate() error` — value receiver, lowercase/unexported, returns the first
  failing rule via `fmt.Errorf`. `idlexacts/idlexacts.go:46-60`.
- `type workload struct { config Config; logger log.Logger }` — unexported, implements
  `noisia.Workload`. `idlexacts/idlexacts.go:63-66`.
- `func NewWorkload(config Config, logger log.Logger) (noisia.Workload, error)` — calls
  `config.validate()` first, returns `nil, err` on failure, else `&workload{config, logger}, nil`.
  `idlexacts/idlexacts.go:69-75`.
- `func (w *workload) Run(ctx context.Context) error` — pointer receiver. Connects to Postgres,
  defers close, runs the loop. `idlexacts/idlexacts.go:78-98`.

`noisia.Workload` is the whole interface (`noisia.go`):
```go
type Workload interface { Run(context.Context) error }
```

### Run loop pattern

idlexacts uses a goroutine-per-job channel guard (`idlexacts/idlexacts.go:101-129`), but that is
the *multi-threaded* pattern. backend-killer is single-threaded, so the closer reference loops are
**rollbacks** and **tempfiles** (single conn/pool, rate-limited `for` loop, stop on ctx).

### Rate limiting via `golang.org/x/time/rate`

Only two workloads rate-limit, both via the same idiom (`rollbacks/rollbacks.go:119-140`,
`tempfiles/tempfiles.go:130-155`):
```go
limiter := rate.NewLimiter(rate.Limit(r), 1)   // r is float64 ops/sec, burst 1
for {
    if limiter.Allow() {
        // ... do one unit of work ...
    }
    select {
    case <-ctx.Done():
        return ...        // clean stop, return nil
    default:
    }
}
```
Note: this is a busy `Allow()` poll, NOT `limiter.Wait(ctx)`. No workload uses `Wait`. For
backend-killer's pacing you can follow this exact `Allow()` pattern (each allowed tick = issue one
more unique PREPARE). Import is `"golang.org/x/time/rate"`; `rate.Limit` is a type alias for float64.
`golang.org/x/time v0.11.0` in `go.mod:14`.

### Logger usage inside a workload

`log.Logger` is stored on the struct and passed down to loop helpers as a plain `log.Logger` arg
(see `startLoop(ctx, w.logger, ...)`). Mid-run recoverable problems use `Warnf` and continue;
fatal setup failures are returned as `error` from `Run`. Examples:
`idlexacts/idlexacts.go:118` (`log.Warnf("start idle transaction failed: %s", err)`),
`rollbacks/rollbacks.go:106` (`Infof` summary at worker end).

---

## 2. db package interfaces (`db/db.go`, `db/postgres.go`)

Three interfaces in `db/db.go`:

- `DB` (lines 10-15) — pool. `Begin`, `Exec(ctx,sql,args)->(int64,string,error)`,
  `Query(ctx,sql,args)->(pgx.Rows,error)`, `Close()`.
- `Tx` (lines 17-22) — `Commit`, `Rollback`, `Exec`, `Query`.
- `Conn` (lines 24-29) — single dedicated connection: `Begin`, `Exec`, `Query`,
  `Close() error`. **This is what backend-killer should use** (one session = one `db.Conn`).

`Exec` returns `(rowsAffected int64, commandTag string, err error)` — three values, not the raw
pgx tag (`db/postgres.go:54-61`, `:136-143`).

### Getting a single dedicated connection (the backend-killer case)

```go
conn, err := db.Connect(ctx, conninfo)   // db/postgres.go:113-122 -> pgx.Connect
// ... use conn.Exec / conn.Query ...
_ = conn.Close()                          // Conn.Close() returns error
```
`db.Connect` wraps `pgx.Connect` (a real single backend session — exactly what we want so that
ONE backend's RSS grows). `rollbacks/runWorker` (`rollbacks/rollbacks.go:96`) is the model: one
`db.Connect`, all work on that one conn, the same backend for the whole run.

Caveat observed: `rollbacks/runWorker` does NOT call `conn.Close()` (relies on process exit /
ctx). backend-killer should `defer conn.Close()` for cleanliness; `failconns` and `forkconns` do
close. The plan-cache leak persists for the connection's lifetime regardless.

### Pool option (only if a pool is genuinely needed)

`db.NewPostgresDB(ctx, conninfo) (DB, error)` — `db/postgres.go:17-40`. pgx v5 pools are lazy, so
it calls `pool.Ping(ctx)` to fail fast on bad conninfo. Sets `application_name=noisia`
(`db/postgres.go:23`). For backend-killer a pool is WRONG: a pool spreads queries across multiple
backends, defeating "one session inflates one backend." Use `db.Connect` (single `Conn`).

### Conninfo handling / never log it

`Conninfo` is passed as a plain string field on `Config` and handed to `db.Connect` /
`pgxpool.ParseConfig`. **It is never logged anywhere** in the codebase — grep confirms no workload
prints `Conninfo`. Errors are logged but not the DSN. backend-killer MUST keep this rule: never put
`c.Conninfo` into any log call.

pgx v5 query mechanics: `conn.Exec(ctx, sql, args...)` and `conn.Query(ctx, sql, args...)`; rows via
`rows.Next()` / `rows.Scan(&x)` / `rows.Close()` (see `targeting/targeting.go:13-31`).

---

## 3. CLI wiring (`cmd/main.go`, `cmd/app.go`)

Adding a workload touches BOTH files. There are effectively **5 edit sites**.

### How an existing workload is wired (idlexacts as template)

1. **Flag declarations** — `cmd/main.go:25-27`. kingpin/v2 syntax:
   ```go
   idleXacts           = kingpin.Flag("idle-xacts", "Run idle transactions workload").Default("false").Envar("NOISIA_IDLE_XACTS").Bool()
   idleXactsNaptimeMin = kingpin.Flag("idle-xacts.naptime-min", "Min transactions naptime").Default("5s").Envar("NOISIA_IDLE_XACTS_NAPTIME_MIN").Duration()
   ```
   Conventions: boolean enable flag = workload short name (`--idle-xacts`); tuning flags are dotted
   sub-flags (`--idle-xacts.naptime-min`). Every flag has `.Default(...)` and
   `.Envar("NOISIA_<UPPER_SNAKE>")`. Type-terminated builder: `.Bool()`, `.Duration()`,
   `.Float64()`, `.Uint16()`, `.String()`, `.Enum(...)`.
   Shared/global flags already exist: `--conninfo` (`main.go:22`), `--jobs` (`:23`),
   `--duration` (`:24`). backend-killer is single-threaded so it should NOT consume `--jobs`.

2. **`config` struct fields** — `cmd/app.go:18-47`. Add a `bool` enable field + tuning fields,
   matching names used in step 3.

3. **Populate `config{}` from parsed flags** — `cmd/main.go:59-88`. Add lines like
   `backendKiller: *backendKiller,` dereferencing each flag pointer.

4. **Launch block in `runApplication`** — `cmd/app.go:55-149`. Each workload is an
   `if c.<enable> { log.Info(...); wg.Add(1); go func(){ err := start<Name>Workload(ctx,c,log); if err != nil { log.Errorf(...) }; wg.Done() }() }`.
   Add one such block. `ctx` here is already wrapped with the shared `c.duration` timeout
   (`cmd/app.go:50: ctx, cancel := context.WithTimeout(ctx, c.duration)`), so the workload stops
   when `--duration` elapses. All workloads share ONE `WaitGroup` and `runApplication` blocks on
   `wg.Wait()` (`cmd/app.go:151`).

5. **`start<Name>Workload` helper** — bottom of `cmd/app.go` (e.g.
   `startIdleXactsWorkload`, `cmd/app.go:157-171`). Builds the package `Config{}` from `c`, calls
   `pkg.NewWorkload(cfg, logger)`, returns `workload.Run(ctx)`. Also add the package import to the
   import block `cmd/app.go:3-16`.

### Concrete step-by-step for backend-killer

- main.go: declare `backendKiller = kingpin.Flag("backend-killer", ...).Default("false").Envar("NOISIA_BACKEND_KILLER").Bool()`
  plus tuning flags, e.g. `--backend-killer.rate` (`.Float64()`, statements/sec pacing) and any
  growth/step flag. Avoid wiring `--jobs` into it.
- main.go: add `backendKiller: *backendKiller,` (and tuning fields) to the `config{}` literal.
- app.go: add `backendKiller bool` (+ tuning) to `type config struct`.
- app.go: add the `if c.backendKiller { ... go startBackendKillerWorkload ... }` block in
  `runApplication`.
- app.go: add `import "github.com/lesovsky/noisia/backendkiller"` and the
  `startBackendKillerWorkload(ctx, c, log)` helper.

Flag-name conventions confirmed: enable flag uses hyphenated short name; sub-flags use
`<name>.<param>`; envars `NOISIA_<UPPER_SNAKE>`; rate flags are `Float64()` named `<wl>.rate`
(rollbacks/tempfiles) — but forkconns/terminate use `Uint16()` for rate. Pick `Float64` for
fine-grained pacing consistent with rollbacks/tempfiles.

---

## 4. log package (escalation-panel output)

`log/log.go:4-11` — `Logger` interface: `Info/Infof/Warn/Warnf/Error/Errorf` (no `Debug`, no
structured fields). `log/zerolog.go` is the only impl (zerolog `ConsoleWriter` to stdout, RFC3339
timestamps, level from `--log-level`). Construct with `log.NewDefaultLogger(level)`
(`log/zerolog.go:21`).

For the escalation panel: there is no dedicated metrics/structured API — emit a periodic
`logger.Infof("...")` line. Pattern to follow: tempfiles prints a one-line summary at the end
(`tempfiles/tempfiles.go:99`). For a *live* panel, drive a `time.Ticker` (or reuse the rate loop)
inside `Run` and call `logger.Infof("mem: %s ▲ %s/s · %s", ...)` each cadence. Self-report values
(accumulated count of statements/bytes, current rate, uptime) must be computed in-process from
counters the workload increments — NOT queried from Postgres (no system-state polling per spec).

---

## 5. targeting package — NOT needed

`targeting/targeting.go` has one function: `TopWriteTables(db db.DB, n int) ([]string, error)`
(lines 9-32) — queries `pg_stat_user_tables` for the top-N most updated/deleted tables. Used by
idlexacts (and waitxacts) to pick victim tables for write-amplifying idle xacts.

backend-killer self-generates load (issues unique PREPARE statements; no real table needed) and
the interview confirms "no pre-seeding." **It does not need `targeting`.** Do not import it.
(Unique prepared statements can be built over a constant trivial query like `SELECT $1::int` with a
unique statement name each iteration — no table dependency.)

---

## 6. Test harness

### Per-package layout

Each package has TWO test files:
- `main_test.go` — exactly one line of body (`targeting/main_test.go:10`):
  ```go
  package <pkg>
  import ( "os"; "testing"; "github.com/lesovsky/noisia/internal/dbtest" )
  func TestMain(m *testing.M) { os.Exit(dbtest.RunMain(m)) }
  ```
  Present in all 9 workload/targeting packages (grep: deadlocks, forkconns, idlexacts, failconns,
  rollbacks, tempfiles, terminate, waitxacts, targeting).
- `<pkg>_test.go` — the actual tests.

### dbtest / TestConninfo

`internal/dbtest/dbtest.go:27-58` — `RunMain(m *testing.M) int` starts a throwaway
`postgres:15-alpine` testcontainer (db `noisia_fixtures`, user/pass `noisia`), waits for
"ready to accept connections" x2, sets `db.TestConninfo = dsn` (with `sslmode=disable`), runs
`m.Run()`, terminates the container on exit. Heavy testcontainers deps live only in this internal
package so they never reach the binary (package doc, `internal/dbtest/dbtest.go:1-4`).

`db.TestConninfo` is a package var (`db/testing.go:10`), populated at test time. Tests get a pool
via `db.NewTestDB()` (`db/testing.go:13-15`) or a conn via `db.Connect(ctx, db.TestConninfo)`.

### Test patterns (from `idlexacts/idlexacts_test.go`)

- **`TestConfig_validate`** — table test (`idlexacts_test.go:12-33`): slice of
  `{valid bool; config Config}`, loop with `assert.NoError` / `assert.Error`. Cover each
  validation rule with both a passing and failing case. Uses `github.com/stretchr/testify/assert`.
  Signature: `func TestConfig_validate(t *testing.T)`.
- **`TestWorkload_Run`** — integration (`idlexacts_test.go:35-55`): build `Config` with
  `Conninfo: db.TestConninfo`, `context.WithTimeout`, `NewWorkload(config, log.NewDefaultLogger("info"))`,
  `w.Run(ctx)` expects `NoError`; then set `config.Conninfo = "database=noisia_invalid"` and expect
  `Run` to `Error`. Signature: `func TestWorkload_Run(t *testing.T)`.
- Loop/helper tests pass `db.NewTestDB()` pools and short `WithTimeout` contexts directly.

For backend-killer (test the mechanics, not the catastrophe — per interview): assert the session
opens, the self-report counter grows monotonically over N iterations (capture two snapshots), and
`Run` returns `nil` on ctx cancel/timeout. Do NOT drive a real OOM in CI.

### Serial constraint `-p 1`

`Makefile:27-33`: `go test -race -timeout 300s -p 1 ...`. `-p 1` serializes package tests because
each package spins its own testcontainer and workloads mutate server-wide state. Tight per-test
timeouts (e.g. 2s) depend on this. New package's tests must keep short, deterministic timings.

---

## 7. Other relevant facts

- **Connection errors mid-run**: setup failures (connect, table prep) are returned as `error`
  from `Run` and surface in `cmd/app.go` as `log.Errorf("... workload failed: %s", err)`. Per-tick
  query errors are logged with `Warnf("... continue")` and the loop proceeds
  (`rollbacks` even *expects* every query to fail). For backend-killer: when the OOM finally kills
  the backend, the next `conn.Exec` will error with a broken-connection error — that is the SUCCESS
  signal in a real run, not a bug. The loop should detect `ctx.Err() == nil` before logging
  (tempfiles idiom, `tempfiles/tempfiles.go:141`) to avoid noisy ctx-cancel warnings.
- **Stop mechanism**: context only. Every loop has `case <-ctx.Done(): return nil`. The shared
  `--duration` timeout wraps the ctx in `runApplication` (`cmd/app.go:50`). No internal timers for
  total run length.
- **Rate types**: rollbacks/tempfiles use `float64` rate; forkconns/terminate use `uint16`.
  Naptime computed as `time.Second / time.Duration(rate)` (forkconns) OR `rate.NewLimiter`
  (rollbacks/tempfiles). Recommend `rate.NewLimiter(rate.Limit(r), 1)` + `Allow()` for pacing.
- **Stack versions** (`go.mod`): Go 1.25, pgx v5.10.0, kingpin/v2 v2.4.0, zerolog v1.19.0,
  golang.org/x/time v0.11.0, testify v1.11.1, testcontainers-go v0.43.0.
- **README**: workloads are listed in two places — the bullet list (`README.md:7-16`) and the
  impact table (`README.md:79-88`). Adding backend-killer should add a row to both
  (impact: **Yes** — can OOM-kill the instance). Not code, but part of "register a new workload."
- **Copyright header**: every workload `.go` starts with the 3-line BSD header
  (`idlexacts/idlexacts.go:1-3`). Match it.

---

## Files to create

- `backendkiller/backendkiller.go` — package doc + Config/validate/workload/NewWorkload/Run +
  single-connection plan-cache-leak loop + escalation panel.
- `backendkiller/main_test.go` — `TestMain` -> `dbtest.RunMain`.
- `backendkiller/backendkiller_test.go` — `TestConfig_validate` table test + `TestWorkload_Run`
  integration + helper tests asserting monotonic growth.

## Files to edit

- `cmd/main.go` — flag declarations (~line 48 area) + `config{}` literal (~line 88).
- `cmd/app.go` — import (line 3-16), `config` struct (line 18-47), launch block in
  `runApplication` (after line 149), `startBackendKillerWorkload` helper (after line 281).
- `README.md` — workload bullet (line 7-16) + impact table (line 79-88).
