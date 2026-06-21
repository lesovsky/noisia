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

---

## Updated: 2026-06-21 — Tech-spec implementation details

Copy-pasteable, line-accurate facts for writing the tech-spec. All excerpts verified against
HEAD on 2026-06-21.

### 1. Single-threaded rate-limited loop idiom

There are TWO single-connection / pool templates. The **cleanest single-`db.Conn` template is
`rollbacks` `startLoop` (`rollbacks/rollbacks.go:111-141`)** — backend-killer should mirror this
shape (one `db.Conn`, `rate.NewLimiter` + `Allow()` busy-poll, `select{ctx.Done()}` stop):

```go
// rollbacks/rollbacks.go:111-141
func startLoop(ctx context.Context, conn db.Conn, r float64) (int, int, error) {
	table, err := createTempTable(ctx, conn)
	if err != nil {
		return 0, 0, err
	}

	var commits, rollbacks int

	limiter := rate.NewLimiter(rate.Limit(r), 1)   // burst = 1
	for {
		if limiter.Allow() {
			q, args := newErrQuery(table)
			_, _, err = conn.Exec(ctx, q, args...)
			if err != nil {
				rollbacks++
			} else {
				commits++
			}
		}

		select {
		case <-ctx.Done():
			return commits, rollbacks, nil   // clean stop -> nil
		default:
		}
	}
}
```

The `tempfiles` variant (`tempfiles/tempfiles.go:127-156`) is the same loop but spawns a goroutine
per tick (pool, async) — NOT what backend-killer wants. Its one extra-useful idiom is the
**`ctx.Err()==nil` guard before `Warnf`** to suppress ctx-cancel noise:

```go
// tempfiles/tempfiles.go:138-146
go func() {
	err := execQuery(ctx, pool)
	if err != nil && ctx.Err() == nil {            // <- guard
		log.Warnf("executing tempfiles query failed: %v, continue", err)
	}
	wg.Done()
}()
```

**rate==0 / unlimited handling — NOT supported by existing workloads.** Both rollbacks and
tempfiles `validate()` REJECT `Rate <= 0` (`rollbacks/rollbacks.go:48-50`,
`tempfiles/tempfiles.go:44-46`). So there is no in-repo precedent for `0 = unlimited`. The spec
requires `--backend-killer.rate` default `0` = no limit. Mechanism to implement it (the library
supports it natively): `rate.NewLimiter` accepts `rate.Inf` (`= Limit(math.MaxFloat64)`,
`golang.org/x/time@v0.11.0/rate/rate.go:22`), and an `Inf`-limit limiter's `Allow()` always
returns true (every event permitted). So:

```go
lim := rate.Limit(r)
if r <= 0 {
	lim = rate.Inf
}
limiter := rate.NewLimiter(lim, 1)
```

This keeps the SAME `Allow()`+`select{ctx.Done()}` loop shape for both throttled and unlimited
cases (no separate busy-loop branch), satisfying the spec edge case "rate=0 не должен
патологически крутить busy-loop — используется та же идиома цикла". `rate.Limit` is
`type Limit float64` (`rate.go:19`); `NewLimiter(r Limit, b int) *Limiter` (`rate.go:100`);
`Allow()` (`rate.go:109`). No workload uses `Wait(ctx)`.

**Per-tick error handling:** mid-run query errors → `Warnf(...continue)` and loop continues
(rollbacks even expects every query to fail). The `ctx.Err()==nil` guard (tempfiles) distinguishes
"real error" from "ctx cancelled". For backend-killer, a post-success `Exec` error while
`ctx.Err()==nil` IS the climax (backend died) → print culmination line, return nil.

### 2. db package exact signatures

- `func Connect(ctx context.Context, connString string) (Conn, error)` —
  `db/postgres.go:113-122`. Wraps `pgx.Connect(ctx, connString)`; returns a single real backend
  session (`*PostgresConn` behind the `Conn` interface). This is what backend-killer uses.
- `Conn` interface (`db/db.go:24-29`):
  ```go
  type Conn interface {
  	Begin(ctx context.Context) (Tx, error)
  	Exec(ctx context.Context, sql string, arguments ...interface{}) (int64, string, error)
  	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
  	Close() error
  }
  ```
- `Conn.Exec` impl (`db/postgres.go:136-143`): returns `(tag.RowsAffected(), tag.String(), err)` —
  `(int64, string, error)`. On any pgx error it returns `(0, "", err)`.
- `Conn.Close() error` (`db/postgres.go:150-152`): `c.conn.Close(context.Background())`.

**Detecting a broken/lost connection from an Exec error (pgx v5):** there is NO helper in this
codebase and the wrapper discards the typed pgx error structure — it only re-returns `err`. The
repo's own convention (see Run-loop section above) is exactly: **treat any `Exec` error that occurs
while `ctx.Err() == nil` as the failure/climax** — i.e. `if err != nil && ctx.Err() == nil`. The
codebase does not import `errors`/`github.com/jackc/pgx/v5/pgconn` for `PgError`/`IsClosed`
inspection anywhere in a workload loop. Recommendation for the tech-spec: do NOT try to classify
pgx error types; follow the established idiom — `ctx.Err()==nil` + non-nil `Exec` err on a
`PREPARE` ⇒ "connection lost ... likely OOM-restarted". (Note: the spec already prescribes the
cautious "likely" wording precisely because the client cannot reliably distinguish OOM from other
disconnects.)

**Existing single-`Conn` usage (the model):** `rollbacks/runWorker` (`rollbacks/rollbacks.go:93-108`):
```go
conn, err := db.Connect(ctx, config.Conninfo)   // one dedicated backend
if err != nil {
	return err                                   // setup failure -> error
}
commits, rollbacks, err := startLoop(ctx, conn, config.Rate)
```
`rollbacks/runWorker` does NOT `conn.Close()`. `forkconns/makeConnectionLoop`
(`forkconns/forkconns.go:95-108`) and `tempfiles/countTempBytes` (`tempfiles/tempfiles.go:181-186`,
`defer func(){ _ = conn.Close() }()`) DO close. backend-killer should `defer conn.Close()`.

### 3. Full small workload to mirror (rollbacks, minus the loop helpers)

`rollbacks/rollbacks.go:32-90` is the exact Config/validate/struct/NewWorkload/Run skeleton shape
(value-receiver `validate`, first-failing-rule `fmt.Errorf`, `NewWorkload` validates then returns
`&workload{config, logger}`):

```go
// rollbacks/rollbacks.go:32-69
type Config struct {
	Conninfo string
	Jobs     uint16
	Rate     float64
}

func (c Config) validate() error {
	if c.Jobs < 1 {
		return fmt.Errorf("jobs must be greater than zero")
	}
	if c.Rate <= 0 {
		return fmt.Errorf("rate must be positive")
	}
	return nil
}

type workload struct {
	config Config
	logger log.Logger
}

func NewWorkload(config Config, logger log.Logger) (noisia.Workload, error) {
	err := config.validate()
	if err != nil {
		return nil, err
	}
	return &workload{config, logger}, nil
}
```

**Defaults/zero-value validation convention:** `validate()` checks lower bounds with
`fmt.Errorf` (e.g. `Jobs < 1`, `Rate <= 0`, `Interval < 10*time.Millisecond` in
`terminate/terminate.go:50-58`). For backend-killer note the deliberate DIVERGENCE: `Rate` must
allow `0` (= unlimited per spec), so do NOT copy the `Rate <= 0` reject; validate `plan-size >= 1`
and `report-interval > 0` instead. The `workload` struct here has NO `pool`/`conn` field — it's
built fresh in `Run` (tempfiles adds a `pool db.DB` field, `tempfiles/tempfiles.go:52-56`, and
passes `nil` in `NewWorkload`; backend-killer can keep the simpler rollbacks shape and open the
conn inside `Run`).

### 4. CLI wiring — exact edit points (rollbacks as the template)

**`cmd/main.go` flag declarations** (inside `main()`'s `var(...)` block, ends at line 49) —
rollbacks lines `cmd/main.go:28-29`:
```go
rollbacks     = kingpin.Flag("rollbacks", "Run rollbacks workload").Default("false").Envar("NOISIA_ROLLBACKS").Bool()
rollbacksRate = kingpin.Flag("rollbacks.rate", "Rollbacks rate per second (per worker)").Default("1").Envar("NOISIA_ROLLBACKS_RATE").Float64()
```
Add the 5 backend-killer flags here (after line 48, before `)` on line 49):
- `--backend-killer` `.Default("false").Envar("NOISIA_BACKEND_KILLER").Bool()`
- `--backend-killer.rate` `.Default("0").Envar("NOISIA_BACKEND_KILLER_RATE").Float64()`
- `--backend-killer.plan-size` `.Default("<const>").Envar("NOISIA_BACKEND_KILLER_PLAN_SIZE").Int()`
- `--backend-killer.show-memory` `.Default("false").Envar("NOISIA_BACKEND_KILLER_SHOW_MEMORY").Bool()`
- `--backend-killer.report-interval` `.Default("1s").Envar("NOISIA_BACKEND_KILLER_REPORT_INTERVAL").Duration()`

Builder terminators available in this codebase: `.Bool()`, `.Float64()`, `.Uint16()`,
`.Duration()`, `.String()`, `.Enum(...)`. **`.Int()` is NOT currently used** in main.go (only
`Uint16` for integer flags) — kingpin/v2 does provide `.Int()`; if you prefer to stay within
already-used types use `.Int()` (valid in kingpin/v2 v2.4.0) for `plan-size`, or `Uint16()` if a
65535 cap is acceptable. (Recommend `.Int()` for plan-size headroom.)

**`cmd/main.go` config{} literal** — rollbacks entries `cmd/main.go:67-68`:
```go
rollbacks:             *rollbacks,
rollbacksRate:         *rollbacksRate,
```
Add `backendKiller: *backendKiller,` + the 4 tuning derefs in the `config{...}` literal
(`cmd/main.go:59-88`, closes at line 88).

**`cmd/app.go` config struct fields** — rollbacks fields `cmd/app.go:26-27`:
```go
rollbacks             bool
rollbacksRate         float64
```
Add the 5 fields to `type config struct` (`cmd/app.go:18-47`, closes at line 47).

**`cmd/app.go` launch if-block in `runApplication`** — rollbacks block `cmd/app.go:67-77`:
```go
if c.rollbacks {
	log.Infof("start rollbacks workload for %s", c.duration)
	wg.Add(1)
	go func() {
		err := startRollbacksWorkload(ctx, c, log)
		if err != nil {
			log.Errorf("rollbacks workload failed: %s", err)
		}
		wg.Done()
	}()
}
```
Add an analogous `if c.backendKiller { ... go startBackendKillerWorkload(...) ... }` block before
`wg.Wait()` (i.e. after the forkconns block which ends at line 149). `ctx` here is already wrapped
with `--duration` timeout at `cmd/app.go:50`.

**`cmd/app.go` start helper** — rollbacks helper `cmd/app.go:173-186`:
```go
func startRollbacksWorkload(ctx context.Context, c config, logger log.Logger) error {
	workload, err := rollbacks.NewWorkload(
		rollbacks.Config{
			Conninfo: c.postgresConninfo,
			Jobs:     c.jobs,
			Rate:     c.rollbacksRate,
		}, logger,
	)
	if err != nil {
		return err
	}
	return workload.Run(ctx)
}
```
Add `startBackendKillerWorkload` after the last helper (`startForkconnsWorkload` ends at line 281).
**Add the import** `"github.com/lesovsky/noisia/backendkiller"` to the import block
(`cmd/app.go:3-16`) — alphabetical position is between `failconns` (line 6) and `forkconns` (line 7).
Do NOT add `Jobs: c.jobs` to the backend-killer Config (single-threaded).

So: 6 distinct edit sites total (5 from prior research + the import line).

### 5. Duration-typed flags in kingpin/v2

Declared with the `.Duration()` terminator and a Go-duration string `.Default(...)`. Examples in
`cmd/main.go`:
- `duration = kingpin.Flag("duration", "Duration of tests").Default("10s").Envar("NOISIA_DURATION").Duration()` (`cmd/main.go:24`)
- `terminateInterval = kingpin.Flag("terminate.interval", "...").Default("1s").Envar("NOISIA_TERMINATE_INTERVAL").Duration()` (`cmd/main.go:39`)
- idlexacts naptime min/max `cmd/main.go:26-27` (`.Default("5s")`, `.Default("20s")`).

So `--backend-killer.report-interval` mirrors line 39 exactly:
```go
backendKillerReportInterval = kingpin.Flag("backend-killer.report-interval", "Escalation panel print interval").Default("1s").Envar("NOISIA_BACKEND_KILLER_REPORT_INTERVAL").Duration()
```
The config field type is `time.Duration` (see `terminateInterval time.Duration`, `cmd/app.go:36`).

### 6. Test patterns — exact

**`main_test.go` (one line of body)** — `rollbacks/main_test.go:1-10`:
```go
package rollbacks

import (
	"os"
	"testing"

	"github.com/lesovsky/noisia/internal/dbtest"
)

func TestMain(m *testing.M) { os.Exit(dbtest.RunMain(m)) }
```
(backend-killer: same, `package backendkiller`.)

**`TestConfig_validate` table test** — `rollbacks/rollbacks_test.go:12-29`:
```go
func TestConfig_validate(t *testing.T) {
	testcases := []struct {
		valid  bool
		config Config
	}{
		{valid: true, config: Config{Jobs: 1, Rate: 1}},
		{valid: false, config: Config{Jobs: 0, Rate: 1}},
		{valid: false, config: Config{Jobs: 1, Rate: 0}},
	}

	for _, tc := range testcases {
		if tc.valid {
			assert.NoError(t, tc.config.validate())
		} else {
			assert.Error(t, tc.config.validate())
		}
	}
}
```
NOTE for backend-killer: `Rate: 0` must be a VALID case (unlimited), unlike rollbacks. Cover
`plan-size < 1` and `report-interval <= 0` as the invalid cases instead.

**`TestWorkload_Run` integration** — `rollbacks/rollbacks_test.go:51-61`:
```go
func TestWorkload_Run(t *testing.T) {
	config := Config{Conninfo: db.TestConninfo, Jobs: 2, Rate: 2}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	w, err := NewWorkload(config, log.NewDefaultLogger("info"))
	assert.NoError(t, err)
	err = w.Run(ctx)
	assert.Nil(t, err)
}
```
Imports used by the test file (`rollbacks/rollbacks_test.go:3-10`): `context`, `db`, `log`,
`github.com/stretchr/testify/assert`, `testing`, `time`. `db.TestConninfo` is the testcontainer DSN
(`db/testing.go:10`, populated by `dbtest.RunMain`). Pattern uses `context.WithTimeout` ~1s
(serialized by `-p 1`).

**Asserting an observable effect** — `rollbacks/rollbacks_test.go:70-81` (`Test_startLoop`) asserts
the loop's RETURN counters (`assert.Equal(t, 0, c)`, `assert.Positive(t, r)`), and
`Test_createTempTable` (lines 83-92) asserts the side-effect via a returned value. For
backend-killer the spec wants **monotonic growth of the in-process prepared-stmt counter over N
iterations** — mirror `Test_startLoop`: expose the loop helper so it returns the final counter (or
take two snapshots), open a `db.Connect(context.Background(), db.TestConninfo)` conn directly, run a
~1s `WithTimeout` ctx, and `assert.Positive` on the count. Do NOT drive a real OOM.

`Makefile:27-33` runs `go test -race -timeout 300s -p 1 ./...` — keep timings short/deterministic.

### 7. "Heavy plan" construction — feasibility + simplest form

No query-building helpers exist for this (rollbacks builds literal SQL inline with `fmt.Sprintf`,
`rollbacks/rollbacks.go:157-236`; forkconns/tempfiles use constant SQL strings). This is the one
genuinely new piece — build it inline with `strings.Builder` / `fmt`.

Feasibility: confirmed. A server-side `PREPARE <unique_name> AS SELECT <N expressions>` is a plain
`conn.Exec(ctx, sql)` (literal, NOT parameterized — the statement name and the target-list width
must be in the SQL text). Each `PREPARE` adds a cached plan to the backend's `CacheMemoryContext`;
never `DEALLOCATE` ⇒ plan cache (and backend RSS) grows monotonically. The `plan-size` int scales
the number of target-list expressions, so each cached plan is heavier.

Simplest construction (unique name from a monotonic counter `i`, width from `plan-size = n`):
```go
// pseudo: build "PREPARE noisia_bk_<i> AS SELECT 1 AS c0, 1 AS c1, ... , 1 AS c<n-1>"
var b strings.Builder
fmt.Fprintf(&b, "PREPARE noisia_bk_%d AS SELECT ", i)
for j := 0; j < n; j++ {
	if j > 0 {
		b.WriteString(", ")
	}
	fmt.Fprintf(&b, "%d AS c%d", j, j)   // constant exprs; no FROM, no table needed
}
conn.Exec(ctx, b.String())
```
Notes:
- Use literal SQL (no `$1` args): `PREPARE` itself defines parameters, so passing pgx args here is
  wrong. Keep it a pure DDL-style `Exec` with no args (like `createTempTable`,
  `rollbacks/rollbacks.go:148`).
- A trivial constant target-list (`SELECT 1 AS c0, ...`) needs no table — matches the spec's
  "no pre-seeding / self-generated load". Distinct prepared-statement NAMES (from the monotonic
  counter) guarantee uniqueness across millions of statements; the SELECT body can be identical.
- `--show-memory` reads OWN backend via
  `SELECT sum(used_bytes) FROM pg_backend_memory_contexts` (PG 14+) using
  `conn.Query`/`rows.Scan` (pattern: `tempfiles/countTempBytes`, `tempfiles/tempfiles.go:188-198`).
  On PG < 14 this errors (relation missing) → degrade softly (skip the mem field, `Warnf` once).
  A memory-read error is per-tick recoverable and must NOT trigger the "likely OOM-restarted"
  culmination — only a `PREPARE` Exec failure does.

### README rows to add (exact tables)

- Bullet list `README.md:7-16`: add e.g.
  `- `backend-killer` - one session leaks prepared statements (plan-cache growth) until the backend
  OOMs and the instance restarts.`
- Impact table `README.md:79-88` (alphabetical, rows are `| name | impact |`): add
  `| backendkiller | **Yes**: a single session can grow backend RSS until OOM-kill and full
  instance restart |`. (Existing rows use the package name, e.g. `forkconns`, `idlexacts`.)
