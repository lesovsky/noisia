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
