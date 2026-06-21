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
- **Current harness:** `db/testing.go` hardcodes `TestConninfo = "host=postgres user=noisia database=noisia_fixtures"`. In CI a `postgres` service container provides that host; **locally this does not resolve** — a PostgreSQL reachable as host `postgres` must be provided.
- **Planned:** migrate the harness to **testcontainers-go** (`modules/postgres`) so the same test code runs identically locally and in CI. See [revival-plan.md](revival-plan.md).
- **Run:** `make test` → `go test -race -timeout 300s -coverprofile=... ./...`.

### Verifying a workload manually

Run the workload against a scratch PostgreSQL and confirm the expected symptom appears:
- idle transactions → `idle in transaction` rows in `pg_stat_activity`
- deadlocks → rising `pg_stat_database.deadlocks`
- temp files → rising `pg_stat_database.temp_files`
- failconns → new client connections rejected (`max_connections` exhausted)

### Local integration test baseline (verified)

Reproduce CI locally without code changes:
```bash
docker network create noisia-test
docker run -d --name noisia-pg --network noisia-test --network-alias postgres \
  -e POSTGRES_DB=noisia_fixtures -e POSTGRES_USER=noisia \
  -e POSTGRES_HOST_AUTH_METHOD=trust postgres:15-alpine
docker run --rm --network noisia-test -v "$PWD":/app -w /app \
  golang:1.19 go test -race -timeout 300s ./...
```

---

## Reference Implementations

### Driver abstraction
**File:** `db/db.go`, `db/postgres.go`
**Shows:** how workloads stay decoupled from pgx via `DB`/`Tx`/`Conn` interfaces. Primary touch point for the pgx v4→v5 migration.

### Workload package shape
**File:** `idlexacts/` (incl. `idlexacts_test.go`)
**Shows:** the canonical `Config` + `validate()` + `NewWorkload` + `Run` structure and its test layout (config table tests + DB-backed tests).
