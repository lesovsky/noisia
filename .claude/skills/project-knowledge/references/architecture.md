# Architecture

## Purpose
Technical architecture overview for AI agents. Helps agents understand HOW the system is built.

---

## Tech Stack

**Language:** Go (currently `go 1.19` in go.mod; target for the infra release is the current stable Go).
- **Why:** single static binary, trivial cross-compilation, first-class PostgreSQL drivers.

**PostgreSQL driver:** `github.com/jackc/pgx` (currently **v4 v4.6.0**; being migrated to **v5** — see [revival-plan.md](revival-plan.md)).
- **Why:** the most capable native PostgreSQL driver for Go; needed for low-level control over connections and protocol behaviour that the workloads exploit.

**CLI parser:** `gopkg.in/alecthomas/kingpin.v2` (unmaintained — flagged for replacement during the infra release).

**Logging:** `github.com/rs/zerolog`, wrapped behind the project's own `log.Logger` interface.

**Testing:** `github.com/stretchr/testify` + integration tests against a real PostgreSQL instance.

---

## Project Structure

The repo is a Go module (`github.com/lesovsky/noisia`) with one package per workload, plus shared infrastructure:

```
/
├── noisia.go          [root package: the Workload interface (Run(ctx) error)]
├── cmd/               [CLI entrypoint: main.go (flags via kingpin), app.go (wires & starts workloads)]
├── db/                [DB abstraction: DB/Tx/Conn interfaces, pgx-backed impl, test connection helper]
├── log/               [Logger interface + zerolog implementation]
├── targeting/         [helper to pick target tables in the database]
├── idlexacts/         [workload: idle transactions]
├── rollbacks/         [workload: rollbacks]
├── waitxacts/         [workload: waiting/locking transactions]
├── deadlocks/         [workload: deadlocks]
├── tempfiles/         [workload: temp-file spill]
├── terminate/         [workload: terminate/cancel backends]
├── failconns/         [workload: exhaust connections]
├── forkconns/         [workload: excessive backend forking]
├── Dockerfile         [multi-stage build → scratch image]
├── Makefile           [dep/lint/test/build/docker targets]
└── .github/workflows/ [CI: default.yml (lint+test), release.yml (test+docker+goreleaser)]
```

---

## Key Components

- **`Workload` interface** (`noisia.go`): the single contract — `Run(context.Context) error`. Every workload package implements it and exposes `NewWorkload(config, logger)`. Context is the kill switch (cancel/timeout stops the workload).
- **`db` package**: abstracts pgx behind `DB`, `Tx`, `Conn` interfaces so workloads depend on interfaces, not directly on pgx. This is the **main blast radius of the pgx v4→v5 migration** — most pgx API surface is concentrated here.
- **`cmd` package**: CLI; each workload is a separate boolean flag with its own tuning flags. `app.go` launches enabled workloads as goroutines under a shared timeout (`duration`) and waits on a `sync.WaitGroup`.
- **Dual use**: the workload packages are importable as a library; `cmd` is just one consumer.

---

## Key Dependencies

- `github.com/jackc/pgx` — PostgreSQL driver; the core of every workload. Migrating v4 → v5.
- `github.com/rs/zerolog` — structured logging behind the `log.Logger` interface.
- `golang.org/x/time` — rate limiting for workloads with a configurable rate.
- `gopkg.in/alecthomas/kingpin.v2` — CLI flag parsing (unmaintained; replacement candidate).

---

## External Integrations

**Target PostgreSQL instance**
- **Purpose:** every workload connects to a user-supplied PostgreSQL and generates activity against it. This is the only external dependency.
- **Auth method:** standard libpq connection string (`Conninfo`), passed via CLI flag or library config.

---

## Data Flow

CLI flags (or library `Config`) → enabled workloads constructed via `NewWorkload` → each `Run(ctx)` opens its own pgx connection(s) to the target PostgreSQL and issues the queries/transactions that produce its harmful state → effects are observed on the server side (e.g. `pg_stat_activity`, `pg_stat_database`, temp-file counters). A shared context timeout stops all workloads.

---

## Data Model

**Database:** Not applicable as owned schema. noisia does not own or migrate a schema; it connects to an arbitrary target PostgreSQL and exercises it. Some workloads create transient/temporary objects (e.g. temp tables) within their own transactions and roll them back.

### Sensitive Data

No PII stored. The only sensitive value handled is the target database connection string (may contain a password) — must never be logged.

---

## Architectural Invariants

- **Workloads depend on the `db` interfaces, not on pgx directly** — keeps the driver migration (v4→v5) contained to the `db` package. Workload packages should import pgx types only where unavoidable.
- **Context is mandatory** — every `Run` must honour `ctx` cancellation; no unbounded loops.
- **Connection strings must never be logged.**

Static-analysis enforcement (go-arch-lint / golangci-lint rules) is **not yet configured** beyond the existing `golangci-lint` setup; tightening it is part of the infra-release backlog.
