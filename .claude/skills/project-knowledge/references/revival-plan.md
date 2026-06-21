# Infra Release / Revival Plan

## Purpose
Tracks the current initiative: an **infrastructure release** that brings noisia's
dependencies up to date and closes known CVEs, before any new features are added.
New workloads are deliberately deferred until the base is healthy.

Started: 2026-06-21.

---

## Goal

Modernize the project foundation so new features can be built on a healthy base:
- bump Go and all dependencies to current versions,
- close all reachable CVEs,
- migrate the PostgreSQL driver pgx v4 → v5,
- make the integration test harness portable (local == CI),
- modernize CI/release pipeline.

No new workloads in this release.

---

## Key Decisions (ADR-style)

- **Migrate pgx v4 → v5.** noisia is a CLI/utility, not a 24/7 service, so breaking
  changes in the driver API are acceptable. v5 is also the **only** way to close
  `GO-2026-4518` (DoS in `pgproto3/v2`, which has **no fix in the v2 branch**).
- **Tests-first ("characterization / golden master").** The existing integration
  tests are the safety net. Order: get them green on v4 → migrate harness → migrate
  pgx → then modernize. Never change the test harness and the driver at the same time.
- **Integration tests via testcontainers-go** (`modules/postgres`) for identical
  local and CI runs. Accepted trade-off: testcontainers pulls a large *test-only*
  dependency tree (docker SDK) that govulncheck may flag — it does not ship in the
  binary, and we will NOT split a separate test module for it (overkill).
- **Test isolation:** workloads mutate server-wide state (`pg_stat_*`, temp files),
  so tests run **serialized** against a **shared container** (no `t.Parallel()`),
  with `pg_stat_reset()` between tests where needed. Container-per-test only where
  a shared container proves insufficient.

---

## Plan (steps)

1. **Step 0 — Baseline.** Run existing tests as-is on v4; confirm green. ✅ DONE
2. **Step 1 — Test harness → testcontainers-go.** Test-only change; baseline stays the
   oracle (tests must remain green).
3. **Step 2 — Migrate pgx v4 → v5** under green tests. Blast radius concentrated in `db/`.
4. **Step 3 — Modernize deps & toolchain:** current Go, testify, zerolog, x/time,
   replace unmaintained `kingpin.v2` CLI parser, update golangci-lint.
5. **Step 4 — CI/release cosmetics:** `actions/checkout` & `setup-go` to current major,
   drop `go-version: 1.15` in goreleaser job, goreleaser `--rm-dist` → `--clean`,
   Dockerfile base `golang:1.16` → current, golangci-lint version bump.
6. **Verify:** `govulncheck` clean on reachable vulns; full green CI; smoke-run binary.

---

## Progress

- ✅ Repo cloned to `/home/lesovsky/Git/github.com/lesovsky/noisia`.
- ✅ Builds on Go 1.25.10 (host) with the old deps unchanged.
- ✅ **Baseline green** — all 9 workload test packages pass under `golang:1.19` +
  `postgres:15-alpine` (CI replica).
- ✅ CVE surface measured with govulncheck (snapshot below).
- ✅ **Step 1 — testcontainers harness DONE.** `db.TestConninfo` is now a `var`
  populated at test time; container logic lives in `internal/dbtest` (imported only
  from `*_test.go`, so it never enters the binary — verified). Each test package has
  a one-line `TestMain` calling `dbtest.RunMain`. Tests run with `go test` on any host
  with Docker (local == CI), green under `-race -p 1` (stable across repeated runs).
- ⬜ Step 2 (pgx v4 → v5) — next.

### Step 1 notes / findings

- **Go directive bumped to 1.25.0** (required by testcontainers-go v0.43). This pulls
  part of Step 3 forward; acceptable, baseline protects us. testify→1.11, x/text→0.37
  (the latter already closes GO-2021-0113).
- **`-p 1` is mandatory** (baked into `make test`). Each package starts its own PG
  container and workloads mutate server-wide state, so packages must run serially;
  parallel runs also broke a 20ms timing in `idlexacts/Test_startSingleIdleXact`.
- **Q13 regression fixed (real utility bug).** `rollbacks.newErrQuery` case 13 used
  `::numeric(1,2)`, which PostgreSQL 15 made *valid* (PG15 relaxed numeric scale), so
  on an empty table the query succeeded instead of erroring → a commit instead of a
  rollback. Replaced with `::numeric(1001,2)` (invalid precision, fails at parse time
  on every PG version). `Test_startLoop` count assertion relaxed (`r > 0` instead of
  exactly 2) since the rate-limiter yields 2–3 iterations per second.

- ✅ **Step 2 — pgx v4 → v5 DONE.** Import paths `pgx/v4*` → `pgx/v5*` (confined to
  `db/db.go`, `db/postgres.go`; v4 fully removed from the module graph). API change:
  `pgxpool.ConnectConfig` → `pgxpool.NewWithConfig`. **Behavior restored:** v5 pools
  are lazy (v4's ConnectConfig was eager), so `NewPostgresDB` now `Ping`s the pool to
  fail fast on a bad/unreachable DB — this also keeps the v4-era semantics that two
  idlexacts tests relied on. Tests green under `-race -p 1` (repeated runs).
- ⬜ Step 3 (modernize deps & toolchain) — next.

### CVE status after Step 2 (govulncheck)

All third-party reachable vulns are **closed**: pgx SQLi (GO-2024-2605/2606),
pgproto3 DoS (GO-2026-4518, via pgx v5 → pgproto3 v3), x/text OOB (GO-2021-0113),
and the x/crypto/ssh ones pulled transitively by testcontainers (bumped to v0.53.0).
`govulncheck ./cmd` (the binary) reports **0 third-party vulns**.

Remaining: stdlib `GO-2026-5037` (crypto/x509), **Fixed in go1.25.11**. The dev host
is on go1.25.10 → close it by building on go1.25.11+ (handle in Step 3 / CI toolchain).

- ✅ **Step 3 — modernize deps & toolchain DONE.**
  - **kingpin**: unmaintained `gopkg.in/alecthomas/kingpin.v2` → maintained successor
    `github.com/alecthomas/kingpin/v2` v2.4.0 (drop-in; same package API, only the
    import path changed). CLI verified (`--help`/flags intact; bools now render as
    `--[no-]flag`).
  - **toolchain**: pinned `toolchain go1.25.11` in go.mod → closes stdlib `GO-2026-5037`.
  - testify/zerolog/x/time/x/text/x/crypto already on current versions from earlier steps.
  - **`govulncheck ./...` → "No vulnerabilities found"** (fully clean, incl. test deps).
- ✅ **Step 4 — CI/release modernization DONE.**
  - **CI test job restructured:** runs directly on `ubuntu-latest` (no `container:
    golang`, no `services: postgres`) because testcontainers needs the host Docker
    daemon and starts PostgreSQL itself. Go version comes from `go-version-file: go.mod`.
  - actions bumped: `checkout@v2→v4`, `setup-go@v2→v5`, `goreleaser-action@v2→v6`,
    Docker Hub login → `docker/login-action@v3` (no more password on the CLI).
  - goreleaser: `--rm-dist`→`--clean`; `.goreleaser.yml` migrated to `version: 2`
    (`builds[].id`, `archives.ids`+`formats`, `nfpms.ids`). Dropped the `go-version: 1.15`
    pin in the goreleaser job.
  - **golangci-lint:** Makefile `lint` dropped the removed `golint` linter; CI installs
    golangci-lint **v2.12.2**. Fixed the 6 findings it surfaced — deprecated `rand.Seed`
    (auto-seeded since Go 1.20) removed from idlexacts/deadlocks/waitxacts/rollbacks.
  - **Dockerfile:** build stage `golang:1.16`→`golang:1.25`.
  - Local verification: `make lint` (0 issues), full tests green, `goreleaser check`
    OK, `goreleaser release --snapshot` produced tar.gz + deb + rpm, `docker build` OK.
    The GitHub Actions runs themselves can only be confirmed after pushing the branch.

## Done

The infrastructure release is complete: all CVEs closed (`govulncheck` clean),
pgx on v5, dependencies and Go toolchain current, integration tests portable
(testcontainers), CI/release pipeline modernized. New workloads (deferred from this
release) can now be built on a healthy base.

---

## CVE Snapshot (govulncheck, 2026-06-21, pre-release)

5 reachable vulnerabilities:

| ID | Type | Module | Closed by |
|---|---|---|---|
| (stdlib, x509) | — | Go stdlib | building on current Go |
| GO-2026-4518 | DoS | `pgproto3/v2` | **no v2 fix → requires pgx v5** |
| GO-2024-2606 | SQL injection | `pgproto3/v2`, `pgx/v4` | pgx v4.18.2 / pgproto3 v2.3.3 (or v5) |
| GO-2024-2605 | SQL injection | `pgx/v4` | pgx v4.18.2 (or v5) |
| GO-2021-0113 | OOB read | `x/text` | x/text v0.3.7 |

Plus 25 vulnerabilities in required modules that the code does not call (ignored).

Re-run before release: `go run golang.org/x/vuln/cmd/govulncheck@latest ./...`
