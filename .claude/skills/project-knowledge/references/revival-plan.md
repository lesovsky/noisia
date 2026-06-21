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
  `postgres:15-alpine` (CI replica). See local-baseline recipe in [patterns.md](patterns.md).
- ✅ CVE surface measured with govulncheck (snapshot below).
- ⬜ Step 1 (testcontainers harness) — next.

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
