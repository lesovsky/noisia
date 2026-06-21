---
status: planned                    # planned -> in_progress -> done
depends_on: ["01"]                 # ID задач-зависимостей (строки: ["01", "02"])
wave: 2                            # волна параллельного выполнения
skills: [code-writing]             # МАССИВ скиллов для загрузки
verify: bash                       # инструмент верификации (опционально: curl, bash, user)
reviewers: [dev-code-reviewer, dev-security-auditor, dev-test-reviewer]  # явно указать. Пусто = fallback на defaults
teammate_name:                     # имя агента-исполнителя (опционально; если не задано — генерируется по описанию задачи)
---

# Task 02: Wire backend-killer into the CLI

## Required Skills

Перед выполнением задачи загрузи:
- `/skill:code-writing` — [skills/code-writing/SKILL.md](~/.claude/skills/code-writing/SKILL.md)

## Description

Task 01 creates the `backendkiller` package (`Config` + `validate()` + `NewWorkload` + `Run(ctx)`).
This task makes that workload reachable from the `noisia` binary by registering it in the CLI,
exactly the way every other workload is wired.

Registering a workload in noisia touches **two files** and consists of **6 distinct edit sites**:

1. Flag declarations in `cmd/main.go` (the `var(...)` block inside `main()`).
2. The `config{}` literal in `cmd/main.go` (dereference each parsed flag pointer).
3. The `config` struct fields in `cmd/app.go`.
4. The package import in `cmd/app.go` (alphabetical: between `failconns` and `forkconns`).
5. The launch `if`-block in `runApplication` (`cmd/app.go`) under the shared `--duration` context.
6. The `startBackendKillerWorkload` helper at the bottom of `cmd/app.go`.

The workload is **single-threaded**: it must NOT consume the shared `--jobs` flag (no `Jobs` field on
`backendkiller.Config`). `Conninfo` must follow the existing pattern — passed through from
`c.postgresConninfo`, never logged. Mirror the existing workloads' wiring verbatim (rollbacks for the
`Float64` rate flag, terminate for the `Duration` flag).

This is CLI plumbing only — no business logic. It is verified by `go build ./...` succeeding and
`noisia --help` listing the five new flags, not by unit tests (the wiring has no testable unit; the
workload mechanics are covered in Task 01).

## What to do

1. **`cmd/main.go` — flag declarations.** Add five `kingpin.Flag(...)` declarations to the `var(...)`
   block inside `main()` (after the `forkconnsRate` line, before the closing `)` of the block). Every
   flag gets `.Default(...)` and `.Envar("NOISIA_…")`, terminated by the correct type builder:
   - `--backend-killer` — Bool, enable flag — `.Default("false").Envar("NOISIA_BACKEND_KILLER").Bool()`
   - `--backend-killer.rate` — Float64, statements/sec, `0` = unlimited — `.Default("0").Envar("NOISIA_BACKEND_KILLER_RATE").Float64()`
   - `--backend-killer.plan-size` — Int, target-list width per PREPARE — `.Default("1000").Envar("NOISIA_BACKEND_KILLER_PLAN_SIZE").Int()`
   - `--backend-killer.show-memory` — Bool — `.Default("false").Envar("NOISIA_BACKEND_KILLER_SHOW_MEMORY").Bool()`
   - `--backend-killer.report-interval` — Duration — `.Default("1s").Envar("NOISIA_BACKEND_KILLER_REPORT_INTERVAL").Duration()`

2. **`cmd/main.go` — `config{}` literal.** Add five entries to the `config{...}` literal,
   dereferencing each flag pointer (e.g. `backendKiller: *backendKiller,` plus the four tuning fields).

3. **`cmd/app.go` — `config` struct fields.** Add five fields to `type config struct`, matching the
   names used in step 2 with the correct Go types: `backendKiller bool`,
   `backendKillerRate float64`, `backendKillerPlanSize int`, `backendKillerShowMemory bool`,
   `backendKillerReportInterval time.Duration`.

4. **`cmd/app.go` — import.** Add `"github.com/lesovsky/noisia/backendkiller"` to the import block, in
   alphabetical position **between** the `failconns` and `forkconns` imports.

5. **`cmd/app.go` — launch block.** Add an `if c.backendKiller { … }` block in `runApplication`,
   mirroring the rollbacks block: `log.Info(...)`, `wg.Add(1)`, a goroutine that calls
   `startBackendKillerWorkload(ctx, c, log)`, logs `log.Errorf("backend-killer workload failed: %s", err)`
   on error, and `wg.Done()`. Place it before `wg.Wait()` (after the forkconns block). The `ctx` here is
   already wrapped with the shared `--duration` timeout, so no extra timeout handling is needed.

6. **`cmd/app.go` — start helper.** Add `startBackendKillerWorkload(ctx, c, log)` after the last helper
   (`startForkconnsWorkload`). It builds `backendkiller.Config{}` from `c` (Conninfo, Rate, PlanSize,
   ShowMemory, ReportInterval — **no Jobs**), calls `backendkiller.NewWorkload(cfg, logger)`, returns
   the init error if any, otherwise returns `workload.Run(ctx)`. Match the exact field names declared on
   `backendkiller.Config` in Task 01.

7. Verify with `go build ./...` and `go run ./cmd --help` (or the built binary `--help`).

## TDD Anchor

<!-- CLI wiring has no testable unit (the workload mechanics are tested in Task 01). Verification is
manual: build + --help. Kept as an explicit manual-verification anchor instead of unit tests. -->

Manual verification (no unit tests for CLI wiring):

- `go build ./...` — compiles cleanly (import resolves, struct/Config field names match Task 01).
- `noisia --help` lists all five flags: `--backend-killer`, `--backend-killer.rate`,
  `--backend-killer.plan-size`, `--backend-killer.show-memory`, `--backend-killer.report-interval`,
  each with its default shown.

## Acceptance Criteria

- [ ] Five flags declared in `cmd/main.go` with `.Default()` and `.Envar("NOISIA_…")`, correct types
      (Bool / Float64 / Int / Bool / Duration).
- [ ] Five entries added to the `config{}` literal in `cmd/main.go` (each flag pointer dereferenced).
- [ ] Five fields added to `type config struct` in `cmd/app.go` with matching names and Go types.
- [ ] `backendkiller` import added in `cmd/app.go` between `failconns` and `forkconns`.
- [ ] `if c.backendKiller { … go startBackendKillerWorkload … }` launch block added in `runApplication`,
      under the shared `--duration` context, mirroring the rollbacks block.
- [ ] `startBackendKillerWorkload` helper added; builds `backendkiller.Config` from `c`, calls
      `NewWorkload`, returns `Run(ctx)`; **no `Jobs` field** is set.
- [ ] `--jobs` is NOT wired into backend-killer (single-threaded).
- [ ] `Conninfo` is passed from `c.postgresConninfo` and never appears in any log line.
- [ ] `go build ./...` succeeds.
- [ ] `noisia --help` lists all five flags with their defaults.

## Context Files

**Feature artifacts:**
- [001-feat-backend-killer.md](001-feat-backend-killer.md) — user-spec
- [001-feat-backend-killer-tech-spec.md](001-feat-backend-killer-tech-spec.md) — tech-spec (see Task 2,
  Architecture "What we're building/modifying", Data Models for the flag set, Decisions 7 & 9)
- [001-feat-backend-killer-code-research.md](001-feat-backend-killer-code-research.md) — section 4 and
  the "CLI wiring — exact edit points" addendum (line-accurate edit sites)
- [001-feat-backend-killer-decisions.md](001-feat-backend-killer-decisions.md) — decisions log

**Project knowledge:**
- [project.md](../../../.claude/skills/project-knowledge/references/project.md)
- [architecture.md](../../../.claude/skills/project-knowledge/references/architecture.md)
- [patterns.md](../../../.claude/skills/project-knowledge/references/patterns.md) — coding conventions,
  testing/verification, git workflow

**Code files:**
- [cmd/main.go](../../../cmd/main.go) — add flag declarations + `config{}` literal entries
- [cmd/app.go](../../../cmd/app.go) — add import, struct fields, launch block, start helper
- [backendkiller/backendkiller.go](../../../backendkiller/backendkiller.go) — read to confirm the exact
  `Config` field names produced by Task 01 (Conninfo, Rate, PlanSize, ShowMemory, ReportInterval)

## Verification Steps

- Run `go build ./...` — must compile with no errors.
- Run the binary's help (`go run ./cmd --help`, or build then `noisia --help`) — confirm the output
  lists `--backend-killer`, `--backend-killer.rate` (default `0`), `--backend-killer.plan-size`
  (default `1000`), `--backend-killer.show-memory`, `--backend-killer.report-interval` (default `1s`).
- Confirm `--jobs` does not appear anywhere in the backend-killer config wiring (grep `cmd/app.go` for
  `backendKiller` — no `Jobs:` line).
- Optional sanity: `make lint` clean for `cmd/` (no unused vars, gofmt clean).

## Details

<!-- All details for task execution — technical, organizational, any other. -->

**Files:**

- `cmd/main.go`
  - Current state: `main()` holds a `var(...)` flag block ending at the `forkconnsRate` declaration
    (the closing `)` of the block follows it), then `kingpin.Parse()`, then the `config{...}` literal
    that ends with `forkconnsRate: *forkconnsRate,`. The file is `package main` with no `time` import.
  - Change: add the 5 flag declarations after the last existing flag in the `var(...)` block; add the 5
    matching entries after `forkconnsRate: *forkconnsRate,` in the `config{}` literal. No new import is
    needed in `main.go` — kingpin produces the typed pointers and the literal only dereferences them.

- `cmd/app.go`
  - Current state: import block lists workloads alphabetically — `deadlocks`, `failconns`, `forkconns`,
    `idlexacts`, `rollbacks`, `tempfiles`, `terminate`, `waitxacts`; `time` is already imported.
    `type config struct` ends with `forkconnsRate uint16`. `runApplication` wraps `ctx` with
    `context.WithTimeout(ctx, c.duration)`, runs one `if c.<workload> { … }` block per workload, then
    `wg.Wait()`. The last launch block is the forkconns block; the last helper is
    `startForkconnsWorkload`.
  - Change: insert the `backendkiller` import between `failconns` and `forkconns`; add the 5 struct
    fields after `forkconnsRate uint16`; add the `if c.backendKiller { … }` block after the forkconns
    block (before `wg.Wait()`); add `startBackendKillerWorkload` after `startForkconnsWorkload`.

**Dependencies:**

- Depends on Task 01 (`backendkiller` package must exist and export `Config` + `NewWorkload`). Read
  `backendkiller/backendkiller.go` first to copy the EXACT `Config` field names — do not assume; if a
  name differs from this task's wording, follow the actual package and note it in the decisions log.
- Packages: no new third-party packages. Uses `github.com/alecthomas/kingpin/v2` (already imported in
  `main.go`) and `github.com/lesovsky/noisia/backendkiller` (new import in `app.go`).

**Edge cases:**

- `--backend-killer.rate` default is `0` (= unlimited). The `0` default is intentional and is handled by
  the workload (Decision 2); the CLI just passes the float through. Do not reject or special-case it here.
- `.Int()` is a valid kingpin/v2 v2.4.0 terminator though not yet used elsewhere in `main.go` (existing
  integer flags use `.Uint16()`). Use `.Int()` for `plan-size` (headroom), matching the tech-spec's
  `PlanSize int`.
- The `wait-xacts` enable flag in `main.go` reuses the `NOISIA_IDLE_XACTS` envar (a pre-existing quirk
  in the file). Do NOT copy that mistake — give backend-killer its own unique `NOISIA_BACKEND_KILLER`
  envar and unique sub-flag envars.

**Implementation hints:**

- Reference block to mirror for the launch `if` (rollbacks, `cmd/app.go`):
  `if c.rollbacks { log.Infof("start rollbacks workload for %s", c.duration); wg.Add(1); go func(){ err := startRollbacksWorkload(ctx, c, log); if err != nil { log.Errorf("rollbacks workload failed: %s", err) }; wg.Done() }() }`.
- Reference helper to mirror (rollbacks): build `Config{}`, `if err != nil { return err }`,
  `return workload.Run(ctx)`. Drop the `Jobs:` line for backend-killer.
- Reference for the Duration flag declaration: `terminate.interval` in `main.go`
  (`.Default("1s").…Duration()`), and the matching `terminateInterval time.Duration` struct field in
  `app.go`.
- Reference for the Float64 rate flag: `rollbacks.rate` in `main.go` and `rollbacksRate float64` in
  `app.go`.
- Keep alphabetical ordering of the import line; place it manually between `failconns` and `forkconns`.
- Decision 9 (no Conninfo in logs): the only thing this task logs is the fixed
  `"start backend-killer workload"` / `"backend-killer workload failed: %s"` — never interpolate
  `c.postgresConninfo`.

## Reviewers

- **dev-code-reviewer** → `001-feat-backend-killer-task-02-dev-code-reviewer-review.json`
- **dev-security-auditor** → `001-feat-backend-killer-task-02-dev-security-auditor-review.json`
- **dev-test-reviewer** → `001-feat-backend-killer-task-02-dev-test-reviewer-review.json`

## Post-completion

- [ ] Записать краткий отчёт в [001-feat-backend-killer-decisions.md](001-feat-backend-killer-decisions.md) (Summary: 1-3 предложения, ревью со ссылками на JSON, без таблиц файндингов и дампов)
- [ ] Если отклонились от спека — описать отклонение и причину (особенно если фактические имена полей `backendkiller.Config` отличаются от формулировок задачи)
- [ ] Обновить user-spec/tech-spec если что-то изменилось
