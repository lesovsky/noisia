# Decisions Log: backend-killer

## Task 01 — Implement `backendkiller` workload package

Created the `backendkiller` package (`backendkiller.go`, `backendkiller_test.go`, `main_test.go`)
implementing the canonical workload shape over a single dedicated `db.Connect` connection that
issues unique heavy literal server-side `PREPARE` statements in a rate-limited loop, with a separate
report-ticker goroutine reading only atomics, optional own-backend memory read on the loop goroutine,
and connection-loss climax detection. All 9 tech-spec Decisions honored (notably `application_name=noisia`
locally on the connection per D8, no `Conninfo`/raw DSN in any log or error per D9, and the first-`Exec`
guard returning an init error at counter==0 per D5). Verified: `go test -race -p 1 ./backendkiller/`
green (3x), `gofmt -l` clean, `go vet` clean.

### Reviews (2 rounds)

- dev-code-reviewer — round 1 [json](001-feat-backend-killer-task-01-dev-code-reviewer-review-round1.json)
  (1 critical: data race on a plain `int64` snapshot in the growth test), round 2
  [json](001-feat-backend-killer-task-01-dev-code-reviewer-review.json) — approved_with_suggestions, resolved.
- dev-security-auditor — round 1 [json](001-feat-backend-killer-task-01-dev-security-auditor-review-round1.json)
  (2 minor: no unit test for `sanitize`, no Decision 9 regression test), round 2
  [json](001-feat-backend-killer-task-01-dev-security-auditor-review.json) — approved, resolved.
- dev-test-reviewer — round 1 [json](001-feat-backend-killer-task-01-dev-test-reviewer-review-round1.json)
  (3 major: builder dead-loop assertion, test data race, vacuous monotonic assertion), round 2
  [json](001-feat-backend-killer-task-01-dev-test-reviewer-review.json) — passed, resolved.

All blocking findings fixed; remaining items were optional production-code suggestions intentionally
not changed (best-effort `sanitize` denylist, panel-rate skew) per spec known-limitations.

## Task 02 — Wire backend-killer into the CLI

Wired the workload into `cmd/main.go` (5 flags with unique envars + `config{}` literal entries) and
`cmd/app.go` (import placed first/alphabetically before `deadlocks`, config struct fields, launch
`if`-block under the shared `--duration` context, and the `startBackendKillerWorkload` helper). No
`--jobs` (single-threaded); the `wait-xacts`→`NOISIA_IDLE_XACTS` envar quirk was deliberately not
copied (all envars unique). Verified: `go build ./...`, `go vet ./...`, `gofmt -l cmd/` clean; `--help`
lists all 5 flags with correct defaults.

### Reviews
- dev-code-reviewer — [json](001-feat-backend-killer-task-02-dev-code-reviewer-review.json) — approved, 0 findings.
- dev-security-auditor — [json](001-feat-backend-killer-task-02-dev-security-auditor-review.json) — approved, 0 findings.
- dev-test-reviewer — [json](001-feat-backend-killer-task-02-dev-test-reviewer-review.json) — passed; 1 minor
  (untested error-propagation branch in the helper) intentionally not addressed — `cmd/` has no wiring
  tests for any workload; reviewer recommended not adding a one-off.

## Task 03 — Update README

Added `backend-killer` to the supported-workloads bullet list (CLI-flag form, with the plan-size
caveat) and a row to the Workload impact table (package-name form `backendkiller`, sorted first,
impact = Yes). Verified via `grep` and a well-formedness check of the table.

### Reviews
- dev-code-reviewer — [json](001-feat-backend-killer-task-03-dev-code-reviewer-review.json) — approved, 0 findings.

## Task 04 — Pre-deploy QA

Final-wave acceptance testing (no deploy — releases are tag-driven). `make lint` clean (0 issues,
golangci-lint v2). `make test` green across all packages (serial `-p 1`, testcontainers): backendkiller
5.5s @ 88.7% coverage, no regressions in the existing 8 workload packages; total coverage 63.7%. Build +
`noisia --help` confirm all 5 backend-killer flags with defaults. All acceptance criteria from the
user-spec and tech-spec verified (single dedicated connection, monotonic counter, panel, --show-memory
PG14+ graceful, error/climax handling, Conninfo never logged, application_name set, README updated).
No reviewers (QA is self-verifying).
