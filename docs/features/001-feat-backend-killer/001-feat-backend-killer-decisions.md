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
