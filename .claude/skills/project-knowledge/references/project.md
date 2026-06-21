# Project Context

## Purpose
This file provides high-level project overview for AI agents. Helps agents understand WHAT we're building and WHY.

---

## Project Overview

**Name:** noisia

**Description:** Harmful workload generator for PostgreSQL — deliberately produces problematic database activity (idle transactions, lock waits, deadlocks, temp files, connection exhaustion, etc.).

Used to reproduce and study pathological PostgreSQL situations on purpose, so monitoring, alerting and operational runbooks can be validated against real symptoms instead of theory.

---

## Target Audience

**Primary users:** PostgreSQL DBAs, SRE/performance engineers, and developers of PostgreSQL monitoring/observability tools.

**Use case:** Need to reproduce specific failure modes on demand — to test monitoring dashboards and alerts, validate runbooks, demo incidents, or benchmark how tooling reacts to abnormal database states.

---

## Core Problem

Pathological PostgreSQL states (long idle-in-transaction backends, lock pile-ups, deadlocks, temp-file spill, connection exhaustion) are hard to reproduce reliably for testing. Engineers either wait for them to happen in production or craft fragile ad-hoc scripts. noisia provides a single tool that generates each of these states on demand, in a controlled way, against a target database.

---

## Key Features

Each workload is an independent, separately-toggleable generator:

- **idle transactions** — open transactions on hot-write tables that stay idle for their lifetime (drives bloat).
- **rollbacks** — fake invalid queries that error out and inflate the rollback counter.
- **waiting transactions** — lock hot-write tables then idle, making concurrent transactions get stuck.
- **deadlocks** — concurrent transactions that hold locks each other wants.
- **temporary files** — queries that spill to on-disk temp files due to insufficient `work_mem`.
- **terminate backends** — kill random backends/queries via `pg_terminate_backend()` / `pg_cancel_backend()`.
- **failed connections** — exhaust `max_connections` so other clients cannot connect.
- **fork connections** — run single short queries each in a fresh connection (excessive backend forking).

Usable both as a CLI binary and as an importable Go library (each workload package exposes a `Workload` with `NewWorkload` + `Run(ctx)`).

---

## Out of Scope

- Not a benchmarking/load-testing tool (not pgbench) — the goal is *harmful* states, not throughput measurement.
- No safety guardrails against running on production — the user is fully responsible (see disclaimer in README).
- Targets PostgreSQL only — no other database engines.
