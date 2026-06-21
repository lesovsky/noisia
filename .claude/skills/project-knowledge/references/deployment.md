# Deployment & Operations

## Purpose
Deployment process, infrastructure, and production operations for AI agents.

---

## Deployment Platform

**Platform:** GitHub Releases (binaries + deb/rpm packages) and Docker Hub (`lesovsky/noisia`).

**Type:** Distributed artifact — this is a CLI tool, not a running service. Users download a binary / package or pull the Docker image and run it against their own PostgreSQL.

**Why this platform:** Standard distribution for an open-source Go CLI; goreleaser produces cross-format artifacts from a single tag.

---

## Build & Release Pipeline

Driven by GitHub Actions:

- **`default.yml`** — on push/PR to `master`: runs `make lint` and `make test` inside a `golang` container with a `postgres` service container (integration tests need a live DB).
- **`release.yml`** — on tag `v*`: runs tests, then `make docker-build` + `make docker-push` to Docker Hub, then `goreleaser` to publish binaries and deb/rpm packages to GitHub Releases.

**Release artifacts (goreleaser):** linux/amd64 binary, plus `.deb` and `.rpm` packages. Docker image is built from a multi-stage Dockerfile to a `scratch` base.

> Modernization needed (tracked in [revival-plan.md](revival-plan.md)): `actions/checkout@v2` and `actions/setup-go@v2` are outdated, the goreleaser job pins `go-version: 1.15`, goreleaser uses the deprecated `--rm-dist` flag, the Dockerfile builds on `golang:1.16`, and golangci-lint is pinned to v1.50.

---

## Access Information

**Credentials location:** GitHub Actions secrets — `DOCKER_USERNAME`, `DOCKER_PASSWORD` (Docker Hub push), `GITHUB_TOKEN` (releases).

No servers to SSH into — there is no hosted environment.

---

## Environment Variables

No application environment variables. Runtime configuration is entirely via CLI flags (target conninfo, which workloads to run, tuning parameters). See built-in `--help`.

CI/secrets variables are listed under Access Information.

---

## Deployment Triggers

**Release:** Push a `v*` tag → `release.yml` builds and publishes binaries, packages and Docker image.

**No staging/production environments** — the artifact is consumed by end users.

---

## Rollback Procedure

Releases are immutable artifacts. "Rollback" = users pin/download a previous version tag or Docker image tag. A bad release is handled by publishing a fixed new tag.

---

## Monitoring & Observability

### Logging

**Where:** stdout (the running CLI / container logs).
**Format:** zerolog, console-formatted by default.

### Error Tracking

Not applicable — short-lived CLI invocation, no error-tracking service.

### Health Checks

Not applicable — not a long-running service.
