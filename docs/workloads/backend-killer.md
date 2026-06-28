# backend-killer — demo & tuning guide

How to drive a reliable `backend-killer` demo and tune the pressure so the OOM kill (and instance restart) happens predictably.

`backend-killer` drives a single backend toward OOM by leaking prepared statements — each one is
`PREPARE`d and then `EXECUTE`d once, so the backend caches both the parse tree and the generic plan.
How fast you reach the OOM kill (and the instance restart) depends on the stand:

- **Cap the backend's memory** so OOM is reached in minutes, not hours: a systemd `MemoryMax=` on the
  PostgreSQL service (you then get a cgroup/`CONSTRAINT_MEMCG` OOM), a container `--memory=...` limit, or
  run on a small-RAM host.
- **Disable swap** on the stand (`swapoff -a`): with swap the system slowly thrashes (the panel rate
  drops toward `0/s` — memory pressure, not a hang) instead of OOM-killing promptly.
- **Turn up the pressure** with `--backend-killer.plan-size` (heavier plans per `PREPARE`); raise
  `--backend-killer.rate` is unbounded by default.

When OOM fires, the backend is killed, the instance restarts, noisia's connection drops, and it logs a
`connection lost … target likely OOM-restarted` line — its self-report survives the crash.
