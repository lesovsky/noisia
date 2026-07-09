# waitxacts — demo & tuning guide

How to drive a `waitxacts` demo: from one held table lock, build a stable pile of **blocked waiter
sessions** and — when let off the leash — climb until `max_connections` is exhausted and new `app`
clients get `FATAL: sorry, too many clients already`. This guide covers the mechanism, the two
chapter demos (observation and cascade), the `superuser_reserved_connections` role trick, the
self-report line, and the tuning knobs.

`waitxacts` picks **one** target table — the first of the top-write tables, or the dedicated fixture
table `_noisia_waitxacts_workload` when `--wait-xacts.fixture` is set (or no top-write table exists).
A single **holder** connection cycles an `ACCESS EXCLUSIVE` lock on that table; a rate-limited
**manager** opens N separate client connections, each blocking on a `SELECT` against the locked
table. So the count of blocked sessions tracks the count of established connections as a climbing,
flickering staircase. The symptom is **server-side** — `wait_event_type = 'Lock'` in
`pg_stat_activity`, the `waiting` counter in `pgcenter top` — plus noisia's own one-line self-report.

## Mechanism

- **One holder, one lock, a near-zero OFF window.** A dedicated connection loops
  `BEGIN; LOCK TABLE <t> IN ACCESS EXCLUSIVE MODE;` holds it a random interval in
  `[--wait-xacts.locktime-min .. --wait-xacts.locktime-max]`, `ROLLBACK`s, then re-acquires
  immediately. That brief release-and-retake is the **flicker**: because the OFF window is
  effectively zero, the blocked-session count does not fall to zero between cycles.
- **N waiters, each its own backend.** The manager opens waiter connections at
  `--wait-xacts.waiters-rate` per second (token bucket), up to the `--wait-xacts.waiters` cap. Each
  waiter is a **separate real client connection** (its own backend, the role from the connection
  string, `application_name = 'noisia'`) that loops `SELECT * FROM <t>`. That `SELECT` conflicts with
  the `ACCESS EXCLUSIVE` lock and parks in `Lock` until the holder's OFF window, then re-issues
  (with a short pause between attempts so the OFF window is not a query storm). An empty fixture
  table blocks a `SELECT` just as well — no rows are needed.
- **The pile tracks the established gauge.** Waiters climb to the cap and hold there; a waiter the
  server kills frees its slot and the manager refills it within the rate budget. With
  `--wait-xacts.waiters=0` there is no cap, so the pile grows until the connection slots run out.

## Observation demo — the flickering staircase

The small-cap run: build a stable, observable pile of ~10 blocked sessions for the screenshots.

```
noisia --wait-xacts --wait-xacts.fixture
```

Defaults give `waiters=10` and `waiters-rate=1`, so noisia creates the fixture table, the holder
takes the lock, and the manager adds ~1 blocked waiter per second up to 10, then holds the pile
there. In a second terminal watch the blocked sessions:

```sql
SELECT pid, state, wait_event_type, wait_event, query
FROM pg_stat_activity
WHERE application_name = 'noisia' AND wait_event_type = 'Lock';
```

or watch the `waiting` counter climb and hover around 10 in `pgcenter top`.

**Honest caveat:** "never drops to zero" is a sampling property, not a hard guarantee. It holds only
while the holder's OFF window (the instant between `ROLLBACK` and the next `LOCK`) is shorter than
pgcenter's sample interval. If you sample faster than the flicker, you will occasionally catch the
gap and see the counter dip.

## Cascade demo — exhaust max_connections

The unbounded run: keep opening waiters until the server runs out of connection slots.

```
noisia --wait-xacts --wait-xacts.fixture --wait-xacts.waiters=0 --wait-xacts.waiters-rate=1 --duration 5m
```

`--wait-xacts.waiters=0` removes the cap, so on a `max_connections=100` stand the established pile
climbs ~1/s toward the ceiling (~97 = 100 − `superuser_reserved_connections` of 3). At the ceiling,
new waiter connects — **and any new `app` client** — get `FATAL: sorry, too many clients already`.
This is the **intended observable, not a bug**: noisia, being the one opening all those
connections, is expected to hit the ceiling first. It logs the refusal (rate-limited, at most once
per second so a cascade does not flood the log), keeps every already-established waiter, increments
its `refused` counter, and does **not** crash. Any connect error is treated as this refusal without
inspecting the SQLSTATE, so near the ceiling you may also see `too many connections for role`.

### The `superuser_reserved_connections` role trick

The point of the cascade is that a `postgres` monitor stays alive while `app` clients are refused.
Waiters connect under the **same non-superuser role** from the connection string (`app`). PostgreSQL
keeps `superuser_reserved_connections` (default 3) slots that only superusers can take, so once the
non-superuser pile fills the rest, a superuser monitor still gets in:

```
sudo -u postgres pgcenter top -d app_db
```

The role must be **non-superuser** for the reservation to bite — a superuser workload role would
consume the reserved slots too and lock the monitor out.

## Self-report panel

Every `--wait-xacts.report-interval` (default `1s`) noisia prints one line, read purely from
in-process counters (it never polls `pg_stat_activity`):

```
waitxacts: established=47 target=unlimited refused=312 locked_table=_noisia_waitxacts_workload
```

- `established` — a **live gauge** of currently open waiter connections. It rises as the pile fills
  and dips if the server kills a waiter, then recovers as the manager refills. Monotonic growth is
  only the normal, un-killed path.
- `target` — the `--wait-xacts.waiters` cap, or `unlimited` when it is `0`.
- `refused` — a **cumulative** count of `too many clients` refusals (only grows).
- `locked_table` — the name of the locked table.

## CLI

- `--wait-xacts` — enable the workload (env `NOISIA_WAIT_XACTS`).
- `--wait-xacts.fixture` — lock the dedicated fixture table `_noisia_waitxacts_workload` instead of a
  real top-write table. The recommended deterministic path for demos (no dependency on existing
  tables, and it is dropped on exit).
- `--wait-xacts.waiters` (uint, default `10`) — cap on blocked waiter connections. **`0` = unlimited,
  not off** — it drives the cascade toward exhausting `max_connections`.
- `--wait-xacts.waiters-rate` (float, waiters/sec, default `1`) — rate at which new waiters are
  opened (token bucket). It also paces retries during a cascade.
- `--wait-xacts.locktime-min` / `--wait-xacts.locktime-max` (duration, default `5s` / `20s`) — the
  lock hold interval per holder cycle, i.e. the flicker period.
- `--wait-xacts.report-interval` (duration, default `1s`) — self-report line cadence.
- `--jobs` — **no longer applies to this workload.** There is a single holder; the flag stays global
  but `waitxacts` ignores it (raising it does not create parallel locks or more waiters).

## Tuning & the contract

- **Pile size vs pile growth.** `--wait-xacts.waiters` caps the pile; `--wait-xacts.waiters-rate`
  sets how fast it fills (and how fast it retries at the ceiling). For the observation demo the
  default cap of 10 is enough to be visible without stressing the stand; for the cascade set
  `--wait-xacts.waiters=0` and pick a rate you can watch climb (`1`/s reads cleanly).
- **The flicker period.** `--wait-xacts.locktime-min` / `--wait-xacts.locktime-max` control how long
  each lock is held before the brief release. Keep them well above pgcenter's sample interval so the
  staircase looks stable; shortening them makes the OFF window easier to catch (see the observation
  caveat).
- **Connection budget.** Each waiter is one connection. On a `max_connections=100` stand the pile
  tops out near `100 − superuser_reserved_connections`. noisia hitting the ceiling and logging
  `too many clients` is the cascade's success condition, not a failure.
- **Fixture is the deterministic path.** With `--wait-xacts.fixture` the workload owns its table and
  has no dependency on live traffic. Against a real top-write table the `ACCESS EXCLUSIVE` lock
  blocks **all** access to that table for the duration — only appropriate on a dedicated stand.
- **Clean shutdown.** On `Ctrl-C` or `--duration` expiry the lock is released, every waiter
  connection is closed, and the fixture table is dropped through a **fresh** connection (so cleanup
  works even if the holder/waiter connections were poisoned by the cancellation). A real top-write
  table is never dropped. A failed `DROP` is logged so the fixture can be removed manually.
- **Secrets.** The connection string is never logged — every error line is sanitized first.
