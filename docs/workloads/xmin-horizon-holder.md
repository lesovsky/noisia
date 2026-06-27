# xmin-horizon-holder — demo & tuning guide

How to drive an `xmin-horizon-holder` demo: the held-horizon mechanism, the headline **reveal** that proves
causality, what to watch **outside** noisia, the modest-rate-vs-bloat-churn distinction, the wraparound
asymptote, the `idle_in_transaction_session_timeout` caveat, the managed-PostgreSQL (no-superuser) note,
remediation framing, and how it differs from `idlexacts` and a hypothetical `bloat-churn`.

`xmin-horizon-holder` seeds one large `(id bigint PK, payload bytea)` table, opens **one long-lived
`REPEATABLE READ` transaction** on a dedicated connection that pins the **cluster-wide oldest-xmin horizon**,
and fans out `--jobs` workers looping a **scattered random-id** `UPDATE` to manufacture dead tuples. Autovacuum
keeps running, but the pinned horizon makes every one of those dead tuples **non-removable**, so bloat creeps
across the database. The instance survives — this is degradation, not catastrophe — and the symptom lives
entirely **outside** noisia (`pg_stat_user_tables`, `pg_stat_activity`, `pgstattuple`), never in noisia's own
panel.

## Mechanism — two load-bearing organs

The workload is two organs that only teach the lesson together:

- **The holder freezes the horizon.** A single session runs a raw `BEGIN ISOLATION LEVEL REPEATABLE READ`,
  takes a snapshot with `SELECT 1` (which pins `backend_xmin`), and forces an xid assignment with
  `SELECT txid_current()` (which pins `backend_xid`). It then sits idle-in-transaction for the whole run. As
  long as that transaction is open, PostgreSQL must assume the holder's old snapshot might still read any row
  version newer than its xmin — so the **global** oldest-xmin horizon cannot advance, cluster-wide.
- **The churn manufactures the garbage.** `--jobs` workers, each on its own connection, run autocommit
  scattered `UPDATE`s over random ids across the whole heap. Every `UPDATE` leaves a dead tuple behind.

Neither half is interesting alone: churn without the holder is just garbage that autovacuum reclaims; the
holder without churn freezes a horizon with nothing dead to protect. **Churn produces the dead tuples; the
holder makes them un-reclaimable.** The holder is therefore **mandatory** — if it cannot be established at
startup, `Run` fails and the workload does not start (otherwise it would degrade into plain bloat-churn and
lose its identity).

## The reveal — read this first

The headline of this demo is not "watch bloat grow"; it is the **reveal** that proves *what* caused it. Run it
in front of the junior in two acts:

1. **Act one — autovacuum is powerless.** Start `noisia --xmin-horizon-holder --jobs 4 …`. In a separate
   session, watch `pg_stat_user_tables.n_dead_tup` on the seed table **climb and never fall**, even though
   `last_autovacuum` keeps updating — autovacuum *runs* and finds nothing it is allowed to remove. In
   `pg_stat_activity`, the noisia holder backend shows a **non-empty `backend_xmin` and `backend_xid`**, both
   stuck at an old value. This is the counterintuitive symptom: "autovacuum runs, but `n_dead_tup` only grows."
2. **Act two — stop noisia and watch it drain.** Ctrl+C noisia (or let `--duration` expire). The holder's
   transaction closes, the horizon advances, and the **next `VACUUM`** (manual or autovacuum) **immediately**
   reclaims the dead tuples — `n_dead_tup` collapses.

The second act is the whole point. Without it, a skeptic can object that "some of that bloat would have
accumulated anyway, the holder just made it worse." The reveal closes that gap: the moment the snapshot is
released, the *same* dead tuples that VACUUM refused to touch become reclaimable **at once**. That proves the
dead rows were always collectable and it was the **held horizon**, not the churn volume, that pinned them. The
reveal reproduces in CI; on a real stand it is simply more visceral.

## External observation — noisia never sees the bloat

The climax is entirely **outside** noisia. noisia self-reports only its own atomics and **never polls DB
state** (ADR-002-3) — it cannot and does not print `n_dead_tup`. Watch the server yourself:

- **`pg_stat_activity`** — the holder backend (`application_name='noisia'`) has a non-empty `backend_xmin`
  **and** `backend_xid`, and an old `xact_start`. This is the frozen horizon, made visible.
- **`pg_stat_user_tables.n_dead_tup`** — on the seed table, grows monotonically while the holder lives;
  `last_autovacuum` advances (autovacuum runs) yet the count does not fall.
- **`pgstattuple`** — `pgstattuple('<seed table>')` shows the physical `dead_tuple_percent` / free-space climb,
  the on-disk bloat behind the row-stat counter.
- **`VACUUM (VERBOSE)`** — run against the seed table while the holder lives: it reports tuples that are
  **"dead but not yet removable"** and names the oldest xmin holding them back. This is the single most direct
  proof of the mechanism.

noisia's own panel reports **only its own counters**:

```
xmin-horizon-holder: held=12m34s churned=9120000 (720000.0/min) holder-restarts=0 elapsed=12m40s
```

- `held=<dur>` — the age of the **current** pinned snapshot (= the age of the frozen horizon = the measure of
  damage). It **resets to zero on a holder reconnect** (see the timeout caveat), so a non-monotonic `held` is a
  signal, not a glitch.
- `churned=<N>` — successful `UPDATE`s noisia issued (the dead tuples *we* produced). Re-updating the same
  random row recounts; this is churn volume, not distinct dead rows.
- `(<rate>/min)` — average churn rate since the workload started (`churned ÷ elapsed minutes`), not a
  windowed instantaneous rate: it converges on the steady-state rate but reads low during the startup ramp.
- `holder-restarts=<n>` — count of holder reconnects; it makes a `held` reset **visible** instead of hiding it.
- `elapsed=<Z>` — total workload uptime.

There is no `n_dead_tup`, no bloat figure, no horizon age from the server in this panel — by design. The damage
is real but only the *cause* (a held snapshot) and *our contribution* (churn) are self-reported.

## Modest rate vs bloat-churn — a horizon attack, not a rate attack

The default `--xmin-horizon-holder.rate 3000` (UPDATEs/sec **per worker**) is deliberately **modest but
lively**: with default autovacuum throttling, this churn rate alone is one that autovacuum would normally keep
up with. The bloat here does **not** come from outrunning autovacuum — it comes from the held snapshot making
autovacuum *unable to reclaim anything at all*. That is the conceptual core: this is an **attack on the
horizon, not on the rate**.

Contrast a hypothetical `bloat-churn` workload, which would attack by **speed** — UPDATE/DELETE faster than
autovacuum can vacuum, so dead tuples pile up because autovacuum falls *behind*. There, raising the rate is the
attack. Here, the rate only sets how *fast* the (already un-reclaimable) bloat accumulates; even a slow churn
bloats without bound because *nothing* is ever collectable while the horizon is pinned.

Dial `--xmin-horizon-holder.rate` to taste: lower it for a slow, clearly-horizon-driven creep that no one can
mistake for a rate attack; raise it (or set `0` for unbounded) to fill the table faster for a short demo or to
march toward the wraparound asymptote below.

**Table-size guidance.** Keep the default `--xmin-horizon-holder.table-size 1GB` (floor 64 MiB) or larger. On a
table that is too small, the scattered churn quickly revisits every page, the heap "warms up," and bloat growth
flattens into something uninformative — you want a heap big enough that random `UPDATE`s keep landing on
not-yet-bloated pages for the length of the demo. Set `--duration` generously: the held horizon only surfaces
observable bloat over time.

## The wraparound asymptote

Hold the horizon long enough and the secondary, far-end limit appears. Run with **`--xmin-horizon-holder.rate
0`** (unbounded) and the **maximum `--jobs`** you can sustain, and **never let the holder reconnect**. Every
autocommit `UPDATE` consumes **one xid**, so the age of the cluster's oldest xid climbs relentlessly. At
roughly **2.1 billion** write-transactions, PostgreSQL enters its protective **read-only** state:

> `ERROR: database is not accepting commands to avoid wraparound data loss`

This is a **refusal to accept commands, NOT a PANIC or a crash.** The instance does not fall over; it stops
accepting writes to protect itself. In noisia it surfaces as an ordinary **write error in a churn worker** (the
`UPDATE` fails, the error is sanitized and logged, the reporter keeps printing) — there is **no special
wraparound detection** (Decision 8). Do not promise the junior a dramatic crash; the lesson is the *refusal*.

| Sustained write rate | Time to ~2.1B write-xacts |
|----------------------|---------------------------|
| ~5,000 txn/s | ~5 days |
| ~50,000 txn/s | ~12 hours |
| ~100,000 txn/s | ~6 hours |

These are **order-of-magnitude** orientation figures (one xid per autocommit `UPDATE`; real throughput depends
on hardware, payload, and contention). The asymptote is reached in **hours to days**, is **environment-
dependent**, and is **not reproducible in CI** — it is an overnight stand exercise. Recovery is the usual story:
close the holder, let aggressive `VACUUM` advance the frozen xid, and in a severe case single-user mode.

## The `idle_in_transaction_session_timeout` caveat

The holder is, by its nature, an **idle-in-transaction** session. If the target has
`idle_in_transaction_session_timeout` set to a non-zero value — and **several managed providers enable it by
default** — the server will **periodically kill the holder**. When that happens:

- the holder's connection drops, noisia does **one** best-effort reconnect and re-establishes a *fresh*
  snapshot,
- `held` **resets to zero** (the horizon genuinely unfroze in the gap — a new snapshot is a new starting
  point), and
- `holder-restarts` **increments**, making the reset visible.

The net effect is a horizon that **"breathes"** — repeatedly unfreezing and re-freezing — instead of holding
monotonically, and each unfreeze lets autovacuum catch up, blunting the bloat. Treat a **rising
`holder-restarts`** as the indicator that this GUC is firing. On the stand, **check it** (`SHOW
idle_in_transaction_session_timeout`) and **raise it, or set it to `0`** for the duration of the demo so the
holder can hold uninterrupted. A reconnect also resets progress toward the wraparound asymptote (the holder's
xid is reassigned), so an active timeout makes the overnight scenario above effectively unreachable.

## Managed PostgreSQL — no superuser required (but mind the timeout)

The holder needs **no elevated privilege**: just `connect`, `CREATE` (to seed the table), and
`SELECT`/`txid_current()` (both available to `PUBLIC`). There is **no startup privilege gate** (Decision 5).
So, unlike `checkpoint-storm` — which requires superuser or `pg_checkpoint` and **will not start** on managed
PostgreSQL — `xmin-horizon-holder` **runs on RDS, Cloud SQL, Supabase, and Neon** under an ordinary customer
role. That is the deliberate selling point of this workload.

Temper that advantage with the caveat above: those same managed providers are the ones most likely to ship a
default `idle_in_transaction_session_timeout`, so on a managed instance check that GUC first and watch
`holder-restarts`. The workload *starts* anywhere; whether the horizon *holds* depends on that timeout.

## Remediation framing

On production, the cure is to find and end the long-running transaction, not to tune vacuum harder:

- **Find it.** In `pg_stat_activity`, look for a backend with an old `backend_xmin` (or `backend_xid`) and an
  old `xact_start` — the oldest such transaction is pinning the cluster's xmin horizon. Cross-check the cluster
  oldest-xmin / the table's "oldest removable xid" from `VACUUM (VERBOSE)`.
- **End it.** `pg_terminate_backend(pid)` the offending session; once it is gone the horizon advances and the
  next vacuum reclaims the accumulated dead tuples.
- **Monitor it.** Alert on the **oldest-xmin horizon age** and on the **age of the oldest transaction**
  (`xact_start`), not just on bloat — bloat is the lagging symptom, the long transaction is the cause. Consider
  a sane `idle_in_transaction_session_timeout` as a prophylactic against accidental holders (the same GUC that
  is a *nuisance* for this demo is a *guardrail* in production).

## This is NOT idlexacts / NOT bloat-churn

Juniors confuse this with two neighbors; the distinctions are the lesson:

- **NOT `idlexacts`.** `idlexacts` opens **many short-lived, local** idle transactions — noise that comes and
  goes. `xmin-horizon-holder` is **one long-lived transaction** holding a **single global** snapshot for the
  whole run. The damage is not the *number* of idle transactions; it is the *age* of the **one** that pins the
  cluster-wide horizon. One old holder out-damages a thousand short idlers.
- **NOT a hypothetical `bloat-churn`.** `bloat-churn` is a **rate attack** — churn faster than autovacuum can
  keep up, so it falls behind. `xmin-horizon-holder` is a **horizon attack** — autovacuum keeps up *fine* but
  is **forbidden** to reclaim anything while the snapshot is held. Same visible symptom (rising `n_dead_tup`),
  opposite cause: in `bloat-churn` autovacuum is *too slow*; here it is *handcuffed*.

The reveal (stop noisia → `VACUUM` drains instantly) is what tells them apart in practice: a rate-attack
backlog drains *gradually* as vacuum catches up; a held-horizon backlog drains the **instant** the snapshot is
released, because the rows were collectable all along.

## CLI flags

| Flag | Envar | Type | Default | Purpose |
|------|-------|------|---------|---------|
| `--xmin-horizon-holder` | `NOISIA_XMIN_HORIZON_HOLDER` | bool | `false` | Enable the workload |
| `--xmin-horizon-holder.table-size` | `NOISIA_XMIN_HORIZON_HOLDER_TABLE_SIZE` | bytes (base-2) | `1GB` | Target seed-table size; floor `64MiB`. Big enough that random UPDATEs keep landing on not-yet-bloated pages |
| `--xmin-horizon-holder.payload-bytes` | `NOISIA_XMIN_HORIZON_HOLDER_PAYLOAD_BYTES` | int | `8192` | Payload size (bytes) per UPDATE; floor `1` |
| `--xmin-horizon-holder.rate` | `NOISIA_XMIN_HORIZON_HOLDER_RATE` | float64 | `3000` | UPDATEs/sec **per worker**; `0` = unbounded (`rate.Inf`). Modest by design — this is a horizon attack, not a rate attack |
| `--xmin-horizon-holder.report-interval` | `NOISIA_XMIN_HORIZON_HOLDER_REPORT_INTERVAL` | duration | `1s` | How often the self-report panel is printed |

The holder is **parameterless** — it is always one `REPEATABLE READ` snapshot plus `txid_current()`, with no
flag of its own. The churn worker count comes from the **global** `--jobs` flag (there is no per-workload jobs
flag); a bare `noisia --xmin-horizon-holder` (`--jobs=1`) is valid — one worker churns, the lone holder still
freezes the horizon. The workload runs under the shared `--duration` timeout like every other noisia workload.

## Recommended parameters & example invocation

For the **headline reveal**, a modest run is plenty — let it hold for a few minutes, then stop it and vacuum:

```
noisia --xmin-horizon-holder --jobs 4 \
  --xmin-horizon-holder.table-size=1GB \
  --conninfo="host=127.0.0.1 user=postgres dbname=postgres" --duration=10m
```

While it runs, in a second session watch `n_dead_tup` climb and `backend_xmin` stay old:

- `SELECT n_dead_tup, last_autovacuum FROM pg_stat_user_tables WHERE relname LIKE 'noisia_xminhold_%';`
- `SELECT pid, backend_xmin, backend_xid, xact_start FROM pg_stat_activity WHERE application_name = 'noisia';`

Then Ctrl+C noisia and run `VACUUM (VERBOSE) <table>` — `n_dead_tup` collapses. That is the reveal.

For the **wraparound asymptote** (overnight stand only), remove the rate cap and push `--jobs`, and make sure
`idle_in_transaction_session_timeout` is `0` so the holder never reconnects:

```
noisia --xmin-horizon-holder --jobs 64 --xmin-horizon-holder.rate 0 \
  --conninfo="host=127.0.0.1 user=postgres dbname=postgres" --duration=24h
```

> The headline reveal (held horizon → autovacuum powerless → stop → VACUUM drains) reproduces in CI. Real,
> physically-observable bloat (`pgstattuple`, `n_dead_tup` over time) and the wraparound read-only asymptote are
> **hand-verified stand demos** — environment-dependent and measured in hours to days, never in CI. Unlike
> `checkpoint-storm`, no superuser is needed, so the stand can be a managed instance — but check
> `idle_in_transaction_session_timeout` first, or the holder will not hold.
