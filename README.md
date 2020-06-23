# Noisia

**Harmful workload generator for PostgreSQL.**

---

Supported workloads:
- `idle transactions` - transactions that do nothing during its lifetime.

- `rollbacks` - transactions performed some work but rolled back in the end.

- `waiting transactions` - transaction locked by other transactions and thus waiting.

- `deadlocks` - simultaneous transactions where each hold locks that the other transactions wants.

- `temporary files` - queries that produce on-disk temporary files due to lack of `work_mem`.

- ...see built-in help for more runtime options.

---
**ATTENTION: Use only for testing purposes, don't execute against production, reckless usage will cause problems.**

**DISCLAIMER: THIS SOFTWARE PROVIDED AS-IS WITH NO CARES AND GUARANTEES RELATED TO YOU DATABASES. USE AT YOUR OWN RISK.**
