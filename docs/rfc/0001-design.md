# RFC 0001: zero-downtime-resharding design

- **Status:** Accepted (M1 implemented)
- **Author:** EquinoxWN
- **Created:** 2026

## Problem

A multi-tenant SaaS starts with every tenant in one PostgreSQL database. When that database fills
up, or one tenant grows large enough to slow the others down, tenants have to be spread over
several databases (shards), and later moved between them. The naive move (stop the tenant, dump,
restore, switch) means minutes of downtime for that customer. A move without downtime needs three
things: every request must find the tenant's current shard through a routing layer, the tenant's
rows must be copied while it keeps writing, and the switch-over must be short and safe.

## Goals

- **M1 (this RFC):**
  - Every table leads its primary key with `tenant_id`, and every request carries a tenant id.
  - A versioned shard map, stored in a control database and changed only by compare-and-swap,
    says which shard serves each tenant and whether it is being moved.
  - A router library caches the map (re-reading it after a maximum age, or at once for an unknown
    tenant), keeps one connection pool per shard, and runs each tenant's work in a transaction on
    its shard with the tenant id set in `app.tenant_id` (ready for row-level security).
  - A move tool copies one tenant to another shard while it keeps writing: a publication on the
    source filtered to `tenant_id = n` on every table, and a subscription on the target that copies
    a consistent snapshot and then streams every later insert, update and delete. It reports the
    copy's progress and the replication lag in bytes, refuses unsafe moves, and can abort without
    leaving anything behind.
  - A verifier compares a tenant's rows on two shards by count and an md5 of all rows in key order.
- **M2:** cutover: when the lag is near zero, fence the tenant's writes for a few hundred
  milliseconds, drain the last changes, flip the map, and have clients retry fenced writes.
- **M3:** before deleting the source copy, compare counts and checksums and roll the map back on
  any mismatch; k6 load during every move, recording error rate and p99 latency.

## Non-goals

- Moving parts of a tenant: the unit of placement is the whole tenant.
- Schema changes during a move.
- Cross-shard queries and transactions.

## Proposed design

```
           control database: shard_map (version, body)  <-- compare-and-swap updates
                     |
            router (cached map) ----> shard-a  (tenant 3 lives here until cutover)
                                          |  publication zdr_move_3:
                                          |  accounts WHERE tenant_id = 3, orders WHERE tenant_id = 3
                                          v
                                       shard-b  subscription zdr_move_3 (copy_data = true):
                                                 1. copy a consistent snapshot of tenant 3
                                                 2. stream every change committed after it
```

- **Shard map:** `{version, shards: {id: dsn}, tenants: {id: {shard, moving_to, phase}}}`. Every
  update reads the current map, applies a change, validates it (known shards, a move needs a
  different target and a phase) and writes version+1 only if the stored version is unchanged;
  conflicting writers retry. Two operators can therefore never start two moves of one tenant.
- **Moving tenant:** during `copying` and `replicating` the tenant is still served by its source;
  the target only receives replicated rows. The router reads `shard`, never `moving_to`, until M2
  flips the map.
- **Why logical replication:** PostgreSQL's subscription with `copy_data = true` takes the snapshot
  copy and starts streaming from exactly the position where the snapshot ends, so no write between
  the two is lost or applied twice. Row filters (PostgreSQL 15+) restrict both to one tenant; they
  are allowed for updates and deletes because `tenant_id` is part of every primary key, which is
  the replica identity.
- **Lag:** `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)` of the move's replication
  slot on the source: how much WAL the target has not yet confirmed as applied.
- **Safety:** a move to the same shard, of an unknown tenant, of a tenant already moving, or onto a
  shard that already holds rows of that tenant is refused before anything is created, and the map
  is left stable. If creating the publication or the subscription fails, everything created so far
  is dropped. Abort drops the subscription (and with it the slot), the publication, and the copied
  rows, and marks the tenant stable on its source.
- **SQL built from data:** tenant ids are integers (formatted, not interpolated text); table names
  must be plain identifiers and are quoted; the connection details in `CREATE SUBSCRIPTION` are
  quoted for libpq and again as one SQL literal.

## Alternatives considered

| Option | Why not (yet) |
|---|---|
| Dump and restore during a maintenance window | Minutes of downtime per tenant; exactly what this project removes. |
| Dual writes from the application | Every write path must change, and a failure between the two writes leaves the shards different with nothing to repair them. |
| Trigger-based change capture (copy table plus triggers) | Adds write cost on the source for every table and needs its own catch-up logic; logical replication is built in. |
| A CDC pipeline (Debezium and Kafka) | Correct, but heavy for moving one tenant between two PostgreSQL databases; worth it when the target is not PostgreSQL. |
| Hash-based placement (tenant id modulo shards) | Moving one tenant would mean changing the function for everyone; an explicit map moves one tenant at a time. |

## Measurement plan

- Correctness: after writes during and after the copy, counts and checksums of every table are
  equal on both shards, and no other tenant's rows reach the target (integration tests and demo).
- During a move: transactions served, failures, p99 latency, and lag in bytes over time (demo).
- M3: k6 error rate and p99 during every move, including the cutover.

## Milestones

- **M1 (done):** shard map with compare-and-swap, router, snapshot copy plus logical replication,
  lag, abort, verifier, integration tests on real PostgreSQL 17.
- **M2:** write fence, drain, map flip, client retries.
- **M3:** verify-then-delete with automatic rollback, k6 under load.

## Risks and open questions

- Replication slots keep WAL on the source until the target confirms it; a stuck move must be
  aborted or the source's disk fills. M2 adds a lag alarm.
- Sequences and DDL are not replicated by PostgreSQL; this schema uses keys chosen by the
  application, and schema changes are out of scope during a move.
- The subscription stores the source's password in the target's catalog; a dedicated replication
  role with only the needed rights is the production setup.
