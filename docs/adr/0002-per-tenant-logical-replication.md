# ADR 0002: Move a tenant with a row-filtered publication and a copying subscription

- **Status:** Accepted

## Context

A tenant's rows have to reach the new shard while the tenant keeps writing. The copy must be
consistent (taken at one point in time) and every change after that point must follow, exactly
once. Building that from a dump plus a change capture of our own means finding the exact handover
point between the two, which is where data gets lost or duplicated.

## Decision

Use PostgreSQL's logical replication for both steps. On the source, `CREATE PUBLICATION
zdr_move_42 FOR TABLE accounts WHERE (tenant_id = 42), orders WHERE (tenant_id = 42)` for tenant 42. On the
target, `CREATE SUBSCRIPTION ... WITH (copy_data = true)`: PostgreSQL creates a replication slot,
copies a snapshot of the filtered rows, and streams changes from the slot's position, so the
handover point is chosen by the database. Every table's primary key starts with `tenant_id`, which
makes the row filter valid for updates and deletes.

## Consequences

- Writes during the copy are neither lost nor duplicated: the integration test runs about 7,500
  transactions from four writers through a move and finds identical counts and checksums.
- Other tenants never reach the target: the row filter is applied on the source.
- Removing the filter or the snapshot copy makes the integration tests fail (checked by
  injecting each change).
- The source keeps WAL for the slot until the move finishes or is aborted, so moves must not be
  left running unattended.
