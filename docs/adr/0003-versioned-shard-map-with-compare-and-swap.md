# ADR 0003: A versioned shard map, changed only by compare-and-swap, served from a cache

- **Status:** Accepted

## Context

Routers in every application process must agree on where a tenant lives, and a move must change
that in steps (copying, replicating, and in M2 fenced and flipped) without two operators
interfering. A configuration file cannot be updated atomically by a tool, and a table of tenant
rows updated in place gives no way to tell whether a router's copy is stale.

## Decision

Keep the whole map as one JSON document with a version number in a control database. Every change
reads the map, applies a function, validates the result and writes version+1 with `UPDATE ...
WHERE version = $3`, where `$3` is the version it read; if no row matched, someone else won and the change is retried on
the new map. Routers cache the map and re-read it after a maximum age, or at once when asked for a
tenant they do not know.

## Consequences

- Two moves of the same tenant cannot both start (the second sees the first's phase), and an
  invalid map (unknown shard, move to itself, a phase without a target) is never stored; unit tests
  cover twenty concurrent writers losing no update.
- A router can be behind by at most its maximum age; in M1 that is harmless because a moving
  tenant is served by its source until the flip. M2's fence makes stale routers safe at cutover by
  refusing writes that arrive with an old version.
- The map is small (one entry per tenant); very large tenant counts would move it to rows with a
  global version, which is a later change.
