# Security policy

## Reporting a vulnerability

Please report security problems privately through GitHub:
**Security > Report a vulnerability** on this repository
(`https://github.com/EquinoxWN/zero-downtime-resharding/security/advisories/new`). Do not open a public issue.

Include what you found, how to reproduce it, and the impact you expect. You will get an answer
within 7 days. Fixes are released as soon as they are ready, and you are credited unless you ask
not to be.

## Supported versions

This project is pre-1.0. Only the latest commit on `main` receives fixes.

## Scope

- **In scope:** A move that loses, duplicates or leaks rows (another tenant's data reaching a shard), SQL injection through tenant ids, table names or connection details, routing a tenant to the wrong shard, and credentials printed in logs or errors.
- **Out of scope:** PostgreSQL's own replication behaviour (report it upstream), and denial of service from unlimited local load.

## How this repository protects itself

- Every GitHub Action is pinned to a full commit SHA, and workflows run with read-only
  permissions and without persisted credentials.
- Dependabot proposes dependency and action updates weekly as reviewable pull requests.
- CI runs lint, tests and a known-vulnerability check (govulncheck) on every push and pull request.
