# Repository Hygiene Summary

Date: 2026-03-17
Status: Completed repository hygiene pass
Owner: Codex

## What Was Kept Active

The active working set is now intentionally small:

- `docs/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/OPERATIONS_PLAYBOOK.md`
- `docs/OPERATIONS_HARDENING_CHECKLIST.md`
- `docs/README.md`
- `docs/architecture/architecture-invariants.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/maintainer-guide.md`
- `docs/architecture/current-state.md`
- `docs/architecture/target-architecture.md`
- `docs/architecture/module-inventory.md`
- `docs/data_protocols/*`
- `docs/migrations/*`
- `docs/specs/*`

These files now represent the canonical architecture, operating, protocol, migration, and maintainer guidance.

## What Was Archived

Historical but still useful material was moved out of the active working set:

- root-level historical plans and execution notes -> `docs/archive/legacy-plans/`
- historical architecture audits, trackers, investigations, and optimization passes -> `docs/architecture/archive/`
- historical runtime parity reports, recovery backups, and older audit outputs -> `backend/runtime/archive/`

This preserves project memory without leaving older plans and investigations mixed into the active docs directories.

## What Was Deleted

True clutter was deleted rather than archived:

- `docs/.DS_Store`
- stale runtime logs
- stale local-app pid/session files
- conflicted SQLite WAL copies
- one temporary ticker-diff scratch output

None of these were durable knowledge surfaces.

## Canonical Permanent Guidance

For current maintenance or extension work, start here:

1. `docs/README.md`
2. `docs/ARCHITECTURE_AND_OPERATING_MODEL.md`
3. `docs/OPERATIONS_PLAYBOOK.md`
4. `docs/architecture/architecture-invariants.md`
5. `docs/architecture/dependency-rules.md`
6. `docs/architecture/maintainer-guide.md`
7. `docs/architecture/current-state.md`
8. `docs/architecture/target-architecture.md`
9. `docs/architecture/module-inventory.md`

Use archived material only for historical context, prior investigations, or implementation archaeology.

## Cleanup Outcome

The active docs surface is smaller and easier to scan.

The key historical waves that were cluttering `docs/` and `docs/architecture/` are still available, but they no longer compete with the permanent architecture and operating guidance.

Future contributors should avoid reintroducing:

- new top-level plan sprawl when the work is already complete
- active docs that are really one-time investigation notes
- runtime logs or scratch outputs checked into the repository working set
- duplicate architecture guidance that competes with the canonical active docs listed above
