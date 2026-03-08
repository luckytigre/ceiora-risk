# Docs Index

## Core Operations
- `OPERATIONS_PLAYBOOK.md`: runbook for refresh, retention, validation, and recovery.
- `OPERATIONS_HARDENING_CHECKLIST.md`: pre-run hygiene, smoke checks, and rollback guardrails.
- `OPERATING_MODEL_PLAN.md`: operating model for universe maintenance, source ingest, core cUSE4 recompute, serving refresh, Neon pruning, and frontend observability.
- `specs/cUSE4_engine_spec.md`: cUSE4 model/foundation spec.
- `Makefile` / `scripts/operator_check.sh`: quick operator validation of `/api/health`, `/api/operator/status`, and parity artifact presence.
- Data page in the app is the live operator cockpit; the playbook mirrors that runtime behavior.

## Migration Notes
- `migrations/neon/`: Neon migration plans, SQL, and runbooks.

## Data Protocols
- `data_protocols/TRBC_CLASSIFICATION_PIT_PROTOCOL.md`: canonical TRBC PIT classification protocol (`security_classification_pit` based).
- `data_protocols/UNIVERSE_ADD_RUNBOOK.md`: approved onboarding workflow for adding new RICs to the universe and backfilling canonical source tables.

## Reference Data
- `../data/reference/security_master_seed.csv`: versioned seed artifact for the canonical universe registry.

## Archived Planning Notes
- `archive_notes/`: historical plans and execution notes kept for context only; do not run commands from these files as active operational guidance.

## User Notes
- `user_notes/`: user-facing/ad hoc notes.
