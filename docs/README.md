# Docs Index

## Core Operations
- `OPERATIONS_PLAYBOOK.md`: runbook for refresh, retention, validation, and recovery.
  - canonical reference for the three horizons:
    - active Barra model history
    - risk-model lookback
    - local/Neon source retention
  - current live model is a 45-factor set with 14 style factors and no standalone `Value` factor
- `OPERATIONS_HARDENING_CHECKLIST.md`: pre-run hygiene, smoke checks, and rollback guardrails.
- `ARCHITECTURE_AND_OPERATING_MODEL.md`: canonical project architecture, data flow, runtime roles, operating model, and known open limitations.
- `architecture/`: active architecture package.
  - start with `architecture/architecture-invariants.md`, `architecture/dependency-rules.md`, `architecture/maintainer-guide.md`, `architecture/current-state.md`, `architecture/target-architecture.md`, and `architecture/module-inventory.md`
  - historical audits, plans, investigations, and pass summaries now live under `architecture/archive/`
- `specs/cUSE4_engine_spec.md`: cUSE4 model/foundation spec.
- `specs/USE4_US_CORE_MARKET_ADR_2026-03-15.md`: accepted ADR for the US-core `Market` factor migration, one-stage WLS target, and post-cutover cleanup end state.
- `Makefile` / `scripts/operator_check.sh`: quick operator validation of `/api/health`, `/api/operator/status`, and parity artifact presence.
- Health page in the app is the live operator/runtime cockpit, including refresh warnings and top-level model quality.
- Data page is the maintenance surface for source-table lineage, coverage, cache surfaces, and integrity diagnostics.
- Health page diagnostics are intentionally on-demand and section-lazy; this is expected behavior, not missing content.

## Migration Notes
- `migrations/neon/`: Neon migration plans, SQL, and runbooks.

## Data Protocols
- `data_protocols/TRBC_CLASSIFICATION_PIT_PROTOCOL.md`: canonical TRBC PIT classification protocol (`security_classification_pit` based).
- `data_protocols/UNIVERSE_ADD_RUNBOOK.md`: approved onboarding workflow for adding new RICs to the universe and backfilling canonical source tables.

## Reference Data
- `../data/reference/security_master_seed.csv`: versioned registry-only seed artifact for the canonical universe registry.

## Archived Planning Notes
- `archive/legacy-plans/`: historical root-level plans and execution notes kept for context only; do not run commands from these files as active operational guidance.
- `architecture/archive/`: historical architecture audits, restructure/remediation trackers, investigations, and optimization pass records.

## User Notes
- `user_notes/`: user-facing/ad hoc notes.
