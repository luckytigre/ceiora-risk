# Docs Index

This repo keeps the active docs surface intentionally small.

Use this file to find the current source of truth first, then drop into supporting docs only when needed.

## Start Here By Task

- Architecture and operating model:
  - `architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
  - supporting rules: `architecture/architecture-invariants.md`, `architecture/dependency-rules.md`, `architecture/maintainer-guide.md`
- Universe redesign plan:
  - `architecture/UNIVERSE_REGISTRY_AND_MODEL_GATING_PLAN.md`
- Current cUSE/cPAR remediation plan:
  - `architecture/CUSE_CPAR_AUTHORITY_AND_READ_SURFACE_PLAN.md`
- cUSE runtime and operator workflow:
  - `operations/OPERATIONS_PLAYBOOK.md`
  - `operations/CLOUD_NATIVE_RUNBOOK.md` for Cloud Run topology and deploy/runtime split
  - `operations/FULL_CLOUD_COMPUTE_CUTOVER_PLAN.md` while the active cutover window remains open
  - `operations/OPERATIONS_HARDENING_CHECKLIST.md` for pre/post refresh hygiene
- cUSE model specification:
  - `reference/specs/cUSE4_engine_spec.md`
  - `reference/specs/USE4_US_CORE_MARKET_ADR_2026-03-15.md`
- cPAR overview and active contracts:
  - `architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md`
  - `operations/CPAR_OPERATIONS_PLAYBOOK.md`
- Universe and registry maintenance:
  - `reference/protocols/UNIVERSE_ADD_RUNBOOK.md`
  - `reference/protocols/TRBC_CLASSIFICATION_PIT_PROTOCOL.md`
  - `../data/reference/security_registry_seed.csv`
  - `../data/reference/security_master_seed.csv` as compatibility artifact only

## Active Doc Buckets

### Architecture

Canonical architecture docs live under `docs/architecture/`.

Primary files:
- `ARCHITECTURE_AND_OPERATING_MODEL.md`
- `architecture-invariants.md`
- `dependency-rules.md`
- `maintainer-guide.md`
- `MODEL_FAMILIES_AND_OWNERSHIP.md`
- `UNIVERSE_REGISTRY_AND_MODEL_GATING_PLAN.md`
- `CUSE_CPAR_AUTHORITY_AND_READ_SURFACE_PLAN.md`

cPAR-specific active architecture docs stay in the same folder:
- `CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md`
- `CPAR1_MATH_KERNEL.md`
- `CPAR_PERSISTENCE_LAYER.md`
- `CPAR_ORCHESTRATION.md`
- `CPAR_BACKEND_READ_SURFACES.md`
- `CPAR_FRONTEND_SURFACES.md`

### Operations

Live runbooks and checklists live under `docs/operations/`.

Primary files:
- `OPERATIONS_PLAYBOOK.md`
- `CLOUD_NATIVE_RUNBOOK.md`
- `FULL_CLOUD_COMPUTE_CUTOVER_PLAN.md` while the cutover is still open
- `OPERATIONS_HARDENING_CHECKLIST.md`
- `CPAR_OPERATIONS_PLAYBOOK.md`

Temporary or specialized active docs:
- `architecture/FRONTEND_AUTH_AND_CUSTOM_DOMAIN_PLAN.md`
  - active implementation record for the frontend auth boundary, `/home` migration, private-backend cutover, and `app.ceiora.com` restoration path
- `architecture/TEST_SUITE_TIGHTENING_PLAN.md`
  - engineering cleanup tracker, not an operational source of truth

### Reference

Stable specs, protocols, and schema docs live under `docs/reference/`.

Primary files:
- `reference/specs/cUSE4_engine_spec.md`
- `reference/specs/USE4_US_CORE_MARKET_ADR_2026-03-15.md`
- `reference/specs/FRONTEND_PALETTE_AND_THEME_SYSTEM.md`
- `reference/protocols/TRBC_CLASSIFICATION_PIT_PROTOCOL.md`
- `reference/protocols/UNIVERSE_ADD_RUNBOOK.md`
- `reference/migrations/`
- `reference/migrations/README.md`

## Archive Taxonomy

Historical material is preserved, but it is not active guidance.

- `archive/implementation-trackers/`
  - completed rollout trackers, implementation trackers, and archived planning specs
  - includes the archived repo-tightening tracker:
    `archive/implementation-trackers/REPO_TIGHTENING_PLAN_2026-03-28.md`
  - includes archived cloud migration trackers:
    - `archive/implementation-trackers/CLOUD_NATIVE_IMPLEMENTATION_PLAN_2026-03-21.md`
    - `archive/implementation-trackers/GCP_CLOUD_RUN_TERRAFORM_PLAN_2026-03-22.md`
- `archive/one-time-protocols/`
  - procedures that were useful for a specific cleanup or migration slice and are no longer live runbooks
- `archive/execution-logs/`
  - one-off execution logs and post-cutover run records
- `archive/legacy-plans/`
  - older planning docs retained as-is from earlier cleanup phases
- `archive/migrations/`
  - retired migration notes and operator runbooks
- `architecture/archive/`
  - historical architecture audits, investigations, inventories, and remediation notes

If an archived file conflicts with an active doc, the active doc wins.

## Working Rules

- Project documentation should live under `docs/`, not at the repo root.
- Root-level Markdown should be limited to repo meta/instructions such as `AGENTS.md`.
- Completed trackers and one-time cleanup notes should be archived promptly instead of remaining in the active architecture or reference surface.
- Persisted cleanup slice notes and one-off execution records belong under `archive/one-time-protocols/` or `archive/execution-logs/`, not in active architecture docs or at repo root.
