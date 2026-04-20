# Docs Index

This repo keeps the active docs surface intentionally small.

Read the canonical docs first. Use workstream plans and archived material only when the active docs explicitly point you there.

## Canonical Docs

### Architecture

- `architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md`
- `architecture/MODEL_FAMILIES_AND_OWNERSHIP.md`
- `architecture/architecture-invariants.md`
- `architecture/dependency-rules.md`
- `architecture/maintainer-guide.md`
- `architecture/UNIVERSE_REGISTRY_AND_MODEL_GATING_PLAN.md`

### Operations

- `operations/OPERATIONS_PLAYBOOK.md`
- `operations/CLOUD_NATIVE_RUNBOOK.md`
- `operations/CPAR_OPERATIONS_PLAYBOOK.md`

### Stable Reference

- `reference/specs/cUSE4_engine_spec.md`
- `reference/specs/USE4_US_CORE_MARKET_ADR_2026-03-15.md`
- `reference/protocols/UNIVERSE_ADD_RUNBOOK.md`
- `reference/protocols/TRBC_CLASSIFICATION_PIT_PROTOCOL.md`
- `reference/migrations/README.md`

## Current System Notes

These are the current repo-level decisions worth knowing before you start editing:

- `cUSE` remains the default application family. `cPAR` is parallel and explicitly namespaced.
- Neon is the operating source of truth for runtime reads when `DATA_BACKEND=neon`; local SQLite remains ingest, archive, mirror, and scratch.
- Risk pages now optimize first useful render:
  - `cUSE` first render uses `/api/cuse/risk-page`
  - `cPAR` first render uses `/api/cpar/risk`
  - heavier diagnostics, covariance, and history are lazy or supplemental
- Explore pages now use compact family-owned bootstrap surfaces:
  - `/api/cuse/explore/context`
  - `/api/cpar/explore/context`
- What-if previews are intentionally preview-only until explicit apply, and preview scope is the staged account set rather than the whole book by default.
- The live web topology is one public frontend at `app.ceiora.com` with private `serve` and `control` backends behind the frontend proxy.

## Temporary Active Workstreams

Keep these only while the work is actually active:

- `operations/FULL_CLOUD_COMPUTE_CUTOVER_PLAN.md`
  - temporary cutover execution plan while cloud compute cutover remains open
- `architecture/CPAR_HEDGE_WORKSTREAM_PLAN_2026-04-20.md`
  - temporary hedge-package implementation plan while `/cpar/risk` hedge popovers and `/cpar/hedge` are still in flight

## Supporting Drill-Down Docs

Use these when you need more detail than the canonical docs provide:

- cPAR math and storage:
  - `architecture/CPAR1_MATH_KERNEL.md`
  - `architecture/CPAR_PERSISTENCE_LAYER.md`
  - `architecture/CPAR_ORCHESTRATION.md`
- cPAR read and frontend surfaces:
  - `architecture/CPAR_BACKEND_READ_SURFACES.md`
  - `architecture/CPAR_FRONTEND_SURFACES.md`
- historical recovery and implementation records:
  - `archive/implementation-trackers/CUSE_CPAR_AUTHORITY_AND_READ_SURFACE_PLAN_2026-04-15.md`
  - `archive/implementation-trackers/FRONTEND_AUTH_AND_CUSTOM_DOMAIN_PLAN_2026-04-14.md`

## Archive Rules

- Completed rollout trackers, superseded plans, and one-off execution notes belong under `docs/archive/*`.
- Active folders should describe the current system, not preserve old execution plans as stubs or pointers.
- If an archived file conflicts with a canonical doc, the canonical doc wins.
