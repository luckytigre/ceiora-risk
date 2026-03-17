# Refactor Roadmap

Date: 2026-03-16
Status: Sequenced restructuring roadmap
Owner: Codex

## Roadmap Principles

- incremental and reversible
- documentation updated in every batch
- behavior preserved unless change is explicitly justified
- simplify before abstracting

## Phase 1: Architecture Documentation And First Truth-Surface Batch

Goals:
- establish one architecture documentation package
- centralize dashboard-serving route assembly
- make routes thinner and payload ownership clearer

Changes:
- create `docs/architecture/*`
- extract `dashboard_payload_service`
- move route-local serving payload loading/normalization out of:
  - `api/routes/exposures.py`
  - `api/routes/risk.py`
  - `api/routes/portfolio.py`
- keep route contracts stable

Why first:
- low risk
- high clarity win
- consistent with recent operator/data service extraction

## Phase 2: Orchestration Decomposition

Status:
- completed for the original scope

Goals:
- shrink `run_model_pipeline.py`
- separate profile metadata from execution

Changes:
- extract `profiles.py`
- extract stage planning
- extract post-run publish/report logic
- extract orchestration runtime-policy helpers
- leave `run_model_pipeline.py` as thin composition layer

Risks:
- tests and monkeypatch-heavy call sites may still expect imports from the old file

## Phase 3: Refresh Pipeline Decomposition

Goals:
- split `analytics/pipeline.py` by role

Changes:
- `refresh_context.py`
- `reuse_policy.py`
- `publish_payloads.py`
- `refresh_persistence.py`
- thin refresh coordinator

Why after orchestration:
- the orchestrator and refresh pipeline are the two largest workflow hotspots
- separating them in sequence is safer than changing both at once without stable docs

## Phase 4: Storage Surface Cleanup

Status:
- completed for the original scope

Goals:
- reduce fragility in `core_reads.py` and `model_outputs.py`

Changes:
- split transport, source-date, and source-read helpers out of `core_reads.py`
- split `model_outputs.py` into schema, state, payload, and writer helpers
- preserve explicit Neon-first durability
- migrate tests off facade-private monkeypatch hooks where the newer module seams are now stable

## Phase 5: Frontend Contract Cleanup

Status:
- completed for the original scope

Goals:
- reduce page-level truth assembly and oversized contract surfaces

Changes:
- split `frontend/src/lib/types.ts` into domain-specific contract modules behind a stable barrel
- keep `analyticsTruth.ts` as a narrow shared helper
- update page-level consumers to rely on clearer backend payloads

## Phase 6: Documentation Consolidation

Status:
- completed for the original scope

Goals:
- reduce plan sprawl
- keep canonical guidance obvious

Changes:
- keep `docs/architecture/restructure-plan.md` as active restructuring tracker
- demote old plan docs to historical/subordinate status where appropriate

## Deferred Items

- full Neon-native rebuild execution without scratch SQLite
- distributed refresh locking
- deeper route contract standardization across every endpoint
- broad test architecture cleanup
