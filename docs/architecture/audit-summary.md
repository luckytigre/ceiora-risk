# Architecture Audit Summary

Date: 2026-03-16
Reviewer: Independent architecture audit
Status: Post-restructure summary

Update note:

- Follow-up Batches 1 through 3 later on 2026-03-16 remediated the first four issues listed below:
  - route/data dependency-rule violations
  - `operator_status_service.py` importing the orchestrator
  - hidden runtime path mutation
  - `stage_runner.py` remaining a branch-heavy workflow core
- The remaining items in this summary should still be treated as active unless separately noted.

## Overall Judgment

This architecture is not merely cosmetically cleaner. The refactor delivered real structural improvements.

It is also not fully “done.” The repo is now in a better, more navigable state, but several important exceptions remain in the exact places where operational complexity still lives.

Assessment:

- stable enough for ordinary feature work: **yes**
- over-engineered overall: **no**
- under-structured in key operational areas: **yes**
- appropriately balanced overall: **mostly yes**, with a few remaining hot spots

## Top 10 Remaining Architectural Issues

### 1. Routes still violate the documented dependency rules

Several API routes still import `backend.data` directly, which contradicts the declared architecture.

Primary files:

- `backend/api/routes/exposures.py`
- `backend/api/routes/risk.py`
- `backend/api/routes/portfolio.py`
- `backend/api/routes/health.py`
- `backend/api/routes/universe.py`
- `backend/api/routes/readiness.py`

### 2. `operator_status_service.py` depends on the orchestrator module

It imports `DATA_DB` from `backend/orchestration/run_model_pipeline.py`, which is the wrong direction of dependency for an operator payload builder.

### 3. Hidden global state mutation still exists in rebuild workflows

`backend/orchestration/runtime_support.py` mutates config and module-level DB paths during execution. This is one of the highest-risk remaining sources of surprise behavior.

### 4. `stage_runner.py` is still a large branch-heavy workflow core

The orchestrator is decomposed, but stage implementation still lives in one large stage family module.

### 5. `backend/services/` still mixes application services and infrastructure modules

This weakens ownership clarity.

Primary examples:

- `dashboard_payload_service.py` vs `neon_mirror.py`
- `operator_status_service.py` vs `neon_authority.py`
- `holdings_service.py` vs `neon_holdings.py`

### 6. Several major operational modules remain god files

Most important:

- `backend/services/neon_mirror.py`
- `backend/analytics/health.py`
- `backend/services/neon_holdings.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/data/cross_section_snapshot.py`

### 7. The plan/docs overstate how complete the target architecture is

Phases are marked complete even though important structural exceptions remain.

### 8. Frontend contract cleanup is only partly realized

The type files were split, but consumers still overwhelmingly import the barrel:

- `frontend/src/lib/types.ts`

This is an organizational improvement, not yet a full dependency-surface cleanup.

### 9. Compatibility leftovers still exist in transport and orchestration edges

Examples:

- `backend/api/routes/operator.py` compatibility re-exports
- `backend/orchestration/__init__.py` convenience export surface

### 10. Operator/runtime truth is centralized but still assembled from too many stores

This is better than scattering, but the underlying truth model is still transitional and drift-prone.

Primary owner:

- `backend/services/operator_status_service.py`

## What Is Now Clean And Well-Structured

### 1. The architecture documentation package is real and useful

`docs/architecture/` is now a meaningful working artifact, not just decoration.

### 2. Dashboard-serving payload assembly is much better centralized

`backend/services/dashboard_payload_service.py` is a good refactor and a good pattern.

### 3. Operator/data diagnostics payload assembly moved out of routes

This materially improved the route layer.

### 4. The main orchestrator is no longer a single monolith

The split into profiles/planning/execution/finalization/runtime support is a real gain.

### 5. The refresh pipeline split is real

The extracted modules around refresh context, reuse, publish, and persistence are a solid improvement.

## What Is Still Fragile Or Confusing

- path mutation during workspace-based rebuild flows
- service/orchestrator coupling around refresh and operator status
- mixed service package ownership
- route inconsistency outside the main dashboard payload paths
- large remaining Neon/health/risk-model operational modules

## What Should Be Fixed Next

These are the highest-leverage small next steps.

1. Remove direct `backend.data` imports from the remaining routes by pushing those reads behind services or very small presenter/helpers.
2. Remove `operator_status_service.py` dependency on `backend/orchestration/run_model_pipeline.py`.
3. Retire the compatibility export block in `backend/api/routes/operator.py`.
4. Simplify `backend/orchestration/__init__.py` so it stops acting as a convenience surface for the whole workflow package.
5. Clarify docs where current exceptions still exist instead of continuing to label those areas “complete.”

## What Should Not Be Touched Further Right Now

- do not add a new abstraction layer for repositories/managers/common utilities
- do not aggressively rename directories to force the code into the exact target tree from `target-architecture.md`
- do not split small helper modules further just to satisfy an abstract layering ideal
- do not churn the frontend type surface again unless the backend contracts change materially

## Final Verdict

This architecture is **meaningfully improved and usable**, not fake-clean.

But it is **not fully clean yet**. The remaining issues are concentrated and understandable:

- route/data leakage
- service/orchestrator coupling
- hidden runtime mutation
- a handful of oversized operational modules

That is a good outcome for a major refactor. It means the next work should be selective, not another broad sweep.
