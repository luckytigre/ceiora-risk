# Audit: Simplification Opportunities

Date: 2026-03-16
Reviewer: Independent architecture audit
Status: Simplification-focused review

## Summary

The refactor did not obviously over-engineer the whole repo. The bigger problem is a smaller one:

- some new module splits are good
- some facade layers are still in a middle state
- several compatibility seams and mixed-ownership files remain

The right next moves are mostly subtraction and clarification, not more architecture.

## Highest-Leverage Simplifications

### 1. Stop routes from importing `backend.data` directly

This is the cleanest simplification still available.

Right now several routes still directly wire:

- `load_runtime_payload`
- `cache_get`
- `history_queries`

If the route should stay thin, the route should only delegate to:

- a service
- or a very small presenter/helper

This is a simplification because it reduces variation in route patterns. It does not require adding more layers than the repo already has.

### 2. Remove the operator-route compatibility export block

`backend/api/routes/operator.py` still re-exports service internals such as:

- `profile_catalog`
- `job_runs`
- `core_reads`
- `runtime_state`
- `sqlite`
- `_risk_recompute_due`

This is purely compatibility scaffolding now. It keeps old testing/patching expectations alive in the route layer.

That should be retired once the remaining tests stop depending on it.

### 3. Stop importing `DATA_DB` from the orchestrator into the operator service

`backend/services/operator_status_service.py` should not depend on `backend/orchestration/run_model_pipeline.py` for a path constant.

This should be simplified to a direct config/data-layer dependency.

### 4. Simplify `backend/orchestration/__init__.py`

`backend/orchestration/__init__.py` currently re-exports the main workflow entrypoint. That looks convenient, but it also makes the package surface less explicit and encourages package-level coupling.

If the real public entrypoint is `backend.orchestration.run_model_pipeline`, importing it directly is clearer.

### 5. Either commit to the `core_reads.py` / `model_outputs.py` facades or slim them further

Right now these modules are in a middle state:

- smaller than before
- still wrapper-heavy
- still carrying internal helper names

There are only two clean end states:

1. keep them as stable public facades and stop treating the helper modules as quasi-public
2. or make callers import the real submodules directly and shrink the facades further

The current hybrid state is a little more indirection than value.

### 6. Reassess whether the frontend type split should stay purely barrel-based

`frontend/src/lib/types.ts` is now a barrel over smaller modules, but almost every consumer still imports the barrel.

That means the change improved internal file organization more than actual dependency narrowing.

That is fine if the goal was just file organization. It is not strong evidence of frontend simplification yet.

The repo should either:

- accept that the barrel is the real public surface
- or gradually narrow imports by feature

What should be avoided is continuing to count this as a fully realized cleanup win when it is mostly internal.

## Layers That Could Be Collapsed Safely

### 1. Some tiny one-function wrappers in facades

In `backend/data/core_reads.py`, several private wrapper functions are mostly plumbing:

- `_use_neon_core_reads`
- `_missing_tables`
- `_resolve_latest_barra_tuple`
- `_resolve_latest_well_covered_exposure_asof`

Not all need to disappear, but some of this wrapper density could be reduced.

### 2. Duplicate policy wrappers

The repo still has multiple thin wrappers over the same recompute logic.

Those should be collapsed where practical, especially if they are only passing through constants.

## Modules That Should Probably Stay As-Is For Now

These should not be churned further without a concrete bug or ownership win:

- `backend/services/dashboard_payload_service.py`
- `backend/services/data_diagnostics_service.py`
- `backend/services/operator_status_service.py` as the central operator payload builder
- `backend/analytics/refresh_context.py`
- `backend/analytics/reuse_policy.py`

These are doing real structural work now.

## Modules That Still Want Deletion-Oriented Cleanup

- `backend/api/routes/operator.py` compatibility re-export block
- `backend/orchestration/__init__.py` public re-export convenience
- refactor-era wrapper/helper names that no longer serve a testing or public-surface need

## Bottom Line

The next simplification pass should be conservative and subtractive:

1. remove compatibility leftovers
2. tighten route boundaries
3. reduce unhelpful wrapper indirection

Do not add a new abstraction layer. The repo does not need one.
