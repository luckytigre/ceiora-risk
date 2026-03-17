# Final Architecture Audit

Date: 2026-03-16
Status: Final architecture check
Owner: Codex

## Boundary Discipline

### Strong

- `backend/api/routes/*` is materially thinner than before and now delegates through service modules for the previously audited surfaces.
- `backend/services/operator_status_service.py` no longer imports `backend.orchestration.run_model_pipeline`.
- runtime-path retargeting is explicit rather than hidden in cross-module mutation.
- `backend/data/cross_section_snapshot.py`, `backend/services/neon_holdings.py`, and `backend/services/data_diagnostics_service.py` are now facades over clearer internal helper seams.

### Still Mixed But Acceptable

- `backend/services/` still contains both route-facing application services and Neon-heavy operational modules.
- `backend/analytics/health.py` still mixes deep diagnostics concerns in one large domain-facing file.
- `backend/orchestration/run_model_pipeline.py` remains the central integration shell, though no longer a god module in the old sense.

## Dependency Direction

Observed direction is consistent with the active rules:
- routes -> services
- services -> analytics / portfolio / universe / risk_model / data
- orchestration -> lower layers plus narrow operational service surfaces
- data does not import API or frontend layers

One deliberate exception shape remains:
- `backend/services/refresh_manager.py` imports the orchestration entrypoint because it is the runtime controller that launches jobs, not because it is inspecting static metadata

That is an acceptable service -> workflow dependency.

## Hidden Coupling / Leakage

### Reduced Successfully

- hidden global path mutation is gone
- operator truth no longer leaks through orchestrator constants
- route-local storage wiring was removed from the audited route surfaces

### Remaining

- configuration-backed module defaults still exist in several data and workflow facades
- some tests still target deliberate compatibility seams like `_run_stage`

These are explicit and bounded, not hidden architectural leakage.

## Over-Abstraction vs Under-Abstraction

Current balance is good.

The repository is not over-engineered:
- no generic repository layer
- no heavy DI framework
- no framework-like shared package

It is also no longer under-structured in the areas that mattered most:
- operator truth
- dashboard payload serving
- orchestration
- refresh pipeline
- holdings workflows
- cross-section snapshot rebuild

## Remaining “God Modules”

The real remaining concentration points are:
- `backend/services/neon_mirror.py`
- `backend/analytics/health.py`
- `backend/risk_model/daily_factor_returns.py`

These are not silent failures of the restructure. They are explicitly deferred dense modules.

## Ownership Clarity

Ownership is now mostly clear:
- operator truth -> `operator_status_service.py`
- dashboard-serving truth -> `dashboard_payload_service.py`
- data diagnostics -> `data_diagnostics_service.py`
- refresh lifecycle -> `refresh_manager.py`
- job sequencing -> `backend/orchestration/*`
- holdings workflows -> `neon_holdings.py` facade + helper modules
- cross-section snapshot rebuild -> `cross_section_snapshot.py` facade + helper modules

## Final Judgement

The architecture is structurally sound and materially cleaner than before.

It is not perfectly pure, but the remaining impurity is concentrated, documented, and mostly deliberate.
