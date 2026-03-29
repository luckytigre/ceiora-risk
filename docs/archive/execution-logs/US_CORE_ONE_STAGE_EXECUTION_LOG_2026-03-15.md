# US-Core One-Stage Migration Execution Log

Archived execution log.

Use active architecture and operations docs for the live contract.

Date: 2026-03-15
Branch: `use4-us-core-market-migration`
Status: Completed

## Purpose

Track each implementation phase, the concrete edits made, and the validation run after each stage.

## Phase 0

### 0.1 Branch and baseline setup

- Created branch: `use4-us-core-market-migration`
- Confirmed dirty pre-existing docs worktree and preserved it as in-flight user state

### 0.2 Phase-0 documentation

- Added ADR:
  - `docs/specs/USE4_US_CORE_MARKET_ADR_2026-03-15.md`
- Added this execution log:
  - `docs/archive/execution-logs/US_CORE_ONE_STAGE_EXECUTION_LOG_2026-03-15.md`

### 0.3 Initial seam inventory

Identified current live seam areas:

- sequential two-phase estimator in `backend/risk_model/wls_regression.py`
- monolithic regression assembly in `backend/risk_model/daily_factor_returns.py`
- raw/string-based factor identity in `backend/risk_model/risk_attribution.py`
- overloaded payload eligibility semantics in:
  - `backend/analytics/contracts.py`
  - `backend/analytics/services/universe_loadings.py`
  - `backend/analytics/services/risk_views.py`
  - `backend/analytics/pipeline.py`
  - `frontend/src/lib/types.ts`
  - `frontend/src/lib/factorLabels.ts`
- health/operator assumptions in:
  - `backend/analytics/health.py`
  - `backend/analytics/services/cache_publisher.py`
- local/Neon boundary documented in:
  - `docs/OPERATIONS_PLAYBOOK.md`
  - `docs/ARCHITECTURE_AND_OPERATING_MODEL.md`

### 0.4 Baseline validation results

- `backend/.venv/bin/pytest`
  - result: `143 passed in 177.68s`
- `cd frontend && npm run typecheck`
  - result: passed
- `cd frontend && npm run build`
  - result: passed

### 0.5 Local/Neon boundary notes captured

- Local SQLite remains the heavy-compute and first-durable-write authority.
- Local cache tables remain the factor-return / residual workspace.
- Neon remains the bounded serving mirror and holdings authority when configured.
- Cloud-serving consumes durable serving payloads and bounded mirrored analytics state rather than recomputing the core model.

## Phase 1

### 1.1 Architectural scaffolding added

New backend modules:

- `backend/risk_model/factor_catalog.py`
  - stable factor-id generation
  - factor family inference
  - catalog builder and serializer
- `backend/risk_model/model_status.py`
  - `core_estimated / projected_only / ineligible`
- `backend/risk_model/regression_frame.py`
  - `RegressionFrameBuilder`
  - `RegressionFrameSummary`
  - `RegressionFrameBuildResult`

### 1.2 Existing code refactored to use scaffolding

- `backend/risk_model/risk_attribution.py`
  - now sources factor identity semantics from the factor-catalog layer
- `backend/risk_model/daily_factor_returns.py`
  - now uses `RegressionFrameBuilder` for date-level regression assembly
- `backend/analytics/services/universe_loadings.py`
  - now emits `model_status`
- `backend/analytics/services/risk_views.py`
  - now carries `model_status` into position payloads
- `backend/analytics/contracts.py`
  - added `model_status` fields to typed payloads
- `frontend/src/lib/types.ts`
  - added model-status support to relevant interfaces

### 1.3 New test coverage

Added:

- `backend/tests/test_factor_catalog.py`
- `backend/tests/test_model_status.py`
- `backend/tests/test_regression_frame_builder.py`

### 1.4 Validation

- `backend/.venv/bin/pytest backend/tests/test_factor_catalog.py backend/tests/test_model_status.py backend/tests/test_regression_frame_builder.py backend/tests/test_risk_attribution_market_factor.py backend/tests/test_risk_views_service.py backend/tests/test_universe_loadings_service.py backend/tests/test_cuse4_priority_efficiency.py`
  - result: `21 passed`
- `cd frontend && npm run typecheck`
  - result: passed

## Phase 2

### 2.1 US-core regression membership and projected non-US path

- Refit style canonicalization so the standardized/orthogonalized style surface is anchored on the US-core estimation universe.
- Added explicit separation between:
  - `core_estimated` names that define factor returns, and
  - `projected_only` names that remain in coverage and downstream portfolio analytics.
- Restricted the core regression universe to `US` names while preserving non-US residual continuity.

### 2.2 Files changed

- `backend/risk_model/descriptors.py`
- `backend/risk_model/regression_frame.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/analytics/services/universe_loadings.py`

### 2.3 Validation

- `backend/.venv/bin/pytest backend/tests/test_style_canonicalization.py backend/tests/test_daily_factor_returns_us_core.py backend/tests/test_regression_frame_builder.py backend/tests/test_universe_loadings_service.py backend/tests/test_cuse4_priority_efficiency.py`
  - result: `18 passed`

## Phase 3

### 3.1 One-stage constrained WLS and Market factor cutover

- Replaced the retired two-phase solver with a single-stage constrained WLS.
- Promoted `Market` to the canonical structural baseline factor.
- Removed `Country: US` from factor-return estimation, covariance publishing, and public risk buckets.

### 3.2 Files changed

- `backend/risk_model/wls_regression.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/risk_model/risk_attribution.py`
- `backend/analytics/services/universe_loadings.py`
- `backend/portfolio/models.py`

### 3.3 Validation

- `backend/.venv/bin/pytest backend/tests/test_wls_regression.py backend/tests/test_risk_attribution_market_factor.py backend/tests/test_daily_factor_returns_us_core.py backend/tests/test_universe_loadings_service.py`
  - result: `20 passed`

## Phase 4

### 4.1 Public contract cleanup and factor-ID cutover

- Removed `eligible_for_model` from public payload contracts.
- Converted public factor-bearing payloads to stable `factor_id` identity.
- Added serialized `factor_catalog` metadata to serving surfaces that publish factor data.
- Converted health diagnostics factor payloads to stable `factor_id` identity and added `factor_catalog` there too.
- Switched the exposure-history API to `factor_id` as the canonical query surface, with backend resolution to stored factor names.
- Updated frontend types and factor-label helpers to consume catalog-driven factor identity.
- Updated explore, exposures, and what-if UI surfaces to read `market`, `model_status`, and `factor_id`.
- Updated health UI surfaces to display `Market` variance share and catalog-driven factor labels.

### 4.2 Files changed

- Backend:
  - `backend/analytics/contracts.py`
  - `backend/analytics/pipeline.py`
  - `backend/analytics/services/risk_views.py`
  - `backend/analytics/services/universe_loadings.py`
  - `backend/analytics/services/cache_publisher.py`
  - `backend/analytics/health.py`
  - `backend/services/portfolio_whatif.py`
- Frontend:
  - `frontend/src/lib/types.ts`
  - `frontend/src/lib/factorLabels.ts`
  - `frontend/src/app/exposures/page.tsx`
  - `frontend/src/app/explore/page.tsx`
  - `frontend/src/components/ExposureBarChart.tsx`
  - `frontend/src/components/CovarianceHeatmap.tsx`
  - `frontend/src/components/FactorDrilldown.tsx`
  - `frontend/src/components/RiskDecompChart.tsx`
  - `frontend/src/components/ExposurePositionsTable.tsx`
  - `frontend/src/features/explore/components/TickerQuoteCard.tsx`
  - `frontend/src/features/whatif/WhatIfPreviewPanel.tsx`
  - `frontend/src/features/whatif/useWhatIfScenarioLab.ts`
  - `frontend/scripts/explore_whatif_refresh_regression.mjs`

### 4.3 Final cleanup

- Removed the dead two-phase solver code path from `backend/risk_model/wls_regression.py`.
- Removed public/test-facing `country` bucket residue from route fixtures, goldens, and operator-facing contracts.
- Updated the engine spec to describe the live US-core one-stage market model rather than the retired country-split design.

## Final Validation

- `backend/.venv/bin/pytest`
  - result: `157 passed in 170.34s`
- `cd frontend && npm run typecheck`
  - result: passed
- `cd frontend && npm run build`
  - result: passed
- `cd frontend && npm run test:explore-whatif`
  - result: `Explore what-if refresh regression passed.`

## Final State

- The canonical structural factor is `Market`.
- The core estimator is single-stage constrained WLS.
- `US` names define the core model; non-US names are `projected_only`.
- Public factor-bearing APIs use stable `factor_id` identity plus `factor_catalog`.
- Public payloads no longer expose `eligible_for_model` or `country` as the primary systematic bucket concept.
