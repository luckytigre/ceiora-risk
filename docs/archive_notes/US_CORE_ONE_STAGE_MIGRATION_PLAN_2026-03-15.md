# US-Core One-Stage Regression Migration Plan

Date: 2026-03-15
Status: Proposed implementation plan
Owner: Shaun + Codex
Scope: Migrate the live risk engine from mixed-US/non-US two-phase estimation to a cleaner US-core estimation architecture with one-stage constrained WLS, factor-catalog-driven metadata, and non-US names excluded from core factor-return estimation but retained in portfolio coverage.

## 0) Intent

This plan covers three tightly related changes that should be executed as one coordinated backend/model migration rather than as isolated patches:

1. Remove non-US names from the core factor-return estimation universe.
2. Replace the current sequential two-phase estimator with a single-stage constrained WLS estimator closer to public Barra USE4 mechanics.
3. Replace hardcoded factor-name/category logic with a factor catalog that becomes the single source of truth for factor identity, grouping, ordering, and publication rules.

The operating goal is:

- keep the model centered on the actual book, which is primarily US single-name equities
- stop letting occasional foreign names distort the core factor-return fit
- keep those foreign names visible and risk-bearing in portfolio analytics
- move the regression plumbing toward a more Barra-like single-stage market/industry/style setup
- reduce stringly typed factor logic across backend and frontend

This is not a full GEM-style global model build. It is a disciplined US-core migration with a deliberate place for foreign-name coverage.

## 0.1) Foundational Spec To Freeze Before Coding

The original draft left several statistical and contract decisions open. Those decisions are now locked for this migration so implementation does not drift:

- the live core model becomes a `US-core one-stage constrained WLS` model
- `Market` replaces `Country: US` as the structural baseline factor for the US-core model
- there is no separate reported intercept factor in the live US-core regression; `Market` is the baseline common-move factor
- industry returns are estimated jointly with `Market` and styles in one regression, with a cap-weighted industry-sum-to-zero constraint
- style standardization and orthogonalization for the core model are anchored on the US-core universe only
- non-US names remain in coverage and portfolio analytics, but become `projected-only` names rather than core-estimation members
- projected-only non-US residuals are computed explicitly after the US-core fit and feed specific-risk forecasting
- the migration uses a temporary compatibility adapter for downstream `country`-bucket consumers, but the live factor catalog and live regression will no longer include `Country: US`

These decisions are not optional implementation details. They are the governing model spec for the migration.

## 1) Current State Audit

### 1.1 Live estimation method

The live engine currently uses a sequential two-phase estimator:

- [backend/risk_model/wls_regression.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/wls_regression.py)
- [backend/risk_model/daily_factor_returns.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/daily_factor_returns.py)

Current mechanics:

- phase A fits `intercept + Country: US + constrained TRBC business-sector dummies`
- phase B fits style exposures on phase-A residuals
- robust inference is HC1 on the sequential estimator

This is internally coherent, but it is not the public USE4-style single-stage constrained setup.

### 1.2 Live universe and non-US handling

Structural eligibility does not require `US` domicile:

- [backend/risk_model/eligibility.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/eligibility.py)

As long as a non-US equity has:

- style data
- market cap
- TRBC classification
- `hq_country_code`
- non-non-equity economic sector

it can enter the regression member set.

Current implication:

- non-US names such as `BABA` or `TAK` can participate directly in the core daily factor-return estimation
- they enter the same country/industry/style fit as US names
- they influence factor returns, covariance, and specific-risk inputs

That is the main architectural mismatch for the actual trading book.

### 1.3 Hardcoded factor-name logic

Factor identity and grouping are currently spread across multiple modules:

- [backend/risk_model/risk_attribution.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/risk_attribution.py)
- [backend/analytics/pipeline.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/pipeline.py)
- [backend/analytics/services/universe_loadings.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/services/universe_loadings.py)
- [backend/risk_model/eligibility.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/eligibility.py)
- [frontend/src/lib/factorLabels.ts](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/frontend/src/lib/factorLabels.ts)

Current issues:

- style-factor names are derived from `STYLE_COLUMN_TO_LABEL`
- country is hardcoded as `Country: US`
- style-factor sets are duplicated
- frontend ordering logic still encodes the current phase-A/phase-B hierarchy
- risk attribution infers categories from strings instead of model metadata
- contracts and tests assume a `country` systematic bucket instead of a `market` bucket

This makes a regression migration harder than it needs to be.

## 2) Target State

### 2.1 Estimation architecture

The target live model will be a US-core model with a single-stage constrained WLS estimator.

Core estimation universe:

- US structurally eligible names only

Coverage / projection universe:

- US and non-US structurally eligible names

Estimator design:

- one-stage constrained WLS
- explicit market factor
- industry block estimated jointly with market and style
- style block estimated jointly, not on residuals from an earlier phase
- no separate reported intercept factor in the live US-core model
- cap-weighted industry-sum-to-zero constraint using the same market-cap weighting regime as the WLS fit
- US-core normalization anchor for style standardization and orthogonalization

Public-mechanics target:

- USE4-like structure, not proprietary-identical parity

### 2.2 Non-US handling

Non-US names will no longer define factor returns.

They will still be:

- present in `universe_loadings`
- projectable into portfolio views
- assigned exposures and specific risk
- visible in what-if, holdings, exposures, and portfolio risk surfaces

Initial policy:

- no manual specific-risk uplift yet
- no GEM-style country/currency block yet
- no foreign-name exclusion from portfolio analytics
- explicit `projected-only` status in downstream payloads

### 2.3 Factor metadata

A factor catalog will become the system-of-record for:

- factor id
- factor display label
- factor family
- factor block
- ordering
- publication policy
- category for risk attribution
- whether the factor participates in covariance display
- whether the factor is expected in security-level exposures
- stable factor id distinct from display label
- compatibility aliases for transitional payload publication when needed

This will eliminate most hardcoded string logic.

Final target after the first model cutover:

- backend internals use stable `factor_id` as the identity of each factor
- frontend rendering uses factor catalog metadata for labels and ordering
- public payloads move away from label-as-identity maps and toward factor-id keyed structures with explicit display metadata

### 2.4 Contract And Cutover Strategy

The move from `country` to `market` is not treated as a late cleanup.

Cutover rule:

- the regression engine, factor catalog, covariance, and risk attribution logic move to `Market` semantics
- downstream payloads carry a temporary compatibility adapter for one transition window where needed
- frontend and new consumers switch to `market` semantics first
- legacy `country` compatibility aliases are removed only after payload, snapshot, and operator-surface parity is verified

This prevents the migration from becoming an all-at-once rename across backend, serving, tests, and frontend consumers.

Because this is a single-user system, long-lived backward compatibility is not a final-state requirement.

Planning rule:

- if any temporary compatibility alias or adapter is used during implementation, it is treated as short-lived migration scaffolding only
- the post-cutover plan must explicitly remove:
  - `country` aliases
  - `eligible_for_model`
  - label-as-identity payload conventions
- no compatibility field is allowed to survive as steady-state architecture just because it made the first cutover safer

### 2.5 Local Runtime And Neon Boundary

This migration must respect the existing project operating model:

- local SQLite plus local cache remain the historical ingest and core-compute authority
- local orchestrator runs the heavy model-building lanes
- durable SQLite analytics tables remain the first persistent landing zone for recomputed model state
- Neon remains the pruned serving-oriented database and holdings authority when a Neon DSN is configured
- cloud-serving processes should consume the published serving payloads and bounded mirrored analytics state, not recompute the core model themselves

Implications for this migration:

- the new `Market` factor and one-stage regression land first in local compute and local durable SQLite outputs
- only after local outputs are stable do they mirror into Neon
- Neon parity checks must compare like-for-like bounded windows and active method versions
- local diagnostics remain local-instance truth; operator and serving surfaces must distinguish local compute state from mirrored Neon state where relevant
- no payload/schema change is considered complete until both the local publisher and the Neon-backed serving readers are validated

## 3) Non-Goals For This Migration

This plan does not include:

- building a full GEM-style multi-country global model
- adding FX or currency factors
- adding ETFs or non-equity overlays
- manually inflating specific risk for non-US names
- changing the style factor set itself
- rewriting the frontend UX beyond what is needed to consume factor catalog metadata
- introducing a full multi-country country-factor block

Those are later evolutions.

## 4) Design Decisions Locked For This Migration

1. The core factor-return estimation universe becomes US-only.
2. Non-US names remain in the coverage universe and portfolio analytics.
3. The core regression becomes one-stage constrained WLS.
4. The live structural baseline factor is `Market`.
5. `Country: US` is removed from the live regression and live factor catalog.
6. A separate reported intercept factor is not used in the live US-core model.
7. Industry effects become market-relative through the one-stage constrained design.
8. Style normalization and orthogonalization are anchored on the US-core universe.
9. Projected-only non-US residuals are generated explicitly and feed specific-risk forecasting.
10. Factor identity and categorization become catalog-driven.
11. Payloads carry explicit security model status:
    - `core_estimated`
    - `projected_only`
    - `ineligible`
12. `eligible_for_model` is transitional only and is removed in the post-cutover cleanup phases.
13. Public factor payloads move to stable factor ids after the first model cutover is stable.
14. Migration will be staged behind a model-method version bump and validation gates.

## 5) Proposed New Intermediate Structures

The current pipeline assembles too much logic inline inside [backend/risk_model/daily_factor_returns.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/daily_factor_returns.py). The migration should introduce explicit intermediate structures.

### 5.1 `FactorCatalogEntry`

Proposed fields:

- `factor_id`
- `factor_name`
- `short_label`
- `family`
  - `market`
  - `industry`
  - `style`
- `block`
  - `core_structural`
  - `core_style`
  - `coverage_only`
- `source_column`
  - for style factors derived from raw cross-section columns
- `display_order`
- `covariance_display`
- `exposure_publish`
- `compatibility_aliases`
- `active`
- `method_version`

Notes:

- Industry entries may be generated from the active business-sector set, but they must receive stable catalog ids distinct from raw display labels.
- Style entries should be generated from descriptor schema definitions, not a repeated hardcoded set.
- Frontend rendering should consume catalog entries, not define its own factor taxonomy.

### 5.2 `RegressionMembershipFrame`

One row per security on a regression date.

Proposed columns:

- `ric`
- `ticker`
- `date`
- `return`
- `raw_return`
- `market_cap`
- `hq_country_code`
- `is_us`
- `is_core_regression_member`
- `is_coverage_member`
- `is_projectable`
- `model_status`
  - `core_estimated`
  - `projected_only`
  - `ineligible`
- `business_sector`
- `industry_group`
- all style exposure columns

Purpose:

- cleanly separate who defines the core model and who is merely carried by it
- make audits and tests explicit

### 5.3 `RegressionFrameBuilder`

Proposed responsibilities:

- assemble date-level rows for all structural-eligible names
- apply membership rules
- produce US-core normalization anchors
- attach stable factor ids and ordered exposures
- emit both the `core estimation` frame and the `projection` frame

Purpose:

- pull membership, normalization, and matrix-input assembly out of `daily_factor_returns.py`
- make the solver swap a contained change instead of a rewrite inside a monolithic loop

### 5.4 `DesignMatrixSpec`

Proposed content:

- ordered factor list
- structural block names
- style block names
- constraint definitions
- required inputs
- method version digest

Purpose:

- produce deterministic factor ordering
- decouple matrix construction from solver code

### 5.5 `ConstraintSpec`

Proposed content:

- `constraint_name`
- `affected_factors`
- `weight_rule`
- `constraint_vector`
- `tolerance`

Initial USE-like core rule:

- cap-weighted sum of industry factor returns equals zero
- constraint residuals must be held to numerical tolerance and persisted as diagnostics

### 5.6 `ProjectionResidualFrame`

Proposed content:

- `ric`
- `date`
- `model_status`
- `factor_fitted_return`
- `model_residual`
- `raw_residual`
- `specific_risk_input`

Purpose:

- explicitly preserve residual history for projected-only names after they leave the core regression
- prevent non-US names from silently losing idiosyncratic-risk coverage

### 5.7 `RegressionResult`

Proposed content:

- factor returns
- robust standard errors
- t-stats
- fitted values
- residuals
- raw residuals
- weighted `R²`
- condition number
- constraint residuals
- factor ordering digest
- core regression member counts
- projected-only counts
- normalization-basis digest

## 6) Module-Level Workstreams

### Workstream A: Factor Catalog Foundation

Goal:

- remove factor identity logic from scattered constants and naming conventions

Primary files:

- [backend/risk_model/risk_attribution.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/risk_attribution.py)
- [backend/analytics/pipeline.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/pipeline.py)
- [backend/analytics/services/universe_loadings.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/services/universe_loadings.py)
- [backend/risk_model/eligibility.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/eligibility.py)
- [frontend/src/lib/factorLabels.ts](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/frontend/src/lib/factorLabels.ts)

Implementation steps:

1. Create a backend factor catalog module.
2. Generate style-factor entries from descriptor schema and score-column mappings.
3. Add market and industry catalog entry builders with stable ids.
4. Refactor category resolution to use catalog metadata instead of string heuristics.
5. Add a small API-serializable factor catalog payload.
6. Update frontend label/order helpers to consume catalog metadata instead of encoding the current regression phases.

Acceptance criteria:

- no backend risk categorization relies on `startswith("Country:")`
- no duplicated style-factor set remains as a hardcoded unordered constant
- frontend sorting and short labels can be driven from catalog data

### Workstream B: Regression Frame And Matrix Assembly

Goal:

- pull membership, normalization, design-matrix assembly, and ordered factor selection out of the monolithic daily factor-return loop

Primary files:

- [backend/risk_model/daily_factor_returns.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/daily_factor_returns.py)
- [backend/risk_model/eligibility.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/eligibility.py)
- [backend/risk_model/wls_regression.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/wls_regression.py)

Implementation steps:

1. Create a `RegressionFrameBuilder`.
2. Move membership-rule evaluation into the builder.
3. Move US-core normalization-anchor calculation into the builder.
4. Move ordered factor and exposure-matrix assembly into deterministic builder outputs.
5. Reduce `daily_factor_returns.py` to orchestration, persistence, and diagnostics only.

Acceptance criteria:

- `daily_factor_returns.py` no longer owns end-to-end inline assembly for membership, normalization, factor ordering, and matrix construction
- regression inputs can be snapshotted directly in tests
- the old and new solver paths can share the same assembled frame

### Workstream C: Estimation-Universe Split

Goal:

- exclude non-US names from core factor-return estimation without removing them from coverage

Primary files:

- [backend/risk_model/eligibility.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/eligibility.py)
- [backend/risk_model/daily_factor_returns.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/daily_factor_returns.py)
- [backend/analytics/services/universe_loadings.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/services/universe_loadings.py)
- [backend/analytics/services/risk_views.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/services/risk_views.py)

Implementation steps:

1. Add explicit membership flags:
   - `is_structural_eligible`
   - `is_core_regression_member`
   - `is_projectable`
   - `model_status`
2. Define `is_core_regression_member = structural_eligible AND hq_country_code == 'US' AND has_return`.
3. Preserve `is_projectable` for non-US structurally eligible names.
4. Anchor style normalization and orthogonalization on the US-core members only.
5. Refactor daily factor-return assembly to use core members only for factor-return estimation.
6. Continue to publish coverage-level exposures for non-US names downstream.
7. Continue to compute portfolio projections for non-US names using the core factor set.

Acceptance criteria:

- `BABA`/`TAK`-like names no longer influence estimated factor returns
- they still appear in `portfolio`, `risk`, `exposures`, and `universe_loadings`
- coverage summary surfaces distinguish core-estimated from projected-only names

### Workstream D: One-Stage Constrained WLS Solver

Goal:

- replace the sequential two-phase estimator with a one-stage constrained solver

Primary files:

- [backend/risk_model/wls_regression.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/wls_regression.py)
- [backend/risk_model/daily_factor_returns.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/daily_factor_returns.py)
- [backend/tests/test_cuse4_priority_efficiency.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/tests/test_cuse4_priority_efficiency.py)

Proposed solver shape:

- one-stage cap-weighted WLS
- explicit `Market` factor with unit exposure for all core-regression names
- no separate reported intercept factor in the live regression
- industry block
- style block
- linear constraints applied directly to the one-stage fit

Implementation steps:

1. Add a new solver result type for one-stage constrained estimation.
2. Implement constraint-matrix construction.
3. Build a KKT-based or null-space-based constrained solver.
4. Compute HC1 robust inference for the constrained one-stage estimator.
5. Persist additional diagnostics:
   - constraint residual norm
   - design-rank warnings
   - factor-order digest
6. Keep the old solver temporarily behind a compatibility path for benchmark comparison.

Acceptance criteria:

- industry constraint holds within tolerance on every computed date
- factor returns are emitted in deterministic catalog order
- robust inference remains available
- old and new estimators can be compared on the same date set before cutover

### Workstream E: Core Design Matrix And Market Factor

Goal:

- move from `Country: US` to an explicit market-relative US core structure

Primary files:

- [backend/risk_model/daily_factor_returns.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/daily_factor_returns.py)
- [backend/risk_model/risk_attribution.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/risk_attribution.py)
- [backend/analytics/services/universe_loadings.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/services/universe_loadings.py)

Implementation steps:

1. Introduce a market factor entry in the catalog.
2. Remove `Country: US` from the regression block.
3. Add `Market` exposure = `1.0` for all core US names.
4. Keep industry exposures as one-hot business-sector exposures.
5. Apply US-core-anchored style exposures to both core-estimated and projected-only names.
6. Change risk-attribution categories from `country / industry / style` to `market / industry / style`.
7. Add a temporary compatibility adapter for downstream payloads still expecting `country`.
8. Update frontend labels and health semantics accordingly.

Acceptance criteria:

- no live regression depends on `Country: US`
- portfolio risk surfaces expose a `market` systematic bucket
- historical covariance display remains coherent for style factors

### Workstream F: Projection And Specific-Risk Handling For Non-US Names

Goal:

- keep non-US names in coverage without letting them define the core model

Primary files:

- [backend/risk_model/daily_factor_returns.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/daily_factor_returns.py)
- [backend/risk_model/specific_risk.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/specific_risk.py)
- [backend/analytics/services/universe_loadings.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/services/universe_loadings.py)

Initial implementation policy:

- estimate factor returns using US-only core members
- project non-US names against the factor set using their exposures
- derive non-US residuals relative to the US-core factor returns
- forecast specific risk from those residuals without adding a manual uplift yet
- persist projected-only residual history explicitly

Acceptance criteria:

- non-US names receive factor exposures and specific risk
- they remain visible in portfolio risk outputs
- they do not alter core factor-return estimation
- projected-only names do not silently fall back to `0.0` specific risk

### Workstream G: Durable Outputs, Serving, And Frontend Contracts

Goal:

- carry the new factor identities and membership semantics cleanly through local durable outputs, Neon mirror surfaces, and frontend-serving payloads

Primary files:

- [backend/analytics/contracts.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/contracts.py)
- [backend/analytics/services/cache_publisher.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/services/cache_publisher.py)
- [backend/data/model_outputs.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/data/model_outputs.py)
- [backend/data/serving_outputs.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/data/serving_outputs.py)
- [frontend/src/lib/types.ts](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/frontend/src/lib/types.ts)
- [frontend/src/lib/factorLabels.ts](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/frontend/src/lib/factorLabels.ts)

Local/Neon boundary inventory to cover explicitly:

- local SQLite durable analytics tables:
  - `model_factor_returns_daily`
  - `model_factor_covariance_daily`
  - model-output tables carrying published risk-engine state
- local cache/workspace tables:
  - `daily_factor_returns`
  - `daily_specific_residuals`
  - risk-engine cache keys and staged serving payloads
- Neon serving/mirror surfaces:
  - `serving_payload_current`
  - mirrored analytics windows used for parity and bounded history
  - holdings tables used by serving projection when Neon is configured

Implementation steps:

1. Add factor-catalog-aware payload types.
2. Add portfolio-position metadata indicating whether a name is:
   - core-estimated
   - projected-only
   - ineligible
3. Add factor-category support for `market`.
4. Add a temporary compatibility adapter or dual-write strategy for `country`-bucket consumers.
5. Remove UI language that assumes phase-A / phase-B ordering.
6. Update exposure/risk views to render market factors coherently.
7. Update local durable output writers so the new factor identities, model-status fields, and projected-only residual paths persist cleanly in SQLite first.
8. Update Neon mirror/parity planning so the mirrored factor-return, covariance, and serving payload surfaces understand `Market` semantics and the active method version.
9. Validate that Neon-backed holdings reads and cloud-serving payload consumers remain compatible with the new payload structure before removing compatibility aliases.

Acceptance criteria:

- first-cutover payloads work end to end with `Market` semantics
- local durable outputs, Neon mirror surfaces, and frontend-serving payloads agree on factor families and model-status fields
- any temporary compatibility field is clearly marked for removal in the next cleanup phase

### Workstream H: Post-Cutover Contract, Identity, And Operator Cleanup

Goal:

- remove migration scaffolding and finish the architectural cleanup after the first model cutover is stable

Primary files:

- [backend/analytics/contracts.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/contracts.py)
- [backend/analytics/services/cache_publisher.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/services/cache_publisher.py)
- [backend/analytics/services/risk_views.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/services/risk_views.py)
- [backend/analytics/health.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/health.py)
- [backend/api/routes/data.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/api/routes/data.py)
- [frontend/src/lib/types.ts](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/frontend/src/lib/types.ts)
- [frontend/src/lib/factorLabels.ts](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/frontend/src/lib/factorLabels.ts)
- health/operator-facing frontend components that still assume `country` semantics or legacy booleans

Implementation steps:

1. Remove temporary `country` compatibility aliases from payloads and tests.
2. Remove `eligible_for_model` from contracts, payloads, frontend types, and UI logic.
3. Make `model_status` the only authoritative security-model-state field.
4. Convert public factor payload identity from label-keyed maps to stable factor-id keyed structures.
5. Keep factor display text entirely in factor catalog metadata instead of inline map keys.
6. Update operator and health payloads so they natively report:
   - `market` systematic bucket
   - `core_estimated / projected_only / ineligible` counts
   - projected-only specific-risk coverage
   - local method version and Neon mirrored method version
   - one-stage regression diagnostics such as constraint residuals and skipped-date counts
7. Remove any hardcoded short-label and family heuristics that survived the first cutover.
8. Refresh golden fixtures and docs to match the final post-migration contract.

Acceptance criteria:

- no active payload field named `country` remains where the concept is actually `market`
- no active payload field named `eligible_for_model` remains
- public factor payloads are keyed by stable factor ids, not labels
- frontend pages render purely from factor catalog metadata and stable ids
- health/operator surfaces are native to the post-migration model, not compatibility wrappers

## 7) Execution Sequence

### Phase 0: Statistical ADR, Baseline Audit, And Golden Outputs

Before changing logic:

1. write a short ADR locking:
   - `Market` as the replacement for `Country: US`
   - no separate reported intercept factor
   - cap-weighted industry-sum-to-zero constraint
   - US-core normalization anchors
   - projected-only non-US residual semantics
   - payload compatibility strategy during cutover
2. snapshot current factor-return history
3. snapshot current portfolio/risk/exposure payloads
4. record current:
   - factor count
   - average daily `R²`
   - condition number distribution
   - portfolio risk-share outputs for representative books
5. create benchmark portfolios:
   - concentrated US-only book
   - mixed US long/short book
   - book with `BABA` and `TAK`
   - book with a few non-US names and no US names
6. add a baseline diagnostic inventory for:
   - current core vs coverage counts
   - non-US presence in the live regression
   - current specific-risk coverage for foreign names
7. inventory the local-vs-Neon boundary for all model outputs touched by this migration:
   - what is computed only locally
   - what is durably written in SQLite
   - what is mirrored to Neon
   - what is read by cloud-serving processes
   - which operator surfaces reflect local runtime state vs Neon parity state

Purpose:

- establish regression and portfolio-level comparison anchors

### Phase 1: Factor Catalog, Status Semantics, And Frame Builder Without Regression Change

This phase should be isolated first because it reduces migration risk for all later work.

Deliverables:

- backend factor catalog module
- explicit `core_estimated / projected_only / ineligible` status semantics
- `RegressionFrameBuilder`
- frontend factor metadata consumption
- tests proving no output changes yet other than metadata and status fields where explicitly expected
- docs/spec updates so the migration target is visible before the math changes land

### Phase 2: US-Core Split And Projected-Only Residual Continuity With Existing Solver

Before changing the estimator:

- switch core regression membership to US-only
- keep the two-phase solver temporarily
- generate explicit projected-only residuals for non-US names after the US-core fit
- keep temporary payload compatibility for legacy `country` consumers
- validate local durable SQLite writes first, then verify Neon mirror/parity on the bounded publish windows

Purpose:

- isolate the effect of excluding non-US names from core estimation
- prove that projected-only names retain valid specific-risk coverage
- measure the impact independently from the one-stage regression migration

### Phase 3: One-Stage Constrained Solver Introduction

After the universe split is stable:

- introduce one-stage solver under a new method version
- run side-by-side benchmark comparisons vs the two-phase US-only variant
- validate stability and interpretation
- keep payload compatibility adapters in place until surface migration is complete
- validate local-runtime `core-weekly`, `cold-core`, and `serve-refresh` behavior separately from any cloud-serving read path

### Phase 4: Market-Factor Cutover And Surface Migration

After solver confidence is high:

- switch downstream risk decomposition to `market / industry / style`
- update frontend ordering and labels
- remove `Country: US` from active payloads and active surfaces
- update health, operator, and serving diagnostics to the new bucket semantics
- complete Neon-serving and cloud-serving validation before declaring the cutover complete

### Phase 5: Migration-Scaffolding Cleanup

- remove temporary `country` compatibility aliases
- remove transitional `eligible_for_model` handling
- remove dead two-phase-specific terminology
- update active model spec and docs

### Phase 6: Stable Factor-Id API Conversion

After the model and serving surfaces are stable:

- convert public factor payloads from label-keyed maps to stable factor-id keyed structures
- expose display name, short label, family, and ordering through the factor catalog only
- update frontend consumers to render from factor ids plus catalog metadata
- remove any remaining label-as-identity assumptions from backend and frontend contracts

### Phase 7: Final Operator And Contract Cleanup

After factor-id payloads are live:

- make health/operator payloads fully native to `market` and `model_status`
- remove any remaining compatibility adapters or branch-specific translation code
- refresh all goldens, docs, and runbooks to final-state terminology
- close the migration only after local runtime, durable SQLite, Neon mirror, and cloud-serving all operate on the same final contract

## 8) Detailed Test Plan

### 8.1 Unit tests

#### Factor catalog

- catalog builds expected style, market, and industry entries
- catalog emits stable ids distinct from display labels
- catalog ordering is deterministic
- category lookup does not depend on factor-name string patterns
- frontend metadata helpers remain consistent with backend catalog payload
- final-state catalog supports factor-id keyed exposure payloads cleanly

#### Membership logic

- US names can be structural eligible and core-regression members
- non-US names can be structural eligible but not core-regression members
- non-US names remain projectable
- status assignment is deterministic:
  - `core_estimated`
  - `projected_only`
  - `ineligible`
- missing-country/missing-style/missing-market-cap exclusions remain correct

#### One-stage solver

- constrained solver returns exact coefficient vector shape
- cap-weighted sum of industry returns is numerically zero within tolerance
- condition number is finite for normal fixtures
- robust SE and t-stat vectors align with factor ordering
- degenerate/singular structural cases fail gracefully or produce explicit diagnostics

#### Projection logic

- non-US names receive exposures and residuals despite exclusion from the core regression
- projected-only names do not enter the factor-return fit
- projected-only names retain specific-risk inputs
- projected-only names use US-core normalization anchors

#### Post-cutover contract cleanup

- `eligible_for_model` removal does not break final frontend consumers
- no API contract still depends on `country` as a market alias
- factor-id keyed payload maps round-trip cleanly through backend serializers and frontend readers

### 8.2 Integration tests

#### Daily factor returns

- regression dates are still bounded correctly
- factor-return cache signatures invalidate on method-version change
- US-only core membership changes daily factor-return outputs as expected on mixed-country fixtures
- one-stage solver path persists factor rows and residual rows correctly
- projected-only residual rows are persisted correctly
- no projected-only name silently loses specific-risk history
- local durable SQLite factor-return and covariance writers persist the new method-version / factor semantics correctly before Neon mirror

#### Universe loadings

- US names publish market + industry + style exposures
- non-US names publish projectable exposures without entering core fit
- no eligible name is published with empty exposures
- every published name carries a model-status field
- local serving payload staging and Neon mirrored serving payloads agree on factor names, model status, and risk bucket semantics
- final-state payloads use stable factor ids rather than labels as identity

#### Risk attribution

- risk decomposition emits `market` bucket instead of `country`
- factor details categorize market/industry/style correctly
- exposure modes and drilldowns remain coherent
- temporary compatibility adapter behaves as expected during the transition window
- final-state payloads no longer rely on the temporary compatibility adapter

#### Portfolio what-if

- projected-only non-US names remain supported in preview and apply flows
- factor deltas remain stable under the new factor catalog ordering
- Neon-backed holdings reads still produce valid portfolio/risk/exposure payloads after the factor-schema migration

### 8.3 Regression / golden tests

New golden snapshots should be created for:

- `/api/portfolio`
- `/api/risk`
- `/api/exposures`
- `/api/health`
- `/api/operator/status`

Reference fixtures:

- US-only book
- mixed-country book
- non-US-only mini-book

### 8.4 Statistical validation tests

For a rolling benchmark window:

- compare old vs new average daily `R²`
- compare condition-number distributions
- compare factor-return autocorrelation and variance
- compare portfolio volatility forecasts vs realized volatility
- inspect major sign flips in core factors
- check residual-vol distributions for US and non-US names separately
- compare skipped-date counts before and after US-core filtering

### 8.5 Operational smoke checks

Run after each major phase:

- `backend/.venv/bin/pytest`
- portfolio/risk/exposures route smoke
- mixed-country benchmark notebook/script
- rebuild / refresh on local runtime
- check serving payload parity between SQLite and Neon
- inspect operator/health counts for `core_estimated`, `projected_only`, and non-US specific-risk coverage
- explicitly smoke:
  - local `serve-refresh`
  - local `core-weekly`
  - local `cold-core` when relevant
  - Neon-backed serving reads
  - cloud-serving payload consumption if enabled
- after Phase 5/6, confirm:
  - no `country` alias remains in active payloads
  - no `eligible_for_model` field remains in active payloads
  - factor-id keyed payloads render correctly in the frontend

## 9) Required Audits And Checks During Execution

### 9.1 Seams audit

Before each major merge step, audit:

- where factor names are assumed
- where categories are inferred
- where output ordering is relied upon
- where method-version changes trigger cache invalidation

### 9.2 Data-quality audit

Specifically review:

- non-US name coverage in `barra_raw_cross_section_history`
- country-code integrity in classification PIT
- business-sector completeness
- foreign-name style-score sparsity
- stability of US-core normalization anchors over time

### 9.3 Model-behavior audit

For each benchmark portfolio:

- systematic-risk share changes
- top factor exposures
- top factor contributions
- position-level risk contributions
- specific-risk forecasts for foreign names
- `market` vs legacy `country` bucket continuity during transition

### 9.4 Rollback audit

Before cutover:

- confirm the prior two-phase method can still be rebuilt from source
- keep old and new method versions distinguishable in cache and durable outputs
- maintain a rollback path that does not require source-table changes

### 9.5 Contract And Consumer Audit

Before each cutover gate, confirm:

- serving payloads still satisfy active frontend consumers
- compatibility aliases are still working where expected
- golden fixtures and operator surfaces have been updated intentionally
- deprecated `country` semantics are not lingering in new code paths

After Phase 5 and later, confirm additionally:

- `country` compatibility aliases are fully removed
- `eligible_for_model` is fully removed
- all active consumers read factor ids plus catalog metadata rather than label-keyed factor maps

### 9.6 Local Runtime And Neon Audit

Before each cutover gate, confirm:

- local SQLite remains the authority for heavy recompute and the first durable write target
- Neon receives only the intended mirrored/bounded windows for the active method version
- parity checks compare the same factor set, bounded date windows, and inference fields
- cloud-serving processes are reading the new payload/category semantics from durable serving payloads rather than stale local assumptions
- operator status clearly distinguishes local compute health from Neon mirror/parity health

## 10) Agents To Launch During Execution

These are execution-time read-only or bounded-write agent roles I would use while implementing. They are intentionally disjoint so they can run in parallel.

### Agent 1: Regression Seam Mapper

Scope:

- solver path
- daily factor-return assembly
- covariance and specific-risk dependencies

Owned files:

- `backend/risk_model/wls_regression.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/risk_model/covariance.py`
- `backend/risk_model/specific_risk.py`

Output:

- seam map
- hidden assumptions
- required method-version bumps

### Agent 2: Factor Catalog Auditor

Scope:

- hardcoded factor names
- category inference
- ordering assumptions
- frontend/backend label duplication

Owned files:

- `backend/risk_model/risk_attribution.py`
- `backend/analytics/pipeline.py`
- `frontend/src/lib/factorLabels.ts`
- `frontend/src/lib/types.ts`

Output:

- replacement inventory
- migration checklist for catalog adoption

### Agent 3: Coverage-Vs-Core Universe Auditor

Scope:

- eligibility flags
- US-only regression membership
- non-US projection path

Owned files:

- `backend/risk_model/eligibility.py`
- `backend/analytics/services/universe_loadings.py`
- `backend/analytics/services/risk_views.py`

Output:

- membership-rule audit
- list of payload surfaces needing new status flags

### Agent 4: Contract Compatibility Auditor

Scope:

- `country` to `market` transition seams
- payload adapters
- frontend/backend schema compatibility

Owned files:

- `backend/analytics/contracts.py`
- `backend/analytics/services/cache_publisher.py`
- `frontend/src/lib/types.ts`
- `frontend/src/lib/factorLabels.ts`

Output:

- payload migration checklist
- compatibility adapter test inventory

### Agent 5: Test Harness Builder

Scope:

- new unit tests
- integration tests
- golden snapshot updates

Owned files:

- `backend/tests/*`
- optionally frontend type/golden fixtures where needed

Output:

- missing-test inventory
- reusable fixtures for mixed-country benchmark cases

### Agent 6: Runtime And Serving Auditor

Scope:

- cache publisher
- durable output tables
- serving payload consistency
- rollback controls
- local-runtime vs Neon boundary validation

Owned files:

- `backend/analytics/services/cache_publisher.py`
- `backend/data/model_outputs.py`
- `backend/data/serving_outputs.py`

Output:

- cutover checklist
- rollback checklist
- parity test additions
- local/Neon boundary checklist

### Agent 7: Spec And Docs Auditor

Scope:

- engine spec
- operations playbook
- docs index
- migration notes

Output:

- post-implementation doc patch list

## 11) Cutover And Rollout Strategy

### Stage 1: Internal benchmark-only mode

- new catalog
- new membership logic
- new status semantics
- old solver still live

### Stage 2: US-only core estimator live behind new method version

- one-stage solver off by default but benchmarked side-by-side
- projected-only residual and specific-risk path must already be live
- local durable SQLite writes and Neon parity for the new semantics must already be validated

### Stage 3: one-stage solver enabled in local runtime

- benchmark and portfolio-level checks must pass

### Stage 4: durable output cutover

- publish new factor identities and risk categories
- keep temporary compatibility aliases only where still required
- confirm cloud-serving and Neon-backed holdings-serving paths consume the new payloads correctly

### Stage 5: migration-scaffolding cleanup

- remove old phase-A/phase-B assumptions from frontend and diagnostics
- remove deprecated `country` compatibility aliases
- remove deprecated `eligible_for_model`

### Stage 6: factor-id payload cutover

- switch active payload identity to stable factor ids
- render labels exclusively via factor catalog metadata

### Stage 7: final-state contract confirmation

- verify no compatibility fields remain
- verify operator/health surfaces are fully native to the new model contract
- freeze the final-state schema and docs

## 12) Acceptance Gates

The migration is considered successful only if all of the following hold:

1. `backend/.venv/bin/pytest` passes.
2. New unit and integration tests for catalog, membership split, and one-stage constraints pass.
3. Mixed-country reference portfolios show non-US names absent from the core regression member set.
4. Portfolio/risk/exposure routes still include non-US positions.
5. No serving payload publishes an eligible security with empty exposures.
6. Projected-only names retain valid residual history and specific-risk forecasts.
7. Constraint residuals are within tolerance on all recomputed dates.
8. Daily `R²`, condition-number distributions, and skipped-date counts are not materially degraded without explanation.
9. Factor ordering and category labels are deterministic and catalog-driven.
10. SQLite and Neon serving payload parity remains intact.
11. `Market` is the live systematic bucket and `Country: US` no longer exists in live regression or live factor catalog output.
12. Local recompute lanes and Neon-backed serving reads both operate correctly under the new method version.
13. No active payload field named `eligible_for_model` remains after the cleanup phases complete.
14. Final-state factor payloads use stable factor ids instead of labels as identity.
15. Health/operator surfaces report final-state `market` and `model_status` semantics without compatibility wrappers.

## 13) Deferred Questions After Initial Cutover

1. After the US-core migration is stable, should foreign-name overlays evolve into explicit regional or country buckets?
2. Should the compatibility window for deprecated `country` consumers last exactly one method version or one application release?
3. After the one-stage cutover, should health/operator surfaces expose additional market-factor diagnostics such as constraint residual trend and market-factor variance share?

## 14) Recommended First Implementation Slice

To reduce risk, the first coding slice should be:

1. write the ADR and update the engine spec stub
2. build the factor catalog
3. add `core_estimated / projected_only / ineligible` status fields
4. build the `RegressionFrameBuilder`
5. add benchmark tests and payload metadata

Only after that stabilizes should the one-stage constrained solver land.

This gives us:

- a cleaner architecture immediately
- a measurable isolation of the non-US-estimation effect
- a safer path into the larger solver rewrite
- an explicit follow-on cleanup path instead of indefinite migration scaffolding

## 15) Files Most Likely To Change

Backend:

- [backend/risk_model/wls_regression.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/wls_regression.py)
- [backend/risk_model/daily_factor_returns.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/daily_factor_returns.py)
- [backend/risk_model/eligibility.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/eligibility.py)
- [backend/risk_model/risk_attribution.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/risk_attribution.py)
- [backend/risk_model/__init__.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/risk_model/__init__.py)
- [backend/analytics/pipeline.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/pipeline.py)
- [backend/analytics/contracts.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/contracts.py)
- [backend/analytics/services/universe_loadings.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/services/universe_loadings.py)
- [backend/analytics/services/risk_views.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/analytics/services/risk_views.py)
- [backend/data/model_outputs.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/data/model_outputs.py)
- [backend/data/serving_outputs.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/data/serving_outputs.py)
- local/Neon mirror and parity code paths touched by the active model-output publish flow

Frontend:

- [frontend/src/lib/factorLabels.ts](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/frontend/src/lib/factorLabels.ts)
- [frontend/src/lib/types.ts](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/frontend/src/lib/types.ts)
- exposure and health components that assume current category/order semantics

Tests:

- [backend/tests/test_cuse4_priority_efficiency.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/tests/test_cuse4_priority_efficiency.py)
- [backend/tests/test_risk_attribution_country_factor.py](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/backend/tests/test_risk_attribution_country_factor.py)
- new tests for factor catalog, one-stage constraints, and coverage/core split
