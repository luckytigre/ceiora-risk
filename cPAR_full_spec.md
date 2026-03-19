# cPAR (Parsimonious and Actionable Regression)
## Full Mission, Design, and Execution Specification

Repo: `luckytigre/ceiora-risk`
Local path: `/Users/shaun/Library/CloudStorage/Dropbox/040 - Creating/ceiora-risk`

This document is the canonical execution brief for cPAR planning and implementation.

It must be treated as self-contained. Do not rely on a separate "original prompt" or unwritten context.

This document is now intended to be implementation-planning ready.
Its job is to lock the contracts that were still too loose in the prior draft:

- operating surfaces and authorities
- orchestration and cadence
- exact method specification
- deterministic hedge generation
- explicit status and warning rules
- layer ownership inside this repo
- durable tables, payloads, and routes
- validation and non-regression requirements

The goal is not to implement cPAR here.
The goal is to make the next implementation-planning pass concrete enough that build work can begin without architectural drift.

---

# 1. MISSION

cPAR is a new risk system designed to complement, not replace, cUSE4.

- cUSE4 explains risk from a richer, descriptor-based, cross-sectional angle
- cPAR provides a more tradable, actionable view on risk using ETF proxies and returns-based regression

cPAR must be:

- actionable
- interpretable
- parsimonious
- stable

This is not an academic model.
This is a practical trading and hedging model.

Its central question is:

> What is driving this instrument, and how do I hedge it using real, tradable instruments?

If a design choice weakens that answer, it should be reconsidered.

---

# 2. CURRENT REPO REALITY

The current repo has no existing cPAR system.

Important context:

- the repo currently reflects a cUSE4-first architecture
- cUSE4 is already implemented, polished, and documented
- docs, architecture, serving contracts, and frontend pages currently describe the cUSE4 system and its adjacent projection-only support
- cPAR is brand new and must be added alongside the current system, not blended into it

Planning and implementation must therefore assume:

- cPAR is a new parallel model family
- cPAR may reuse current repo patterns and infrastructure where appropriate
- cPAR must not be framed as an extension of cUSE4 factor logic
- cPAR should live beside the current system and surface through its own app section

This repo is layer-first, not sub-app-first.

That means:

- pure cPAR domain and model logic may live in `backend/cpar/*`
- integration-layer code must still live in the repo's normal layer surfaces:
  - `backend/api/routes/*`
  - `backend/services/*`
  - `backend/data/*`
  - `backend/orchestration/*`

`backend/cpar/*` is allowed.
It must not become a parallel mini-backend.

---

# 3. OPERATING DATA ASSUMPTIONS

Be explicit about the existing operating model:

- local SQLite remains the only direct LSEG ingest landing zone and optional deep archive
- Neon is the intended primary operating database for app-serving reads and, once configured, for authoritative rebuilds
- current app-facing surfaces are Neon-primary for serving and history where enabled
- cPAR should respect this operating model rather than invent a separate one

Implications for cPAR:

- cPAR may share:
  - `security_master`
  - `security_prices_eod`
  - `security_classification_pit` for metadata and warning labels only
  - holdings / account plumbing for later portfolio integration
  - Neon sync and durability patterns
  - generic UI primitives and charts
- cPAR must not share:
  - factor definitions
  - factor catalogs
  - model outputs
  - model logic
  - cUSE4-serving payload families
  - cUSE4 runtime-state semantics
  - cUSE4 what-if analytics logic

Normal-operation requirement:

- cPAR model outputs and cPAR serving surfaces must be designed for Neon-primary app reads in normal operation
- local SQLite remains the direct ingest / archive surface
- cPAR should not depend on local-only runtime artifacts for standard frontend use

The planner must account for Neon explicitly:

- cPAR durable outputs must persist and serve through the Neon-first operating model
- cPAR source usage must remain consistent with the existing local-ingest -> Neon publish architecture
- cPAR must not route normal app reads through request-time local-only computation

---

# 4. OPERATING SURFACES AND AUTHORITIES

This section freezes the repo-facing operating contract for cPAR.

## Shared Source Inputs

The following remain shared canonical inputs:

- `security_master`
- `security_prices_eod`
- `security_classification_pit` for metadata and warning labels only

Holdings tables are reusable later for portfolio integration.
They are not required for the core v1 ticker and hedge product.

## Durable cPAR Surfaces

Primary cPAR app reads in v1 come from durable relational `cpar_*` tables in Neon.

The minimum durable cPAR table family is:

- `cpar_package_runs`
- `cpar_proxy_returns_weekly`
- `cpar_proxy_transform_weekly`
- `cpar_factor_covariance_weekly`
- `cpar_instrument_fits_weekly`

These are the authoritative app-serving surfaces for cPAR in normal operation.

Hedge outputs are not persisted as first-class durable tables in v1.
They are derived on request from:

- the active row in `cpar_package_runs`
- the current instrument row in `cpar_instrument_fits_weekly`
- the active package covariance in `cpar_factor_covariance_weekly`

## Active Package Authority

The active cPAR package in v1 is the latest successful package in `cpar_package_runs`.

Do not introduce a cPAR runtime-state key just to point to the active package in v1.
The package table is the authority.

## Current-Payload Surface Decision If Needed

V1 does not require a dedicated cPAR current-payload table.

The baseline design is:

- ticker-detail and search reads come from durable relational `cpar_*` tables
- no cPAR equivalent of the current cUSE4 blob-style dashboard payloads is required for v1

If the implementation-planning pass later determines that cPAR needs a current metadata payload surface, it must explicitly choose one of these two options before Slice 2:

1. reuse `serving_payload_current` with safe family-scoped replace semantics
2. create a separate `cpar_serving_payload_current`

That choice must be made intentionally.
It must not be left fuzzy.

The revised spec does not prematurely force a separate cPAR current-payload table.
It also does not allow silent reuse of the existing table without family-safe semantics.

## Runtime-State Rule

V1 does not add cPAR runtime-state keys by default.

If a later planning pass adds background cPAR package lanes that need operator/runtime truth, the keys must be family-scoped, for example:

- `cpar_package_build_status`
- `cpar_package_runtime_health`

They must not overload:

- `risk_engine_meta`
- `neon_sync_health`
- `__cache_snapshot_active`

## Authority By Runtime Role

`local-ingest`:

- may build cPAR packages
- may publish cPAR packages into Neon
- may run Neon parity and mirror checks for `cpar_*` tables

`cloud-serve`:

- may read existing cPAR packages
- may derive hedge previews from stored cPAR package data
- may not trigger cPAR package builds
- may not depend on local SQLite or local-only runtime artifacts

---

# 5. CORE PHILOSOPHY

We explicitly prioritize:

- tradability over theoretical purity
- interpretability over complexity
- stability over maximum R^2
- sparse outputs over dense outputs

We do not want:

- large factor sets
- opaque factors without ETF proxies
- unstable factor selection
- overfit hedge packages

---

# 6. MODEL SCOPE

cPAR is an instrument-level model.

It should support:

- US equities
- ex-US equities, with explicit caution labeling
- ETFs / ETPs
- any instrument with sufficient weekly price history and acceptable continuity

It should not emit a fit output for names that do not have enough history for a minimally stable fit.

Portfolio integration is explicitly deferred until the core v1 ticker and hedge surfaces are stable.

---

# 7. FACTOR DESIGN

Use a fixed v1 factor registry.

Required market factor:

- SPY

Required sector factors:

- XLB
- XLC
- XLE
- XLF
- XLI
- XLK
- XLP
- XLRE
- XLU
- XLV
- XLY

Required style factors:

- MTUM = momentum
- VLUE = value
- QUAL = quality
- USMV = low volatility
- IWM = size

V1 factor-set rule:

- do not add other style factors in v1
- do not add international or bespoke factors in v1
- do not infer factors from cUSE4 factor definitions

Proxy-mapping rule:

- every required proxy must resolve through `security_master`
- package build fails closed if any required proxy mapping is missing
- package build fails closed if the active package window does not have sufficient weekly prices for a required proxy

The factor registry is code-owned under `backend/cpar/*`.
It is not sourced from cUSE4 factor catalogs.

---

# 8. TIME-SERIES DESIGN

Base design:

- weekly returns
- Friday-ending cadence
- rolling 52-week window
- exponential weighting
- half-life = 26 weeks
- minimum observations = 39 weekly returns

Return-source rule:

- use `adj_close` where available
- fall back to `close` only when `adj_close` is absent for that instrument and week
- apply the same rule to ETF proxies and modeled instruments

Weekly-anchor rule:

- define each package on a Friday-ending XNYS weekly anchor
- if the market is closed on Friday, use the previous XNYS session as the weekly anchor
- for each instrument and proxy, the weekly price is the latest available eligible price on or before that anchor within the same Monday-Friday trading week

This is a weekly system.
It is not an intraday system.

On-demand rule:

- on-demand operation is allowed only as an operator-triggered package build in `local-ingest`
- on-demand does not mean ad hoc per-ticker request-time fitting for frontend reads
- every cPAR build still produces a weekly-aligned durable package

---

# 9. REGRESSION STRUCTURE

Step 1: market fit

- regress the instrument weekly return series on SPY first

Step 2: orthogonalization

- orthogonalize sector proxies to market
- orthogonalize style proxies to market only
- do not orthogonalize styles to industry in v1

Step 3: joint post-market estimation

- estimate sectors and styles jointly in one post-market block
- the dependent variable for the post-market block is the market-step residual

Important:

- do not run separate sector and style regressions and then add predictions together
- do not conflate orthogonalization with hedge construction

---

# 10. REGULARIZATION

Use ridge regression in v1.

Do not use pure lasso in v1.

Requirements:

- standardize regressors internally for ridge estimation
- convert coefficients back into raw ETF-space units for interpretation and hedging

V1 penalty rule:

- market is fit separately and is not ridge-penalized
- sectors and styles are ridge-penalized in the post-market block
- use block-specific fixed penalties on weighted-standardized regressors

Fixed v1 penalties:

- sectors: lambda = 4.0
- styles: lambda = 8.0

These are fixed constants for v1.
Do not add per-ticker penalty tuning or cross-validation loops in v1.

---

# 11. SPARSITY

Apply sparsity only as a post-regression cleanup step in raw ETF space.

Rules:

- market is not thresholded in v1
- non-market factors are thresholded after raw ETF-space back-transform
- fixed v1 threshold = 0.05 in absolute raw ETF beta units

Goal:

- remove obvious noise
- preserve interpretability
- avoid unstable factor selection dynamics

Storage rule:

- store both raw unthresholded ETF-space loadings and thresholded ETF-space loadings in the durable fit output

---

# 12. EXACT METHOD SPECIFICATION

This section freezes the v1 fit math.

## Package-Level Proxy Panel

For package date `T`, define the last 52 Friday-ending weekly anchors:

- `W_1, W_2, ..., W_52`
- `W_52 = T`

For each proxy ETF and each modeled instrument:

- weekly price `P_t` is the latest valid weekly price for that anchor under Section 8
- weekly return is `r_t = (P_t / P_(t-1)) - 1`

Package weights:

- `weight_t = exp(-ln(2) * age_weeks_t / 26)`
- `age_weeks_t = 0` for the most recent week

## Fit Sample Rule

For each instrument, use only weeks where the instrument return exists.
Proxy rows for those same weeks are required.

Fit status rules:

- `insufficient_history` if observed weekly returns < 39
- `insufficient_history` if the longest consecutive missing-week gap inside the 52-week window is > 4
- `limited_history` if observed weekly returns are between 39 and 51 inclusive
- `limited_history` if the longest consecutive missing-week gap is 3 or 4
- `ok` otherwise

Continuity warning:

- add warning `continuity_gap` if any consecutive missing-week gap is > 2

## Step 1: Market Fit

For instrument `i` on observed weeks `O_i`:

`y_i,t = alpha_market_i + beta_market_i * m_t + eps_market_i,t`

Where:

- `y_i,t` is the instrument weekly return
- `m_t` is the raw weekly SPY return
- estimation uses weighted least squares with the package weights restricted to `O_i`

Store:

- `alpha_market_i`
- `beta_market_i`
- `eps_market_i,t`

## Step 2: Package-Level Orthogonalization

Compute package-level orthogonalization once per package using proxy returns only.

For each sector proxy `s_j,t`:

`s_j,t = a_j + b_j * m_t + u_sector_j,t`

For each style proxy `v_k,t`:

`v_k,t = c_k + d_k * m_t + u_style_k,t`

Where:

- estimation uses weighted least squares over the full 52-week proxy panel
- `u_sector_j,t` and `u_style_k,t` are the orthogonalized residual series

Store the package transform needed to recover raw ETF-space coefficients:

- intercept terms
- market-loading terms
- factor identity metadata

## Step 3: Joint Post-Market Block

Use the market residual as the dependent variable:

`eps_market_i,t = alpha_block_i + Z_i,t * theta_i + eta_i,t`

Where:

- `Z_i,t` contains all orthogonalized sector and style regressors on observed weeks `O_i`
- each regressor is weighted-standardized to weighted mean 0 and weighted standard deviation 1 on `O_i`
- estimation uses weighted ridge with the fixed v1 block penalties from Section 10
- `alpha_block_i` is included and is not penalized

## De-Standardization

After the weighted ridge solve:

- convert coefficients from standardized orthogonalized units back into orthogonalized return units
- then apply the stored package transform to convert the coefficients into raw ETF return-space coefficients

The final raw ETF-space coefficient vector is:

- one SPY market beta from Step 1
- raw ETF-space sector betas from the back-transform
- raw ETF-space style betas from the back-transform

## Thresholding Order

Threshold only after the raw ETF-space coefficient vector has been recovered.

Do not threshold:

- market beta
- orthogonalized coefficients directly

Do threshold:

- non-market raw ETF-space coefficients using the fixed 0.05 absolute threshold

## Covariance Surface

The cPAR risk and hedge display covariance surface is:

- exponentially weighted covariance of raw weekly ETF proxy returns
- on the same 52-week package window
- using the same 26-week half-life

This covariance surface is package-level and durable.

---

# 13. THREE DISTINCT SPACES

These spaces must remain conceptually and operationally distinct:

1. Raw proxy return panel
   - raw ETF weekly returns
   - used for interpretation and covariance

2. Orthogonalized estimation panel
   - used internally for regression estimation only
   - not shown directly as hedge weights

3. Raw ETF trade space
   - used for final displayed loadings
   - used for hedge construction
   - used for post-hedge residual display

These must not be conflated in implementation, storage, naming, or UI explanation.

Storage rule:

- raw proxy panel, orthogonalization transform, covariance, and final raw ETF-space fit outputs must remain separable in the data model

---

# 14. DETERMINISTIC HEDGE ENGINE CONTRACT

Hedging is a first-class product feature, not a side utility.

## Required Hedge Types

1. `market_neutral`
2. `factor_neutral`

## Core Hedge Principle

cPAR hedges are built directly in raw ETF trade space.

Because the final fit outputs are raw ETF-space coefficients, the v1 hedge engine is intentionally simple:

- hedge weights are derived from the thresholded raw ETF-space coefficient vector
- no separate optimization layer is introduced in v1

## Inputs

Required inputs for hedge generation:

- active package date
- thresholded raw ETF-space loadings for the instrument
- raw ETF covariance surface for the active package
- hedge mode

Displayed notional convention:

- report hedge weights per `$1` of underlying long notional
- client-side scaling to any absolute notional is allowed
- storage and route contracts should remain normalized to `$1`

## Market-Neutral Hedge

If `abs(beta_market) < 0.10`:

- return no market hedge leg
- return status `hedge_ok`
- return `hedge_reason = below_market_materiality_threshold`

Else:

- hedge weight for SPY = `-beta_market`

## Factor-Neutral Hedge

Start from the thresholded raw ETF-space loadings.

Candidate set:

- all non-market factor loadings whose absolute thresholded beta is >= 0.05
- SPY if `abs(beta_market) >= 0.10`

Initial hedge weights:

- for every selected factor `j`, hedge weight = `-beta_j`

## Correlated-Substitute Pruning

Use the active-package raw ETF covariance surface to compute correlations.

If two candidate hedge ETFs have absolute correlation > 0.90:

- keep the ETF with the larger absolute thresholded beta
- drop the smaller one

Repeat until no violating pair remains or only one ETF remains from each highly correlated cluster.

## Max-Leg Rule

Maximum hedge size in v1:

- 5 ETFs total, including SPY

If more than 5 candidates remain after correlated-substitute pruning:

- keep SPY if market exposure is material
- keep the remaining ETFs with the largest absolute thresholded betas until the 5-leg cap is reached

## Tiny-Position Rule

Any hedge leg with absolute normalized weight < 0.05 is dropped.

Because thresholding already occurs at 0.05 in raw ETF space, this rule should usually be redundant.
Keep it explicit anyway.

## Post-Hedge Exposure Calculation

For any selected hedge package:

- post-hedge raw ETF-space exposure = underlying thresholded raw ETF-space exposure + hedge weights
- selected hedge factors should neutralize to zero by construction
- omitted factors remain residual

## Post-Hedge Risk Display

Use the active-package raw ETF covariance surface to show:

- pre-hedge factor variance proxy
- post-hedge factor variance proxy
- gross hedge notional
- net hedge notional
- remaining non-zero factor exposures

## Hedge Statuses

Return exactly one hedge status:

- `hedge_ok`
- `hedge_degraded`
- `hedge_unavailable`

Rules:

- `hedge_unavailable` if fit status is `insufficient_history`
- `hedge_degraded` if pruning or leg caps leave residual non-market gross exposure reduction below 50 percent
- `hedge_ok` otherwise

## Stability Diagnostics

For every factor-neutral hedge preview, compute against the previous successful package for the same ticker when available:

- leg-overlap ratio
- gross hedge notional change
- net hedge notional change

Display these as diagnostics only.
Do not add extra hedge-stabilization heuristics in v1.

---

# 15. DATA RULES

Run cPAR only on instruments with sufficient weekly history and acceptable continuity.

Latest classification metadata may be used for caution labels and display.
It is not part of the core regression math.

Ex-US rule for v1:

- no FX normalization in v1
- ex-US names may still receive cPAR fit and hedge output if they satisfy the weekly-history rules
- all such outputs must carry the `ex_us_caution` warning

Required status values:

- `ok`
- `limited_history`
- `insufficient_history`

Required warning values:

- `continuity_gap`
- `ex_us_caution`

Required fit metadata fields on ticker-detail responses:

- `package_date`
- `package_run_id`
- `data_authority`
- `fit_status`
- `warnings`
- `observed_weeks`
- `lookback_weeks`
- `longest_gap_weeks`
- `price_field_used`

---

# 16. STATUSES AND WARNINGS

This section freezes the trigger rules used by routes and frontend rendering.

## Fit Status

`ok`:

- observed weekly returns = 52
- longest missing-week gap <= 2

`limited_history`:

- observed weekly returns between 39 and 51 inclusive
  or
- longest missing-week gap is 3 or 4

`insufficient_history`:

- observed weekly returns < 39
  or
- longest missing-week gap > 4

## Warning Flags

`continuity_gap`:

- set whenever longest missing-week gap > 2

`ex_us_caution`:

- set when the latest available classification row for the instrument has `hq_country_code != 'US'`

## Frontend Rendering Rules

- `insufficient_history` blocks fit and hedge display and shows explicit unavailability messaging
- `limited_history` allows fit and hedge display with a visible caution badge
- `ex_us_caution` allows fit and hedge display with a visible caution badge
- `continuity_gap` is a warning badge, not a blocker by itself

Do not reuse current cUSE4 readiness or freshness wording for these states.

---

# 17. FRONTEND DESIGN

cPAR must be a distinct top-level app section, not a mode inside the cUSE4 Risk tab.

## V1 Navigation

Use one top-level `cPAR` tab in the existing shell.

Do not add multiple top-level cPAR tabs in v1.

## V1 Page Structure

V1 pages:

- `/cpar`
- `/cpar/explore`

Deferred beyond v1:

- `/cpar/hedge` as a standalone page
- side-by-side cUSE4 vs cPAR comparison pages
- cPAR portfolio integration pages

## V1 Page Responsibilities

`/cpar`:

- landing page
- search entry point
- concise methodology and warning explanation
- links or redirects into `/cpar/explore`

`/cpar/explore`:

- primary v1 page
- ticker search
- ticker detail
- displayed raw ETF-space loadings
- fit metadata
- warnings
- embedded hedge panel
- post-hedge residual display

## Frontend Truth Contract

Do not reuse current cUSE4 `analyticsTruth` freshness semantics.

Every cPAR response must carry its own truth metadata:

- `package_date`
- `package_run_id`
- `data_authority`
- `fit_status`
- `warnings`

The frontend may build lightweight cPAR-specific truth helpers from those fields.

## Shared Versus cPAR-Specific UX

Shared:

- app shell
- navigation shell
- generic API client patterns
- generic chart primitives
- generic typeahead behavior

cPAR-specific:

- cPAR pages
- cPAR hooks
- cPAR types
- cPAR status and warning rendering
- cPAR detail and hedge panels

---

# 18. ARCHITECTURE AND OWNERSHIP

## Pure cPAR Package

`backend/cpar/*` is the home for pure cPAR domain and model logic only.

Allowed examples:

- factor registry
- weekly proxy panel builders
- orthogonalization logic
- regression engine
- raw-space back-transform logic
- hedge engine

Forbidden:

- route handlers
- persistence adapters
- app-facing response assembly
- orchestration entrypoints

`backend/cpar/*` must not become a parallel mini-backend.

## Integration Layers

Routes:

- `backend/api/routes/cpar_*.py`
- routes stay thin
- routes validate, authorize, delegate, and translate errors

Services:

- `backend/services/cpar_*`
- services own cPAR app-facing payload assembly

Data:

- `backend/data/cpar_*`
- data modules own cPAR persistence and read facades

Orchestration:

- `backend/orchestration/cpar_*` or family-scoped additions in existing orchestration modules
- orchestration owns cPAR package build sequencing

## Reusable Infrastructure

Reusable only where clearly appropriate:

- universe / identifier registry
- canonical price-history source tables
- generic search and identifier normalization helpers
- holdings and account plumbing for later portfolio integration
- Neon sync / parity patterns
- generic UI primitives

## Forbidden Leakage

Do not:

- reuse cUSE4 factor definitions
- reuse cUSE4 factor IDs
- reuse cUSE4 model outputs
- share cPAR model math with `backend/risk_model/*`
- present cPAR outputs as another cUSE4 mode
- reuse cUSE4 runtime-state keys for cPAR state
- reuse cUSE4 payload names for cPAR APIs

The following current cUSE4 surfaces are not reusable as-is:

- `backend/services/universe_service.py`
- `backend/services/portfolio_whatif.py`
- current cUSE4 dashboard payload assembly

They may inspire patterns.
They may not define cPAR semantics.

---

# 19. NAMING APPENDIX

Use explicit cPAR namespace conventions from the start.

Recommended conventions:

- durable tables: `cpar_*`
- routes: `/api/cpar/*`
- frontend pages: `/cpar/*`
- frontend types: `Cpar*`
- services: `cpar_*`
- data modules: `cpar_*`

If a current-payload surface is added later:

- payload names must be `cpar_*`

Avoid generic shared names that could blur model-family boundaries.

---

# 20. REQUIRED REPO DISCOVERY CHECKLIST

Before implementation planning or implementation, the agent must inspect the current repo and current docs.

Required discovery includes at minimum:

Architecture and operating model:

- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/dependency-rules.md`
- `docs/architecture/architecture-invariants.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`

Backend structure:

- `backend/orchestration/profiles.py`
- `backend/orchestration/run_model_pipeline.py`
- `backend/orchestration/stage_source.py`
- `backend/orchestration/stage_serving.py`
- `backend/analytics/pipeline.py`
- `backend/analytics/refresh_persistence.py`
- `backend/data/serving_outputs.py`
- `backend/data/model_outputs.py`
- `backend/data/runtime_state.py`
- `backend/services/universe_service.py`
- `backend/services/operator_status_service.py`
- `backend/api/router_registry.py`

Frontend structure:

- `frontend/next.config.js`
- `frontend/src/components/TabNav.tsx`
- `frontend/src/hooks/useApi.ts`
- `frontend/src/lib/analyticsTruth.ts`
- `frontend/src/app/explore/page.tsx`
- `frontend/src/app/health/page.tsx`
- `frontend/src/app/data/page.tsx`

The planner must treat the current cUSE4 system as context and inspiration only.
It must not assume cPAR already has a place in the codebase.

---

# 21. ORCHESTRATION AND CADENCE

This section freezes how cPAR runs operationally in this repo.

## Package Cadence

cPAR is package-based in v1.

Each package is keyed by:

- `package_date`
- the completed Friday-ending weekly anchor

Normal frontend reads consume the latest successful package only.

## V1 cPAR Build Lanes

Expected operational lanes:

- `cpar-weekly`
- `cpar-package-date`

`cpar-weekly`:

- builds the latest package for the most recent completed weekly anchor
- allowed only in `local-ingest`

`cpar-package-date`:

- operator-only rebuild for an explicit package date
- allowed only in `local-ingest`

## No cPAR Serving-Only Lane In V1

Do not add a cPAR equivalent of cUSE4 `serve-refresh` in v1.

Reason:

- cPAR primary reads are durable relational package reads
- there is no cPAR need to republish a small blob-style serving family before the package model is stable

## Cloud-Serve Rule

In `cloud-serve`:

- cPAR routes are read-only
- cPAR routes may derive hedge previews from stored package data
- cPAR routes may not trigger a package build

## Local-Ingest Rule

In `local-ingest`:

- cPAR package builds are allowed
- cPAR package builds must publish durable `cpar_*` outputs into Neon
- cPAR package builds must not piggyback on cUSE4 `serve-refresh`, `core-weekly`, or `cold-core`

---

# 22. EXECUTION PLAN

## Phase 0 - Final Brief

Confirm the revised spec is internally consistent.

Acceptance gate:

- remaining open choices are bounded and explicitly named

## Phase 1 - System Discovery

Map:

- backend pipelines
- data flow
- serving patterns
- frontend patterns
- orchestration patterns
- current cUSE4 isolation
- Neon operating assumptions
- safe reuse vs must-remain-separate boundaries

Acceptance gate:

- discovery findings are repo-grounded and reference actual modules, docs, routes, and surfaces

## Phase 2 - Multi-Agent First Review

Required lenses:

- Quant / model
- Systems / architecture
- Data / Neon / operating model
- Product / UX
- Delivery / testing / sequencing

Each agent must:

- critique the design
- identify risks
- recommend changes

Acceptance gate:

- disagreements and risks are captured explicitly, not smoothed over

## Phase 3 - First Synthesis

Combine the agent perspectives into:

- a unified cPAR architecture
- a refined methodology
- clean backend/frontend boundaries
- a practical v1 product shape

## Phase 4 - Second Review

Run another independent critique pass on the synthesized design.

Specifically test:

- factor parsimony and tradability
- weekly, ridge, and threshold design
- orthogonalization correctness
- hedge practicality and stability
- boundary hardening against cUSE4 leakage
- Neon-first authority alignment
- UI coherence

## Phase 5 - Contract Freeze

Before any implementation slice beyond the math kernel, freeze:

- exact authority surfaces
- exact durable table family
- exact route set
- exact payload set
- exact warning and status rules
- exact ownership map

Acceptance gate:

- no contract-critical item remains as "to be decided later"

## Phase 6 - Final Implementation Blueprint

The final blueprint must define:

- exact conceptual model
- exact package / module layout
- exact durable table names
- exact route names
- exact payload names
- exact build order
- exact test plan
- explicit deferred items

## Phase 7 - Controlled Implementation

Only after the design phases above are complete.

Implementation must proceed in narrow validated slices.

---

# 23. IMPLEMENTATION SLICES

Preferred v1 slice sequence:

## Slice 1

Pure math kernel only:

- factor registry
- weekly return panel
- orthogonalization
- market-step fit
- post-market ridge fit
- raw-space back-transform
- thresholding
- deterministic hedge generation

Slice 1 goal:

- prove factor registry
- prove weekly return construction
- prove orthogonalization
- prove ridge estimation
- prove thresholding
- prove raw-space hedge construction

No serving or route work belongs in Slice 1.

## Contract-Freeze Gate

Before Slice 2 begins, the implementation-planning pass must explicitly lock:

- durable tables
- route contract
- payload contract
- active package selection rule
- current-payload decision if one is needed

## Slice 2

Durable storage and read contract:

- `cpar_*` durable table migrations
- Neon sync and parity expectations for `cpar_*` tables
- package selection read path
- one ticker-detail service read path
- one thin cPAR ticker-detail route

## Slice 3

Hedge preview integration:

- hedge preview service
- hedge preview route
- route and service status handling

## Slice 4

Frontend v1:

- `/cpar`
- `/cpar/explore`
- embedded hedge panel on the explore page

## Slice 5

Explicitly deferred unless Slices 1-4 are stable:

- broader cPAR portfolio integration
- holdings-driven cPAR analytics
- standalone `/cpar/hedge` page

Each slice must have explicit validation before the next begins.

---

# 24. VALIDATION AND NON-REGRESSION

Validation must include:

## Model Tests

- exact regression math
- orthogonalization correctness
- ridge and weighted-standardization correctness
- thresholding behavior
- raw-space back-transform correctness

## Stability Tests

- rolling-window stability
- representative tickers such as AAPL, JPM, XOM, SPY
- hedge leg-overlap diagnostics across adjacent packages

## Hedge Tests

- market-neutral hedge correctness
- factor-neutral hedge correctness
- correlated-substitute pruning
- post-hedge exposure reduction
- hedge status transitions

## Boundary Tests

- no cUSE4 leakage
- import-boundary tests for cPAR integration modules
- no cPAR route direct imports of `backend.data`
- no cPAR model math under `backend/risk_model/*`
- no writes from cPAR serving or orchestration into canonical source tables such as `security_prices_eod`

## Neon / Operating-Model Tests

- Neon schema and migration tests for `cpar_*` tables
- Neon parity and mirror tests for `cpar_*` tables
- package reads work in `cloud-serve` without local-only assumptions
- cPAR build lanes are blocked in `cloud-serve`
- cPAR app reads fail closed when no successful package exists

## API Tests

- cPAR route contracts
- status and warning payload correctness
- cPAR payload family isolation from cUSE4 payload families

## Frontend Tests

- `insufficient_history` rendering
- `limited_history` rendering
- `ex_us_caution` rendering
- `continuity_gap` rendering
- search and detail flow
- embedded hedge flow

## cUSE4 Non-Regression

- existing cUSE4 routes remain unchanged
- existing cUSE4 payload families remain unchanged
- existing cUSE4 operator/runtime surfaces remain unchanged unless intentionally extended in a family-safe way

---

# 25. DURABLE TABLES / PAYLOADS / ROUTES APPENDIX

This appendix freezes the minimum named surfaces for v1 planning.

## Durable Tables

Required minimum table family:

- `cpar_package_runs`
- `cpar_proxy_returns_weekly`
- `cpar_proxy_transform_weekly`
- `cpar_factor_covariance_weekly`
- `cpar_instrument_fits_weekly`

Expected row-grain intent:

- `cpar_package_runs`: one row per package build
- `cpar_proxy_returns_weekly`: one row per package, week, and factor proxy
- `cpar_proxy_transform_weekly`: one row per package transform component
- `cpar_factor_covariance_weekly`: one row per package and factor pair
- `cpar_instrument_fits_weekly`: one row per package and instrument fit output

## Current-Payload Surface

No cPAR current-payload surface is required in v1 by default.

If one is added later, it must be one of:

- shared `serving_payload_current` with family-scoped replace semantics
- separate `cpar_serving_payload_current`

That decision must be made before Slice 2.

## API Routes

Minimum v1 route set:

- `GET /api/cpar/meta`
- `GET /api/cpar/search`
- `GET /api/cpar/ticker/{ticker}`
- `GET /api/cpar/ticker/{ticker}/hedge`

Route intent:

- `/api/cpar/meta`: factor registry and active package metadata
- `/api/cpar/search`: namespaced search surface backed by shared identifier infrastructure plus cPAR fit availability metadata
- `/api/cpar/ticker/{ticker}`: durable ticker-detail read path
- `/api/cpar/ticker/{ticker}/hedge`: hedge preview from stored package data

## API Payload Families

Minimum v1 API payload families:

- `cpar_meta`
- `cpar_search_results`
- `cpar_ticker_detail`
- `cpar_hedge_preview`

These are API contract names.
They are not required to be persisted as blob-style current payloads in v1.

## Frontend Pages

Minimum v1 page family:

- `/cpar`
- `/cpar/explore`

Deferred:

- `/cpar/hedge`
- cPAR portfolio pages
- cUSE4 vs cPAR comparison pages

---

# 26. REQUIRED OUTPUT FORMAT

The implementation-planning pass must return its output in this structure:

1. system discovery findings
2. five-agent critiques
3. synthesis
4. contract-freeze decisions
5. final implementation blueprint
6. recommended first implementation slice

The output must explicitly include:

- authority surfaces
- durable tables
- route contracts
- payload contracts
- ownership map
- deferred items

---

# 27. IMPLEMENTATION-PLANNING CONFIRMATION CHECKLIST

The next implementation-planning pass must explicitly confirm:

- whether v1 truly needs any current-payload surface at all
- if yes, whether that surface reuses `serving_payload_current` safely or uses `cpar_serving_payload_current`
- whether cPAR build lanes are added to the shared orchestrator profile registry or exposed through a dedicated cPAR operational entrypoint under `backend/orchestration`
- whether Slice 5 remains deferred

These are bounded planning confirmations.
They are not excuses to reopen the whole architecture.

---

# 28. GUARDRAILS

- do not redesign cUSE4
- do not introduce fuzzy shared abstractions
- do not blur payload families
- do not mix factor definitions or outputs
- prefer clear, durable architecture
- prefer simple, robust v1 choices
- avoid overengineering
- keep cPAR aligned with the repo's Neon-first operating model for app-serving data
- do not let `backend/cpar/*` become a parallel mini-backend

---

# 29. DEFERRED V1 EXCLUSIONS

Out of scope until the core cPAR ticker and hedge surfaces are stable:

- cUSE4 vs cPAR side-by-side comparison pages
- FX normalization
- blended multi-model truth surfaces
- broader portfolio integration
- holdings-driven cPAR analytics
- standalone `/cpar/hedge` page

---

# 30. FINAL GOAL

Deliver:

- a clean, independent cPAR system
- stable and interpretable outputs
- actionable hedge packages
- seamless app integration
- zero interference with cUSE4
