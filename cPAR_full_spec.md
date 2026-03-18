# cPAR (Parsimonious and Actionable Regression)
## Full Mission, Design, and Execution Specification

Repo: `luckytigre/ceiora-risk`
Local path: `/Users/shaun/Library/CloudStorage/Dropbox/040 - Creating/ceiora-risk`

This document is the canonical execution brief for cPAR planning and implementation.

It must be treated as self-contained. Do not rely on a separate "original prompt" or unwritten context.

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
  - holdings / account / what-if plumbing
  - serving persistence patterns
  - Neon sync and durability patterns
- cPAR must not share:
  - factor definitions
  - factor catalogs
  - model outputs
  - model logic
  - serving payload families

Normal-operation requirement:
- cPAR model outputs and cPAR serving payloads should be designed for Neon-primary app reads in normal operation
- local SQLite remains the direct ingest / archive surface
- cPAR should not depend on local-only runtime artifacts for standard frontend use

The planner must account for Neon explicitly:
- cPAR durable model outputs should be designed to persist and serve through the Neon-first operating model
- cPAR should not depend on local-only runtime artifacts for normal app use
- cPAR source usage must remain consistent with the existing local-ingest -> Neon publish architecture

---

# 4. CORE PHILOSOPHY

We explicitly prioritize:

- tradability over theoretical purity
- interpretability over complexity
- stability over maximum R²
- sparse outputs over dense outputs

We do not want:
- large factor sets
- opaque factors without ETF proxies
- unstable factor selection
- overfit hedge packages

---

# 5. MODEL SCOPE

cPAR is an instrument-level model.

It should support:
- US equities
- ex-US equities, with explicit caution labeling where appropriate
- ETFs / ETPs
- any instrument with sufficient weekly price history and acceptable continuity

It should not emit a regression output for names that do not have enough history for a minimally stable fit.

---

# 6. FACTOR DESIGN

## Market
- SPY only

## Sectors
Use the fixed 11 SPDR sector ETFs:

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

## Styles
Use the fixed v1 style ETF set:

- MTUM = momentum
- VLUE = value
- QUAL = quality
- USMV = low volatility
- IWM = size

Do not add other style factors in v1 unless the planning phase presents a strong, repo-grounded case for doing so.

---

# 7. TIME-SERIES DESIGN

Base design:
- weekly returns
- Friday-ending cadence
- rolling 52-week window
- exponential weighting
- half-life candidate = 26 weeks
- minimum observations candidate = 39

The final blueprint must explicitly decide:
- whether 52 / 26 is the correct v1 choice
- the exact minimum-observation rule
- the exact fallback rule for shorter but still usable histories

Return-source rule:
- use adjusted close or total-return-like series where available
- must be consistent across ETF proxies and modeled instruments
- the final blueprint must say explicitly what source is used in practice in this repo

This is a weekly system.
It is not an intraday system.

On-demand rule:
- on-demand runs are allowed in v1
- on-demand runs must still produce a weekly-aligned cPAR package
- do not design ad hoc per-ticker daily-fit behavior in v1

---

# 8. REGRESSION STRUCTURE

## Step 1: Market
Regress instrument returns on SPY first.

## Step 2: Orthogonalization
- sectors are orthogonalized to market
- styles are orthogonalized to market only
- styles must not be orthogonalized to industry in v1

## Step 3: Joint Post-Market Estimation
- sectors and styles are estimated jointly in one post-market block

Important:
- do not run separate sector and style regressions and then add predictions together
- do not conflate orthogonalization with hedge construction

The final blueprint must provide exact regression equations.

---

# 9. REGULARIZATION

Use ridge regression in v1.

Do not use pure lasso in v1.

Requirements:
- standardize regressors internally for ridge estimation
- convert outputs back into raw ETF-space units for interpretation and hedging

The final blueprint must explicitly decide:
- whether penalties are one shared lambda or block-specific lambdas
- how sectors vs styles are penalized
- whether market is excluded from penalty
- how standardization and de-standardization are done

Default directional intent:
- market = no penalty
- sectors = moderate penalty
- styles = stronger penalty

---

# 10. SPARSITY

Apply sparsity only as a post-regression cleanup step.

Rules:
- market is not thresholded in v1
- non-market factors may be thresholded
- initial candidate threshold is around 0.05

Goal:
- remove obvious noise
- preserve interpretability
- avoid unstable factor selection dynamics

The final blueprint must specify the exact thresholding rule and whether it applies uniformly across all non-market factors.

---

# 11. THREE DISTINCT SPACES

These spaces must remain conceptually and operationally distinct:

1. Raw proxy return panel
   - raw ETF returns
   - used for display, interpretation, and covariance

2. Orthogonalized estimation panel
   - used internally for regression estimation only

3. Raw ETF trade space
   - used for hedge construction and hedge display

These must not be conflated in implementation, storage, naming, or UI explanation.

Covariance requirement:
- the final blueprint must explicitly decide what covariance surface is used for cPAR risk and hedge display
- default v1 recommendation: covariance of raw weekly ETF proxy returns
- if a different covariance design is proposed, it must be justified explicitly

---

# 12. HEDGE GENERATION

Hedging is a first-class product feature, not a side utility.

## Required Hedge Types

1. Market-neutral hedge
2. Full factor-neutral hedge

## Constraints

- max 4 to 5 ETFs
- no tiny positions
- allow long and short
- avoid highly correlated substitutes together
- SPY should generally be included if market exposure is material
- hedge generation should use thresholded exposures for consistency in v1

## Critical Requirement

Hedges must be computed in raw ETF trade space.

This requires an explicit back-transform from orthogonalized estimation space into raw ETF-space hedge weights.

The final blueprint must define:
- exact market-neutral hedge logic
- exact full-hedge logic
- ETF selection / pruning rules
- no-tiny-position rule
- notional scaling rule
- post-hedge exposure calculation and display
- how hedge-package stability will be evaluated across adjacent runs

## Hedge Philosophy

- good-enough neutralization
- not perfect smallest-notional mathematical exactness
- favor simple, actionable, stable packages
- do not introduce bespoke hedge-stabilization heuristics by default
- only propose additional stability mechanisms if instability is observed and justified

---

# 13. DATA RULES

Run cPAR only on instruments with sufficient weekly history and acceptable continuity.

If history is not sufficient, surface explicit states such as:
- `insufficient_history`
- `limited_history`

Ex-US rule for v1:
- no FX normalization in v1
- apply an ex-US caution label

The final blueprint must specify:
- exact insufficient-history threshold
- exact limited-history rule
- exact frontend warnings / markers for those states

Required status / warning states:
- `insufficient_history`
- `limited_history`
- `ex_us_caution`

The final blueprint must define the exact trigger rules for each state.

---

# 14. FRONTEND DESIGN

cPAR must be a distinct top-level app section, not a mode inside the cUSE4 Risk tab.

Best v1 page structure:
- `/cpar`
- `/cpar/explore`
- `/cpar/hedge`

Frontend must emphasize:
- exposures
- instrument-level details
- hedge packages
- post-hedge exposures
- warnings for limited history / insufficient history / ex-US caution

Defer side-by-side cUSE4 vs cPAR comparisons in v1.

---

# 15. ARCHITECTURE

## Strict Separation

cPAR must not pollute cUSE4.

Separate:
- `backend/cpar/*`
- durable cPAR tables
- cPAR payload families
- cPAR routes under `/api/cpar/*`
- frontend cPAR types
- frontend cPAR pages

## Reusable Infrastructure

Reusable only where clearly appropriate:
- universe / identifier registry
- canonical price-history source tables
- holdings and what-if plumbing
- serving payload persistence patterns
- Neon sync / parity patterns
- UI primitives / generic charts

## Forbidden Leakage

Do not:
- reuse cUSE4 factor definitions
- reuse cUSE4 factor IDs
- reuse cUSE4 model outputs
- share model math with `backend/risk_model/*`
- present cPAR outputs as another cUSE4 mode

The final blueprint must define the exact boundary protections, including import-boundary tests or equivalent hardening.

## Naming Appendix

Use explicit cPAR namespace conventions from the start.

Recommended conventions:
- durable tables: `cpar_*`
- serving payloads: `cpar_*`
- routes: `/api/cpar/*`
- frontend pages: `/cpar/*`
- frontend types: `Cpar*`

Names should remain concrete and operationally distinct from cUSE4 names.
Avoid generic shared names that could blur model-family boundaries.

---

# 16. REQUIRED REPO DISCOVERY CHECKLIST

Before planning or implementation, the agent must inspect the current repo and current docs.

Required discovery includes at minimum:

Architecture and operating model:
- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/dependency-rules.md`
- `docs/operations/OPERATIONS_PLAYBOOK.md`

Backend structure:
- orchestration profiles/stages
- current serving payload persistence
- universe/ticker/search/history services
- existing cUSE4 model package boundaries

Frontend structure:
- top-level app pages
- API hooks and types
- risk / explore / what-if UI patterns

The planner must treat the current cUSE4 system as context and inspiration only.
It must not assume cPAR already has a place in the codebase.

---

# 17. EXECUTION PLAN

## Phase 0 — Final Brief
Confirm the spec is internally consistent.

Acceptance gate:
- all unresolved ambiguities are listed explicitly before deeper planning begins

## Phase 1 — System Discovery
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
- discovery findings are repo-grounded and reference actual modules / docs / routes / surfaces

## Phase 2 — Multi-Agent First Review

Agents:
- Quant / model
- Systems / architecture
- Product / UX
- Delivery / operations

Each agent must:
- critique the design
- identify risks
- recommend changes

Acceptance gate:
- disagreements and risks are captured explicitly, not smoothed over

## Phase 3 — First Synthesis
Combine the agent perspectives into:
- a unified cPAR architecture
- a refined methodology
- clean backend/frontend boundaries
- a practical v1 product shape

## Phase 4 — Second Review
Run another independent critique pass on the synthesized design.

Specifically test:
- factor parsimony and tradability
- weekly / ridge / threshold design
- orthogonalization correctness
- hedge practicality and stability
- boundary hardening against cUSE4 leakage
- UI coherence

## Phase 5 — Final Implementation Blueprint

The final blueprint must define:
- exact conceptual model
- exact package / module layout
- exact durable table names
- exact route names
- exact payload names
- exact frontend pages and types
- exact build order
- exact testing strategy
- explicit deferred items

## Phase 6 — Controlled Implementation
Only after the design phases above are complete.

Implementation must proceed in narrow validated slices.

---

# 18. IMPLEMENTATION SLICES

Preferred v1 slice sequence:

## Slice 1
- factor registry
- weekly return panel
- orthogonalization
- regression engine
- ridge + thresholding
- orthogonalized-space to raw-ETF-space back-transform

Slice 1 goal:
- prove factor registry
- prove weekly return construction
- prove orthogonalization
- prove ridge estimation
- prove thresholding
- prove hedge back-transform
- do this before broader serving / UI work

## Slice 2
- durable storage
- cPAR universe / ticker API
- one ticker-detail read path

## Slice 3
- hedge generation
- cPAR hedge API

## Slice 4
- frontend cPAR explore page

## Slice 5
- broader cPAR portfolio / serving integration

Each slice should have explicit validation before the next begins.

---

# 19. VALIDATION AND TESTING

Validation must include:

Model tests:
- exact regression math
- orthogonalization correctness
- ridge / standardization correctness
- thresholding behavior

Stability tests:
- rolling-window stability
- representative tickers such as AAPL, JPM, XOM, SPY

Hedge tests:
- hedge-package plausibility
- raw ETF-space back-transform correctness
- post-hedge exposure reduction
- hedge-package stability across adjacent runs

Boundary tests:
- no cUSE4 leakage
- import-boundary tests
- payload isolation tests

API tests:
- cPAR route contracts
- durable payload correctness

UI tests:
- markers for insufficient history / limited history / ex-US caution
- explore and hedge flows

---

# 20. REQUIRED OUTPUT FORMAT

The planning output must be returned in this exact structure:

1. system discovery findings
2. first-round agent critiques
3. first synthesis
4. second-round agent critiques
5. refined synthesis
6. final implementation blueprint
7. recommended first implementation slice

---

# 21. KEY QUESTIONS THAT MUST BE ANSWERED

The final blueprint must explicitly answer:

- Is 52-week EW regression with half-life 26 the best v1 design?
- What exact minimum-observation rule should be used?
- What exact ridge specification should be used?
- What exact threshold should be used for non-market loadings?
- Should market always remain unthresholded in v1?
- What is the cleanest way to compute hedge packages in raw ETF space after orthogonalized estimation?
- What exact architecture best prevents cPAR from polluting cUSE4?
- What is the narrowest but strongest first implementation slice?

---

# 22. GUARDRAILS

- do not redesign cUSE4
- do not introduce fuzzy shared abstractions
- do not blur payload families
- do not mix factor definitions or outputs
- prefer clear, durable architecture
- prefer simple, robust v1 choices
- avoid overengineering
- keep cPAR aligned with the repo's Neon-first operating model for app-serving data

---

# 23. FINAL GOAL

Deliver:

- a clean, independent cPAR system
- stable and interpretable outputs
- actionable hedge packages
- seamless app integration
- zero interference with cUSE4
