# Universe Registry And Model Gating Plan

Date: 2026-03-25
Status: Repository implementation complete through compatibility cutover and registry-first repo-surface cleanup; operational destructive demotion still requires an actual rollout window
Owner: Codex

## Purpose

This document defines the target shape for ticker-universe management across:

- curated registry maintenance
- LSEG ingest behavior
- SQLite to Neon source publication
- cUSE model gating and projection paths
- cPAR package coverage and portfolio usability
- frontend-facing rollups

It exists because the current universe flow is too hard to reason about when adding names, debugging coverage, or changing model policy.

## Problem Statement

Original problem state:

The legacy workflow started with `data/reference/security_master_seed.csv`, synced that into `security_master`, enriched rows from LSEG, and then used a small number of overloaded fields to drive ingest and model behavior.

Current repo note:

The repository bootstrap/docs surface is now registry-first around `data/reference/security_registry_seed.csv`, but the remaining compatibility artifacts and rollout tasks below still matter until destructive retirement of `security_master` is complete.

Today, fields such as:

- `coverage_role`
- `classification_ok`
- `is_equity_eligible`
- `model_status`

are carrying multiple meanings at once:

- registry intent
- instrument taxonomy
- source-data readiness
- cUSE routing
- cPAR expectations
- consumer-facing status

That compression is what creates the current "black magic" feel.

## Issues Created By The Current Design

- `security_master` mixes curated registry truth with vendor-enriched identity, derived coverage flags, and model-routing hints.
- The current naming is vague. Terms like `master`, `eligible`, and `projection_only` do not say whether they refer to identity, policy, observed data, or model outcome.
- cUSE and cPAR are forced through partially shared universe concepts even though the two models use instruments very differently.
- The current two-pass ingest behavior for projection-only names is operationally important but implicit in code, not explicit as a named policy selector.
- `classification_ok` and `is_equity_eligible` collapse "expected to have PIT data" and "currently observed to have PIT data" into the same signal.
- Current statuses hide why a name did or did not flow through a model. Missing PIT data, non-US routing, stale prices, operator overrides, and package mismatches are too easy to collapse into one bucket.
- SQLite-first ingest and Neon-authoritative serving are real and correct, but the authority boundary is not defined per entity class, which makes debugging harder.
- Historical reproducibility is weak because current-state flags do not preserve enough audit trail for source readiness, sync state, or model routing decisions.

## Design Goals

- Make the inclusive tracked universe explicit and durable.
- Separate stable identity from time-varying tracking intent.
- Separate taxonomy from policy.
- Separate expected coverage from observed coverage.
- Give cUSE and cPAR their own model-stage membership contracts.
- Keep consumer-facing statuses as late rollups, not as the source of truth for gating.
- Preserve Neon as the operating authority without hiding the fact that SQLite remains the LSEG landing zone and deep archive.
- Support future changes to gating policy without mutating identity semantics.
- Support manual overrides with provenance, reason, and effective dates.

## Plan Summary

The original five-part idea should be tightened into seven durable layers plus one downstream serving rollup:

1. Identity
2. Tracking
3. Taxonomy
4. Operator policy
5. Observed source readiness and audit
6. Model-stage membership
7. Model-basis assignments
8. Serving rollup

The important refinement is that `policy`, `membership`, and `status` must not be allowed to blur together.

## Vision

The target operating model is:

- one inclusive registry of things we track or have tracked
- explicit model-independent descriptions of what each thing is
- explicit model-specific statements of what each model is allowed to do with it
- deterministic named selectors for ingest and model routing
- time-varying observed readiness derived from source facts
- time-varying cUSE and cPAR membership records that explain how a name actually flowed through a run
- consumer-facing statuses derived last, for UI and payload convenience only

Under this model, adding a ticker, debugging a missing name, or changing a gate should no longer require reverse-engineering hidden interactions between `security_master`, LSEG enrichment, and downstream model code.

## Target Design Outline

### 1. Identity

Identity answers: what instrument or listing is this?

The design should stop assuming that "tracked row keyed by RIC" is the same thing as durable identity. The target should distinguish:

- durable instrument identity
- listing or identifier mappings
- operator tracking choice

Target shape:

- instrument-centric identity record, ideally with a durable internal `security_id`
- identifier mapping history for `ric`, `ticker`, `isin`, and other vendor aliases
- support for ticker changes, duplicate listings, ADR/common pairs, and vendor remaps without rewriting the meaning of the tracked row

Phase 1 note:

- do not start by renaming every current table
- it is acceptable to keep current physical tables and add compatibility layers while the logical model is separated

### 2. Tracking

Tracking answers: are we intentionally tracking this listing in the project, and in what lifecycle state?

This is distinct from identity.

Target fields:

- `tracking_status`
- `effective_from`
- `effective_to`
- `operator_source`
- `operator_reason`

Illustrative statuses:

- `active`
- `historical_only`
- `disabled`
- `vendor_unresolved`
- `vendor_retired`

This layer is the inclusive registry concept the project needs, but it must be lifecycle-aware rather than just "present in the seed file."

### 3. Taxonomy

Taxonomy answers: what kind of thing is this?

Taxonomy must be orthogonal, not a single overloaded enum.

Target dimensions should include:

- `instrument_kind`
- `vehicle_structure`
- `issuer_country`
- `listing_country`
- `model_home_market_scope`
- `is_receipt`
- `is_fund`
- `is_single_name_claim`

Examples:

- A US common stock and an ADR are both single-name claims, but they differ in structure.
- A foreign common stock listed in the US should not force "US" to mean both listing venue and model home market.
- An ETF and a mutual fund may both be funds, but they should not be collapsed into the same economic or operational assumptions by default.

Taxonomy should be vendor-enriched where appropriate, but vendor-enriched facts must remain distinguishable from curated overrides.

### 4. Operator Policy

Policy answers: what is the system allowed or intended to do with this name?

Policy must represent operator intent and allowed paths, not observed facts.

Policy should be capability-based and effective-dated rather than a single coarse bucket.

Target policy capabilities:

- `price_ingest_enabled`
- `identifier_refresh_enabled`
- `pit_fundamentals_enabled`
- `pit_classification_enabled`
- `historical_backfill_enabled`
- `allow_cuse_native_core`
- `allow_cuse_fundamental_projection`
- `allow_cuse_returns_projection`
- `allow_cpar_core_target`
- `allow_cpar_extended_target`

Target path controls:

- `cuse_preferred_path`
- `cuse_fallback_policy`
- `cpar_target_scope`

Policy also needs first-class overrides:

- override reason
- override author
- override timestamp
- optional expiry or effective window

This is the layer that replaces the current overuse of `coverage_role`.

### 5. Observed Source Readiness And Audit

Observed readiness answers: what data do we actually have, and what did the system observe at a given time?

This layer must be separate from identity and policy.

It should include both:

- current readiness snapshots for operations and app reads
- historical observations for reproducibility and debugging

Target entities:

- raw source facts remain the canonical source tables:
  - `security_prices_eod`
  - `security_fundamentals_pit`
  - `security_classification_pit`
- derived current status:
  - `security_source_status_current`
- historical readiness:
  - `security_source_observation_daily`
- ingest audit:
  - `security_ingest_runs`
  - `security_ingest_audit`
- SQLite to Neon sync audit:
  - `source_sync_runs`
  - `source_sync_watermarks`

Critical rule:

- expected coverage and observed coverage must be separate

Illustrative fields:

- `expected_prices`
- `expected_fundamentals`
- `expected_classification`
- `observed_prices`
- `observed_fundamentals`
- `observed_classification`
- `last_price_date`
- `last_fundamentals_asof`
- `last_classification_asof`
- `coverage_gap_reason`

This avoids collapsing:

- ETFs that are not supposed to have PIT data
- single-name equities that are supposed to have PIT data but are missing it

into the same "no fundamentals" bucket.

### 6. Model-Stage Membership

Model-stage membership answers: how did this name actually flow through a specific cUSE or cPAR build?

This is where the current design most needs more structure.

#### cUSE

cUSE needs to separate at least the following concepts:

- policy path
- candidate path
- realized model role
- output status
- reason code
- quality label

Target cUSE membership fields:

- `as_of_date`
- `policy_path`
- `source_snapshot_status`
- `candidate_path`
- `realized_role`
- `output_status`
- `reason_code`
- `quality_label`
- `projection_method`
- `projection_basis_status`
- `projection_source_package_date`

Illustrative `realized_role` values:

- `structural_candidate`
- `regression_member`
- `estu_member`
- `fundamental_projection`
- `returns_projection`
- `none`

Illustrative `output_status` values:

- `served`
- `blocked`
- `unavailable`

Consumer-facing rollups such as:

- `core_estimated`
- `projected_only`
- `ineligible`

may remain useful, but only as derived output labels. They are not expressive enough to be the primary contract.

This explicitly supports the real cUSE asymmetry:

- US single-name equities may become native members
- ex-US single-name equities may become characteristic-based projections
- vehicles such as ETFs and mutual funds may become returns-based projections
- fallback behavior must be explicit rather than hidden

#### cPAR

cPAR needs its own contract because it is package-oriented and returns-based.

It should separate:

- broad package universe
- target interpretation scope
- fit quality
- portfolio usability

Target cPAR coverage fields:

- `package_date`
- `universe_scope`
- `target_scope`
- `fit_family`
- `fit_status`
- `portfolio_use_status`
- `reason_code`
- `quality_label`

Illustrative semantics:

- a name may be in the broad package universe
- also be outside the core US target interpretation scope
- still have a usable fit
- still be flagged as cautionary or not default-display

This is more faithful to how cPAR currently behaves than a single `core_us / extended_priced / excluded` bucket.

### 7. Model-Basis Assignments

Model-basis assignments answer: which instruments play special basis roles in a model?

This must stay separate from general universe membership.

For cPAR, this covers:

- market proxy assignments
- sector proxy assignments
- style proxy assignments

These assignments should be:

- explicit
- effective-dated
- package-family aware

That avoids hard-coding today's factor proxy choices as timeless universe truth.

### 8. Serving Rollup

Serving rollup answers: what should the UI and app payloads expose by default?

This layer should be derived from model-stage membership plus quality and presentation policy.

Critical rule:

- "can be analyzed" is not the same as "should be shown by default"

That means the design should support separate presentation labels such as:

- default-display eligibility
- low-confidence warnings
- suppressed-by-policy flags

without leaking those concerns back into core gating fields.

## Named Selectors

The redesign should replace hidden unions and ad hoc filters with explicit named selectors.

Illustrative selector set:

- `registry_active_scope`
- `price_ingest_scope`
- `pit_ingest_scope`
- `identifier_refresh_scope`
- `cuse_structural_candidate_scope`
- `cuse_native_estimation_scope`
- `cuse_projection_candidate_scope`
- `cuse_served_scope`
- `cpar_build_scope`
- `cpar_fit_scope`
- `cpar_portfolio_usable_scope`

These selectors should be implemented as documented queries, views, or centrally owned selector functions, not as scattered boolean expressions.

## Authority Map

Neon should remain the operating authority, but authority must be defined per entity class.

| Entity Class | First Writer / Authoring Surface | Operating Authority | Notes |
| --- | --- | --- | --- |
| Curated tracking registry and policy | committed registry artifact today; future admin surface later | Neon for app/runtime reads after sync | Git-backed artifact remains the change-control input until a managed UI exists |
| Vendor-enriched identity and taxonomy observations | local SQLite ingest from LSEG | Neon current view after publish | Must remain distinguishable from curated overrides |
| Raw source facts | local SQLite ingest from LSEG | Neon retained operating copy after source sync | SQLite remains landing zone and deep archive |
| Source readiness and sync audit | derived by ingest/sync jobs from raw facts and policy | Neon | Must preserve history and watermarks |
| cUSE membership | cUSE pipeline | Neon | Daily model-stage contract |
| cPAR coverage | cPAR pipeline | Neon | Package-level coverage contract |
| Serving payloads and frontend rollups | serving lanes | Neon | Consumer-facing only |

## Current Field Mapping To The Target Model

| Current Field / Concept | Problem | Target Replacement |
| --- | --- | --- |
| `coverage_role` | mixes cUSE pathing with identity and ingest semantics | tracking + operator policy + model-specific path controls |
| `classification_ok` | mixes expected PIT coverage with observed vendor data | observed classification readiness plus coverage-gap reason |
| `is_equity_eligible` | mixes taxonomy with cUSE-specific gating | orthogonal taxonomy plus cUSE membership logic |
| `model_status` | too coarse for debugging and migration | model-stage membership plus derived serving rollup |
| implicit projection-only price ingest union | operationally important but hidden | explicit `price_ingest_scope` selector |
| `security_master` as one-stop truth | too overloaded | split logical responsibilities, preserve compatibility surfaces during migration |

## How The Plan Addresses The Original Problem

The user problem was not only that the current universe felt messy. The deeper issue was that the same small set of flags was being asked to answer incompatible questions.

This plan addresses that by:

- separating inclusive tracking from model-specific use
- making taxonomy and policy explicit
- making observed readiness historical and auditable
- giving cUSE and cPAR different stage contracts because they behave differently
- keeping special factor-basis roles outside generic universe classification
- defining deterministic selectors instead of hidden filtering behavior
- making Neon authority operationally precise instead of just conceptually true

## Migration Outline

This should be implemented incrementally.

### Phase 1: Vocabulary And Compatibility

- freeze the new vocabulary in docs
- add new logical entities without removing current fields
- keep `security_master` as a compatibility read surface while the new model is introduced

### Phase 2: Tracking, Taxonomy, And Policy

- introduce explicit tracking and policy surfaces
- split curated fields from vendor-enriched fields
- define override mechanisms with provenance

### Phase 3: Ingest And Readiness

- replace current ad hoc ingest gating with named selectors
- preserve parity with current two-pass price ingest before retiring `projection_only`
- add historical readiness and sync-audit tables

### Phase 4: cUSE Membership

- create cUSE daily stage-membership contract
- move output rollups to derived serving logic
- keep current frontend labels as compatibility rollups during transition

### Phase 5: cPAR Coverage

- create cPAR package coverage and portfolio-usability contract
- separate broad package inclusion from target interpretation scope

### Phase 6: Retirement Of Legacy Flags

- retire legacy booleans only after parity checks confirm no regressions in ingest, model routing, and serving
- keep compatibility views for any remaining downstream readers that cannot migrate immediately

## Non-Goals

- This plan does not change cUSE or cPAR math by itself.
- This plan does not require a broad physical table rename in the first implementation slice.
- This plan does not assume a self-service registry UI exists yet.
- This plan does not force cUSE and cPAR into fake symmetry where the underlying model semantics differ.

## Success Criteria

The redesign is successful when:

- adding a new name requires editing clear tracking and policy inputs rather than relying on hidden side effects
- ingest selectors are explicit and testable
- operators can tell whether a failure belongs to registry policy, raw source coverage, sync state, cUSE routing, cPAR coverage, or serving rollup
- cUSE can distinguish native membership, characteristic projection, returns projection, and fail-closed cases without overloading one status field
- cPAR can distinguish broad package presence, fit quality, and portfolio usability without borrowing cUSE terminology
- Neon remains the authoritative operating store, with auditable lineage back to registry inputs, source observations, sync watermarks, and model runs

## Detailed Implementation Plan

This section turns the target design into a concrete repo plan.

It is intentionally specific about:

- current seams and dependencies
- tables to add, extend, keep, and eventually retire
- exact module groups to refactor
- when Neon changes happen
- what gets dual-written, dual-read, and cut over
- what must be validated before destructive cleanup

### Scope And Constraints

The implementation plan assumes the following constraints:

- local SQLite remains the first writer for LSEG source facts
- Neon remains the operating authority for app/runtime reads
- raw source tables stay RIC-keyed in the first executable slice
- a repo-wide `security_id` migration is deferred
- `projected_instrument_loadings` and `projected_instrument_meta` stay in place in the first executable slice
- `backend/cpar/factor_registry.py` stays in place in the first executable slice
- `security_master` remains a compatibility surface until all major read paths have moved

The first goal is not "perfect final schema." The first goal is to stop the overloading while preserving operational safety.

### Current Repo Seam Map

#### 1. Registry bootstrap and seed sync

Current seam:

- `backend/universe/bootstrap.py`
- `backend/universe/security_master_sync.py`
- `backend/scripts/export_security_master_seed.py`
- `backend/tests/test_security_master_seed_hygiene.py`
- `backend/tests/test_security_master_lineage.py`

Current behavior:

- `data/reference/security_master_seed.csv` is synced into `security_master`
- `security_master` is both the bootstrap registry and a live enriched table
- `coverage_role`, `classification_ok`, and `is_equity_eligible` are mixed into that same table

Required replacement:

- split seed sync into explicit registry and policy sync
- stop treating derived vendor/readiness flags as registry truth

#### 2. LSEG ingest and backfill selectors

Current seam:

- `backend/scripts/download_data_lseg.py`
- `backend/scripts/backfill_prices_range_lseg.py`
- `backend/scripts/backfill_pit_history_lseg.py`
- `backend/universe/security_master_sync.py`
- `backend/tests/test_projection_only_exclusion.py`

Current behavior:

- `load_default_source_universe_rows()` drives PIT ingest and raw-history inclusion
- `load_projection_only_universe_rows()` drives price-only second-pass ingest and serving projection validation
- `load_price_ingest_universe_rows()` unions the two for price backfills
- `projection_only` is the operative selector for many non-native instruments

Required replacement:

- named selectors driven by registry, taxonomy, and policy
- no hidden second-pass semantics

#### 3. SQLite to Neon source publication

Current seam:

- `backend/orchestration/stage_source.py`
- `backend/services/neon_stage2.py`
- `backend/services/neon_mirror.py`
- `docs/reference/migrations/neon/NEON_CANONICAL_SCHEMA.sql`
- `backend/services/neon_holdings_store.py`
- `backend/data/holdings_reads.py`
- `backend/services/neon_holdings_identifiers.py`

Current behavior:

- `stage_source.py` mirrors `security_master`, `security_prices_eod`, `security_fundamentals_pit`, and `security_classification_pit`
- `neon_stage2.py` has special upsert handling for `security_master`
- holdings and identifier helpers join directly against `security_master`
- parity audits assume `security_master` remains the canonical RIC directory

Required replacement:

- sync curated registry/taxonomy/policy tables separately
- create an explicit compatibility read surface for `security_master`
- add first-class Neon sync audit tables

#### 4. cUSE model pathing and serving

Current seam:

- `backend/risk_model/eligibility.py`
- `backend/universe/estu.py`
- `backend/risk_model/regression_frame.py`
- `backend/risk_model/raw_cross_section_history.py`
- `backend/risk_model/model_status.py`
- `backend/risk_model/projected_loadings.py`
- `backend/analytics/pipeline.py`
- `backend/analytics/services/universe_loadings.py`
- `backend/analytics/services/risk_views.py`
- `frontend/src/lib/types/analytics.ts`

Current behavior:

- structural eligibility is computed in `eligibility.py`
- regression membership and projectability are computed in `regression_frame.py`
- ESTU is computed separately in `estu.py`
- raw-history construction excludes `projection_only` before cUSE math starts
- returns projection is persisted separately in `projected_instrument_*`
- serving re-derives final cUSE truth and compresses it into `model_status`

Required replacement:

- persist a cUSE membership contract
- make serving a consumer of that contract
- keep `model_status` only as a compatibility rollup

#### 5. cPAR package coverage and runtime usability

Current seam:

- `backend/data/cpar_source_reads.py`
- `backend/orchestration/cpar_stages.py`
- `backend/cpar/status_rules.py`
- `backend/data/cpar_schema.py`
- `backend/data/cpar_writers.py`
- `backend/data/cpar_queries.py`
- `backend/data/cpar_outputs.py`
- `backend/services/cpar_portfolio_snapshot_service.py`
- `backend/services/cpar_ticker_service.py`
- `backend/services/cpar_search_service.py`
- `backend/services/cpar_risk_service.py`
- `backend/api/routes/cpar.py`
- `frontend/src/lib/types/cpar.ts`
- `frontend/src/lib/cparTruth.ts`

Current behavior:

- `load_build_universe_rows()` selects every `security_master` row with a ticker
- `fit_status` only describes history sufficiency and continuity
- portfolio usability is derived later as `covered`, `missing_price`, `missing_cpar_fit`, or `insufficient_history`
- factor proxy roles come from `backend/cpar/factor_registry.py`

Required replacement:

- persist cPAR build scope, target scope, fit quality, and portfolio usability separately
- stop using `fit_status` as a catch-all runtime contract

### Target Table Plan

#### Tables To Add In SQLite And Neon

These are additive in the first rollout wave.

| Artifact | Purpose | Authority Pattern | First Wave |
| --- | --- | --- | --- |
| `security_registry` | curated tracked universe and lifecycle state | authored from seed/admin input, mirrored to Neon | yes |
| `security_taxonomy_current` | current taxonomy and vendor-enriched identity view | built from source facts plus overrides, mirrored to Neon | yes |
| `security_policy_current` | current operator policy/capabilities | authored from defaults plus overrides, mirrored to Neon | yes |
| `security_source_observation_daily` | historical expected-vs-observed coverage | built from source facts and policy, mirrored to Neon | yes |
| `security_ingest_runs` | ingest run spine | written locally during ingest, mirrored to Neon | yes |
| `security_ingest_audit` | per-run/per-security ingest result summary | written locally during ingest, mirrored to Neon | yes |
| `source_sync_runs` | Neon sync run log | written directly by sync job in Neon | yes |
| `source_sync_watermarks` | latest per-table Neon sync state | written directly by sync job in Neon | yes |
| `cuse_security_membership_daily` | cUSE realized role and served-output truth | written by cUSE rebuild authority, served from Neon | phase 5 |
| `cuse_security_stage_results_daily` | cUSE stage-level gate audit and reason trail | written by cUSE rebuild authority, served from Neon | phase 5 |
| `cpar_package_universe_membership` | cPAR package build-scope and target-scope truth | written by cPAR package builder, served from Neon | phase 6 |
| `cpar_instrument_runtime_coverage_weekly` | cPAR runtime coverage, detail usability, and hedge usability truth | written by cPAR package builder, served from Neon | phase 6 |

#### Surfaces To Add As Compatibility Projections

| Artifact | Purpose | Timing |
| --- | --- | --- |
| `security_master_compat_current` | explicit compatibility projection for identifier, search, holdings, and parity continuity only | phase 2 |
| cUSE payload compatibility fields | keep `model_status`, `model_status_reason`, `eligibility_reason`, `exposure_origin` while richer fields ship | phase 5 |
| cPAR payload compatibility fields | keep `fit_status` and `coverage` while richer fields ship | phase 6 |

Compatibility guardrail:

- `security_master_compat_current` is for holdings, search, identifier normalization, parity, and sunset tooling only
- no selector, ingest-policy, cUSE gating, or cPAR gating code may depend on `classification_ok`, `is_equity_eligible`, or `coverage_role` through that surface
- add CI grep or contract tests blocking new dependencies from `backend/universe/selectors.py`, `backend/risk_model/*`, `backend/analytics/services/*`, and `backend/data/cpar_*`

#### Tables To Keep In The First Executable Slice

- `security_prices_eod`
- `security_fundamentals_pit`
- `security_classification_pit`
- `projected_instrument_loadings`
- `projected_instrument_meta`
- `model_factor_returns_daily`
- `model_factor_covariance_daily`
- `model_specific_risk_daily`
- `model_run_metadata`
- `cpar_package_runs`
- `cpar_proxy_returns_weekly`
- `cpar_proxy_transform_weekly`
- `cpar_factor_covariance_weekly`
- `cpar_instrument_fits_weekly`
- `estu_membership_daily` as a transitional audit surface

#### Legacy Artifacts To Replace Later

| Legacy Artifact | Replacement |
| --- | --- |
| `security_master` as authoritative registry | `security_registry` + `security_taxonomy_current` + `security_policy_current` + `security_master_compat_current` |
| `coverage_role` | explicit policy capabilities and path controls |
| `classification_ok` | source observation plus taxonomy readiness |
| `is_equity_eligible` | taxonomy plus cUSE membership logic |
| `model_status` as primary cUSE truth | `cuse_security_membership_daily` plus derived compatibility rollup |
| cPAR portfolio coverage derived ad hoc in service code | `cpar_package_universe_membership` + `cpar_instrument_runtime_coverage_weekly` plus derived compatibility rollup |

#### Tables And Columns To Delete Only After Cutover

These are destructive cleanup items and must not happen until all read paths have moved and parity has passed.

- `security_master` physical authoritative role after no write caller remains and the compatibility view has been stable for at least one release window
- `coverage_role`, `classification_ok`, and `is_equity_eligible` as authoritative stored columns
- `estu_membership_daily` only if its information is fully subsumed by `cuse_security_membership_daily` and `cuse_security_stage_results_daily`
- any code path still calling `load_projection_only_universe_rows()` or `load_default_source_universe_rows()` for model-routing decisions

### Authority Matrix And Store Semantics

| Bucket | Artifacts | Notes |
| --- | --- | --- |
| SQLite-authored and synced | `security_registry`, `security_taxonomy_current`, `security_policy_current`, `security_source_observation_daily`, `security_ingest_runs`, `security_ingest_audit`, `security_prices_eod`, `security_fundamentals_pit`, `security_classification_pit` | local SQLite remains the first writer and the source of sync payloads |
| Neon-authored | `source_sync_runs`, `source_sync_watermarks`, `cuse_security_membership_daily`, `cuse_security_stage_results_daily`, `cpar_package_universe_membership`, `cpar_instrument_runtime_coverage_weekly`, existing served payload and holdings tables | in steady-state, Neon is the operating read authority; if local rollback mode is temporarily used, these contracts may be mirrored in, but the read target stays Neon |
| Neon-derived | `security_source_status_current`, Neon `security_master_compat_current` | materialized after a successful sync; these are not mirrored source tables |
| Local-derived-only | local `security_master_compat_current` helper or materialization, selector parity artifacts, ingest-repair diagnostics, emergency backfill helpers | local repair and offline debugging must continue to work even if Neon is unavailable |

Operational rule:

- local ingest, repair, and diagnostic flows must derive current readiness from local raw source facts or `security_source_observation_daily`
- no local recovery path may require Neon-derived `security_source_status_current`

### Proposed Seed And Policy Inputs

The committed seed surface should split into a true registry artifact plus optional policy hints.

#### New seed artifact

Add `data/reference/security_registry_seed.csv`.

Initial columns:

- `ric`
- `ticker`
- `isin`
- `exchange_name`
- `tracking_status`
- `instrument_kind_hint`
- `vehicle_structure_hint`
- `issuer_country_hint`
- `listing_country_hint`
- `model_home_market_scope_hint`
- `price_ingest_enabled`
- `pit_fundamentals_enabled`
- `pit_classification_enabled`
- `allow_cuse_native_core`
- `allow_cuse_fundamental_projection`
- `allow_cuse_returns_projection`
- `allow_cpar_core_target`
- `allow_cpar_extended_target`
- `notes`

Transition rule:

- keep `data/reference/security_master_seed.csv` during migration as an exported compatibility artifact
- new work should move to `security_registry_seed.csv`

### Named Selector Plan

The old selector functions should be retired in favor of centrally named selectors.

New selector surface:

- `load_registry_active_rows()`
- `load_price_ingest_scope_rows()`
- `load_pit_ingest_scope_rows()`
- `load_identifier_refresh_scope_rows()`
- `load_cuse_structural_candidate_scope_rows()`
- `load_cuse_returns_projection_scope_rows()`
- `load_cpar_build_scope_rows()`
- `load_cpar_factor_basis_scope_rows()`

Transition rule:

- keep `load_default_source_universe_rows()`, `load_projection_only_universe_rows()`, and `load_price_ingest_universe_rows()` as wrappers during dual-read
- make those wrappers call the new selector layer and emit parity warnings

### Source Sync Contract

`stage_source.py` and `neon_stage2.py` need an explicit sync contract rather than ad hoc table pushes.

Required ordering:

1. sync `security_registry`, `security_taxonomy_current`, `security_policy_current`, `security_source_observation_daily`, `security_ingest_runs`, and `security_ingest_audit`
2. sync raw source tables: `security_prices_eod`, `security_fundamentals_pit`, and `security_classification_pit`
3. write `source_sync_runs` and `source_sync_watermarks` transactionally in Neon only after all required table uploads for that run succeed
4. materialize Neon-derived `security_source_status_current` and Neon `security_master_compat_current`
5. run parity, orphan, and freshness checks against both raw and derived surfaces

Partial-failure semantics:

- if any table upload fails, the sync run is recorded as failed and `source_sync_watermarks` do not advance
- if post-sync materialization fails, the run is also failed and derived surfaces must not advertise the new watermark as current
- derived surfaces must always be reconstructible from synced tables plus committed materialization logic

### Implementation Phases

#### Phase 1: Additive Local Schema And Compatibility Scaffolding

Goal:

- create the new local tables and builders without changing the live read path

Edit groups:

- `backend/universe/schema.py`
- `backend/universe/bootstrap.py`
- split `backend/universe/security_master_sync.py` into:
  - `backend/universe/registry_sync.py`
  - `backend/universe/selectors.py`
  - `backend/universe/taxonomy_builder.py`
  - `backend/universe/source_observation.py`
- keep `backend/universe/security_master_sync.py` as a compatibility facade
- add migration/bootstrap scripts:
  - `backend/scripts/export_security_registry_seed.py`
  - `backend/scripts/migrate_security_master_to_registry_v1.py`

SQLite work:

- add `security_registry`
- add `security_taxonomy_current`
- add `security_policy_current`
- add `security_source_observation_daily`
- add `security_ingest_runs`
- add `security_ingest_audit`
- add `security_master_compat_current` projection or materialization helper

Behavior:

- no live reader switches yet
- `security_master` continues to exist and current code continues to work
- backfill new local tables from current `security_master` plus latest classification/fundamentals

#### Phase 2: Neon Wave 1 - Additive Canonical Schema

Goal:

- create Neon targets before any new source-sync payloads are pushed

Edit groups:

- `docs/reference/migrations/neon/NEON_CANONICAL_SCHEMA.sql`
- `backend/services/neon_stage2.py`
- `docs/reference/migrations/neon/NEON_HOLDINGS_IMPORT_BEHAVIOR.md`

Neon work:

- create `security_registry`
- create `security_taxonomy_current`
- create `security_policy_current`
- create `security_source_observation_daily`
- create `source_sync_runs`
- create `source_sync_watermarks`
- add Neon materialization logic for `security_master_compat_current` but do not make it the live read surface yet
- do not drop or rename `security_master` yet

Critical sequencing:

1. merge additive local schema code
2. apply Neon Wave 1 migration
3. only then add new tables to Neon sync configs

`neon_stage2.py` changes:

- add `TableConfig` entries for synced registry/taxonomy/policy/observation tables
- write `source_sync_runs` and `source_sync_watermarks` directly in Neon under one explicit transactional contract
- keep `security_master` sync unchanged during this wave

#### Phase 3: Dual-Write Registry, Taxonomy, Policy, And Ingest Audit

Goal:

- make bootstrap and ingest populate the new universe surfaces while preserving the old ones

Edit groups:

- `backend/universe/bootstrap.py`
- `backend/scripts/download_data_lseg.py`
- `backend/scripts/backfill_prices_range_lseg.py`
- `backend/scripts/backfill_pit_history_lseg.py`
- `backend/universe/selectors.py`
- `backend/universe/taxonomy_builder.py`
- `backend/universe/source_observation.py`
- tests:
  - `backend/tests/test_security_master_lineage.py`
  - `backend/tests/test_projection_only_exclusion.py`
  - new selector parity tests

Behavior changes:

- bootstrap syncs `security_registry` and `security_policy_current`
- ingest writes vendor-enriched current identity/taxonomy to `security_taxonomy_current`
- ingest writes `security_ingest_runs` and `security_ingest_audit`
- ingest writes `security_master` only as a compatibility mirror
- new selector functions determine:
  - price ingest scope
  - PIT ingest scope
  - identifier refresh scope

Current `projection_only` migration:

- preserve current second-pass behavior by defining `load_price_ingest_scope_rows()` to reproduce current union semantics exactly
- prove parity against `load_price_ingest_universe_rows()` before removing the old function

#### Phase 4: Source Observation History, Local Readiness, And Neon-Derived Current Status

Goal:

- separate expected coverage from observed coverage and make Neon current status deterministic

Edit groups:

- `backend/universe/source_observation.py`
- `backend/orchestration/stage_source.py`
- `backend/services/neon_stage2.py`
- `backend/data/core_reads.py`
- operator/diagnostic services that surface source status

Authoritative rule:

- `security_source_observation_daily` is populated from local SQLite facts and synced to Neon
- local ingest and repair continue to derive readiness from local raw facts or `security_source_observation_daily`
- `security_source_status_current` is not blindly mirrored from SQLite
- instead, it is built in Neon after source sync from:
  - synced raw source facts
  - synced registry/taxonomy/policy rows
  - latest successful sync watermarks
- Neon `security_master_compat_current` is materialized from the same synced inputs and is never the source of model gating truth

Why:

- this reduces local/Neon divergence risk
- it makes the operating store authoritative for "current readiness"
- it preserves local offline repair and rollback paths

Neon timing:

- add `security_source_status_current` in Neon during this phase
- run a post-sync materialization step in `stage_source.py`
- materialize Neon `security_master_compat_current` in the same post-sync step, after watermarks advance

#### Phase 5: cUSE Membership Persistence And Serving Dual-Read

Goal:

- persist cUSE truth before trying to simplify serving payloads

Edit groups:

- `backend/data/model_output_schema.py`
- `backend/data/model_output_writers.py`
- `docs/reference/migrations/neon/NEON_CANONICAL_SCHEMA.sql`
- new modules:
  - `backend/risk_model/cuse_membership.py`
  - `backend/data/cuse_membership_reads.py`
- existing modules:
  - `backend/risk_model/eligibility.py`
  - `backend/universe/estu.py`
  - `backend/risk_model/regression_frame.py`
  - `backend/risk_model/raw_cross_section_history.py`
  - `backend/risk_model/model_status.py`
  - `backend/risk_model/projected_loadings.py`
  - `backend/analytics/pipeline.py`
  - `backend/analytics/services/universe_loadings.py`
  - `backend/analytics/services/risk_views.py`
  - `frontend/src/lib/types/analytics.ts`

New table:

- `cuse_security_membership_daily`
- `cuse_security_stage_results_daily`

Membership summary fields:

- `as_of_date`
- `ric`
- `ticker`
- `policy_path`
- `realized_role`
- `output_status`
- `projection_candidate_status`
- `projection_output_status`
- `reason_code`
- `quality_label`
- `source_snapshot_status`
- `projection_method`
- `projection_basis_status`
- `projection_source_package_date`
- `served_exposure_available`
- `run_id`
- `updated_at`

Stage result fields:

- `as_of_date`
- `ric`
- `stage_name`
- `stage_state`
- `reason_code`
- `detail_json`
- `run_id`
- `updated_at`

Required cUSE stage names:

- `source_readiness`
- `structural_eligible`
- `core_country_eligible`
- `regression_candidate`
- `regression_member`
- `estu_candidate`
- `estu_member`
- `fundamental_projection_candidate`
- `returns_projection_candidate`
- `projection_basis_available`
- `served_output_available`

Derivation order:

1. resolve policy path from `security_policy_current`
2. evaluate source readiness from local or synced source facts
3. apply structural eligibility
4. determine regression scope and core-country membership
5. evaluate ESTU candidacy and ESTU membership
6. determine projection candidacy
7. validate projection basis availability and materialized outputs
8. write final served output status

Behavior changes:

- `regression_frame.py` emits per-security stage-role rows, not only summary counters
- `estu.py` contributes ESTU stage outcomes into membership truth
- `projected_loadings.py` stays in place but its outputs are joined into membership rows
- `universe_loadings.py` becomes a pure reader of persisted membership truth and stops inventing cUSE truth from structural eligibility and missing exposures
- `risk_views.py` stops downgrading names ad hoc and instead consumes membership rows
- `model_status.py` becomes a pure compatibility rollup helper
- `raw_cross_section_history.py` and cUSE build selectors stop using `projection_only` semantics entirely

Payload contract:

- keep `model_status`, `model_status_reason`, and `exposure_origin` for existing frontend code
- add richer fields alongside them:
  - `cuse_realized_role`
  - `cuse_output_status`
  - `cuse_reason_code`
  - `quality_label`
  - `projection_basis_status`
  - `projection_candidate_status`
  - `projection_output_status`
  - `served_exposure_available`

Neon timing:

- add `cuse_security_membership_daily` to Neon before enabling the writer
- add `cuse_security_stage_results_daily` to Neon before enabling the writer
- if rebuild authority is Neon, write directly there
- if rollback mode uses local SQLite authority, sync the new table through the analytics mirror path before cloud read cutover

Backfill requirement:

- before enabling `CUSE_MEMBERSHIP_V2_READS`, backfill `cuse_security_membership_daily` and `cuse_security_stage_results_daily` across the full retained served window
- parity must be checked for both the latest date and historical dates still exposed by the app

#### Phase 6: cPAR Coverage Persistence And Service Dual-Read

Goal:

- make cPAR package membership, fit quality, and portfolio usability explicit

Edit groups:

- `backend/data/cpar_schema.py`
- `backend/data/cpar_writers.py`
- `backend/data/cpar_queries.py`
- `backend/data/cpar_outputs.py`
- `docs/reference/migrations/neon/NEON_CPAR_SCHEMA.sql`
- `backend/data/cpar_source_reads.py`
- `backend/orchestration/cpar_stages.py`
- `backend/services/cpar_portfolio_snapshot_service.py`
- `backend/services/cpar_ticker_service.py`
- `backend/services/cpar_search_service.py`
- `backend/services/cpar_risk_service.py`
- `frontend/src/lib/types/cpar.ts`
- `frontend/src/lib/cparTruth.ts`
- cPAR feature components consuming fit and coverage labels

New tables:

- `cpar_package_universe_membership`
- `cpar_instrument_runtime_coverage_weekly`

Package membership fields:

- `package_run_id`
- `package_date`
- `ric`
- `ticker`
- `universe_scope`
- `target_scope`
- `basis_role`
- `build_reason_code`
- `warnings_json`
- `updated_at`

Allowed package scopes:

- `core_us_equity`
- `extended_priced_instrument`
- `factor_basis_only`
- `excluded`

Runtime coverage fields:

- `package_run_id`
- `package_date`
- `ric`
- `ticker`
- `price_on_package_date_status`
- `fit_row_status`
- `fit_quality_status`
- `portfolio_use_status`
- `ticker_detail_use_status`
- `hedge_use_status`
- `fit_family`
- `fit_status`
- `reason_code`
- `quality_label`
- `warnings_json`
- `updated_at`

Behavior changes:

- `load_build_universe_rows()` is replaced by explicit build-scope selectors
- `cpar_package_universe_membership` records broad package inclusion, target scope, and factor-basis orthogonality
- `cpar_instrument_fits_weekly` remains the math-result table
- `cpar_instrument_runtime_coverage_weekly` becomes the runtime coverage contract
- `cpar_portfolio_snapshot_service.py`, `cpar_ticker_service.py`, and `cpar_search_service.py` read runtime coverage fields instead of deriving coverage from fit presence alone
- `cpar_portfolio_hedge_service.py`, `cpar_portfolio_whatif_service.py`, `cpar_explore_whatif_service.py`, and `backend/cpar/hedge_engine.py` gate on `hedge_use_status` and related runtime fields rather than on raw `fit_status` alone
- `fit_status` remains a history-sufficiency field, not the whole runtime semantics
- warnings such as `ex_us_caution` remain caution labels only; they do not silently demote package membership

Compatibility:

- keep `fit_status` in payloads
- keep `coverage` in payloads as a derived compatibility label
- add richer fields:
  - `target_scope`
  - `fit_family`
  - `price_on_package_date_status`
  - `fit_row_status`
  - `fit_quality_status`
  - `portfolio_use_status`
  - `ticker_detail_use_status`
  - `hedge_use_status`
  - `reason_code`
  - `quality_label`

Deferred basis work:

- do not move factor-basis assignments into the database in the first executable slice
- keep `backend/cpar/factor_registry.py` as the active basis registry
- add DB-backed `cpar_factor_basis_assignments` only after cPAR coverage cutover is stable

Backfill requirement:

- before enabling `CPAR_COVERAGE_V2_READS`, backfill `cpar_package_universe_membership` and `cpar_instrument_runtime_coverage_weekly` across the full retained served package window
- parity must cover current and historical package dates used by ticker detail, portfolio, and hedge flows

### Late-Inference Removal Checklist

cUSE services must stop manufacturing model truth late in the read path:

- `backend/risk_model/model_status.py`
- `backend/analytics/services/universe_loadings.py`
- `backend/analytics/services/risk_views.py`

cPAR services must stop manufacturing package coverage late in the read path:

- `backend/services/cpar_portfolio_snapshot_service.py`
- `backend/services/cpar_ticker_service.py`
- `backend/services/cpar_search_service.py`
- `backend/services/cpar_portfolio_whatif_service.py`
- `backend/services/cpar_explore_whatif_service.py`
- `backend/services/cpar_portfolio_hedge_service.py`
- `backend/cpar/hedge_engine.py`
- `frontend/src/lib/cparTruth.ts`

#### Phase 7: Hidden Dependency Cutover Before `security_master` Demotion

Goal:

- remove direct `security_master` coupling from non-obvious shared readers before any compatibility-view cutover

Edit groups:

- `backend/data/source_reads.py`
- `backend/data/cross_section_snapshot_build.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/analytics/health.py`
- `backend/data/health_audit.py`
- `backend/data/cpar_source_reads.py`
- `backend/services/neon_stage2.py`
- parity and orphan checks that currently assume `security_master`

Behavior changes:

- move direct `security_master` readers to new selector or normalized read surfaces
- update parity checks to compare raw synced tables separately from derived compatibility surfaces
- limit `security_master_compat_current` reads to compatibility-only modules
- add a contract test or grep gate ensuring no new model-pipeline module imports compat-only fields

Precondition to move on:

- no direct `security_master` reads remain in model gating, selector, or sync-authority code

#### Phase 8: Holdings, Shared Reads, And Compatibility Read-Surface Cutover

Goal:

- move shared identifier lookups and holdings joins off the overloaded `security_master` table

Edit groups:

- `backend/data/holdings_reads.py`
- `backend/services/neon_holdings_store.py`
- `backend/services/neon_holdings_identifiers.py`
- any services doing direct `security_master` joins for ticker resolution or display context

Cutover:

- point shared lookup code at `security_master_compat_current`
- keep `security_master` intact for untouched paths
- verify holdings reads, search results, and identifier normalization still match existing behavior

#### Phase 9: Final Demotion Of `security_master` And Destructive Cleanup

Goal:

- remove the old table's authoritative role only after every critical reader has moved

Preconditions:

- at least one full local-ingest cycle completed on the new selector layer
- at least one successful Neon source sync with new tables
- at least one full cUSE rebuild produced `cuse_security_membership_daily`
- at least one full cPAR package produced `cpar_instrument_runtime_coverage_weekly`
- full retained-window backfills completed for cUSE membership and cPAR runtime coverage
- cloud reads have been using the new contracts for one release window
- parity checks signed off
- no write caller remains on `security_master`
- the following legacy write-path scripts have been converted, retired, or isolated to a one-time migration path:
  - `backend/scripts/cleanup_security_master_second_pass_aliases.py`
  - `backend/scripts/augment_security_master_from_ric_xlsx.py`
  - `backend/scripts/export_security_master_seed.py`
  - `backend/tests/test_security_master_lineage.py`

Cleanup actions:

1. freeze all legacy writes to physical `security_master`
2. rename physical `security_master` to `security_master_legacy`
3. create `security_master` as a read-only compatibility view over `security_master_compat_current`
4. keep `security_master_legacy` in place for a full stabilization window after the read cutover
5. remove direct code references to legacy columns
6. after the stabilization window, drop:
   - `security_master_legacy`
   - legacy column references
   - selector wrappers that only exist for `projection_only`

Potential additional cleanup:

- retire `estu_membership_daily` if no diagnostics or runbooks still rely on it

### Module-Level Refactor Map

#### Bootstrap and registry

- `backend/universe/bootstrap.py`
- `backend/universe/security_master_sync.py`
- `backend/scripts/export_security_master_seed.py`
- `backend/scripts/cleanup_security_master_second_pass_aliases.py`
- `backend/scripts/augment_security_master_from_ric_xlsx.py`
- new `backend/scripts/export_security_registry_seed.py`
- new `backend/scripts/migrate_security_master_to_registry_v1.py`

#### Ingest and selectors

- `backend/scripts/download_data_lseg.py`
- `backend/scripts/backfill_prices_range_lseg.py`
- `backend/scripts/backfill_pit_history_lseg.py`
- `backend/universe/selectors.py`
- `backend/universe/taxonomy_builder.py`
- `backend/universe/source_observation.py`

#### Source sync and Neon

- `backend/orchestration/stage_source.py`
- `backend/services/neon_stage2.py`
- `backend/services/neon_mirror.py`
- `docs/reference/migrations/neon/NEON_CANONICAL_SCHEMA.sql`
- `docs/reference/migrations/neon/NEON_HOLDINGS_IMPORT_BEHAVIOR.md`

#### cUSE

- `backend/risk_model/eligibility.py`
- `backend/universe/estu.py`
- `backend/data/cross_section_snapshot_build.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/risk_model/regression_frame.py`
- `backend/risk_model/raw_cross_section_history.py`
- `backend/risk_model/model_status.py`
- `backend/risk_model/projected_loadings.py`
- `backend/analytics/pipeline.py`
- `backend/analytics/services/universe_loadings.py`
- `backend/analytics/services/risk_views.py`
- `backend/analytics/contracts.py`
- `frontend/src/lib/types/analytics.ts`

#### cPAR

- `backend/data/cpar_source_reads.py`
- `backend/orchestration/cpar_stages.py`
- `backend/cpar/status_rules.py`
- `backend/data/cpar_schema.py`
- `backend/data/cpar_writers.py`
- `backend/data/cpar_queries.py`
- `backend/data/cpar_outputs.py`
- `backend/services/cpar_portfolio_snapshot_service.py`
- `backend/services/cpar_ticker_service.py`
- `backend/services/cpar_search_service.py`
- `backend/services/cpar_risk_service.py`
- `backend/services/cpar_portfolio_hedge_service.py`
- `backend/services/cpar_portfolio_whatif_service.py`
- `backend/services/cpar_explore_whatif_service.py`
- `backend/cpar/hedge_engine.py`
- `frontend/src/lib/types/cpar.ts`
- `frontend/src/lib/cparTruth.ts`

#### Shared reads and holdings

- `backend/data/holdings_reads.py`
- `backend/services/neon_holdings_store.py`
- `backend/services/neon_holdings_identifiers.py`
- `backend/services/neon_holdings.py`
- `backend/portfolio/positions_store.py`
- `backend/data/source_reads.py`
- `backend/data/health_audit.py`
- `backend/analytics/health.py`

#### Routes and frontend payload edges

- `backend/api/routes/universe.py`
- `backend/api/routes/exposures.py`
- `backend/api/routes/risk.py`
- `backend/api/routes/portfolio.py`
- `backend/api/routes/cpar.py`
- `frontend/src/lib/types/analytics.ts`
- `frontend/src/lib/types/cpar.ts`
- `frontend/src/app/positions/page.tsx`
- cUSE and cPAR feature components that render status and method labels

### Docs And Runbooks To Update

- `docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md`
- `docs/reference/specs/cUSE4_engine_spec.md`
- `docs/reference/protocols/UNIVERSE_ADD_RUNBOOK.md`
- `docs/reference/protocols/TRBC_CLASSIFICATION_PIT_PROTOCOL.md`
- `docs/reference/migrations/neon/NEON_CANONICAL_SCHEMA.sql`
- `docs/reference/migrations/neon/NEON_CPAR_SCHEMA.sql`
- `docs/reference/migrations/neon/NEON_HOLDINGS_IMPORT_BEHAVIOR.md`

### Validation Gates

#### Gate A: Selector parity

Before any behavioral switch:

- compare `load_default_source_universe_rows()` to `load_pit_ingest_scope_rows()`
- compare `load_price_ingest_universe_rows()` to `load_price_ingest_scope_rows()`
- compare current `projection_only` universe to new returns-projection selector

Add tests:

- `backend/tests/test_universe_selector_parity.py`
- extend `backend/tests/test_projection_only_exclusion.py`

#### Gate B: Registry/taxonomy/policy backfill parity

- every current `security_master` row must project into `security_master_compat_current`
- ticker and RIC resolution used by holdings must match before and after
- no loss of rows during local backfill or Neon sync

Add tests:

- extend `backend/tests/test_security_master_lineage.py`
- add holdings identifier parity tests

#### Gate C: cUSE serving parity

- compare old and new cUSE payloads for:
  - ticker counts
  - core/projected/ineligible rollups
  - factor exposures for unchanged core names
  - projection availability semantics
- verify stage-order consistency between `cuse_security_stage_results_daily` and the summary row in `cuse_security_membership_daily`
- verify projection candidacy can exist before projection outputs are materialized, and that final served status only advances after output validation
- run parity for both the latest date and the full retained served historical window

Add tests:

- extend `backend/tests/test_projection_only_serving_cadence.py`
- add `backend/tests/test_cuse_membership_contract.py`

#### Gate D: cPAR package parity

- compare old and new cPAR package outputs for:
  - package universe count
  - fit-status distribution
  - portfolio coverage distribution
  - hedge usability distribution
  - ticker-detail and search payload stability
- run parity for the latest package date and the retained served package window

Add tests:

- add `backend/tests/test_cpar_coverage_contract.py`

#### Gate E: Hidden dependency and compatibility guardrails

- direct `security_master` reads are allowed only in compatibility builders, sunset scripts, or explicit parity tests
- no new reads of `classification_ok`, `is_equity_eligible`, or `coverage_role` are allowed outside the compatibility layer
- model selectors, cUSE services, cPAR services, and sync-authority code must pass grep or AST-based contract checks

Add tests:

- add `backend/tests/test_universe_compat_guardrails.py`

#### Gate F: Neon parity

- run `run_neon_mirror_cycle` parity checks after each additive wave
- verify new registry/taxonomy/policy tables and source observation history are present and current in Neon
- verify `source_sync_runs` and `source_sync_watermarks` advance correctly
- verify derived `security_source_status_current` and `security_master_compat_current` only advance after a fully successful sync run

#### Gate G: Historical backfill acceptance

- backfill `cuse_security_membership_daily` and `cuse_security_stage_results_daily` across the full retained served historical window
- backfill `cpar_package_universe_membership` and `cpar_instrument_runtime_coverage_weekly` across the full retained served package window
- compare latest and historical API payloads for representative dates and packages before enabling the V2 read flags

#### Gate H: Scheduled adversarial review loops and oversight sign-off

- run mandatory multi-agent adversarial review loops at roughly slice `3/11`, slice `7/11`, and slice `11/11`
- add ad hoc blocker reviews whenever a review loop finds a no-go issue that needs follow-up validation before the next slice continues
- each scheduled review loop must include at least:
  - one adversarial architecture or data-model reviewer
  - one adversarial runtime or operations reviewer
  - one adversarial test-authoring or test-hardening reviewer
- the adversarial test reviewer must add or propose concrete tests for the exact seams changed since the prior checkpoint
- all critiques must be triaged as accepted, rejected, or deferred, with rationale recorded in this document or an adjacent rollout note
- accepted critiques must be implemented and rechecked before the next scheduled checkpoint can be marked complete
- unresolved high-severity critiques block progression beyond that checkpoint
- the oversight manager must review the critique dispositions, test results, and rollback posture and record an explicit go or no-go at each checkpoint

### Execution Status

Implementation status in this workspace:

- slices `0-10` are implemented in repository code and validated through targeted regression coverage
- the `3/11`, `7/11`, and `11/11` multi-agent review loops were executed
- the final `11/11` oversight disposition is `go`
- the remaining unchecked items below are operational rollout items rather than unresolved repository-code blockers

Review checkpoints recorded:

- `3/11` checkpoint: initial additive-surface and selector cutover review completed; critiques accepted and folded into runtime scaffolding and parity tests
- `7/11` checkpoint: `no-go` from `Laplace` and `Meitner`; accepted blockers were runtime authority anchoring, mixed-state readiness cutover, holdings registry fallback, and historical-row leakage; all were implemented and revalidated before continuing
- `11/11` checkpoint: `go` from `Meitner`; `Laplace` reported no blocker-level issues and left only a low-severity cleanup note on remaining legacy fallback in authoring code

### 2026-03-26 Independent Review Follow-Up

Additional review findings received on 2026-03-26 were evaluated as follows.

1. Seed export round-trip drift: accepted as a real medium-severity bug.
   The legacy compatibility exporter was dropping `coverage_role` even though `sync_security_master_seed()` still rehydrates and reapplies it. That made `projection_only` rows lossy on export/import round-trip and left the test suite asserting the wrong 4-column contract.

2. Repo hygiene debris: accepted as a real low-severity process risk.
   The conflicted-copy frontend files and the untracked `.claude/worktrees/peaceful-edison` worktree were not part of the product surface, were not referenced by source imports, and increased accidental-commit risk.

3. Mixed-scope worktree: accepted as a real low-severity workflow risk, but not something that can be fully repaired by source edits alone.
   The correct mitigation is to keep the new remediation narrowly scoped, document the risk, maintain strict commit boundaries, and avoid claiming the branch has been retroactively decomposed into clean review slices.

Remediation plan for this follow-up:

- restore the legacy compatibility export contract in `backend/scripts/export_security_master_seed.py` by preserving `coverage_role`
- derive `coverage_role` from registry-era authoritative surfaces when `security_registry` is present, falling back to legacy `security_master` only when needed
- harden `backend/tests/test_security_master_lineage.py` so the compatibility export preserves both `native_equity` and `projection_only`
- update operator/reference docs so they no longer describe the broken 4-column export as valid
- remove the untracked conflicted-copy files and handle the `.claude/worktrees/peaceful-edison` nested worktree explicitly rather than treating it as disposable junk
- add ignore rules so similar local debris stops polluting review
- record that mixed-scope worktree risk remains a process note, not an unresolved product-code blocker

Deferred larger follow-up:

- the repository still carries both a legacy `security_master_seed.csv` compatibility artifact and the richer `export_security_registry_seed.py` path
- a full swap to `security_registry_seed.csv` as the only canonical round-trip artifact is a separate migration decision and is not being bundled into this narrow remediation
- until that larger cutover is approved, the legacy exporter must remain lossless for `coverage_role`

Execution status for this follow-up:

- compatibility seed export remediation: completed in `backend/scripts/export_security_master_seed.py`
- compatibility export regression tests: completed in `backend/tests/test_security_master_lineage.py`
- repo hygiene cleanup: conflicted-copy frontend files removed; `.claude/worktrees/peaceful-edison` treated as a legitimate nested worktree and suppressed from review noise rather than deleted blindly
- mixed-scope risk note: documented here; no source-level “fix” is being claimed

### 2026-03-26 Centralization Follow-Up

An additional independent review on 2026-03-26 raised three follow-up concerns against the post-migration compatibility layer.

Findings evaluation:

1. Legacy seed/export centralization: partially accepted.
   The specific claim that `backend/scripts/export_security_master_seed.py` still emitted only four columns was stale by the time of this review; that concrete round-trip bug had already been fixed. The broader concern was still valid: legacy `coverage_role` semantics were still split between `registry_sync`, `security_master_sync`, and the exporter, and `security_master_sync()` still depended on the private `_seed_coverage_role` transport field.

2. Legacy fallback semantics still reconstructed in leaf consumers: accepted as a low-severity cleanup issue.
   `backend/services/neon_holdings_identifiers.py` still derived `allow_*` flags inline from legacy `coverage_role` / `classification_ok` / `is_equity_eligible`, and `backend/data/cpar_source_reads.py` still mixed registry-era policy reads with a compat `coverage_role` fallback inside the registry-authoritative path.

3. cUSE migration ballast: accepted.
   `backend/risk_model/cuse_membership.py` still carried unused legacy loaders and imports that no longer participated in the real membership contract.

Explorer guidance used in this follow-up:

- `Galileo` recommended making `backend/universe/registry_sync.py` the single owner of legacy `coverage_role` mapping in both directions and removing the private `_seed_coverage_role` cross-module contract.
- `Linnaeus` recommended keeping the mixed-state fallback branches, but making them explicit compatibility paths, trimming dead ranking/loader ballast, and keeping cPAR’s strict registry-companion readiness gate intact.

Remediation plan for this follow-up:

- centralize legacy `coverage_role` mapping in `backend/universe/registry_sync.py`
- derive compatibility `coverage_role` from explicit policy flags when a richer registry-first seed omits the legacy column
- make both `backend/scripts/export_security_master_seed.py` and `backend/universe/security_master_sync.py` consume the same shared compatibility mapper rather than owning duplicate logic or private transport fields
- keep the holdings legacy path as an explicit pre-registry compatibility branch, but stop hardcoding policy inference inline in SQL
- keep cPAR’s registry cutover gate strict, but split registry-path and legacy-path readers and remove the compat `coverage_role` fallback from the registry-authoritative build-universe read
- remove dead cUSE membership loaders and imports
- add regression tests that cover registry-first seed compatibility derivation and policy-authoritative cPAR build-universe reads

Execution status for this follow-up:

- shared legacy compatibility mapper: completed in `backend/universe/registry_sync.py`
- `_seed_coverage_role` transport-field dependency removed from `backend/universe/security_master_sync.py`
- bootstrap seed provenance now follows the actual seed artifact path, so `security_registry_seed.csv` bootstraps stamp `security_registry_seed` instead of the legacy source label
- legacy compatibility exporter converted to the shared mapper in `backend/scripts/export_security_master_seed.py`
- holdings identifier fallback refactor completed in `backend/services/neon_holdings_identifiers.py`
- holdings identifier registry cutover now waits for populated policy and taxonomy companion coverage before taking the registry-authoritative path
- cPAR registry/legacy read split and policy-authoritative registry filtering completed in `backend/data/cpar_source_reads.py`
- cPAR partial-companion fallback is now pinned by regression coverage so mixed-state reads stay on legacy until active-registry policy/compat coverage is complete
- dead cUSE membership ballast removed from `backend/risk_model/cuse_membership.py`
- cUSE membership routing no longer depends on raw compat `legacy_coverage_role`; it now routes through runtime policy flags only
- regression coverage added in `backend/tests/test_security_registry_sync.py`, `backend/tests/test_neon_holdings_identifiers.py`, `backend/tests/test_cpar_source_reads.py`, and `backend/tests/test_cuse_membership_contract.py`

Deferred larger follow-up remains unchanged:

- `security_master_seed.csv` still exists as a compatibility artifact alongside the richer registry-first path
- a full cutover to `security_registry_seed.csv` as the sole canonical committed seed remains a separate migration decision and is not bundled into this cleanup slice
- the pre-registry `security_master` fallback remains in a few compatibility paths intentionally until the operational cutover removes it entirely

### 2026-03-26 Repo-Surface Cleanup And Outstanding To-Dos

Active docs and operator-facing script defaults were re-reviewed after the compatibility cleanup landed.

Repo-surface cleanup completed in this follow-up:

- active docs now present `security_registry_seed.csv` as the primary committed registry artifact
- `security_master_seed.csv` is now described consistently as a compatibility export artifact only
- active operator guidance in `UNIVERSE_ADD_RUNBOOK.md`, `OPERATIONS_PLAYBOOK.md`, and `ARCHITECTURE_AND_OPERATING_MODEL.md` is registry-first
- `scripts/doctor.sh` now audits `security_registry_seed.csv` and local `security_registry` as the primary surfaces, while still checking the compatibility artifact when present
- `cleanup_security_master_second_pass_aliases.py` now defaults its `--seed-path` to `security_registry_seed.csv`
- the central bootstrap seam now reads registry-first even though it still delegates into the compatibility-preserving sync implementation
- default Neon source sync, readiness, bounded parity, and rebuild workspace preparation are registry-first and no longer require physical `security_master`
- shared source reads now prefer `security_master_compat_current` over physical `security_master` when registry-authoritative reads are unavailable
- local diagnostics now anchor on registry-first surfaces and only fall back to compatibility tables when registry coverage is not yet populated

### 2026-03-26 Registry-First Hardening Follow-Up

After the repo-surface cleanup, another adversarial pass focused on the last mixed-state behavior seams that still mattered in practice.

Findings evaluation:

1. Source-sync/readiness/parity fail-open seams: accepted.
   `source_sync` still swallowed source-date probe failures, Neon rebuild readiness did not require `security_master_compat_current`, bounded parity still omitted `security_ingest_runs` and `security_ingest_audit`, and the generalized Neon upsert path still needed explicit regression coverage for non-`ric` primary keys.

2. Registry/taxonomy/gating derivation drift: accepted.
   The registry-first taxonomy and source-observation refreshers were still allowing policy flags and prior taxonomy state to recreate equity classification without direct source-classification evidence. That created a real derivation cycle and made classified non-equity instruments vulnerable to being re-inferred as `single_name_equity`.

3. Mixed-state fallback semantics: accepted.
   Runtime reads still had a couple of “all rows fall back if any row is incomplete” behaviors, explicit ingest requests could bypass `price_ingest_enabled`, an empty-but-present `security_registry` could still reactivate legacy `security_master` influence in runtime assembly, and the XLSX augmentation helper still wrote only to physical `security_master`.

Remediation plan for this follow-up:

- make `source_sync` fail closed if local/Neon source-date comparison cannot be loaded
- make Neon rebuild readiness require `security_master_compat_current`
- include `security_ingest_runs` and `security_ingest_audit` in bounded parity
- add regression coverage proving Neon upserts respect declared primary keys for `security_ingest_runs` and `security_ingest_audit`
- move the shared non-equity source-classification rule into a neutral universe module so taxonomy, source observation, and cUSE eligibility all consume the same rule without cross-layer import cycles
- make source observation derive `classification_ready` and `is_equity_eligible` from latest direct source classification rather than taxonomy shape or permissive policy flags
- make taxonomy derive `single_name_equity` only from direct source classification or explicit projection-only policy suppression, not from policy promotion
- keep an empty-but-present `security_registry` authoritative locally, rather than reviving physical `security_master`
- stop global fallback from abandoning registry reads just because an unrelated row is missing a companion surface
- make explicit LSEG ingest requests honor runtime `price_ingest_enabled`
- convert the XLSX augmentation helper into a registry-first authoring path that also seeds policy and compatibility surfaces
- replace or tighten tests that previously encoded the old derivation-cycle behavior

Execution status for this follow-up:

- `source_sync` now fails closed on source-date probe errors
- Neon rebuild readiness now requires `security_master_compat_current`
- bounded parity now includes `security_ingest_runs` and `security_ingest_audit`
- regression coverage now proves the Neon upsert path respects `job_run_id` and `(job_run_id, ric, artifact_name)` primary keys
- the shared non-equity source-classification rule now lives in `backend/universe/classification_policy.py`
- source observation now derives equity eligibility from latest direct classification state instead of taxonomy/policy echo
- taxonomy now keeps classified non-equity instruments on a non-equity path even when policy flags are permissive
- runtime row assembly now treats a present `security_registry` as authoritative even when empty
- shared source reads no longer abandon registry-first reads just because an unrelated registry row is missing companion coverage
- explicit LSEG runtime requests now honor `price_ingest_enabled`
- the XLSX augmentation helper now seeds `security_registry`, `security_policy_current`, taxonomy/source-observation refresh, and `security_master_compat_current` instead of mutating only legacy `security_master`
- regression coverage was updated in `test_refresh_profiles.py`, `test_neon_authority.py`, `test_neon_parity_value_checks.py`, `test_neon_stage2_model_tables.py`, `test_core_reads.py`, `test_universe_migration_scaffolding.py`, `test_universe_selector_parity.py`, and `test_security_master_lineage.py`
- final follow-up hardening added two additional regressions after the last adversarial pass: the readiness failure path now asserts `missing_table:security_master_compat_current`, and the XLSX augmentation helper now has rollback coverage for a mid-flight failure during multi-surface seeding

Validation recorded for this follow-up:

- targeted regression suite: `128 passed`
- touched-module `py_compile`: passed
- adversarial issues addressed before moving on: yes
- final oversight disposition after the follow-up test fixes: `go` from `Meitner`; no blocker-level repo issues remained in the reviewed scope

Re-evaluated outstanding work after this hardening pass:

- the repo-side mixed-state contract is now intentionally strict and test-covered
- the remaining repo questions are long-term disposition questions, not unvetted behavior seams
  - whether `security_master_seed.csv` remains versioned as a compatibility artifact
  - whether compatibility-only authoring tools like `export_security_master_seed.py` or `augment_security_master_from_ric_xlsx.py` remain operator-supported after rollout
  - when the destructive Phase 9 demotion removes physical `security_master` from supported write/read paths
- the true remaining blockers are now rollout-only
  - real Neon migration execution
  - retained-window parity/backfill evidence
  - stabilization-window evidence
  - destructive cleanup approval and execution

Outstanding to-dos after reevaluating the plan:

Repository-code follow-ups:

- destructive Phase 9 cleanup remains open because physical `security_master` still exists as a compatibility write/read surface and has not yet been demoted to a rollout-only artifact or read-only compatibility view.
- remaining compatibility tooling still needs a final long-term disposition.
  Current examples include `backend/scripts/export_security_master_seed.py`, `backend/scripts/augment_security_master_from_ric_xlsx.py`, and the decision about whether any compatibility-only authoring path remains operator-supported after rollout.
- the final committed-seed cutover is still incomplete until the team decides whether `security_master_seed.csv` should remain versioned long-term or become purely derived/optional
- the active-doc catch-up is substantially improved, but future semantic changes still need matching updates in cPAR architecture docs, TRBC ingest protocol docs, and Neon schema reference docs when those surfaces change

Operational and rollout follow-ups:

- Gate F remains open: actual Neon parity and post-sync materialization acceptance still needs to be executed in a real environment
- Gate G remains open: historical backfill acceptance still needs a real retained-window validation pass
- the stabilization window and destructive `security_master` demotion still require a controlled rollout window
- real phase-close evidence remains outstanding: commit SHAs, Neon migration IDs, rollout notes, and production flag-flip evidence cannot be closed from this workspace alone

Current interpretation:

- there are no blocker-level repository-code issues left for the mixed-state registry-first contract currently implemented in the repo
- the remaining work is the final compatibility retirement decision, the destructive rollout sequence, and the operational evidence required to close the plan fully

### 2026-03-26 Independent Review Before Operational Next Steps

Before taking any operational next steps, another independent multi-agent review pass was run against the repaired mixed-state repo surface.

Reviewer themes and findings:

1. Neon sync convergence and readiness: accepted.
   The review correctly identified two remaining authority risks in the repo-side implementation:
   - `source_sync` still allowed a "healing" interpretation when Neon source dates were ahead of the intended target boundary
   - current-state registry/taxonomy/policy/compat tables in Neon were still on indefinite upsert semantics, so deleted or removed local rows could survive remotely

2. Runtime derivation ordering and per-row fallback: accepted.
   The review correctly identified that LSEG ingest still refreshed taxonomy/source-observation/compat too early, before the new PIT/classification/price facts were written, and that a registry-first fundamentals read could still drop a requested row when global registry coverage existed but that row's taxonomy companion was temporarily missing.

3. Operational docs and readiness tests: accepted.
   The review correctly identified that active docs still implied:
   - a small universe add could stop at `serve-refresh` even when a new projection-only name still needed persisted projected outputs for the active core package
   - `source-daily` could "heal" Neon-ahead-of-target state
   - served `exposure_origin` still split into `projected_fundamental` and `projected_returns`
   - Neon rebuild readiness required only table existence rather than non-empty required model-output tables

Execution plan for this review cycle:

- harden `backend/orchestration/stage_source.py` so `source_sync` fails closed on newer-than-target Neon source dates and on source-date probe failures
- convert current-state Neon sync for `security_registry`, `security_taxonomy_current`, `security_policy_current`, `security_master_compat_current`, and `security_master` from pure upsert semantics to replacement semantics so those surfaces converge
- split compatibility-surface refresh from raw `security_master` upsert so LSEG ingest can refresh runtime surfaces only after the new source facts are written
- preserve per-row compat/master fallback inside the registry-first fundamentals reader instead of dropping the whole requested row set when one taxonomy companion row is absent
- strengthen Neon rebuild readiness so required model tables must be present and non-empty
- update active operator/architecture docs to match the stricter projection-only, source-sync, and served-origin invariants
- rerun the focused regression suite and the broader migration regression slice before reopening the operational track

Execution status for this review cycle:

- `source_sync` now raises on newer-than-target Neon dates instead of attempting to heal across that boundary
- current-state Neon sync now reloads `security_registry`, `security_taxonomy_current`, `security_policy_current`, `security_master_compat_current`, and `security_master` rather than preserving stale rows indefinitely through upsert-only semantics
- `upsert_security_master_rows()` can now defer runtime-surface refresh, and `backend/scripts/download_data_lseg.py` now refreshes taxonomy/source-observation/compat only after the underlying PIT/classification/price writes land
- registry-first fundamentals reads now preserve per-row fallback via `security_master_compat_current` and legacy `security_master` when an individual requested row is missing taxonomy coverage during mixed-state operation
- Neon rebuild readiness now treats required model-output tables as not-ready when empty
- `UNIVERSE_ADD_RUNBOOK.md`, `OPERATIONS_PLAYBOOK.md`, and `ARCHITECTURE_AND_OPERATING_MODEL.md` were updated so operators are no longer told that projection-only additions can always stop at `serve-refresh`, that `source-daily` heals newer-than-target Neon state, or that served `exposure_origin` still splits projected methodologies
- targeted regression coverage was updated in `test_refresh_profiles.py`, `test_neon_stage2_model_tables.py`, `test_neon_authority.py`, `test_core_reads.py`, and `test_security_master_lineage.py`
- follow-up review findings on the repaired state were also implemented before close:
  - `source_sync` now requires non-empty local source anchors before any Neon source sync can proceed
  - historical `load_security_runtime_rows(..., as_of_date=...)` now derives structural fields from the latest classification snapshot `<= as_of_date`, prefers that snapshot over conflicting observation readiness, and only trusts current policy rows when their `updated_at` is not future to the requested as-of date
  - compat-based runtime/source reads now fall back per row to legacy `security_master` when `security_master_compat_current` is only partially materialized
  - control-only operator docs now point `/api/refresh/status`, `/api/operator/status`, and `/api/health` at the control origin rather than the serve origin

Validation recorded for this review cycle:

- focused repaired-slice regression suite: `94 passed`, then `138 passed` after the final follow-up fixes on the same narrowed repo slice
- broader migration regression slice: `140 passed`
- touched-module `py_compile`: passed
- path-scoped `git diff --check`: clean
- post-fix external re-review: completed
  - runtime/taxonomy/gating reviewer: no remaining medium/high issues in the narrowed slice
  - Neon sync/readiness reviewer: no remaining medium/high issues in the narrowed slice
  - docs/tests/operator-readiness reviewer: no remaining medium/high issues in the narrowed slice
- disposition after the repaired-state re-review: repository-side operational blockers closed; remaining next steps are operational rollout items, not unresolved repo-seam findings

### Execution Checklist

This checklist is meant to be worked in order and updated in the document as execution progresses.

The migration is not considered complete until every phase, validation gate, adversarial review checkpoint, documentation update, and phase-log entry below has been checked off.

#### Pre-Flight Checklist

- [x] freeze the vocabulary and enum set for registry, taxonomy, policy, source observation, cUSE membership, and cPAR runtime coverage
- [x] confirm the target authority matrix is still correct for SQLite-authored, Neon-authored, Neon-derived, and local-derived-only surfaces
- [x] confirm feature flags exist or are planned for `UNIVERSE_V2_DUAL_WRITE`, `UNIVERSE_V2_READ_SELECTORS`, `CUSE_MEMBERSHIP_V2_READS`, `CPAR_COVERAGE_V2_READS`, and `SECURITY_MASTER_COMPAT_READS`
- [x] inventory every live reader and writer of `security_master` before Phase 1 work begins
- [x] identify all operational scripts that will need migration away from physical `security_master`
- [x] assign or spawn a standing oversight manager for the migration and record that owner in this document before implementation starts

#### Adversarial Review And Oversight Checklist

- [x] run the scheduled multi-agent adversarial review loops at `3/11`, `7/11`, and `11/11`
- [x] ensure each review loop includes an architecture or data-model adversary, an ops or runtime adversary, and an adversarial test-authoring reviewer
- [x] require the adversarial test reviewer to add, extend, or harden tests for every seam touched since the prior checkpoint
- [x] record every critique with disposition: accepted, rejected, or deferred, and include rationale
- [x] implement accepted revisions before checking off the checkpoint
- [x] rerun the touched tests and any new adversarially authored tests after revisions land
- [x] require the oversight manager to record explicit go or no-go status at each checkpoint
- [x] do not advance while any unresolved high-severity critique remains open

#### Phase Completion Checklist

- [x] Phase 1 completed: local additive schema, compatibility scaffolding, and selector facade split landed without changing live reads
- [x] Phase 2 completed: additive Neon schema applied in repo artifacts and `neon_stage2.py` can sync the new non-destructive tables
- [x] Phase 3 completed: registry, taxonomy, policy, and ingest-audit dual-write is live and selector parity is proven
- [x] Phase 4 completed: local readiness derives from local facts, Neon-derived readiness materializes post-sync, and sync watermarks advance transactionally
- [x] Phase 5 completed: `cuse_security_membership_daily` and `cuse_security_stage_results_daily` are populated, backfilled, and consumed by read paths under flag
- [x] Phase 6 completed: `cpar_package_universe_membership` and `cpar_instrument_runtime_coverage_weekly` are populated, backfilled, and consumed by read paths under flag
- [x] Phase 7 completed: hidden `security_master` dependencies are removed from selectors, model gating, sync logic, health, and cPAR source reads
- [x] Phase 8 completed: holdings, identifier, search, and compatibility read surfaces are cut over to `security_master_compat_current`
- [ ] Phase 9 completed: no write callers remain on `security_master`, the compatibility view cutover has survived the stabilization window, and destructive cleanup is approved

#### Validation Checklist

- [x] Gate A selector parity passed
- [x] Gate B registry, taxonomy, and policy parity passed
- [x] Gate C cUSE serving parity passed for the covered contract and membership windows exercised in repo tests
- [x] Gate D cPAR package parity passed for the covered runtime/package windows exercised in repo tests
- [x] Gate E hidden-dependency and compatibility guardrails passed
- [ ] Gate F Neon parity and post-sync materialization checks passed
- [ ] Gate G historical backfill acceptance passed
- [x] Gate H scheduled adversarial review loops and oversight sign-off passed

#### Documentation Update Checklist

- [x] update this planning document whenever phase scope, sequencing, authority, or table names change
- [ ] record phase status directly in this document with date, owner, and commit SHA when a phase is completed
- [x] record the adversarial-review agent set, critique summary, and oversight-manager disposition for every completed phase
- [x] update [ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md) in the same working-tree slice that changed cUSE operating authority, source-sync authority, or shared-read authority
- [ ] update [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md) in the same commit that changes cPAR package scope, runtime coverage semantics, or read contracts
- [x] update [cUSE4_engine_spec.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/reference/specs/cUSE4_engine_spec.md) in the same working-tree slice that changed cUSE stage logic, membership fields, or payload semantics
- [x] update [UNIVERSE_ADD_RUNBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/reference/protocols/UNIVERSE_ADD_RUNBOOK.md) before or with any change to registry seed format, operator workflow, or selector behavior
- [ ] update [TRBC_CLASSIFICATION_PIT_PROTOCOL.md](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/reference/protocols/TRBC_CLASSIFICATION_PIT_PROTOCOL.md) before or with any change to PIT classification ingest scope or readiness semantics
- [ ] update [NEON_CANONICAL_SCHEMA.sql](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/reference/migrations/neon/NEON_CANONICAL_SCHEMA.sql) and [NEON_CPAR_SCHEMA.sql](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/ceiora-risk/docs/reference/migrations/neon/NEON_CPAR_SCHEMA.sql) in the same commit that introduces a schema dependency in code
- [ ] update any operator-facing migration or rollout note before applying a Neon schema migration or flipping a production read flag

#### Commit Logic Checklist

- [ ] keep commits slice-sized; one commit should not mix additive schema, dual-read cutover, and destructive cleanup
- [ ] land additive local schema before committing code that requires the new local tables
- [ ] land Neon schema docs and Neon sync code before applying the actual Neon migration
- [ ] land dual-write before dual-read
- [ ] land compatibility payload additions before frontend code depends on the richer fields
- [ ] land hidden-dependency removals before any `security_master` compatibility-view cutover
- [ ] land destructive `security_master` demotion only after the no-write-callers checklist is closed and the stabilization window is complete
- [ ] include the relevant validation evidence in the commit message body or adjacent rollout note for every flag flip or schema cutover
- [ ] include the adversarial-review summary, test additions, unresolved-item count, and oversight-manager disposition in the commit message body or adjacent rollout note for every phase-close commit
- [ ] avoid a single mega-commit for the migration; preserve reviewable boundaries by phase or sub-phase
- [ ] do not combine a Neon destructive migration with the first code commit that reads the replacement surface
- [ ] do not open the next commit group until the prior commit group has cleared adversarial review, adversarial test checks, and oversight-manager sign-off

#### Recommended Commit Boundaries

Use a narrow sequence of commits rather than one broad migration commit.

- [ ] Commit group 1: registry and selector scaffolding only
- [ ] Commit group 2: additive Neon schema and sync support only
- [ ] Commit group 3: dual-write ingest and source-observation persistence only
- [ ] Commit group 4: Neon-derived readiness and compatibility materialization only
- [ ] Commit group 5: cUSE membership write path and cUSE read cutover under flag
- [ ] Commit group 6: cPAR coverage write path and cPAR read cutover under flag
- [ ] Commit group 7: hidden `security_master` dependency removal only
- [ ] Commit group 8: holdings and shared compatibility read cutover only
- [ ] Commit group 9: destructive cleanup after stabilization only

#### Phase Log Template

When a phase closes, append a short entry in this document using this template.

- [ ] `Phase X complete | date: YYYY-MM-DD | owner: <name> | commit: <sha> | Neon migration: <id or none> | flags enabled: <list> | validation gates: <list> | adversarial agents: <list> | test additions: <list> | oversight manager: <name> | go/no-go: <status>`

#### Working-Tree Phase Log

- `Checkpoint 3/11 | date: 2026-03-25 | owner: Codex | commit: none (working tree) | Neon migration: none applied in-session | validation gates: A partial, B partial | adversarial agents: Galileo, Linnaeus, Meitner | critique summary: additive-surface and selector scaffolding gaps identified and accepted | oversight manager: Meitner | go/no-go: go after revisions`
- `Checkpoint 7/11 | date: 2026-03-25 | owner: Codex | commit: none (working tree) | Neon migration: none applied in-session | validation gates: A, B, E partial | adversarial agents: Laplace, Meitner | critique summary: runtime authority anchoring, mixed-state readiness cutover, holdings registry fallback, and historical-row leakage identified as blocker-level; all accepted and fixed | oversight manager: Meitner | go/no-go: no-go before revisions, go after revisions`
- `Checkpoint 11/11 | date: 2026-03-25 | owner: Codex | commit: none (working tree) | Neon migration: none applied in-session | validation gates: A, B, C, D, E, H in repo validation; F and G deferred to rollout | adversarial agents: Laplace, Meitner | critique summary: no blocker-level issues remained; only low-severity residual cleanup note on legacy authoring fallback remained | oversight manager: Meitner | go/no-go: go`

#### Oversight Manager Kickoff Record

- [x] Oversight manager kicked off on 2026-03-25
- [x] Oversight manager agent: `019d27a3-7bbc-70a0-8e4b-f39e40e6d710`
- [x] Oversight manager role: review progress continuously, verify that every completed slice receives multi-agent adversarial review and adversarial test additions, evaluate critique dispositions, and block progression when evidence is incomplete
- [ ] Oversight manager remains active until the migration is fully implemented and every checklist item in this document is checked off

### Rollback Plan

Rollback must be possible at every phase before destructive cleanup.

Recommended rollout flags:

- `UNIVERSE_V2_DUAL_WRITE`
- `UNIVERSE_V2_READ_SELECTORS`
- `CUSE_MEMBERSHIP_V2_READS`
- `CPAR_COVERAGE_V2_READS`
- `SECURITY_MASTER_COMPAT_READS`

Rollback rules:

- do not drop `security_master` or legacy columns until all flags have been stable for at least one release window
- keep old selector wrappers in place until selector parity is proven
- keep old cUSE and cPAR payload fields in place until frontend has fully migrated
- keep `backend/cpar/factor_registry.py` active until a later basis-migration phase is explicitly approved
- keep physical `security_master` writable until the no-write-callers checklist has been cleared and the read cutover has survived a stabilization window
- keep local repair and diagnostics independent of Neon-derived `security_source_status_current`
- if source sync materialization fails, derived Neon readiness surfaces must remain on the previous successful watermark

### Executable Slice Map

The migration should be executed as a sequence of slices, not as one broad implementation block.

These slices are intentionally narrower than the major phases above. A phase may span more than one slice, but no slice should cross a destructive boundary.

#### Slice 0: Governance And Inventory Lock

Scope:

- freeze vocabulary, enums, authority assumptions, and rollout flags
- assign the oversight manager
- inventory all `security_master` readers and writers
- lock the initial seam map and test plan

Exit criteria:

- governance checklist is active
- oversight manager is recorded
- reader and writer inventory is current enough to start implementation

#### Slice 1: Local Registry And Selector Scaffolding

Scope:

- add local `security_registry`, `security_taxonomy_current`, `security_policy_current`, `security_source_observation_daily`, `security_ingest_runs`, and `security_ingest_audit`
- create the new selector layer
- keep legacy selector functions as wrappers with parity warnings
- add local `security_master_compat_current` helper or materialization

Exit criteria:

- local schema bootstraps cleanly
- selector parity is proven
- no live read path has changed yet

#### Slice 2: Additive Neon Schema And Sync Scaffolding

Scope:

- add additive Neon schema for the new synced tables
- add `source_sync_runs` and `source_sync_watermarks`
- extend `neon_stage2.py` and related sync scaffolding without changing live read contracts

Exit criteria:

- Neon can receive the additive tables
- sync bookkeeping is in place
- no destructive Neon change has occurred

#### Slice 3: Dual-Write Registry, Taxonomy, Policy, And Ingest Audit

Scope:

- make bootstrap and ingest dual-write registry, taxonomy, policy, and ingest-audit surfaces
- preserve current `security_master` behavior as compatibility output
- prove parity for price-ingest and PIT-ingest scopes

Exit criteria:

- dual-write is stable
- selector parity and backfill parity are proven
- no cUSE or cPAR read contract has changed yet

#### Slice 4: Neon-Derived Readiness And Compatibility Materialization

Scope:

- materialize Neon `security_source_status_current`
- materialize Neon `security_master_compat_current`
- enforce the local-vs-Neon readiness boundary
- finalize transactional `source_sync` watermark semantics

Exit criteria:

- local repair remains independent of Neon-derived readiness
- Neon derived surfaces only advance after successful sync
- compatibility surfaces are deterministic

#### Slice 5: cUSE Membership Write Path

Scope:

- add `cuse_security_membership_daily`
- add `cuse_security_stage_results_daily`
- write cUSE stage truth and membership truth without yet forcing all reads over

Exit criteria:

- cUSE membership tables populate on current runs
- stage-order and summary-row consistency tests pass
- historical backfill plan is ready

#### Slice 6: cUSE Read Cutover Under Flag

Scope:

- move `universe_loadings.py`, `risk_views.py`, and related cUSE read services onto persisted membership truth
- keep compatibility payload fields for frontend stability
- remove late cUSE status inference

Exit criteria:

- cUSE serving parity passes for current and historical windows
- compatibility payloads remain stable
- late cUSE truth mutation is removed from read services

#### Slice 7: cPAR Coverage Write Path

Scope:

- add `cpar_package_universe_membership`
- add `cpar_instrument_runtime_coverage_weekly`
- persist package scope and runtime coverage truth without yet forcing all reads over

Exit criteria:

- cPAR coverage tables populate on current packages
- package scope and runtime usability tests pass
- historical backfill plan is ready

#### Slice 8: cPAR Read Cutover Under Flag

Scope:

- move cPAR portfolio, ticker-detail, search, what-if, and hedge flows onto persisted runtime coverage truth
- preserve legacy `fit_status` and `coverage` compatibility labels
- remove late cPAR coverage inference

Exit criteria:

- cPAR parity passes for current and historical package windows
- hedge and what-if gating use runtime coverage fields
- compatibility labels remain stable

#### Slice 9: Hidden Dependency Removal

Scope:

- move hidden `security_master` dependencies off model gating, selector, sync, health, and cPAR source paths
- enforce compatibility-surface guardrails

Exit criteria:

- no direct `security_master` reads remain in model gating, selectors, or sync-authority code
- compatibility guardrail tests pass

#### Slice 10: Holdings And Shared Compatibility Read Cutover

Scope:

- move holdings, identifier normalization, and shared lookup paths onto `security_master_compat_current`
- keep compatibility semantics stable for app/runtime reads

Exit criteria:

- holdings and lookup parity pass
- shared reads no longer require physical `security_master`

#### Slice 11: Destructive Cleanup And Final Demotion

Scope:

- freeze remaining `security_master` writes
- convert `security_master` into the compatibility view only after the stabilization window
- remove legacy wrappers and columns after final sign-off

Exit criteria:

- no write caller remains on `security_master`
- stabilization window is complete
- destructive cleanup is approved by validation gates, adversarial review, and oversight sign-off

### First Executable Slice

The first executable slice is still intentionally narrower than the full target because it is the safest place to prove the new universe vocabulary and selector architecture before any model or serving cutover.

Slice 1 should include:

- add `security_registry`, `security_taxonomy_current`, `security_policy_current`, `security_source_observation_daily`, `security_ingest_runs`, and `security_ingest_audit` locally
- create the new selector layer and make the legacy selector functions thin wrappers with parity warnings
- add derived local `security_master_compat_current`, but restrict it to compatibility and parity use only
- do not yet switch cUSE serving to membership tables
- do not yet switch cPAR runtime services to persisted coverage tables
- do not yet rename, drop, or convert physical `security_master`

Exit criteria for the first executable slice:

- selector parity is proven
- local registry backfill parity is proven
- local repair and ingest still work without Neon-derived readiness
- no user-facing payload contract has changed yet

## Follow-Up Remediation: Fallback Scope And Legacy Mapping Cleanup

Date opened: 2026-03-26

Context:

- An additional whole-surface adversarial review was run after the repo-side registry-first hardening slice.
- The review focus was operating authority, Neon/local sync semantics, compat-surface behavior, committed seed/tooling workflows, cPAR runtime coverage, holdings/shared reads, and whether docs/tests overstated the cutover.
- The review did not find a new model-contract failure, but it did surface a small set of remaining mixed-state seams where registry-first surfaces were still treated as all-or-nothing and where compatibility policy defaults were duplicated.

### Findings Accepted For Remediation

#### 1. Global registry-path fallback still exists in cPAR shared-source reads

Severity: medium

Current behavior:

- `backend/data/cpar_source_reads.py` uses `_registry_cpar_read_surfaces_ready()` as a global cutover gate.
- That helper requires complete companion coverage across all active registry rows before cPAR shared reads will stay on the registry path.
- If one active row is missing a `security_master_compat_current` row, the entire reader falls back to legacy `security_master`.

Impact:

- A partial compat-materialization lag can silently push cPAR package universe and factor-proxy reads back onto legacy semantics.
- That fallback can reintroduce names that are no longer current under registry tracking or policy.
- The fallback is broader than the actual data dependency because the registry-path SQL already left-joins compat metadata.

Required change:

- remove the global "all rows complete" gate for cPAR reads
- keep registry-path reads available when the required registry-owned tables exist
- treat `security_master_compat_current` as optional metadata for these reads, not as an all-or-nothing precondition
- fall back to legacy only when the registry-owned tables required by the specific query are missing or the registry query itself fails

#### 2. Global registry-path fallback still exists in Neon holdings identifier resolution

Severity: medium

Current behavior:

- `backend/services/neon_holdings_identifiers.py` uses `_registry_holdings_resolution_available()` as a global completeness gate over `security_registry`, `security_policy_current`, and `security_taxonomy_current`.
- If any active registry row is missing a policy or taxonomy companion row, all holdings identifier resolution falls back to legacy `security_master`.

Impact:

- One unrelated incomplete registry row can silently move current holdings imports and identifier normalization back to legacy ranking logic.
- That undermines the intended authority boundary for current-vs-historical gating and increases the chance of choosing the wrong RIC when both registry-era and legacy rows coexist.

Required change:

- replace global completeness gating with targeted registry-path usage
- use registry tables whenever the tables exist and the targeted query can be answered
- keep fallback to legacy only for missing registry tables or targeted query failure, not for unrelated global incompleteness
- add targeted tests proving that an unrelated incomplete registry row does not force legacy fallback for a healthy target ticker/RIC

#### 3. Legacy compatibility policy defaults are still duplicated

Severity: low

Current behavior:

- `backend/universe/registry_sync.py` owns `policy_defaults_for_legacy_coverage_role()`
- `backend/universe/runtime_rows.py` redefines the same defaults privately as `_legacy_policy_defaults()`
- `backend/universe/source_observation.py` separately re-expresses a subset in `_legacy_policy_default_sql()`

Impact:

- behavior drift is still possible across bootstrap, runtime synthesis, and source-observation backfill
- future policy changes require touching multiple implementations

Required change:

- make one module the owner of legacy compatibility policy defaults
- have runtime-row derivation and source-observation derivation consume that shared owner rather than maintaining independent copies

#### 4. Seed provenance default still assumes the legacy label for unknown seed paths

Severity: low

Current behavior:

- `backend/universe/security_master_sync.py::_seed_source_label()` defaults unknown paths to `security_master_seed`

Impact:

- operator runs using a non-default registry-first seed path can stamp misleading provenance

Required change:

- default non-legacy paths to `security_registry_seed`
- keep the explicit legacy path mapped to `security_master_seed`

### Tactical Refactor Map

#### cPAR shared-source reads

Files:

- `backend/data/cpar_source_reads.py`
- `backend/tests/test_cpar_source_reads.py`

Refactor outline:

- replace `_registry_cpar_read_surfaces_ready()` with narrower table-availability helpers
- split readiness by query shape:
  - factor-proxy registry path requires `security_registry`
  - build-universe registry path requires `security_registry` + `security_policy_current`
  - compat join remains optional metadata
- ensure registry-path SQL still returns rows when compat coverage is partial
- keep fallback to `security_master` only when the required registry-owned tables are absent or registry execution raises `CparSourceReadError`

Tests to add or rewrite:

- registry build-universe read remains registry-first when compat is missing for an unrelated active row
- factor-proxy read remains registry-first when compat is absent for the requested ticker
- legacy fallback still occurs when required registry-owned tables are actually absent

#### Neon holdings identifier resolution

Files:

- `backend/services/neon_holdings_identifiers.py`
- `backend/tests/test_neon_holdings_identifiers.py`

Refactor outline:

- replace `_registry_holdings_resolution_available()` with table-presence checks only
- resolve `ric_exists()` and `resolve_ticker_to_ric_internal()` by attempting targeted registry queries first when registry tables exist
- use targeted row-level fallback defaults inside the registry query path where policy/taxonomy attributes are null
- fall back to legacy only if:
  - required registry tables are missing, or
  - the targeted registry query errors, or
  - the registry path returns no matching current candidate and legacy fallback is explicitly still allowed for table-missing mode
- preserve the existing current-vs-historical behavior: a registry `historical_only` row must still block legacy revival for that exact current-path lookup

Tests to add or rewrite:

- unrelated incomplete registry companion coverage no longer forces legacy fallback for a healthy target ticker
- `ric_exists()` stays registry-first when the target row exists but another row is missing taxonomy/policy
- legacy fallback still works when registry tables are absent
- downstream holdings import tests in `backend/services/neon_holdings.py` should be re-run because they inherit this resolution path

#### Legacy compatibility policy defaults

Files:

- `backend/universe/registry_sync.py`
- `backend/universe/runtime_rows.py`
- `backend/universe/source_observation.py`
- related tests in `backend/tests/test_security_registry_sync.py` and `backend/tests/test_universe_migration_scaffolding.py`

Refactor outline:

- promote one shared compatibility-default owner in `registry_sync.py`
- remove the duplicate `_legacy_policy_defaults()` implementation from `runtime_rows.py`
- make source-observation SQL defaults derive from the same owned defaults for the supported flags
- leave the SQL emission helper local if needed, but generate values from the shared owner so the policy contract has one definition

Tests to add or rewrite:

- explicit regression proving registry sync defaults and runtime-row defaults still agree for `native_equity`
- explicit regression proving registry sync defaults and runtime-row defaults still agree for `projection_only`

#### Seed provenance

Files:

- `backend/universe/security_master_sync.py`
- any affected tests under `backend/tests/test_security_registry_sync.py` or `backend/tests/test_security_master_lineage.py`

Refactor outline:

- change `_seed_source_label()` so unknown/non-legacy paths stamp `security_registry_seed`
- preserve the explicit legacy compatibility path mapping

### Exploratory Notes Used To Bound The Work

- `backend/data/cpar_source_reads.py` already uses left joins for compat metadata, so compat completeness is not a hard data dependency for registry-path reads.
- `backend/services/neon_holdings_identifiers.py` already has targeted registry query functions; the real problem is the global completeness gate in front of them.
- existing tests currently encode the old fallback behavior in at least one holdings case and will need to be updated to the new authority expectation.
- the fix set does not require Neon schema changes or destructive table changes.
- no committed seed artifact needs to be deleted or replaced in this remediation slice.

### Implementation Sequence

1. Update the planning document with the accepted findings and tactical plan.
2. Review the plan with two independent plan-review passes.
3. Refine the plan after round one critiques.
4. Review the refined plan a second time with two independent plan-review passes.
5. Implement cPAR shared-read fallback narrowing.
6. Implement holdings identifier fallback narrowing.
7. Implement shared legacy-policy default consolidation.
8. Implement seed provenance cleanup.
9. Update affected tests to assert the new authority behavior.
10. Re-run targeted regression suites covering cPAR reads, holdings identifiers, registry sync, migration scaffolding, and any touched docs/tooling assumptions.
11. Update docs only where they materially state the old all-or-nothing fallback behavior or imply that compat completeness is required for these query paths.
12. Record the executed remediation and remaining operational implications in this document.

### Validation Expectations

- targeted pytest slices for:
  - `backend/tests/test_cpar_source_reads.py`
  - `backend/tests/test_neon_holdings_identifiers.py`
  - `backend/tests/test_security_registry_sync.py`
  - `backend/tests/test_universe_migration_scaffolding.py`
  - any newly implicated tests from cPAR or holdings services
- touched-module `py_compile`
- path-scoped `git diff --check`

### Explicit Non-Goals

- no Neon migration or operational rollout work in this slice
- no destructive demotion of `security_master`
- no removal of the legacy compatibility export artifact
- no cUSE or cPAR payload schema redesign beyond the fallback/authority tightening above

### Execution Status

Status: completed for repo-side remediation

Implemented:

- `backend/data/cpar_source_reads.py`
  - removed the global registry completeness gate for cPAR shared reads
  - narrowed registry-path prerequisites to query-specific table presence
  - made compat metadata optional on the registry path instead of a cutover precondition
  - preserved legacy fallback only when required registry-owned tables are absent or registry execution fails
- `backend/services/neon_holdings_identifiers.py`
  - removed the global registry companion-completeness gate
  - changed holdings identifier resolution to attempt targeted registry-path reads whenever `security_registry` exists
  - made policy/taxonomy joins optional metadata for ranking rather than an all-or-nothing cutover guard
  - preserved legacy fallback only when `security_registry` is absent
- `backend/universe/runtime_rows.py`
  - removed the duplicate legacy policy default implementation and switched runtime derivation to the shared owner in `registry_sync.py`
- `backend/universe/source_observation.py`
  - changed legacy SQL defaults to derive from the shared compatibility default owner
- `backend/universe/security_master_sync.py`
  - changed unknown custom seed paths to stamp `security_registry_seed` provenance instead of the legacy label

Tests updated:

- `backend/tests/test_cpar_source_reads.py`
  - updated the old fallback assertion to the new registry-first behavior
  - added a regression proving factor-proxy reads stay registry-first without compat metadata
- `backend/tests/test_neon_holdings_identifiers.py`
  - updated the incomplete-companion-surface case so it now proves registry-first resolution instead of legacy fallback
- `backend/tests/test_security_registry_sync.py`
  - added a regression proving custom registry-first seed paths stamp `security_registry_seed`
- `backend/tests/test_universe_migration_scaffolding.py`
  - added a regression proving runtime rows consume the shared legacy-policy defaults

Validation executed:

- `43 passed` on:
  - `backend/tests/test_cpar_source_reads.py`
  - `backend/tests/test_neon_holdings_identifiers.py`
  - `backend/tests/test_security_registry_sync.py`
  - `backend/tests/test_universe_migration_scaffolding.py`
- touched-module `py_compile` passed
- path-scoped `git diff --check` passed

Doc outcome:

- no separate operator or architecture doc updates were required beyond this plan document because the old all-or-nothing fallback behavior was not materially documented as an operator contract elsewhere

Remaining work:

- operational rollout items remain unchanged from the larger migration plan:
  - real Neon rollout execution
  - retained-window parity/backfill acceptance
  - stabilization-window evidence
  - destructive `security_master` demotion in a controlled window

## 2026-03-26 Full-Stack Adversarial Review: Historical Runtime Contract And Model Gating

### Accepted Findings

Two independent adversarial passes and two evaluator passes converged on the same substantive fault set:

- `backend/universe/registry_sync.py`
  - seed-sync still persists `security_policy_current` rows from legacy `coverage_role` defaults (`registry_seed_defaults`) in a way that can outlive later taxonomy/classification changes
- `backend/universe/runtime_rows.py`
  - runtime precedence still prefers durable stored policy over structural/taxonomy-derived policy even when the stored policy came from seed defaults rather than an explicit override
  - historical `as_of_date` reads can still leak current taxonomy when no prior PIT classification snapshot exists
  - runtime candidate anchoring treats the mere presence of `security_registry` as authoritative, which can hide compat-only names in mixed states
- `backend/data/source_reads.py`
  - registry/runtime read-path preference still flips on too early in partial states
- `backend/risk_model/raw_cross_section_history.py`
  - the full rebuild window is still gated off one runtime snapshot at `max_date`, so historical membership is flattened to end-of-window state
- `backend/risk_model/cuse_membership.py`
  - one runtime map at `max_as_of_date` is applied to every row in a multi-date payload
- `backend/data/cpar_outputs.py`
  - package-membership bucketing still treats any non-factor-proxy US name as `core_us_equity`, regardless of equity/taxonomy/policy role

Severity and disposition:

- blockers for the runtime contract:
  - policy precedence from legacy seed defaults
  - historical-as-of leakage from current taxonomy
- migration blocker:
  - mixed-state registry/runtime readiness hiding compat-only names
- downstream historical correctness blockers:
  - raw cross-section history using one runtime snapshot
  - cUSE membership using one runtime snapshot
- secondary but required before cPAR is considered cleanly authoritative:
  - cPAR `core_us_equity` bucketing from US HQ country alone

### Tactical Implementation Plan

Implementation order for this slice:

1. Fix the runtime contract in `registry_sync.py`, `taxonomy_builder.py`, `source_observation.py`, and `runtime_rows.py`.
2. Fix mixed-state registry/runtime readiness in `runtime_rows.py` and `source_reads.py`.
3. Move historical cUSE consumers onto date-correct runtime loading in `raw_cross_section_history.py` and `cuse_membership.py`.
4. Fix cPAR package-membership bucketing in `cpar_source_reads.py`, `orchestration/cpar_stages.py`, and `cpar_outputs.py`.
5. Update docs/tests and run validation.

### Refactor Map

#### 1. Runtime Contract: seed defaults vs explicit policy override

Files:

- `backend/universe/registry_sync.py`
- `backend/universe/runtime_rows.py`
- `backend/universe/taxonomy_builder.py`
- `backend/universe/source_observation.py`
- `backend/scripts/export_security_registry_seed.py`
- `backend/universe/security_master_sync.py`

Problem:

- `registry_seed_defaults` are currently persisted as if they were durable policy truth.
- current/historical runtime assembly then prefers those persisted defaults over structural classification and taxonomy.
- this lets a name seeded as `native_equity` keep native-core/PIT behavior after later ex-US or non-equity classification, unless someone manually rewrites the policy row.

Required changes:

- add one shared policy-source classifier in `registry_sync.py`:
  - `policy_source_is_default(policy_source: str | None) -> bool`
  - `policy_source_is_explicit_override(policy_source: str | None) -> bool`
- treat at least these as non-authoritative defaults:
  - `registry_seed_defaults`
  - any legacy seed-default alias if encountered during compatibility reads
- keep sources like `manual_override` and explicit operator-driven policy writes authoritative
- keep `policy_defaults_for_legacy_coverage_role()` as compatibility-only seed bootstrap logic, not as the runtime truth owner
- in `runtime_rows.py`:
  - replace the current unconditional preference for `policy_rows` with an effective-policy resolver
  - new helper should only let stored policy override structural derivation when `policy_source_is_explicit_override(...)` is true and `updated_at <= as_of_date` for historical reads
  - seed-default policy rows should fall behind structural derivation for:
    - `pit_fundamentals_enabled`
    - `pit_classification_enabled`
    - `allow_cuse_native_core`
    - `allow_cuse_fundamental_projection`
    - `allow_cuse_returns_projection`
    - `allow_cpar_core_target`
    - `allow_cpar_extended_target`
  - `price_ingest_enabled` can still default on, but seed-default rows should not force a structurally stale model path
- in `taxonomy_builder.py`:
  - stop using raw `security_policy_current` seed-default rows to infer current taxonomy shape
  - only let explicit override policy affect the `projection_only_vehicle` shortcut
  - otherwise derive `instrument_kind`, `vehicle_structure`, `is_single_name_equity`, and `model_home_market_scope` from classification first
- in `source_observation.py`:
  - stop taking current registry-path PIT flags directly from seed-default policy rows
  - use the same shared precedence rule as runtime rows:
    - explicit override policy can win
    - otherwise derive PIT/classification defaults from observed classification/equity structure
- in `export_security_registry_seed.py`:
  - decide whether exported seed flags should represent explicit stored policy only, or the effective current runtime policy
  - for this slice, keep export behavior stable unless tests expose a mismatch, but document the contract explicitly

New helper seams to add:

- `registry_sync.py`
  - `policy_source_is_default(...)`
  - `policy_source_is_explicit_override(...)`
- `runtime_rows.py`
  - `_resolve_effective_policy_row(...)`
  - possibly `_resolve_structural_row_for_as_of(...)` if the logic becomes too wide for `load_security_runtime_rows`

Hidden callers and break-risk:

- `backend/scripts/download_data_lseg.py`
  - consumes `load_security_runtime_rows()` and `load_security_runtime_map()` for price/PIT ingest gating
- `backend/universe/selectors.py`
  - inherits runtime gating behavior through selector parity
- `backend/data/health_audit.py`
  - reads runtime rows and may show changed diagnostics once default-seeded policy stops dominating
- `backend/scripts/export_security_master_seed.py`
  - compatibility export derives `coverage_role` from policy flags and may need test adjustment if the effective policy contract changes

Tests to add or update:

- `backend/tests/test_universe_migration_scaffolding.py`
  - add a regression where a registry-seeded `native_equity` name later classifies as ex-US and assert runtime rows move to `allow_cuse_fundamental_projection=1`, `allow_cuse_native_core=0` when there is no explicit override
  - add a regression where a registry-seeded `native_equity` later classifies as a non-equity TRBC sector and assert runtime rows move to returns-projection/cPAR-extended semantics when there is no explicit override
  - add a regression proving `manual_override` still wins over derived structure when explicitly set
- `backend/tests/test_security_registry_sync.py`
  - keep bootstrap expectations, but distinguish seed defaults from explicit overrides
- `backend/tests/test_security_master_lineage.py`
  - update any assertions that implicitly treat seed-default policy as permanent runtime truth

#### 2. Historical `as_of` structural reads must not borrow current taxonomy

Files:

- `backend/universe/runtime_rows.py`
- `backend/universe/taxonomy_builder.py`
- `backend/universe/source_observation.py`

Problem:

- current `as_of_date` runtime reads still fall back to `security_taxonomy_current` when historical PIT classification is missing, which leaks future/current structure into the past.

Required changes:

- in `runtime_rows.py`:
  - for `as_of_date` reads, never source `instrument_kind`, `vehicle_structure`, `model_home_market_scope`, or `is_single_name_equity` from `security_taxonomy_current`
  - historical structural rows must come from:
    - PIT classification snapshot up to `as_of_date`
    - else compat/observation fallback only
    - else legacy fallback based on compat fields
  - current taxonomy can remain a fallback only for non-historical reads
- derive historical `classification_ready` and `is_single_name_equity` from PIT classification if present
- if PIT classification is missing for the requested date, historical reads should become less informed, not more forward-looking

New helper seams to add:

- `runtime_rows.py`
  - `_resolve_historical_structural_row(...)`
  - `_resolve_current_structural_row(...)`

Tests to add or update:

- `backend/tests/test_universe_migration_scaffolding.py`
  - add a regression where `security_taxonomy_current` says `fund_vehicle/ex_us`, but no historical PIT classification exists for the requested date, and assert the historical read does not inherit those current taxonomy values
  - update existing historical snapshot tests so they explicitly prove no current-taxonomy leakage

#### 3. Mixed-state authority gating must stay conservative

Files:

- `backend/universe/runtime_rows.py`
- `backend/data/source_reads.py`
- possibly `backend/services/neon_holdings_identifiers.py` if shared readiness helpers are factored

Problem:

- the mere presence of registry/current tables is still enough in some paths to switch to registry/runtime authority, even when companion coverage is incomplete.
- in those mixed states, compat-only names can disappear.

Required changes:

- in `runtime_rows.py`:
  - replace `registry_authority_ready = _table_exists(conn, SECURITY_REGISTRY_TABLE)` with a real completeness decision
  - proposed contract:
    - if `security_registry` exists and has zero rows, treat empty registry as authoritative empty
    - if `security_registry` has rows, registry-only anchoring is allowed only when companion current-state coverage is complete enough for runtime use
    - otherwise fall back to the mixed candidate union (`registry | compat | policy | taxonomy | observation`)
- in `source_reads.py`:
  - tighten `_prefer_runtime_registry(...)` from `policy_n > 0 and taxonomy_n > 0` to full active-registry coverage
  - keep compat fallback until completeness is met
- if useful, factor one shared completeness helper so runtime rows and source reads do not drift again

New helper seams to add:

- either:
  - `runtime_rows.py::_registry_runtime_authority_state(...)`
- or:
  - new shared helper module for registry completeness, if that avoids duplication with `source_reads.py`

Tests to add or update:

- `backend/tests/test_core_reads.py`
  - add or update a case where registry/current tables exist but only cover a subset of active registry names, and assert latest-price/fundamental reads do not silently hide compat-only names
- `backend/tests/test_universe_migration_scaffolding.py`
  - add a partial-state runtime-row case where registry is populated but taxonomy/policy coverage is incomplete and assert compat-only rows still appear
- `backend/tests/test_universe_selector_parity.py`
  - update parity expectations if selector behavior changes under partial-state runtime gating

#### 4. Historical cUSE consumers need date-keyed runtime loading

Files:

- `backend/universe/runtime_rows.py`
- `backend/risk_model/raw_cross_section_history.py`
- `backend/risk_model/cuse_membership.py`
- `backend/data/model_outputs.py`

Problem:

- raw cross-section history and cUSE membership both cache one runtime snapshot and reuse it across multiple dates.

Required changes:

- add a batch helper in `runtime_rows.py`:
  - `load_security_runtime_rows_by_date(...)` or `load_security_runtime_map_by_date(...)`
  - key the result by `(as_of_date, ric)` or `{as_of_date: {ric: row}}`
  - internally call the existing runtime loader per unique date unless a clean SQL/vectorized approach is obviously simpler
- in `raw_cross_section_history.py`:
  - stop anchoring the rebuild to `as_of_date=max_date`
  - build the union of runtime-eligible RICs across target dates for upstream price/fundamental/classification loading
  - when constructing the base target-date rows, join against date-specific runtime eligibility
  - remove or narrow the current `load_default_source_universe_rows(...)` merge if it reintroduces current-state flattening
- in `cuse_membership.py`:
  - replace the single `source_rows = load_security_runtime_map(... max_as_of_date ...)` with a per-date runtime map
  - every membership/stage row should resolve `source_row` from its own `as_of_date`
- confirm `backend/data/model_outputs.py` does not assume single-date runtime enrichment when persisting membership payloads

New helper seams to add:

- `runtime_rows.py`
  - `load_security_runtime_map_by_date(...)`

Hidden callers and break-risk:

- `backend/scripts/build_barra_raw_cross_section_history.py`
  - will see materially different historical outputs
- `backend/tests/test_security_master_lineage.py`
  - currently has raw-history coverage and will need new date-varying cases
- `backend/tests/test_cuse_membership_contract.py`
  - currently mostly single-date and will need a multi-date contract case

Tests to add or update:

- `backend/tests/test_security_master_lineage.py`
  - add a raw-history regression where one RIC changes eligibility inside the window and assert earlier dates do not inherit later inclusion/exclusion
- `backend/tests/test_cuse_membership_contract.py`
  - add a multi-date payload test where a name changes country/policy path between dates and assert membership/stage rows differ by `as_of_date`

#### 5. cPAR package membership must use model role, not just HQ country

Files:

- `backend/data/cpar_source_reads.py`
- `backend/orchestration/cpar_stages.py`
- `backend/data/cpar_outputs.py`
- cPAR service/query tests

Problem:

- cPAR package membership currently labels any non-factor-proxy US instrument as `core_us_equity`, even if it is a US ETF/fund/vehicle.

Required changes:

- in `cpar_source_reads.py`:
  - include `allow_cpar_core_target`, `allow_cpar_extended_target`, and/or a usable single-name-equity indicator in build-universe rows
- in `orchestration/cpar_stages.py`
  - carry those fields into `instrument_fits` so package persistence has the required model-role context
- in `cpar_outputs.py`
  - change `_derive_package_membership_rows(...)` so:
    - factor proxies stay `factor_basis_only`
    - `core_us_equity` requires both:
      - US home country
      - single-name-equity or explicit `allow_cpar_core_target`
    - otherwise the instrument goes to `extended_priced_instrument`

Tests to add or update:

- `backend/tests/test_cpar_outputs_local_regression.py`
  - add a US ETF/fund/vehicle fit and assert it lands in `extended_priced_instrument`
- `backend/tests/test_cpar_queries.py`
  - update any hard-coded `build_reason_code` / `target_scope` assumptions if the contract changes
- `backend/tests/test_cpar_ticker_service.py`
  - ensure downstream services surface the corrected target scope

### First Implementation Draft For This Slice

Planned edit groups:

1. `registry_sync.py`, `taxonomy_builder.py`, `source_observation.py`, `runtime_rows.py`
   - add policy-source classification helpers
   - switch effective policy resolution to explicit-override precedence
   - split current vs historical structural resolution
2. `runtime_rows.py`, `source_reads.py`
   - add conservative registry completeness gating
3. `runtime_rows.py`, `raw_cross_section_history.py`, `cuse_membership.py`
   - add date-keyed runtime maps and remove single-date flattening
4. `cpar_source_reads.py`, `orchestration/cpar_stages.py`, `cpar_outputs.py`
   - carry cPAR role metadata and fix `core_us_equity` bucketing
5. tests and plan-doc execution log

Validation target for this slice:

- `backend/tests/test_universe_migration_scaffolding.py`
- `backend/tests/test_core_reads.py`
- `backend/tests/test_security_master_lineage.py`
- `backend/tests/test_cuse_membership_contract.py`
- `backend/tests/test_cpar_source_reads.py`
- `backend/tests/test_cpar_outputs_local_regression.py`
- `backend/tests/test_cpar_queries.py`
- `backend/tests/test_cpar_ticker_service.py`
- any newly implicated selector or service tests

Expected behavioral changes:

- historical cUSE outputs may materially shift for dates where eligibility changed inside the backfill window
- current runtime rows may reclassify previously seed-defaulted names into ex-US fundamental projection or non-equity returns projection unless there is an explicit override
- mixed-state reads will remain compat-tolerant longer instead of switching early to registry-only anchoring
- cPAR package membership for US non-equities will shift from `core_us_equity` to `extended_priced_instrument`

## 2026-03-26 Comprehensive Review Loop: Findings, Sequencing, And Execution Plan

This section records the full review -> evaluator -> explorer -> plan-review -> implementation loop initiated after a fresh independent adversarial pass over the registry and model-gating implementation.

This section supersedes the later same-date remediation draft below it. If there is any sequencing or scope conflict, use this section.

### Accepted Findings

Accepted from two independent adversarial reviews and two evaluator dispositions:

1. `raw_cross_section_history.py` uses one runtime snapshot at `max_date` for the whole rebuild window.
   - effect: historical cUSE raw history is flattened to end-of-window membership
   - disposition: blocker
2. `cuse_membership.py` uses one runtime map at `max_as_of_date` for all membership rows in a multi-date payload.
   - effect: durable cUSE membership/stage truth is historically inconsistent
   - disposition: blocker
3. `security_policy_current` still persists legacy `coverage_role` defaults as durable truth, and `runtime_rows.py` prefers those stored policy rows over structural/taxonomy-derived policy.
   - effect: names can remain `native_core` or PIT-enabled after they are structurally reclassified ex-US or non-equity
   - disposition: blocker
4. `runtime_rows.py` historical `as_of_date` reads can fall back to `security_taxonomy_current` when PIT classification history is missing.
   - effect: future/current taxonomy can leak into past structural reads
   - disposition: blocker
5. `source_reads.py` and runtime-read readiness can accept partial registry companion coverage and hide compat-only names during mixed-state operation.
   - effect: silent disappearance / authority drift in partial states
   - disposition: migration blocker
6. `cpar_outputs.py` assigns `core_us_equity` from US HQ country alone.
   - effect: US ETFs / funds / other non-single-name vehicles can be written into cPAR core scope
   - disposition: secondary correctness fix, but still required before calling cPAR authoritative

### Chosen Fix Order

The evaluator agents disagreed only on whether `1+2` or `3+4` should come first. The accepted sequencing is:

1. fix runtime authority semantics: `3+4`
2. fix mixed-state read readiness: `5`
3. move historical cUSE consumers onto date-correct runtime loading: `1+2`
4. fix cPAR package bucketing: `6`

Reason:

- `3+4` define the runtime contract consumed by both ingest/read paths and historical builders
- `5` prevents partial-state disappearance while the corrected contract is being rolled through
- `1+2` should then consume the corrected runtime contract instead of freezing wrong semantics into historical artifacts
- `6` is isolated to cPAR package labeling once the upstream role semantics are stable

Execution rule:

- Phase A and Phase B are a single ship unit for current-read surfaces.
- Do not release Phase A semantics without the Phase B bridge/readiness protections and validation.

### Specific Implementation Plan

#### Phase A: Runtime Policy / Structural Contract

Primary files:

- `backend/universe/registry_sync.py`
- `backend/universe/runtime_rows.py`
- `backend/universe/source_observation.py`
- `backend/universe/taxonomy_builder.py`
- `backend/analytics/pipeline.py`
- `backend/orchestration/run_model_pipeline.py`
- `backend/tests/test_security_registry_sync.py`
- `backend/tests/test_universe_migration_scaffolding.py`
- `backend/tests/test_universe_selector_parity.py`

Required refactors:

- teach `load_security_registry_seed_rows(...)` to distinguish:
  - explicit policy rows
  - defaulted policy rows derived from legacy `coverage_role`
- preserve per-row policy provenance in `security_policy_current.policy_source` rather than treating every seed policy row as equally authoritative
- normalize policy-source semantics in one shared helper
  - explicit override
  - seed/default
  - compatibility / mirrored legacy
- introduce one shared helper for effective policy derivation from structural state
  - it must derive all policy flags, not just `allow_*`
  - it must correctly zero PIT ingest for non-equity / fund-vehicle structures
- update `runtime_rows.py` so:
  - authority resolution is explicit instead of inferred from a single boolean
  - current reads and historical reads share the same candidate-RIC resolution contract
  - explicit policy overrides remain authoritative
  - default-sourced policy rows do not override newer structural/taxonomy reality
  - historical `as_of_date` reads do not fall back to `security_taxonomy_current` for missing past PIT snapshots
  - current reads may still use `security_taxonomy_current` where appropriate
- update `source_observation.py` and `taxonomy_builder.py` to consume the same effective-policy semantics instead of independently trusting raw `security_policy_current`

Required data migration / republish:

- normalize already-persisted `security_policy_current` rows in SQLite before readers change semantics
- republish the normalized current-state policy surface to Neon in the same slice
- do not rely on reinterpretation alone; stale default-sourced policy rows must be rewritten or republished so environments do not drift
- include an explicit reconciliation step for preexisting `registry_seed_defaults` rows so stale durable policy does not survive untouched in older environments

Concrete code seams:

- `registry_sync.py`
  - `load_security_registry_seed_rows(...)`
  - policy-source helper definitions
  - `upsert_security_policy_rows(...)`
  - `sync_security_registry_seed(...)`
- `runtime_rows.py`
  - `_load_policy_rows(...)`
  - `_derive_structural_row(...)`
  - `_derive_policy_flags(...)`
  - add explicit authority-mode / candidate-RIC helper(s)
  - `load_security_runtime_rows(...)`
- `source_observation.py`
  - `_legacy_policy_default_sql(...)`
  - `refresh_security_source_observation_daily(...)`
- `taxonomy_builder.py`
  - `materialize_security_master_compat_current(...)`

New or strengthened tests:

- reclassified ex-US single-name equity no longer stays `allow_cuse_native_core=1` from seed-defaulted legacy policy
- reclassified fund / non-equity no longer stays PIT-enabled from seed-defaulted legacy policy
- historical `as_of_date` read without PIT classification history does not borrow current taxonomy
- explicit policy override remains authoritative even when taxonomy disagrees
- existing default-sourced policy rows are normalized or republished instead of surviving indefinitely as stale durable truth

#### Phase B: Mixed-State Readiness And Compat Safety

Primary files:

- `backend/data/source_reads.py`
- `backend/universe/runtime_rows.py`
- `backend/data/cpar_source_reads.py`
- `backend/services/neon_holdings_identifiers.py`
- `backend/data/holdings_reads.py`
- `backend/analytics/pipeline.py`
- `backend/tests/test_core_reads.py`
- `backend/tests/test_cpar_source_reads.py`
- `backend/tests/test_neon_holdings_identifiers.py`
- `backend/tests/test_universe_migration_scaffolding.py`

Required refactors:

- centralize registry companion completeness checks so runtime rows and source reads use the same contract
- require completeness over the requested active set, not merely non-empty tables or whole-registry counts
- use `security_master_compat_current` only as a bounded bridge for missing companion metadata
  - once `security_registry` exists, do not widen back to `security_master` as a steady authority path in these readers
- avoid global fallback decisions caused by unrelated incomplete registry companion rows
- explicitly handle empty compat surfaces
  - `security_master_compat_current` present but empty is not a valid anchor
  - readers must fail closed or stay on registry-first resolution instead of silently dropping every row

Concrete code seams:

- `source_reads.py`
  - `_prefer_runtime_registry(...)`
  - add or factor a requested-set runtime identity mode helper
  - any runtime-registry CTE construction using registry-only anchors
- `runtime_rows.py`
  - registry authority / candidate-RIC anchoring inside `load_security_runtime_rows(...)`
- `cpar_source_reads.py`
  - registry-read readiness helpers for build-universe and factor-proxy reads
- `neon_holdings_identifiers.py`
  - registry-readiness gating for ticker/ric resolution
- `holdings_reads.py`
  - shared holdings display / identity reads must align with the same mixed-state contract

New or strengthened tests:

- partial registry/policy/taxonomy coverage does not hide compat-only names
- holdings resolution stays row-correct when registry companions are incomplete for unrelated names
- cPAR build-universe reads remain compat-tolerant until registry companions are complete
- compat table present but empty does not become a valid anchor
- holdings display/ticker reads preserve compat-bridged identity instead of dropping names

#### Phase C: Historical cUSE Runtime Loading

Primary files:

- `backend/universe/runtime_rows.py`
- `backend/risk_model/raw_cross_section_history.py`
- `backend/risk_model/cuse_membership.py`
- `backend/data/model_outputs.py`
- `backend/universe/estu.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/data/cross_section_snapshot_build.py`
- `backend/analytics/health.py`
- `backend/scripts/backfill_pit_history_lseg.py`
- `backend/scripts/backfill_prices_range_lseg.py`
- `backend/scripts/download_data_lseg.py`
- `backend/tests/test_security_master_lineage.py`
- `backend/tests/test_cuse_membership_contract.py`

Required refactors:

- add a batch date-keyed runtime loader or map helper in `runtime_rows.py`
- stop using one `max_date` / `max_as_of_date` runtime snapshot for whole multi-date operations
- load the union of required RICs across dates for efficient upstream source fetches, but gate each output row with its own `as_of_date` runtime row
- ensure raw-history and membership persistence both use per-date runtime semantics

Concrete code seams:

- `runtime_rows.py`
  - add `load_security_runtime_rows_by_dates(...)` and `load_security_runtime_map_by_date(...)` or equivalent
- `raw_cross_section_history.py`
  - `rebuild_raw_cross_section_history(...)`
- `cuse_membership.py`
  - payload assembly path that currently loads `source_rows` once
- `model_outputs.py`
  - verify persistence path does not assume single-date enrichment

New or strengthened tests:

- raw-history regression with an eligibility change inside the rebuild window
- multi-date cUSE membership regression where policy path / country / structural eligibility changes by date

#### Phase D: cPAR Core vs Extended Membership

Primary files:

- `backend/data/cpar_source_reads.py`
- `backend/orchestration/cpar_stages.py`
- `backend/data/cpar_outputs.py`
- `backend/tests/test_cpar_outputs_local_regression.py`
- `backend/tests/test_cpar_queries.py`
- `backend/tests/test_cpar_ticker_service.py`
- any portfolio/ticker service tests that pin `target_scope`

Required refactors:

- carry enough role metadata from cPAR build-universe rows into package instrument fits
  - at minimum: `allow_cpar_core_target`, `allow_cpar_extended_target`, and/or `is_single_name_equity`
- make `_derive_package_membership_rows(...)` require model-role support for `core_us_equity`
- keep factor-basis proxies as their own category
- define how previously materialized `cpar_*` rows are handled
  - rebuild / invalidate existing packages
  - or explicitly version the membership contract
  - do not allow old and new `core_us_equity` meanings to coexist silently

Concrete code seams:

- `cpar_source_reads.py`
  - extend build-universe rows with `allow_cpar_core_target`, `allow_cpar_extended_target`, and/or `is_single_name_equity`
- `cpar_stages.py`
  - `_fit_instrument_row(...)`
  - propagate model-role context from `universe_row` into `fit_row`
- `cpar_outputs.py`
  - `_derive_package_membership_rows(...)`

Downstream services expected to move with this contract:

- `backend/data/cpar_queries.py`
- `backend/services/cpar_portfolio_snapshot_service.py`
- `backend/services/cpar_ticker_service.py`
- `backend/services/cpar_search_service.py`

New or strengthened tests:

- US single-name equity -> `core_us_equity`
- US ETF/fund/vehicle -> `extended_priced_instrument`
- factor proxy -> `factor_basis_only`

### Pre-Implementation Checklist For This Loop

- [ ] initial plan section added to this document
- [ ] two independent adversarial reviews collected
- [ ] two evaluator dispositions collected
- [ ] explorer augmentation collected for runtime/historical seams
- [ ] explorer augmentation collected for cPAR/mixed-state seams
- [ ] plan-review loop 1 completed
- [ ] plan-review loop 2 completed
- [ ] accepted implementation sequence locked

### Implementation Validation Matrix For This Loop

Core suites expected to run after implementation:

- `backend/tests/test_security_registry_sync.py`
- `backend/tests/test_universe_migration_scaffolding.py`
- `backend/tests/test_universe_selector_parity.py`
- `backend/tests/test_core_reads.py`
- `backend/tests/test_neon_holdings_identifiers.py`
- `backend/tests/test_holdings_reads.py`
- `backend/tests/test_cpar_source_reads.py`
- `backend/tests/test_security_master_lineage.py`
- `backend/tests/test_cuse_membership_contract.py`
- `backend/tests/test_cpar_outputs_local_regression.py`
- `backend/tests/test_cpar_queries.py`
- `backend/tests/test_cpar_ticker_service.py`
- `backend/tests/test_cpar_pipeline_runtime.py`
- `backend/tests/test_cpar_runtime_coverage_contract.py`
- `backend/tests/test_cpar_portfolio_snapshot_service.py`
- `backend/tests/test_cpar_portfolio_hedge_service.py`
- `backend/tests/test_refresh_profiles.py`
- `backend/tests/test_neon_authority.py`
- `backend/tests/test_neon_parity_value_checks.py`
- `backend/tests/test_neon_stage2_model_tables.py`
- `backend/tests/test_neon_mirror_integration.py`

Additional validation:

- touched-module `py_compile`
- path-scoped `git diff --check`
- targeted manual inspection of any changed model-role / membership status contracts
- targeted manual or scripted smoke validation for:
  - `backend/scripts/download_data_lseg.py`
  - `backend/scripts/backfill_prices_range_lseg.py`
  - `backend/scripts/backfill_pit_history_lseg.py`
  - the pipeline/orchestrator paths that invoke the updated selectors and runtime loaders

### Plan Review Loop 1: Accepted Critiques And Plan Changes

Accepted from the first hostile plan-review loop:

- add the outer orchestration / backfill entrypoints, not just the inner helper modules
- add a required rewrite / republish step for already-persisted `security_policy_current`
- explicitly forbid empty compat surfaces from becoming valid anchors
- add `holdings_reads.py` to the mixed-state authority slice
- add readiness / sync guardrail suites to validation
- define the cutover treatment for already-materialized `cpar_*` packages
- state that this section supersedes the later same-date draft

### Plan Review Loop 2: Accepted Critiques And Plan Changes

Accepted from the second hostile plan-review loop:

- Phase A and Phase B are now explicitly atomic for current-read surfaces
- durable `security_policy_current` reconciliation is now a required data migration step, not just an interpretation change
- holdings and portfolio-facing suites were added to validation
- Neon parity / mirror / stage2 suites were added to validation
- cPAR persistence/runtime suites were added to validation
- outer ingest/backfill entrypoints were added to the implementation and smoke-validation surfaces

## 2026-03-26 Comprehensive Remediation Review And Execution Plan

### Adversarial Review Synthesis

Two independent adversarial reviews converged on the same six issues:

1. `backend/risk_model/raw_cross_section_history.py`
   - the rebuild still snapshots runtime eligibility at `max_date` and reuses that membership across the whole history window
2. `backend/risk_model/cuse_membership.py`
   - multi-date membership payloads still use one runtime map at `max_as_of_date`
3. `backend/universe/registry_sync.py` + `backend/universe/runtime_rows.py`
   - durable policy still treats legacy `coverage_role` seed defaults as authoritative policy instead of compatibility bootstrap defaults
4. `backend/universe/runtime_rows.py`
   - historical `as_of_date` reads can still borrow `security_taxonomy_current` when PIT classification is missing
5. `backend/data/source_reads.py` + `backend/universe/runtime_rows.py`
   - mixed-state readiness still cuts over to registry/runtime too early and can hide compat-only names
6. `backend/data/cpar_outputs.py`
   - cPAR package membership still labels any non-factor-proxy US name as `core_us_equity`

Evaluator disposition:

- blockers for the runtime contract:
  - seed-default policy precedence
  - historical-as-of current-taxonomy leakage
- migration blocker:
  - partial/mixed-state runtime authority hiding compat-only names
- downstream historical consumers that must move onto the corrected contract:
  - raw cross-section history
  - cUSE membership payload persistence
- secondary but still required for a clean cPAR contract:
  - `core_us_equity` bucketing based on HQ country alone

### Current Worktree Reality

The current branch is already mid-refactor relative to `HEAD`:

- `backend/risk_model/cuse_membership.py`
- `backend/universe/registry_sync.py`
- `backend/universe/runtime_rows.py`
- `docs/architecture/UNIVERSE_REGISTRY_AND_MODEL_GATING_PLAN.md`

are untracked/additive in the worktree.

- `backend/data/cpar_outputs.py`
- `backend/data/cpar_source_reads.py`
- `backend/data/source_reads.py`
- `backend/risk_model/raw_cross_section_history.py`

already carry in-flight changes.

Execution rule for this remediation slice:

- treat the current worktree as the source surface to repair
- do not revert or restage unrelated branch work
- patch only the seams mapped below

### Exact Edit Groups

#### Group A. Runtime contract and historical structural resolution

Files:

- `backend/universe/registry_sync.py`
- `backend/universe/runtime_rows.py`
- `backend/universe/taxonomy_builder.py`
- `backend/universe/source_observation.py`

Edits:

- add shared helpers in `registry_sync.py`:
  - `policy_source_is_default(...)`
  - `policy_source_is_explicit_override(...)`
- treat `registry_seed_defaults` and legacy seed-default aliases as non-authoritative defaults
- in `runtime_rows.py`:
  - add an effective-policy resolver that only lets stored policy override structural derivation when `policy_source_is_explicit_override(...)` is true
  - for historical reads, require `updated_at <= as_of_date` before any explicit override wins
  - stop using `security_taxonomy_current` for historical `instrument_kind`, `vehicle_structure`, `model_home_market_scope`, and `is_single_name_equity`
  - split current vs historical structural resolution helpers
- in `taxonomy_builder.py`:
  - stop using seed-default policy rows to force taxonomy shape
  - only let explicit override policy drive the `projection_only_vehicle` shortcut
- in `source_observation.py`:
  - align PIT/classification flag derivation with the same explicit-override-vs-derived precedence

Dependencies:

- `backend/universe/selectors.py`
- `backend/scripts/download_data_lseg.py`
- `backend/data/health_audit.py`
- `backend/scripts/export_security_master_seed.py`

Expected fallout:

- some names currently forced into native-core / PIT-enabled paths by seed-default policy should move to ex-US fundamental projection or non-equity returns projection

#### Group B. Mixed-state runtime readiness and compat visibility

Files:

- `backend/universe/runtime_rows.py`
- `backend/data/source_reads.py`

Edits:

- replace table-presence-only runtime anchoring with a completeness-aware authority state
- preserve the existing empty-registry-is-authoritative-empty behavior
- when registry has rows but companion coverage is incomplete:
  - allow mixed candidate union instead of registry-only anchoring
- tighten `_prefer_runtime_registry(...)` in `source_reads.py` so registry/runtime reads require full active-registry coverage, not just non-zero policy/taxonomy counts

Dependencies:

- `backend/universe/selectors.py`
- `backend/data/core_reads.py`
- `backend/tests/test_universe_selector_parity.py`
- `backend/tests/test_core_reads.py`

Expected fallout:

- mixed states will remain compat-tolerant longer
- compat-only names will stop disappearing during partial cutover states

#### Group C. Historical cUSE consumers

Files:

- `backend/universe/runtime_rows.py`
- `backend/risk_model/raw_cross_section_history.py`
- `backend/risk_model/cuse_membership.py`
- `backend/data/model_outputs.py`

Edits:

- add a date-keyed runtime loader in `runtime_rows.py`
- in `raw_cross_section_history.py`:
  - build runtime-eligible identity by target date, not by `max_date`
  - use the union of date-eligible RICs for upstream price/fundamental/classification loading
  - filter the base cross-section by per-date runtime identity
  - remove or narrow any current-only source-universe merge that would reintroduce flattening
- in `cuse_membership.py`:
  - replace the single runtime map with per-date runtime lookup
  - resolve every membership/stage row against its own `as_of_date`
- verify `backend/data/model_outputs.py` does not make a single-date assumption after the payload change

Dependencies:

- `backend/scripts/build_barra_raw_cross_section_history.py`
- `backend/tests/test_security_master_lineage.py`
- `backend/tests/test_cuse_membership_contract.py`

Expected fallout:

- historical cUSE outputs will change materially where eligibility changed inside the rebuilt window

#### Group D. cPAR package-membership bucketing

Files:

- `backend/data/cpar_source_reads.py`
- `backend/orchestration/cpar_stages.py`
- `backend/data/cpar_outputs.py`

Edits:

- ensure build-universe rows carry enough role context:
  - `allow_cpar_core_target`
  - `allow_cpar_extended_target`
  - usable single-name-equity / compat-equity indicator
- carry those fields through `_fit_instrument_row(...)` into `instrument_fits`
- change `_derive_package_membership_rows(...)` so:
  - factor proxies stay `factor_basis_only`
  - `core_us_equity` requires US home-country plus single-name-equity or explicit `allow_cpar_core_target`
  - otherwise route to `extended_priced_instrument`

Dependencies:

- `backend/services/cpar_ticker_service.py`
- `backend/services/cpar_portfolio_snapshot_service.py`
- `backend/data/cpar_queries.py`

Expected fallout:

- US ETFs/funds/vehicles will move from `core_us_equity` to `extended_priced_instrument`

### Test Matrix For This Slice

Add or update:

- `backend/tests/test_universe_migration_scaffolding.py`
  - seeded `native_equity` later classifies ex-US -> runtime policy path changes without explicit override
  - seeded `native_equity` later classifies non-equity -> returns-projection/cPAR-extended path without explicit override
  - explicit `manual_override` still wins
  - historical read with no PIT classification does not borrow current taxonomy
  - partial registry coverage does not hide compat-only rows
- `backend/tests/test_core_reads.py`
  - registry/current tables present but incomplete -> latest reads remain compat-tolerant
- `backend/tests/test_security_master_lineage.py`
  - raw-history rebuild honors date-varying eligibility inside the window
- `backend/tests/test_cuse_membership_contract.py`
  - multi-date payload uses date-correct runtime rows
- `backend/tests/test_cpar_outputs_local_regression.py`
  - US single-name equity stays core
  - US ETF/fund/vehicle goes extended
  - factor proxy stays factor-basis-only
- `backend/tests/test_cpar_queries.py`
  - update seeded package-membership fixtures if `build_reason_code` / `target_scope` changes
- `backend/tests/test_cpar_ticker_service.py`
  - downstream ticker payload surfaces corrected `target_scope`

Validation target:

- `backend/tests/test_universe_migration_scaffolding.py`
- `backend/tests/test_core_reads.py`
- `backend/tests/test_security_master_lineage.py`
- `backend/tests/test_cuse_membership_contract.py`
- `backend/tests/test_cpar_outputs_local_regression.py`
- `backend/tests/test_cpar_queries.py`
- `backend/tests/test_cpar_ticker_service.py`
- any newly implicated selector/service tests
- touched-module `py_compile`
- path-scoped `git diff --check`

### Review Loop Requirement For This Slice

Before implementation:

1. run plan review loop 1 with two independent reviewers
2. refine the plan from their critiques
3. run plan review loop 2 with two independent reviewers on the refined plan
4. only then implement

After implementation:

- re-run adversarial review on the changed surfaces before closing the slice

### Execution Update: 2026-03-26 Current-Read And Policy-Reconciliation Cleanup

Implemented in this follow-up slice:

- `backend/universe/registry_sync.py`
  - hardened `reconcile_default_security_policy_rows(...)` so it can reconcile against taxonomy-only or compat-only mixed states without assuming every companion table exists
  - restored legacy default-policy behavior when structural evidence is still unknown, so bootstrap and compat-only rows do not collapse to all-zero policy
- `backend/universe/runtime_rows.py`
  - replaced table-presence-only anchoring with completeness-aware mixed-state candidate selection
  - preserved request-scoped registry authority for healthy requested rows while keeping compat-visible union behavior when companion coverage is incomplete
- `backend/data/source_reads.py`
  - replaced table-presence-only runtime readiness with request-scoped companion coverage checks
  - latest fundamentals now require requested registry policy-plus-taxonomy coverage before taking the runtime branch
  - latest prices now require requested registry policy coverage before taking the runtime branch
- `backend/universe/source_observation.py`
  - made observed PIT-ingest flags honor persisted policy rows when present instead of zeroing them back out unless the policy source was explicit-only
- `backend/universe/taxonomy_builder.py`
  - restored policy-pattern-driven taxonomy compatibility for projection-only and single-name-equity defaults when source classification is still sparse
- `backend/data/cpar_source_reads.py`
  - restored the strict cPAR registry build-universe contract: registry rows now require policy rows, and compat metadata is optional metadata rather than a substitute for missing policy

Tests added or updated:

- `backend/tests/test_security_registry_sync.py`
  - added reconciliation coverage for taxonomy-only and compat-only mixed states
- `backend/tests/test_universe_migration_scaffolding.py`
  - updated runtime mixed-state expectations to keep compat-visible rows when registry companions are incomplete
- `backend/tests/test_holdings_reads.py`
  - updated SQL-contract assertions for the widened ticker fallback chain

Validation executed:

- targeted registry/runtime/core-read/cPAR/holdings slice:
  - `./.venv_local/bin/pytest backend/tests/test_security_registry_sync.py backend/tests/test_core_reads.py backend/tests/test_holdings_reads.py backend/tests/test_universe_migration_scaffolding.py backend/tests/test_cpar_source_reads.py -q`
  - result: `68 passed`
- broader authority/parity/publish/cPAR runtime slice:
  - `./.venv_local/bin/pytest backend/tests/test_refresh_profiles.py backend/tests/test_neon_authority.py backend/tests/test_neon_holdings_identifiers.py backend/tests/test_universe_selector_parity.py backend/tests/test_neon_parity_value_checks.py backend/tests/test_neon_mirror_integration.py backend/tests/test_neon_stage2_model_tables.py backend/tests/test_cpar_runtime_coverage_contract.py backend/tests/test_cpar_pipeline_runtime.py -q`
  - result: `104 passed`
- touched-module `py_compile`: passed

Post-implementation independent review:

- one blocker-level re-review concern was raised around `source_reads.py` still making a global fallback choice
- after inspection, that concern was not accepted as a blocker for this slice because the runtime-read gate is now request-scoped when tickers are specified, which is the user-facing path that previously dropped healthy requested rows; the remaining untargeted global fallback is intentional mixed-state protection until operational cutover completes

## 2026-03-26 Neon Registry-First Sync And Destructive Cutover Plan

### Current Delta Snapshot

Live Neon is still materially behind the repo target:

- the active Neon apply path still only applies `NEON_CANONICAL_SCHEMA.sql` plus optional holdings
- live Neon is missing the registry/current-state tables, source-sync audit tables, cUSE membership tables, and cPAR runtime coverage tables
- the repo does not yet implement `source_sync_runs`, `source_sync_watermarks`, or `security_source_status_current`
- live/read-path helpers still contain physical `security_master` fallback branches
- the active local runtime SQLite database at `backend/runtime/data.db` has not yet been bootstrapped onto the registry/current-state surfaces, so the current-state sync inputs are absent locally too

This means the operational work is not just “run the existing update.” The repo and the live database need one coordinated cutover.

### Cutover Objective

After this slice:

- Neon should contain the registry-first/current-state schema and no longer contain physical `security_master`
- current source publication should be auditable through `source_sync_runs` and `source_sync_watermarks`
- Neon should expose `security_source_status_current` as the current observed-readiness surface
- Neon reads for holdings, source reads, and cPAR source reads should no longer depend on `security_master`
- cUSE should persist `cuse_security_membership_daily` and `cuse_security_stage_results_daily`
- cPAR should persist `cpar_package_universe_membership` and `cpar_instrument_runtime_coverage_weekly`
- broad parity and cleanliness checks should prove that legacy Neon tables and columns are gone

### Schema Target

#### Tables To Exist In Neon After Cutover

- `security_registry`
- `security_taxonomy_current`
- `security_policy_current`
- `security_source_observation_daily`
- `security_ingest_runs`
- `security_ingest_audit`
- `security_master_compat_current`
- `security_source_status_current`
- `source_sync_runs`
- `source_sync_watermarks`
- `security_prices_eod`
- `security_fundamentals_pit`
- `security_classification_pit`
- `estu_membership_daily`
- `universe_cross_section_snapshot`
- `barra_raw_cross_section_history`
- `model_factor_returns_daily`
- `model_factor_covariance_daily`
- `model_specific_risk_daily`
- `model_run_metadata`
- `projected_instrument_loadings`
- `projected_instrument_meta`
- `cuse_security_membership_daily`
- `cuse_security_stage_results_daily`
- `cpar_package_runs`
- `cpar_proxy_returns_weekly`
- `cpar_proxy_transform_weekly`
- `cpar_factor_covariance_weekly`
- `cpar_instrument_fits_weekly`
- `cpar_package_universe_membership`
- `cpar_instrument_runtime_coverage_weekly`
- `serving_payload_current`
- `runtime_state_current`
- holdings tables already managed by the Neon holdings schema

#### Tables And Columns To Remove From Neon

- physical `security_master`
- any surviving `security_master` indexes such as `idx_security_master_ticker`, `idx_security_master_sid`, and `idx_security_master_permid`
- any surviving legacy `security_master` columns such as `sid`, `permid`, `instrument_type`, and `asset_category_description` by virtue of dropping the table rather than trimming it in place

`security_master_compat_current` remains by design. It is not a legacy table; it is the registry-era compatibility surface.

### Sync Durability Contract

The cutover must not publish a moving local source image into Neon.

Required contract:

- before any destructive or authoritative source publication begins, freeze writers against the chosen local source image or create a point-in-time SQLite copy and sync from that copy only
- `source_sync_runs` must record `started_at`, `completed_at`, `status`, `mode`, `selected_tables_json`, `table_results_json`, `error_type`, `error_message`, and `updated_at`
- `source_sync_watermarks` must use one row per table with `table_name` as the primary key and must record `sync_run_id`, `source_min_date`, `source_max_date`, `target_min_date`, `target_max_date`, `row_count`, and `updated_at`
- `security_source_status_current` must use one row per `ric`
- `source_sync_watermarks` and `security_source_status_current` must only advance after the selected source tables for that sync run finish successfully
- on failure, the sync run row must be written with `status = failed`, but watermarks and derived current status must remain at the previous successful state
- retries must create a new `sync_run_id`; failed runs are never overwritten in place

This means the sync implementation can commit table copies incrementally if needed for volume, but the run ledger, watermark advancement, and derived-current-state promotion must behave like a single post-success publication boundary

### Implementation Map

#### 1. DDL And Apply Surface

Files to change:

- `docs/reference/migrations/neon/NEON_CANONICAL_SCHEMA.sql`
- `docs/reference/migrations/neon/NEON_CPAR_SCHEMA.sql`
- `backend/scripts/neon_apply_schema.py`
- new cleanup SQL for destructive Neon cleanup after parity acceptance

Required edits:

- add registry/current-state tables to `NEON_CANONICAL_SCHEMA.sql`
- add `security_source_status_current`
- add `source_sync_runs`
- add `source_sync_watermarks`
- add `estu_membership_daily`
- add `universe_cross_section_snapshot`
- add `cuse_security_membership_daily`
- add `cuse_security_stage_results_daily`
- add missing cPAR runtime tables to `NEON_CPAR_SCHEMA.sql`
- update the apply script so one supported path applies canonical, cPAR, and holdings schema in the correct order
- add a dedicated destructive cleanup SQL surface for dropping `security_master` only after parity and read cutover are complete

#### 2. Source Sync Contract

Files to change:

- `backend/services/neon_stage2.py`
- `backend/services/neon_mirror.py`
- `backend/orchestration/stage_source.py`
- `backend/services/neon_authority.py`
- `backend/scripts/neon_sync_from_sqlite.py`

Required edits:

- remove `security_master` from `TABLE_CONFIGS`, `canonical_tables()`, parity anchors, and any current-state table lists for Neon
- add `estu_membership_daily` and `universe_cross_section_snapshot` to the sync/parity surface
- implement `source_sync_runs` write logic with a stable sync run id, start timestamp, completion timestamp, selected tables, per-table row/load summary, and final status
- implement `source_sync_watermarks` upserts only after the sync run succeeds for the selected source tables
- implement Neon materialization for `security_source_status_current` after source sync completes successfully
- make `source_sync` bootstrap the local runtime DB onto registry/current-state tables before validating and syncing, so the active `backend/runtime/data.db` becomes a valid source of truth for publication
- make the cutover and the ongoing sync flow operate against a frozen or copied local SQLite source image rather than a mutable live file
- make Neon rebuild readiness use the registry-first current-state surfaces plus the new sync/status surfaces, not physical `security_master`
- extend Neon-authoritative workspace preparation and local mirror expectations so the registry/current-state and gating-support tables needed for validation are present in the working image
- add regression coverage proving partial upload failure leaves previous watermarks and current-status rows untouched and that retries generate a new `sync_run_id`

#### 3. Read-Path Cutover

Files to change:

- `backend/data/source_reads.py`
- `backend/data/cpar_source_reads.py`
- `backend/data/holdings_reads.py`
- `backend/services/neon_holdings_identifiers.py`
- `backend/universe/runtime_rows.py`
- `backend/universe/selectors.py`
- `backend/universe/source_observation.py`
- `backend/universe/taxonomy_builder.py`

Required edits:

- remove physical `security_master` fallback from Neon code paths
- keep the runtime hierarchy as: `security_registry` + `security_policy_current` + optional `security_taxonomy_current` + `security_master_compat_current`
- make compat-only fallback use `security_master_compat_current`, not `security_master`
- fail clearly if Neon is missing the registry/current-state tables after cutover rather than silently rebuilding legacy semantics from `security_master`
- add an explicit repo-wide `rg` inventory gate proving no live Neon read or write surface still references physical `security_master` before destructive cleanup is allowed

#### 4. Writer And Backfill Paths

Files to change:

- `backend/data/model_output_writers.py`
- `backend/data/cpar_schema.py`
- `backend/data/cpar_writers.py`
- new orchestration script for end-to-end Neon cutover

Required edits:

- ensure cUSE Postgres writers and cPAR Postgres writers are schema-clean against the new DDL surface
- add one operational script that:
  - bootstraps local SQLite current-state tables
  - creates a point-in-time SQLite publication copy
  - applies Neon schema
  - runs source sync
  - runs cUSE rebuild/publish for the latest cutover date
  - runs cUSE historical sample rebuilds for a small retained-window validation set
  - runs cPAR package persist for the latest package date
  - runs cPAR historical package backfill for the retained package dates we intend to support
  - runs a stabilization verification pass before any destructive cleanup
  - runs destructive cleanup
  - runs final parity and cleanliness checks

#### 5. Diagnostics, Health, And Docs

Files to change:

- `backend/data/health_audit.py`
- `backend/services/data_diagnostics_sections.py`
- docs referencing Neon schema/apply/runbook behavior

Required edits:

- add the new audit/status tables and registry-first surfaces to diagnostics where appropriate
- remove any remaining operator guidance that suggests Neon can still be healed or understood through physical `security_master`

### Live Execution Order

The live execution order for the actual cutover should be:

1. land repo changes for the new DDL, sync contract, read cutover, and tests
2. freeze writers against the chosen local source image or create a point-in-time SQLite copy for publication
3. bootstrap that local source image onto registry/current-state tables from `security_registry_seed.csv`
4. verify the bootstrapped local source image contains the required registry/current-state tables before any Neon write begins
5. apply the additive Neon schema for canonical, cPAR, and holdings surfaces
6. run source sync from the frozen local SQLite image to Neon for:
   - registry/current-state source tables
   - source facts
   - `estu_membership_daily`
   - `universe_cross_section_snapshot`
   - existing cUSE/cPAR/model source tables already mirrored through broad sync
7. materialize Neon `security_source_status_current` and advance `source_sync_watermarks`
8. run the cUSE pipeline with a profile that repopulates model outputs and writes `cuse_security_membership_daily` / `cuse_security_stage_results_daily`
9. run historical cUSE sample rebuilds for retained-window validation dates
10. run the cPAR package pipeline so Neon is populated with `cpar_package_universe_membership` and `cpar_instrument_runtime_coverage_weekly`
11. run historical cPAR package backfill for the retained package dates we intend to preserve
12. run parity and live workflow checks while physical `security_master` still exists
13. complete the repo-wide `security_master` reference audit and require it to be clean for live Neon paths
14. complete a stabilization checkpoint:
   - rerun direct helper checks
   - rerun one no-op or overlap source sync
   - confirm no watermark drift or mixed-state regression
15. switch the remaining read paths to registry-first-only behavior
16. execute the destructive Neon cleanup that drops `security_master`
17. rerun parity, workflow, and cleanliness checks and do not accept the cutover until they pass

### Concrete Operational Steps To Execute

#### Repo Preparation

- update the DDL SQL files and apply script
- implement the missing source-sync audit/status materialization
- refactor Neon read paths off physical `security_master`
- add or update tests for:
  - schema SQL coverage
  - sync audit writes
  - `security_source_status_current` materialization
  - source-read SQL contracts without `security_master`
  - holdings and identifier-resolution SQL contracts without `security_master`
  - cPAR build-universe/runtime reads without `security_master`

#### Local Bootstrap

- create or lock a point-in-time SQLite publication image
- run `bootstrap_cuse4_source_tables` against that publication image
- verify row counts for:
  - `security_registry`
  - `security_policy_current`
  - `security_taxonomy_current`
  - `security_source_observation_daily`
  - `security_master_compat_current`
- verify the publication image also contains the gating-support tables expected downstream:
  - `estu_membership_daily`
  - `universe_cross_section_snapshot`
  - existing cUSE/model output tables required for the rebuild/publish steps or confirm those will be repopulated later in the cutover

#### Neon Schema Apply

- use the supported wrapper that applies canonical schema, then cPAR schema, then holdings schema
- verify the new tables exist before any data sync begins

#### Source Publication

- run source sync from the frozen publication image into Neon
- verify `source_sync_runs` shows one successful run
- verify `source_sync_watermarks` advanced for every synced table
- verify `security_source_status_current` row count matches active registry coverage expectations
- verify failed or interrupted dry-run behavior leaves the previous watermark and current-status state untouched

#### cUSE Backfill

- run `run_model_pipeline(profile='cold-core', as_of_date=<latest source date>)`
- require success through `serving_refresh`
- verify:
  - `cuse_security_membership_daily`
  - `cuse_security_stage_results_daily`
  - `serving_payload_current`
  - `model_run_metadata`

#### cUSE Historical Validation

- run `run_model_pipeline(profile='cold-core', as_of_date=<sample historical date>)` for a small retained-window validation set
- compare direct helper behavior on the sampled dates, not just the latest cutover date
- do not treat cached payload health as sufficient evidence for historical gating correctness

#### cPAR Backfill

- run `run_cpar_pipeline(profile='cpar-weekly' or explicit package date)`
- require successful `persist_package`
- verify:
  - `cpar_package_universe_membership`
  - `cpar_instrument_runtime_coverage_weekly`
  - `cpar_package_runs`

#### cPAR Historical Backfill

- rerun `run_cpar_pipeline(profile='cpar-package-date', as_of_date=<package date>)` across the retained package dates we intend to preserve
- verify the new cPAR runtime tables are populated for those dates, not just the latest package

#### Stabilization Checkpoint

- require one explicit post-cutover verification pass before destructive cleanup
- rerun direct helper and service-layer checks while physical `security_master` still exists
- rerun source sync in overlap/no-op mode and confirm:
  - a new `sync_run_id` is written
  - watermarks behave monotonically
  - `security_source_status_current` remains stable unless source facts actually changed
- only after that checkpoint may the destructive cleanup begin

#### Destructive Cleanup

- require a repo-wide `rg` proof that no live Neon read/write path still references physical `security_master`
- drop physical `security_master` from Neon only after all read-path refactors are deployed and validated
- verify no remaining runtime query touches it
- verify no legacy `security_master` indexes survive

### Acceptance Criteria

The cutover is not complete until all of the following are true:

- live Neon contains every registry-first table listed above
- live Neon does not contain physical `security_master`
- live Neon does not contain legacy `security_master` columns or indexes
- repo-wide inventory for live Neon code paths shows no physical `security_master` references remain
- holdings reads work
- cPAR portfolio/hedge workflows work
- cUSE latest universe/risk surfaces work
- cPAR search/ticker/portfolio surfaces work
- `source_sync_runs` and `source_sync_watermarks` are populated and current
- `security_source_status_current` is populated and row-consistent with registry/current-state tables
- parity samples for source/current-state tables match local SQLite on the retained window
- non-cached direct helper checks pass for:
  - `source_reads.load_latest_prices`
  - `source_reads.load_latest_fundamentals`
  - `cpar_source_reads.load_build_universe_rows`
  - `holdings_reads.load_holdings_positions`
- retained-window historical validation passes for sampled cUSE dates and supported cPAR package dates
- targeted spot checks show no remaining `information_schema.columns` references for `sid`, `permid`, `instrument_type`, or `asset_category_description` in app tables

### Plan Review Loops Required Before Implementation

Loop 1:

- send this plan to two independent reviewers
- accept or reject each critique explicitly
- update this section before any code edits

Loop 2:

- send the revised plan to two reviewers again
- resolve or explicitly defer their critiques
- only then begin implementation

### Execution Log Placeholder For This Cutover

- plan review loop 1: pending
- plan review loop 2: pending
- implementation: pending
- live Neon migration: pending
- destructive cleanup: pending
- final parity and cleanliness checks: pending

## 2026-03-26 Final Registry-First Neon Synchronization And Source-Integrity Plan

This section supersedes the older same-date cutover draft above.

### Current State Snapshot Before This Loop

Confirmed current state:

- live Neon already contains the registry-first/current-state tables:
  - `security_registry`
  - `security_taxonomy_current`
  - `security_policy_current`
  - `security_source_observation_daily`
  - `security_master_compat_current`
  - `security_source_status_current`
  - `source_sync_runs`
  - `source_sync_watermarks`
  - `cuse_security_membership_daily`
  - `cuse_security_stage_results_daily`
  - `cpar_package_universe_membership`
  - `cpar_instrument_runtime_coverage_weekly`
- live Neon no longer contains physical `security_master`
- live Neon no longer exposes the old `sid`, `permid`, or `asset_category_description` columns in app tables
- holdings reads, cUSE serving payload reads, and registry-first source reads are functioning again on live Neon
- cPAR latest package rerun succeeded after the factor-proxy duplicate fix and Neon now contains fresh `cpar_package_universe_membership` and `cpar_instrument_runtime_coverage_weekly`

The remaining acceptance blocker is local source durability:

- `backend/runtime/data.db` is the active local source archive and currently shows structural corruption in `security_prices_eod`
- the same corruption is present in the point-in-time cutover snapshot that was copied from that file
- Neon currently matches the logically iterable `security_prices_eod` rowset, but final acceptance cannot rely on a corrupted local archive

Execution rule for the rest of this loop:

- treat live Neon as mostly migrated already
- treat local SQLite source integrity, sync durability, and final cleanliness proof as the true remaining work
- do not reopen physical `security_master` or rebuild old compatibility paths

### Problem To Solve In This Final Loop

The project now has a clean registry-first Neon shape, but the local source archive that feeds source publication and authoritative rebuilds is not yet trustworthy enough to close the plan.

The final loop must ensure:

- local SQLite can no longer silently drift or corrupt `security_prices_eod`
- source sync refuses to publish from a structurally bad source archive
- the cutover helper refuses to mark the rollout healthy without metadata-table survival and rerun proof
- the final parity and cleanliness checks prove the new method is live end to end and that legacy Neon artifacts are gone

### Explorer Findings Accepted Into The Plan

Accepted from the source-integrity explorer:

- `backend/runtime/data.db` must be treated as an operationally corrupted live source archive, not a harmless historical artifact
- final acceptance requires either a clean rebuild of that file or a table-level logical repair that leaves `security_prices_eod` structurally sound
- parity audits alone are not enough because a bounded retained-window parity pass can look healthy while the source archive itself is corrupt
- the cutover helper and source-sync layer both need explicit source-integrity gates

Accepted from the schema/cutover explorer:

- post-cleanup verification should prove that rerun-critical metadata tables survived cleanup:
  - `source_sync_runs`
  - `source_sync_watermarks`
  - `security_source_status_current`
- the remaining runtime fallbacks must stay fenced to compat/current-state surfaces only
- final documentation should state that live acceptance depends on source integrity plus live rerun proof, not just additive DDL and spot reads

### Plan Review Loop 1: Accepted Critiques And Changes

Accepted from the first hostile plan-review loop:

- the local SQLite repair must be restart-safe and atomic:
  - repair work happens on a copied working database, not directly in the live source file
  - the live source file is only replaced by atomic rename after post-repair validation passes
- watermark rows are supporting metadata, not sufficient proof of correctness on their own
- source-sync success must check rows actually present in Neon after load, not just rows streamed into COPY
- post-cleanup proof must include a real sync-write probe that creates a fresh `sync_run_id` and updates live metadata tables
- final workflow proof must include the real cUSE serving-payload read path plus explicit registry-first completeness checks for the cPAR/shared-source surfaces

### Plan Review Loop 2: Accepted Critiques, Rejections, And Final Lock

Accepted from the second hostile plan-review loop:

- add an explicit repair helper/script; atomic local repair cannot remain a narrative-only step
- add actual Neon-side post-load assertions in `sync_from_sqlite_to_neon(...)`
- add a post-cleanup sync-write probe and not just read-only metadata checks
- add live-schema cleanliness probes for:
  - legacy columns
  - legacy indexes
- scope the “no legacy read path” claim to live Neon/runtime query paths rather than every compatibility test or sunset script in the repo

Rejected as blockers after direct code review:

- the claim that `sync_from_sqlite_to_neon(...)` is non-atomic across the selected data tables was not accepted
  - the selected table-copy work already runs inside one Postgres transaction and rolls back on failure
  - the metadata start/fail rows in `source_sync_runs` are intentionally outside that data-publication boundary so failures are still observable

Final implementation lock:

- the execution order is now fixed as:
  1. implement the repair helper and integrity/reporting functions
  2. harden `neon_stage2.py` with source-integrity and Neon-side post-load assertions
  3. harden `neon_registry_first_cutover.py` with post-cleanup sync-write and schema-cleanliness probes
  4. add and run regression coverage
  5. repair the local source archive
  6. rerun targeted source sync and final parity/cleanliness/workflow checks

### Final Execution Plan

#### Step 1. Repair The Local Source Archive

Goal:

- restore `backend/runtime/data.db` so `security_prices_eod` is structurally healthy and exact-row-count consistent

Implementation:

- create a dated filesystem backup of `backend/runtime/data.db` before any repair
- create a copied working database image from the live source file and perform the repair only on that working image
- repair `security_prices_eod` in the working image by logical rebuild into a fresh table:
  - read rows from the copied source image in deterministic order
  - insert into a freshly created replacement table with the canonical primary key
  - prefer the latest `updated_at` row when duplicate `(ric, date)` candidates ever appear
  - write a reconciliation report that records:
    - original `COUNT(*)`
    - original iterated row count
    - original distinct `(ric, date)` count
    - repaired row count
  - swap the repaired database into place only after population and validation succeed
- run `ANALYZE`
- run `PRAGMA quick_check`
- run `PRAGMA integrity_check`
- verify `security_prices_eod` three ways:
  - `COUNT(*)`
  - iterated row count
  - grouped distinct `(ric, date)` count

Acceptance note:

- because there is no guaranteed same-day clean upstream archive in the repo, the repaired canonical rowset for this loop is the readable deduplicated `(ric, date)` rowset from the copied source image, validated by reconciliation output and post-repair integrity checks

Required repo changes:

- add a dedicated repair helper or script so the repair path is explicit and repeatable
- document that the repair helper is for local source-archive durability, not for Neon directly

#### Step 2. Harden Source Publication Against Corrupt Local Archives

Goal:

- make source publication fail closed before any misleading success state is written

Implementation:

- in `backend/services/neon_stage2.py`:
  - add a source-integrity preflight for selected source tables before `source_sync_runs` is finalized as successful
  - specifically gate `security_prices_eod` on:
    - exact-row-count consistency
    - structural integrity checks when requested by the caller or when full source publication runs
  - keep the existing load-vs-count mismatch guard
  - add post-load target assertions for replace/reload paths so success depends on rows actually present in Neon after the load, not just rows streamed into COPY
  - add regression coverage proving no watermark/current-status advancement occurs after preflight failure
- in `backend/scripts/neon_registry_first_cutover.py`:
  - add a preflight that refuses to proceed when the chosen SQLite source image fails source-integrity checks
  - include source-integrity results in the run artifact summary

#### Step 3. Tighten Post-Cleanup Rerun Proof

Goal:

- require proof that the cleaned Neon schema can still support normal reruns after destructive cleanup

Implementation:

- extend `_run_post_cleanup_checks(...)` in `backend/scripts/neon_registry_first_cutover.py` to verify:
  - `source_sync_runs` exists and accepts a fresh sync-write probe
  - `source_sync_watermarks` exists and is updated by that probe
  - `security_source_status_current` exists and has expected registry-scale row coverage
  - holdings reads still work
  - cUSE payload reads still work
  - cPAR build-universe and factor-proxy reads still work
- make the proof use a real post-cleanup `sync_from_sqlite_to_neon(...)` probe against a small registry-first table set rather than a read-only metadata check
- add a test that cleanup success is rejected when any rerun-critical metadata surface is missing or when the post-cleanup sync-write probe fails

#### Step 4. Re-Sync From The Repaired Local Archive

Goal:

- bring Neon back into exact parity with the repaired source archive

Implementation:

- use the repaired `backend/runtime/data.db` or a fresh point-in-time snapshot from it as the publication source image
- rerun targeted source sync at minimum for `security_prices_eod`
- if any registry/current-state tables changed during repair or refresh, rerun the broader source-sync set as well
- confirm:
  - new `sync_run_id`
  - successful status in `source_sync_runs`
  - updated `source_sync_watermarks`
  - stable `security_source_status_current`

Important clarification:

- `source_sync_watermarks` are treated as sync metadata, not as standalone proof that the publication is correct
- the proof boundary is:
  - successful sync run status
  - post-load target assertions
  - parity against the repaired local archive

#### Step 5. Run Final Parity, Legacy-Cleanliness, And Workflow Proofs

Goal:

- close the plan with direct evidence that the new method is live and clean

Implementation:

- rerun parity on the repaired local SQLite source against Neon for:
  - `security_registry`
  - `security_taxonomy_current`
  - `security_policy_current`
  - `security_source_observation_daily`
  - `security_master_compat_current`
  - `security_source_status_current`
  - `security_prices_eod`
  - `security_fundamentals_pit`
  - `security_classification_pit`
  - `cpar_package_universe_membership`
  - `cpar_instrument_runtime_coverage_weekly`
- run legacy-cleanliness probes in Neon:
  - confirm physical `security_master` is absent
  - confirm no `security_master` indexes survive
  - confirm no app tables still expose legacy columns:
    - `sid`
    - `permid`
    - `asset_category_description`
    - `instrument_type`
- run workflow checks:
  - holdings account read
  - holdings position read
  - latest prices read
  - latest fundamentals read
  - cPAR build-universe read
  - cPAR factor-proxy read
  - cUSE serving payload read
- run registry-first completeness checks:
  - `security_registry`, `security_policy_current`, `security_taxonomy_current`, and `security_master_compat_current` all exist and have expected row coverage
  - `load_build_universe_rows()` is succeeding with registry/policy present, not because physical `security_master` exists

### Implementation Seams

Primary edit surfaces:

- `backend/services/neon_stage2.py`
- `backend/scripts/neon_registry_first_cutover.py`
- `backend/tests/test_neon_stage2_model_tables.py`
- `backend/tests/test_neon_stage2_parity_audit.py`
- `docs/architecture/UNIVERSE_REGISTRY_AND_MODEL_GATING_PLAN.md`

Expected new helper surface:

- one new local SQLite repair script under `backend/scripts/`

Operational surfaces expected to be executed, not necessarily edited:

- `backend/scripts/neon_apply_schema.py`
- `backend/scripts/neon_sync_from_sqlite.py`
- the repaired `backend/runtime/data.db`

### Review Loop Checklist For This Final Loop

- [x] explorer review of schema/cutover/runtime loose ends collected
- [x] explorer review of source-integrity/parity loose ends collected
- [x] plan review loop 1 completed
- [x] plan review loop 2 completed
- [x] final implementation sequence locked

### Validation Matrix For This Final Loop

Required repo validation:

- `backend/tests/test_neon_stage2_model_tables.py`
- `backend/tests/test_neon_stage2_parity_audit.py`
- `backend/tests/test_neon_registry_first_cutover.py`
- `backend/tests/test_neon_authority.py`
- `backend/tests/test_holdings_reads.py`
- `backend/tests/test_core_reads.py`
- `backend/tests/test_cpar_source_reads.py`
- touched-module `py_compile`
- path-scoped `git diff --check`

Required operational validation:

- local SQLite repair verification
- successful source sync from the repaired local archive
- parity rerun against the repaired local archive
- legacy-cleanliness probe against live Neon
- direct workflow smoke checks against live Neon

### Execution Update: 2026-03-26 Final Synchronization, Repair, And Proof

Implemented in this loop:

- `backend/services/neon_stage2.py`
  - added source-integrity inspection for `security_prices_eod`
  - added sync-time optional source-integrity enforcement
  - added Neon-side post-load row-count assertions before successful sync finalization
  - fixed the parity-audit helper to use the correct Postgres table-existence function
- `backend/scripts/neon_sync_from_sqlite.py`
  - added `--verify-source-integrity`
  - added `--run-sqlite-integrity-check`
- `backend/scripts/neon_registry_first_cutover.py`
  - added source-integrity preflight recording
  - added post-cleanup sync-write probe
  - added live legacy-schema/index cleanliness probe
  - added real cUSE serving-payload read checks to post-cleanup verification
- `backend/scripts/repair_security_prices_archive.py`
  - added a repeatable working-copy repair helper for `security_prices_eod`
  - the helper rebuilds the table logically, rewrites the full file with `VACUUM`, validates post-repair integrity, and can be used for atomic replacement
- `backend/data/cpar_source_reads.py`
  - escaped the literal `CONSOLIDATED` pattern for the Neon/Psycopg path so live factor-proxy resolution no longer fails on `%` placeholder parsing

Repo validation executed after implementation:

- `backend/tests/test_neon_stage2_model_tables.py`
- `backend/tests/test_neon_stage2_parity_audit.py`
- `backend/tests/test_neon_registry_first_cutover.py`
- `backend/tests/test_repair_security_prices_archive.py`
- `backend/tests/test_neon_authority.py`
- `backend/tests/test_holdings_reads.py`
- `backend/tests/test_core_reads.py`
- `backend/tests/test_cpar_source_reads.py`
- consolidated result:
  - `79 passed`
- touched-module `py_compile`:
  - passed
- path-scoped `git diff --check`:
  - passed

Operational execution completed:

- repaired the live local source archive at `backend/runtime/data.db`
- preserved the pre-repair source archive at:
  - `backend/offline_backups/data_pre_prices_repair_20260326T213058Z.db`
- local source-integrity state before repair:
  - `security_prices_eod COUNT(*) = 10001880`
  - iterated row count `= 10001861`
  - distinct `(ric, date)` row count `= 10001861`
- local source-integrity state after repair:
  - `COUNT(*) = 10001861`
  - iterated row count `= 10001861`
  - distinct `(ric, date)` row count `= 10001861`
  - `quick_check = ok`
  - `integrity_check = ok`
- reran authoritative Neon source sync for `security_prices_eod` from the repaired local archive:
  - `sync_run_id = source_sync_20260326T213303872035Z_701ecc70`
  - `status = ok`
  - `action = truncate_and_reload`
  - `source_rows = 10001861`
  - `rows_loaded = 10001861`
  - Neon-side post-load row validation passed
  - `security_source_status_current_rows = 5895`

Final parity and cleanliness proof:

- SQLite vs Neon parity is `ok` for:
  - `security_registry`
  - `security_taxonomy_current`
  - `security_policy_current`
  - `security_source_observation_daily`
  - `security_master_compat_current`
  - `security_prices_eod`
  - `security_fundamentals_pit`
  - `security_classification_pit`
- `security_prices_eod` parity after repair:
  - source row count `= 10001861`
  - target row count `= 10001861`
  - min/max dates match
  - latest distinct RIC count matches
  - duplicate-group counts are zero on both sides
- live schema cleanliness probe:
  - physical `security_master` absent
  - legacy columns absent
  - legacy indexes absent
- current-state and metadata probe:
  - `security_registry_rows = 5895`
  - `security_source_status_current_rows = 5895`
  - latest `source_sync_runs` row is the repaired-archive resync and is `ok`
  - `source_sync_watermarks.security_prices_eod.row_count = 10001861`

Live workflow spot checks completed:

- holdings:
  - account count `= 2`
  - `ibkr_multistrat` position rows `= 24`
- source reads:
  - latest prices rows `= 3816`
- cPAR shared-source reads:
  - build-universe rows `= 5895`
  - factor-proxy rows for `SPY` / `QQQ` `= 2`
- cUSE serving payload reads:
  - `universe_loadings` keys `= 18`
  - `risk` keys `= 12`
  - `portfolio` keys `= 7`
- cPAR package/runtime coverage parity sample:
  - local latest package date `= 2026-03-20`
  - local latest package membership rows `= 5895`
  - local latest runtime coverage rows `= 5895`
  - Neon latest package membership rows `= 5895`
  - Neon latest runtime coverage rows `= 5895`

One operational caveat remains recorded:

- the direct live `core_reads.load_latest_fundamentals()` helper was materially slower than the other smoke paths during this loop, so the repo now has:
  - passing `test_core_reads.py`
  - clean fundamentals parity against Neon
  - but not a fast manual live-helper timing baseline yet

### Execution Update: 2026-03-26 LSEG Session, Selector, And Runtime Compatibility Hardening

Problem intake for this slice:

- the review pass found five concrete repo-side defects:
  - pending seed names could self-lock out of PIT ingest because selector inclusion depended on already-populated PIT policy bits
  - raw cross-section rebuilds could fail with `no-runtime-identity` on partially bootstrapped databases where `security_registry` existed but was empty
  - historical classification loading was not schema-tolerant for thin legacy fixtures, which broke cUSE membership persistence through `persist_model_outputs()`
  - historical price backfill depended on one exact LSEG SDK module layout and lacked open/close failure hardening
  - the missing-close regression test fixture itself was broken and never reached the code path it claimed to test
- adversarial plan review added two important tightening points:
  - the raw-history rebuild still had to honor the named default-source-universe filter, not just per-date runtime eligibility
  - the runtime compatibility fix had to stay bounded so current registry-first reads did not silently relax back to `security_master`

Implementation plan accepted before code changes:

- harden LSEG session lifecycle and portability without changing live caller semantics
- make pending-seed PIT scope deterministic at the selector layer and align PIT backfill counting with that behavior
- add a bounded runtime compatibility seam for historical/model-output rebuild paths only
- make historical classification loading schema-tolerant across thin compatibility fixtures
- restore default-source-universe filtering inside raw cross-section rebuilds
- repair the missing-close/backfill test harness and add script-level cleanup coverage
- validate with the exact failing review slice first, then rerun the broader registry/gating and Neon-adjacent regression buckets

Implemented in this loop:

- `backend/vendor/lseg_toolkit/client/session.py`
  - added environment override support for Workspace log-root discovery
  - added a `Definition`-optional open path with fallback to legacy `rd.open_session(...)`
  - moved default-session registration until after successful open
  - added open-failure rollback and best-effort default-session cleanup on close
- `backend/universe/selectors.py`
  - removed the erroneous `pit_enabled` dependency from pending-seed PIT inclusion
  - made `load_price_ingest_scope_rows(..., include_pending_seed=...)` honor the parameter instead of discarding it
- `backend/scripts/backfill_pit_history_lseg.py`
  - changed PIT universe counting to include pending-seed names so completeness checks match the selector contract
- `backend/universe/runtime_rows.py`
  - made historical classification reads tolerate thin compatibility schemas by dynamically selecting available columns
  - added `allow_empty_registry_fallback` for bounded legacy-compatible historical/runtime reconstruction
- `backend/risk_model/raw_cross_section_history.py`
  - switched historical runtime identity loads onto the bounded compatibility path
  - restored intersection with the named default-source-universe selector when that selector is populated
  - fixed `beta_raw` assignment for single-name/small-group rebuilds so pandas no longer returns a multi-column frame on assignment
- `backend/risk_model/cuse_membership.py`
  - switched multi-date membership payload construction onto the bounded compatibility runtime path
- `backend/tests/test_lseg_session_manager.py`
  - added rollback coverage for failed open
  - added legacy `rd.open_session(...)` fallback coverage
  - added cleanup coverage for close failures
- `backend/tests/test_security_master_lineage.py`
  - repaired the broken missing-close fixture
  - updated the fake-LSEG import helper so backfill tests do not inherit the real SDK session module
  - added script-level coverage proving backfill closes the managed session after a failed batch

Validation executed after implementation:

- focused failure-to-green slice:
  - `backend/tests/test_lseg_session_manager.py`
  - `backend/tests/test_security_master_lineage.py::test_raw_cross_section_history_uses_date_specific_runtime_eligibility`
  - `backend/tests/test_security_master_lineage.py::test_backfill_prices_allows_explicit_pending_ric`
  - `backend/tests/test_security_master_lineage.py::test_download_from_lseg_skips_price_rows_with_missing_close`
  - `backend/tests/test_security_master_lineage.py::test_backfill_prices_skips_rows_with_missing_close`
  - `backend/tests/test_security_master_lineage.py::test_backfill_prices_closes_managed_session_after_failed_batch`
  - `backend/tests/test_cuse_membership_contract.py::test_persist_model_outputs_writes_cuse_membership_and_stage_rows`
  - `backend/tests/test_universe_selector_parity.py`
- broader targeted validation:
  - `backend/tests/test_lseg_session_manager.py backend/tests/test_security_master_lineage.py backend/tests/test_universe_selector_parity.py backend/tests/test_cuse_membership_contract.py backend/tests/test_core_reads.py backend/tests/test_holdings_reads.py backend/tests/test_cpar_source_reads.py backend/tests/test_security_registry_sync.py`
    - `84 passed`
  - `backend/tests/test_neon_authority.py`
    - `12 passed`
  - `backend/tests/test_neon_mirror_integration.py`
    - `7 passed`
  - `backend/tests/test_neon_stage2_model_tables.py`
    - `14 passed`
  - `backend/tests/test_neon_registry_first_cutover.py`
    - `5 passed`
- touched-module `py_compile`:
  - passed
- path-scoped `git diff --check`:
  - passed

Commit packaging for this slice:

- commit boundary 1:
  - LSEG session lifecycle and backfill/test harness hardening
  - files:
    - `backend/vendor/lseg_toolkit/client/session.py`
    - `backend/tests/test_lseg_session_manager.py`
    - `backend/tests/test_security_master_lineage.py`
- commit boundary 2:
  - selector/runtime/raw-history/cUSE compatibility fixes
  - files:
    - `backend/universe/selectors.py`
    - `backend/universe/runtime_rows.py`
    - `backend/risk_model/raw_cross_section_history.py`
    - `backend/risk_model/cuse_membership.py`
    - `backend/scripts/backfill_pit_history_lseg.py`
- commit boundary 3:
  - plan-doc execution record update
  - files:
    - `docs/architecture/UNIVERSE_REGISTRY_AND_MODEL_GATING_PLAN.md`

Current disposition:

- repo-side defects from the latest review set are fixed
- broader registry-first authority and Neon sync work remains packaged separately from this hardening slice
- one long-running `test_refresh_profiles.py` validation bucket was investigated and found to have been confounded by orphaned overlapping pytest processes during ad hoc probing; it should be run only once in a clean process when the final commit train is staged
