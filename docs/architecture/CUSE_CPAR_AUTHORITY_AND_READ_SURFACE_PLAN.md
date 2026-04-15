# cUSE / cPAR Authority And Read-Surface Plan

Date: 2026-04-14
Status: Implemented remediation record and residual hardening plan for cUSE/cPAR authority, serving, projection, and coverage semantics
Owner: Codex

This document is the implementation anchor and permanent remediation record for the 2026-04-15 cUSE/cPAR authority recovery.

It exists to prevent the repo from solving these issues by adding more late-stage conditionals and compatibility branches. The work should instead tighten artifact ownership, identifier semantics, date semantics, and publication/read-path contracts so the system becomes easier to reason about and harder to degrade silently.

Related active docs:
- [ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/architecture/ARCHITECTURE_AND_OPERATING_MODEL.md)
- [UNIVERSE_REGISTRY_AND_MODEL_GATING_PLAN.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/architecture/UNIVERSE_REGISTRY_AND_MODEL_GATING_PLAN.md)
- [CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/architecture/CPAR_ARCHITECTURE_AND_OPERATING_MODEL.md)
- [CPAR_BACKEND_READ_SURFACES.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/architecture/CPAR_BACKEND_READ_SURFACES.md)
- [OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/operations/OPERATIONS_PLAYBOOK.md)
- [CPAR_OPERATIONS_PLAYBOOK.md](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/docs/operations/CPAR_OPERATIONS_PLAYBOOK.md)

## Problem Summary

The current failures share one theme: the repo is still letting read and publish paths invent or compress truth too late.

Observed failures:
- cUSE can rebuild a healthy modeled universe and still publish a degraded one because publish-time membership overlay and projection semantics are too fragile.
- cUSE quote and typeahead are broader than the currently served model payload, but the repo does not clearly separate discoverability from modeling readiness.
- cUSE projected ETFs and non-core equities do not have a clean artifact boundary, so a broken projection load can look like a valid but empty state.
- cPAR active package rows are healthy, but portfolio/dashboard paths can still show `missing_price` and no useful loadings because package fit truth, runtime coverage truth, holdings identity, and live-source valuation are being collapsed too aggressively.
- cPAR portfolio coverage labeling currently treats `missing_price` as a catch-all, which hides identifier mismatch and missing-fit failures.

Methodology clarification:
- cUSE secondary names are excluded from core estimation and receive projected loadings after core estimation via explicit projection paths.
- cPAR extended names are still direct active-package fit targets; they are not a separate post-core projection family analogous to cUSE secondary projections.

What this creates:
- split-brain authority
- stale-but-green reuse/publish states
- semantic drift between package truth, source truth, and UI labels

## Validated Live Findings

These findings were validated directly against the live Neon project on 2026-04-15 via the Neon CLI and repo-local read paths.

### cUSE

- The live published `universe_loadings` payload is the degraded snapshot:
  - `ticker_count = 504`
  - `core_estimated_ticker_count = 0`
  - `projected_only_ticker_count = 0`
  - `as_of_date = 2026-03-13`
- `SPY`, `QQQ`, `URA`, `XLE`, and `SMH` are present only as:
  - `model_status = ineligible`
  - `model_status_reason = projection_unavailable`
  - `projection_method = ols_returns_regression`
  - zero exposures
- `ASML`, `AAL`, and `AAPL` are absent entirely from the live served universe payload.
- The latest successful core rebuild did not write a collapsed membership history. It wrote a mixed-date membership history for `model_run_20260415T052414Z`:
  - `3655` rows at `2026-03-31`
  - `504` rows at `2026-04-13`
  - total persisted membership rows: `4159`
- The split is semantically coherent:
  - `2026-03-31` contains the core/fundamental side:
    - `2885` `core_estimated` + `173` `projected_fundamental` + `592` `ineligible`
  - `2026-04-13` contains the returns-projection side:
    - `70` `projected_returns` + `434` `projection_unavailable`
- Concrete examples from the same successful run:
  - `AAL` persisted as `core_estimated` on `2026-03-31`
  - `ASML` persisted as `projected_fundamental` on `2026-03-31`
  - `SPY`, `QQQ`, `URA`, `XLE`, and `SMH` persisted as `projected_returns` on `2026-04-13`
- The stage-7 publish failure was therefore not “membership persistence only wrote 504 rows.” It was:
  - `refresh_persistence` re-read membership truth via `load_cuse_membership_rows(as_of_dates=None)`
  - that helper resolves to `MAX(as_of_date)` only
  - publish therefore saw only the `2026-04-13` slice (`504` rows), treated it as current truth, and correctly blocked the `4159`-row candidate with:
    - `matched=504`
    - `missing=3655`
    - `total=4159`
- This means the current cUSE blocker is now precisely identified:
  - membership history is mixed-date by design
  - publish authority is incorrectly derived from “latest date in history” rather than from the candidate run’s own membership artifact or an explicit active membership snapshot

This confirms the live cUSE issue is not hypothetical. The current served snapshot in Neon is the broken one.

### cPAR

- The active cPAR package in Neon is healthy at the package level:
  - `package_run_id = cpar_crj_0ff4c9a3050b`
  - `package_date = 2026-04-10`
  - `universe_count = 5812`
  - `fit_ok_count = 3083`
- The live holdings account `ibkr_multistrat` contains 24 positions, including:
  - `ASML.OQ`
  - `QQQ.OQ`
  - `SPY.P`
  - `URA.P`
- The aggregate cPAR risk payload is completely broken:
  - `covered_positions_count = 0`
  - all 24 positions land in `missing_price`
- For names such as `SPY.P`, `QQQ.OQ`, `URA.P`, `ASML.OQ`, and `APO.N`, direct shared source reads return valid 2026-04-10 prices.
- For those same names, the active cPAR fit rows in Neon report:
  - `fit_status = ok`
  - `price_on_package_date_status = missing`
  - `portfolio_use_status = missing_price`
- The holdings book also contains at least one canonical-identity mismatch case:
  - holdings row `IBKR.O`
  - active cPAR fit resolves as `IBKR.OQ`
  - aggregate payload currently labels the holdings row `missing_price` instead of an identity/fit mismatch state

This confirms two separate live cPAR failures:
- persisted runtime coverage for the active package is wrong
- holdings/package identifier mismatch is being mislabeled as `missing_price`

## Implementation Status

Completed on 2026-04-15:
- Phase 2 started with an upstream cPAR runtime-coverage repair in [backend/data/cpar_outputs.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/data/cpar_outputs.py).
- Package-time price presence now reads through the authoritative shared-source price reader instead of the direct SQLite-only path.
- Regression coverage was added in:
  - [backend/tests/test_cpar_outputs_local_regression.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/tests/test_cpar_outputs_local_regression.py)
  - [backend/tests/test_cpar_outputs_neon_primary.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/tests/test_cpar_outputs_neon_primary.py)
- cPAR portfolio coverage labeling now prefers `missing_cpar_fit` over `missing_price` when no persisted fit row exists for the holdings RIC in [backend/services/cpar_portfolio_snapshot_service.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/services/cpar_portfolio_snapshot_service.py).
- That labeling change is intentionally narrow. It fixes the current misclassification without yet introducing holdings-to-package canonicalization heuristics.
- cPAR portfolio support-row assembly now resolves unresolved holdings RICs to a unique active-package fit by ticker before coverage classification:
  - batched package-fit lookup by ticker was added in [backend/data/cpar_queries.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/data/cpar_queries.py) and [backend/data/cpar_outputs.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/data/cpar_outputs.py)
  - the aliasing seam was added in [backend/services/cpar_portfolio_snapshot_service.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/services/cpar_portfolio_snapshot_service.py)
  - portfolio hedge and what-if read paths now pass positions into that resolver so raw holdings identifiers can remain visible while package-fit joins use the canonical active-package RIC
- This is intentionally package-authoritative for cPAR coverage. It does not yet introduce a broader registry-backed identity service for non-package discovery surfaces.
- A live production `cpar-package-date` rebuild for `2026-04-10` was dispatched and completed successfully:
  - pipeline run: `cpar_crj_1dc2fb114459`
  - Cloud Run execution: `ceiora-prod-cpar-build-knjzc`
  - new successful Neon package row was written for `package_date = 2026-04-10`
- That live rebuild did **not** clear the broken cPAR runtime-coverage state:
  - active-package fit rows for `SPY.P`, `QQQ.OQ`, `URA.P`, `ASML.OQ`, `APO.N`, and `IBKR.OQ` still reported `price_on_package_date_status = missing`
  - aggregate cPAR holdings risk remained effectively uncovered, with `covered_positions_count = 0`
- The execution metadata shows the production cPAR build job ran image `us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/control:ab737d3-stagemetrics2`, so the operational conclusion is:
  - rebuild dispatch is healthy
  - authority writes are healthy
  - but the production build job is still running an older image that does not contain the runtime-coverage persistence fix
- Based on adversarial review, `cpar-weekly` was not immediately dispatched afterward because on the current date it is expected to resolve to the same `2026-04-10` weekly anchor, which would add noise rather than a distinct validation step.
- The updated control and serve images were then built and deployed to production as:
  - `us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/control:88a53d4-cusephase1`
  - `us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/serve:88a53d4-cusephase1`
- After the redeploy, a fresh `cpar-package-date` rebuild for `2026-04-10` completed on the new control image and repaired the persisted runtime-coverage rows in Neon:
  - `SPY.P`, `QQQ.OQ`, `URA.P`, `ASML.OQ`, `APO.N`, and `IBKR.OQ` now report `price_on_package_date_status = present`
  - the production-backed cPAR aggregate payload is healthy again with `portfolio_status = ok`, `covered_positions_count = 24`, `missing_price = 0`, and `missing_cpar_fit = 0`
- Phase 3 has now started on the cUSE side:
  - a live-regression publish guard was added in [backend/analytics/reuse_policy.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/analytics/reuse_policy.py), [backend/analytics/refresh_publication.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/analytics/refresh_publication.py), and [backend/analytics/refresh_persistence.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/analytics/refresh_persistence.py) so a materially regressed modeled universe cannot silently replace a healthy live one
  - search and ticker payloads now surface `whatif_ready` / `whatif_ready_detail` in [backend/services/cuse4_universe_service.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/services/cuse4_universe_service.py)
  - the cUSE what-if preview backend now rejects staged rows that do not have a currently published modeled surface in [backend/services/cuse4_portfolio_whatif.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/services/cuse4_portfolio_whatif.py)
  - the frontend what-if builder now disables non-ready typeahead rows and blocks staging of non-ready tickers in [frontend/src/features/whatif/WhatIfBuilderPanel.tsx](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/frontend/src/features/whatif/WhatIfBuilderPanel.tsx) and [frontend/src/features/whatif/useWhatIfScenarioLab.ts](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/frontend/src/features/whatif/useWhatIfScenarioLab.ts)
- Production validation of the deployed cUSE tranche:
  - `/api/universe/ticker/URA` now reports `whatif_ready = false` with an explicit reason instead of implying the ticker is missing from the universe
  - posting `URA` to `/api/portfolio/whatif` now returns HTTP `400` with a concrete rejection message rather than silently producing an unmodeled preview
- The cUSE modeled universe itself is still not healthy. The live payload remains the degraded `504`-ticker snapshot, so the remaining Phase 3/4 work is still required to restore projected ETF and non-core equity coverage rather than merely guard and label the failure.
- The first attempt to restore cUSE through `serve-refresh` on the `88a53d4-cusephase2` image failed by design:
  - `serve-refresh` refused to run because `risk_cache_missing`
  - this confirmed the lane is behaving correctly as a serving-only path and cannot bootstrap missing core artifacts
- The next attempt through `core-weekly` on the same image also failed before model execution:
  - the Cloud Run execution reached `neon_readiness` and failed with `empty_table:model_factor_covariance_daily`, `empty_table:model_specific_risk_daily`, `empty_table:model_run_metadata`, and `stale_raw_history_vs_sources`
  - this validated that the right recovery lane for the current Neon state is `cold-core`, not `core-weekly`
- While debugging the cUSE recovery path, direct Neon validation exposed a second structural issue:
  - `cuse_security_membership_daily` already records `SPY`, `QQQ`, `URA`, `XLE`, and `SMH` as `served` projected-returns names on `2026-04-10` with `served_exposure_available = 1`
  - but authoritative Neon tables `projected_instrument_loadings` and `projected_instrument_meta` are completely empty
  - this means the current system can carry membership truth for projected ETFs without having a durable projection-authority store for serving lanes to reload from
- To correct that stability gap, projected ETF loadings now persist to Neon as well as local SQLite in [backend/risk_model/projected_loadings.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/risk_model/projected_loadings.py).
  - regression coverage was added in [backend/tests/test_projected_loadings.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/tests/test_projected_loadings.py)
  - projection-serving cadence coverage continues to pass in [backend/tests/test_projection_only_serving_cadence.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/tests/test_projection_only_serving_cadence.py)
- That projection-authority fix was built and deployed to the production control surface as:
  - `us-east4-docker.pkg.dev/project-4e18de12-63a3-4206-aaa/ceiora-images/control:88a53d4-cusephase3`
  - control service revision: `ceiora-prod-control-00034-tjk`
- A production `cold-core` rebuild was then dispatched on the `88a53d4-cusephase3` image:
  - pipeline run: `crj_c9cf99d8063a`
  - Cloud Run execution: `ceiora-prod-cold-core-6mjcj`
  - this is the correct bootstrap/recovery lane for the current Neon-authority state because it tolerates missing covariance/specific-risk/model metadata and stale raw-history state while rebuilding those artifacts from scratch
- The live outcome of `crj_c9cf99d8063a` is still in progress at the time of this update, so the cUSE recovery validation remains open.
- The expensive retry of `crj_c9cf99d8063a` was then cancelled intentionally once the root cause had narrowed to serving publication rather than core rebuild math.
- The latest cUSE recovery conclusion is now:
  - no additional `cold-core` run should be used as the next debugging step
  - the current Neon model artifacts are sufficient for diagnosis
  - the next recovery attempt should happen only after the cUSE membership-authority/publish contract is fixed and deployed
- That membership-authority fix was then deployed through the `88a53d4-cusephase5` control image and validated in production:
  - `serve-refresh` execution `ceiora-prod-serve-refresh-hrjlg` completed successfully
  - `runtime_state_current.refresh_status` returned to `status = ok`
  - `serving_payload_current` advanced to `model_run_20260415T060605Z`
  - live cUSE recovered to `ticker_count = 3971`, `core_estimated_ticker_count = 2983`, and `projected_only_ticker_count = 188`
  - `AAL` and `AAPL` are again served as `core_estimated`
  - `ASML` is again served as `projected_only` with `projection_method = native_characteristic_projection`
- That production recovery also isolated one final cUSE serving defect:
  - authoritative Neon tables now contain valid returns-projected ETF rows for `SPY.P`, `QQQ.OQ`, `URA.P`, `XLE.P`, and `SMH.OQ`
  - but the live served universe still downgrades those names to `projection_unavailable`
  - the remaining bug is a serving-only date contract problem: `serve-refresh` is still binding projected ETF reloads to the active core package date instead of the latest persisted projection package date
- A local Phase 3 follow-up patch now addresses that remaining seam in [backend/analytics/pipeline.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/analytics/pipeline.py):
  - serving-only lanes now reuse `projection_package_asof` from persisted projected artifacts when it is newer than the active core state date
  - refresh metadata now persists `projection_package_asof` explicitly so publish-time validation can reason about the correct projection artifact date
  - regression coverage now includes:
    - refresh-meta precedence for projection date selection
    - serving reuse of a newer persisted projection package without recomputing projections
    - downgrade rejection if projection-only tickers are still served as native/ineligible
- Adversarial review on that follow-up patch raised one real architectural caveat:
  - non-production workspace/canonical fallback paths can still blend projection artifacts from different authorities if they share an `as_of_date`
  - this does not block the current production recovery because production `serve-refresh` reads from one Neon authority, but the broader run/package identity issue remains open for later hardening
- Live validation after deploying the projection-date patch showed a narrower remaining bug:
  - `serve-refresh` was successfully loading persisted projected ETF rows at `projection_package_asof = 2026-04-13`
  - but ETF names were still missing from the served universe because the serving lane was discovering the returns-projection scope from local SQLite selector rows instead of the authoritative Neon membership artifact
  - and the projected exposure map was still too brittle against factor-label punctuation drift (`Academic & Educational Services` vs `Academic Educational Services`)
- The final cUSE Phase 3 fixes were then implemented and deployed through `88a53d4-cusephase8`:
  - [backend/data/cuse_membership_reads.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/data/cuse_membership_reads.py) now exposes authoritative latest returns-projection scope rows from persisted cUSE membership history
  - [backend/analytics/pipeline.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/analytics/pipeline.py) now merges that authoritative scope into `projection_universe_rows` on serving-only lanes instead of depending on SQLite selector state
  - [backend/analytics/services/universe_loadings.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/analytics/services/universe_loadings.py) now resolves projected factor tokens through the current factor identity system before mapping them into served factor IDs
  - regression coverage was added in:
    - [backend/tests/test_projection_only_serving_cadence.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/tests/test_projection_only_serving_cadence.py)
    - [backend/tests/test_universe_loadings_service.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/tests/test_universe_loadings_service.py)
- Production `serve-refresh` execution `ceiora-prod-serve-refresh-jzkb5` then completed successfully and restored the full intended cUSE behavior:
  - live `universe_loadings` now serves `ticker_count = 4159`, `core_estimated_ticker_count = 2983`, and `projected_only_ticker_count = 258`
  - `SPY`, `QQQ`, `URA`, `XLE`, and `SMH` now resolve as `projected_only`
  - those ETF rows now carry `exposure_origin = projected_returns`, `projection_method = ols_returns_regression`, `projection_asof = 2026-04-13`, 45 factor exposures each, and non-null specific risk
  - `AAL` and `AAPL` remain healthy `core_estimated` names
  - `ASML` remains healthy as `projected_only` with `projection_method = native_characteristic_projection`

## Recovery Summary

The 2026-04-15 recovery is complete.

- cPAR is healthy again because package-time runtime coverage now uses the authoritative shared-source price path, portfolio coverage no longer collapses identifier-mismatch and no-fit cases into `missing_price`, and the repaired control image was used to rebuild the active `2026-04-10` package.
- cUSE is healthy again because serving publish now uses the candidate run's membership artifact rather than `MAX(as_of_date)` history, projected ETF outputs persist durably to Neon, serving-only lanes honor the latest projection package date, returns-projection scope is sourced from authoritative persisted membership truth, and projected factor tokens are resolved through the factor identity catalog before serving assembly.
- Normal maintenance lanes no longer depend on `cold-core` just to advance served loadings freshness:
  - `source-daily-plus-core-if-due` and `core-weekly` now rebuild a recent daily raw-history window before serving
  - the raw-history builder now gates date slices through date-appropriate runtime/source-observation membership when that history exists, rather than reusing one current runtime universe across every historical date in the batch
- The live end state after `ceiora-prod-serve-refresh-jzkb5` is the intended one:
  - `ticker_count = 4159`
  - `core_estimated_ticker_count = 2983`
  - `projected_only_ticker_count = 258`
  - `SPY`, `QQQ`, `URA`, `XLE`, and `SMH` serve as `projected_only` via `projected_returns`
  - `AAL` and `AAPL` serve as `core_estimated`
  - `ASML` serves as `projected_only` via `native_characteristic_projection`

## Residual Hardening

The following items remain worthwhile, but they are not blockers for the repaired production state:

- harden non-production workspace/canonical fallback so projection artifacts cannot blend just because they share an `as_of_date`
- add stricter fail-closed checks for partial projected factor-vector loss or factor-label drift
- decide whether the next identity seam should remain cPAR-package-specific or move into a broader shared `security_identity_service`
- keep package-time coverage semantics and live valuation semantics separate in future dashboard/read-surface expansions

## Goals

- Make one authority responsible for each artifact class.
- Make identifier resolution explicit and auditable before coverage logic runs.
- Make date semantics explicit rather than inferred.
- Keep read services narrow: normalize, join, and label; do not manufacture model truth.
- Keep search broader than modeling only where the payload explicitly says so.
- Keep cPAR loadings visibility separate from valuation readiness.
- Make publish/reuse behavior fail closed on downgrade instead of silently replacing healthy state with narrower state.

## Non-Goals

- Do not change cUSE or cPAR math in this plan.
- Do not broaden cPAR what-if staging beyond active-package fit rows.
- Do not add route-local SQL fallbacks or UI-only status hacks.
- Do not let local SQLite become a hidden authority when Neon is configured and healthy.

## Design Principles

1. One artifact class, one authority.
2. Dates are not interchangeable.
3. Identity resolution happens before coverage classification.
4. Discoverability is not the same thing as modeling readiness.
5. Loadings visibility is not the same thing as valuation readiness.
6. Publish safety matters more than freshness when the candidate snapshot is degraded.
7. Compatibility fields are allowed only as migration seams, not as the source of truth.

## Target Contract Model

### Artifact Classes

The implementation should explicitly separate these artifact classes:

- Registry identity and policy truth
  - canonical identity, aliases, taxonomy, and operator/model policy
  - authoritative source: Neon-backed registry surfaces

- Source observation truth
  - prices, PIT classifications, PIT fundamentals, sync coverage, and freshness
  - authoritative source: shared source-read path, with Neon operating authority and SQLite mirror/archive

- cUSE model truth
  - core-estimated outputs, returns-projection outputs, non-core/fundamental projection outputs, and cUSE membership truth
  - authoritative source: persisted cUSE build outputs plus `cuse_security_membership_daily`

- cUSE serving truth
  - the published `universe_loadings` and related current payloads
  - authoritative source: serving payload store; this is a materialized serving view, not a place to invent model truth

- cPAR package truth
  - active package run, package fit rows, covariance, package membership
  - authoritative source: persisted cPAR package outputs

- cPAR runtime coverage truth
  - package-time coverage metadata such as package-date price presence and fit usability
  - authoritative source: persisted cPAR runtime coverage, but only as package-time metadata

- Holdings truth
  - loaded positions and account identity
  - authoritative source: holdings store

- Read-time valuation truth
  - latest eligible price on or before the relevant evaluation date
  - authoritative source: shared source reads

### Date Fields

The remediation should stop overloading `as_of_date` or package dates.

At minimum, the repo should preserve these fields separately:
- `source_observed_asof`
- `membership_asof`
- `projection_package_asof`
- `model_package_asof`
- `served_snapshot_asof`
- `package_date`
- `package_price_asof`
- `live_price_date`

Rules:
- no service may infer one date from another unless that mapping is explicitly documented
- any fallback to an older well-covered date must be visible in payloads and operator diagnostics
- a publish candidate must not silently replace a fresher healthy live snapshot with an older narrower one

### Shared Readiness / Coverage State

The repo should assemble a normalized readiness state instead of letting each route improvise one.

For cUSE, the logical readiness state should include:
- `identity_status`
- `admission_status`
- `model_strategy`
- `model_readiness_status`
- `projection_status`
- `specific_risk_status`
- `scenario_stage_status`
- `served_snapshot_status`

For cPAR, the logical coverage state should include:
- `identity_status`
- `package_fit_status`
- `package_price_status`
- `live_price_status`
- `valuation_status`
- `loadings_status`
- `coverage_status`
- `scenario_stage_status`

These are logical states. They can be materialized as payload fields without forcing an immediate schema rewrite.

## Root Causes To Address

### cUSE

- cUSE membership history is mixed-date by design:
  - core/native and fundamental-projection rows inherit the latest well-covered exposure date
  - returns-projection rows inherit the active core package date
- Publish-time membership authority is currently wrong:
  - `refresh_persistence` calls `load_cuse_membership_rows(as_of_dates=None)`
  - that helper resolves to `MAX(as_of_date)` only
  - publish therefore mutates the candidate universe against only the newest membership slice instead of the full current-run membership artifact
- cUSE membership derivation currently happens inside `model_outputs.persist_model_outputs(...)`, so publish cannot directly consume the same current-run membership artifact it just wrote.
- Current integrity checks block catastrophic emptiness, but not every harmful regression against the current healthy live payload.
- The projection path still lets authoritative projected outputs, fallback rows, and UI compatibility labels collide in one row shape.
- Search/typeahead are broader than the active served universe, but what-if and quote consumers still lean too hard on served payload presence.
- The current non-core/fundamental projection story appears semantically richer than the actual served artifact path.

### cPAR

- Portfolio snapshot assembly joins holdings to support rows by exact holdings RIC with no canonical identity normalization first.
- Coverage classification currently returns `missing_price` before it returns `missing_cpar_fit` if both fit and price miss.
- Persisted runtime coverage can null out a valid live-source price row in the snapshot path.
- Runtime coverage derivation has a confirmed authority bug:
  - `backend/data/cpar_outputs.py::_load_package_price_presence_by_ric(...)` always uses `_sqlite_fetch_rows(...)`
  - active cPAR package/runtime coverage in Neon can therefore be stamped from the wrong backing store during package persistence
  - the live Neon evidence above shows runtime coverage marked `missing` while Neon/shared-source reads return valid prices for the same names
- Ticker detail and active package search are healthier than the portfolio/dashboard path, which means the bug is largely in the package-plus-holdings read composition, not the model package itself.

## Proposed Refactor Shape

### Shared Modules

Introduce two shared helpers instead of spreading this logic across route owners:

- `backend/data/security_identity_reads.py`
  - read-only canonical identity and alias lookup from registry-backed sources
  - no service semantics

- `backend/services/security_identity_service.py`
  - canonicalize ticker/RIC inputs for read paths
  - preserve original identifier alongside resolved canonical identity
  - return explicit identity mismatch / ambiguous identity states

- `backend/services/readiness_state.py`
  - assemble normalized cUSE and cPAR readiness/coverage state objects from package/model/source/registry inputs
  - route owners decorate the result; they do not redefine it

These are refactors for ownership clarity, not a new abstraction layer for its own sake.

### cUSE Serving Architecture

Refactor cUSE so `universe_loadings` becomes a materialized serving view over authoritative inputs:
- cUSE membership truth
- authoritative projected-output tables
- risk-engine package identity
- registry/runtime admission truth

That means:
- `universe_loadings` should stop being the place where projected truth and fallback truth are invented ad hoc
- membership history should remain an audit/history table, not the implicit publish authority
- publish should consume the candidate run's own membership artifact directly, or an explicit active membership snapshot keyed by `run_id` / snapshot pointer
- publish must stop deriving authority from `MAX(as_of_date)` in `cuse_security_membership_daily`
- publish should compare the candidate snapshot against the current healthy live snapshot, not merely test the candidate for internal non-emptiness
- publish should block or explicitly reuse the last healthy snapshot when the candidate regresses materially

### cPAR Read Architecture

Refactor cPAR so portfolio/dashboard assembly becomes an explicit composition:
- canonical identity resolution
- active package fit resolution
- package-time runtime coverage read
- live-source valuation read
- normalized coverage-state assembly
- downstream loadings and contribution assembly

That means:
- fit/loadings visibility should be driven by fit availability
- market-value-weighted views should be driven by valuation availability
- package-time price status should remain visible but should not erase live-source valuation silently

## Implementation Plan

### Phase 0: Freeze semantics and evidence

- Add regression fixtures that capture the known failure modes.
- Preserve evidence for the current broken cUSE publish behavior and current cPAR package-vs-portfolio discrepancy.
- Update or replace tests that currently codify incorrect semantics such as `missing_price => no loadings visibility`.
- Add test coverage for:
  - healthy live cUSE payload vs degraded candidate payload
  - projected ETF names like `SPY`, `QQQ`, `URA`, `XLE`, `SMH`
  - non-core / foreign names like `ASML`
  - canonical vs non-canonical holdings identifiers in cPAR
  - fit-present / price-missing
  - fit-missing / price-present
  - runtime-coverage / live-price conflict

### Phase 1: Shared identity normalization

- Implement canonical identifier reads and service-level normalization.
- Apply the same normalization rules across:
  - cUSE quote/search/typeahead
  - cUSE what-if staging
  - cPAR holdings joins
  - cPAR search/ticker/detail
- Preserve original input identifier in payloads for audit/debugging.
- Introduce explicit states for `identity_mismatch` and `identity_ambiguous`.

Success condition:
- the repo no longer depends on exact string equality of raw holdings identifiers to determine whether a name has model coverage

### Phase 2: cPAR runtime-coverage persistence fix

- Replace `cpar_outputs._load_package_price_presence_by_ric(...)` so package-time price presence is derived from the same authoritative shared-source read path used by cPAR read surfaces.
- Stop hard-wiring package-time runtime coverage derivation to `_sqlite_fetch_rows(...)`.
- Add package-build validation that blocks or flags any package where:
  - a large fraction of fit rows are stamped `price_on_package_date_status = missing`
  - while authoritative source reads on the package date show broad price availability
- Backfill or rebuild the active cPAR runtime-coverage rows after the persistence bug is fixed.

Success condition:
- active-package runtime coverage in Neon agrees with authoritative source-price presence for the active package date

### Phase 3: cUSE publish and projection hardening

- Extract cUSE membership derivation out of `model_outputs.persist_model_outputs(...)` so the refresh pipeline owns a concrete current-run membership artifact.
- Persist that artifact to history, but do not re-read “latest date” history during the same publish path.
- Introduce explicit membership snapshot selection:
  - current candidate snapshot: the artifact built by the current refresh run
  - active snapshot: the currently promoted publish snapshot
  - history: all persisted membership rows for audit/debugging
- Add a read helper keyed by explicit authority, not by implicit latest date:
  - by `run_id`
  - or by an explicit active snapshot pointer
- Keep the existing candidate-vs-live regression gate before publish and publish-only.
- Reject or reuse when a candidate payload materially regresses:
  - total modeled rows
  - `core_estimated` rows
  - `projected_only` rows
  - projection-candidate coverage
- Move toward a logical cUSE readiness manifest assembled from membership truth plus authoritative projected outputs.
- Treat projected ETF outputs and non-core/fundamental outputs as first-class artifacts rather than compatibility labels on fallback rows.
- Keep registry-first discovery, but label rows explicitly as:
  - `live_modeled`
  - `projected_only`
  - `admitted_not_served`
  - `registry_runtime_only`
  - `not_admitted`

Concrete implementation seams:
- [backend/analytics/pipeline.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/analytics/pipeline.py)
  - build the cUSE membership artifact once from the candidate `universe_loadings` payload and the current `risk_engine_state`
  - carry that artifact through persistence and publish instead of reconstructing publish authority later
- [backend/data/model_outputs.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/data/model_outputs.py)
  - stop owning membership derivation internally
  - accept the already-built membership/stage payloads from the caller and persist them as history
- [backend/analytics/refresh_persistence.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/analytics/refresh_persistence.py)
  - replace `_load_current_membership_lookup(...as_of_dates=None)` for publish authority
  - overlay the candidate universe from the current-run membership artifact
  - compare candidate vs active promoted snapshot for regression safety
  - keep the fail-closed guard, but apply it against the current-run membership artifact instead of the newest history date
- [backend/data/cuse_membership_reads.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/data/cuse_membership_reads.py)
  - add explicit read helpers by `run_id` and, if needed, by active snapshot pointer
  - keep `as_of_dates=None -> MAX(as_of_date)` only as a diagnostic/history convenience, not as a publish contract
- [backend/tests/test_refresh_persistence_membership_overlay.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/tests/test_refresh_persistence_membership_overlay.py)
  - replace tests that assume “latest membership date” is the right publish authority
  - add regression tests for mixed-date membership history with one run spanning core/fundamental and projection package dates
- [backend/tests/test_cuse_membership_contract.py](/Users/shaun/Library/CloudStorage/Dropbox/045%20-%20Vibing/ceiora-risk/backend/tests/test_cuse_membership_contract.py)
  - add explicit assertions that one run can validly persist a mixed-date membership history without being considered degraded

Success condition:
- projected ETF names remain visible and correctly labeled whenever authoritative projected outputs exist for the active package date
- publish no longer confuses a mixed-date membership history with a degraded latest-date membership snapshot

### Phase 4: cUSE consumer refactor

- Change quote/typeahead services to consume normalized readiness state instead of inferring too much from the served payload alone.
- Change what-if staging to require explicit model readiness rather than silently accepting empty-exposure rows.
- Keep search broader than staging, but make the difference explicit in payloads and UI copy.
- Confirm that non-core equities outside the core estimation universe are either:
  - genuinely modeled and served with a real artifact path
  - or clearly labeled as admitted but not currently modeled

Success condition:
- no cUSE surface implies that a name is fully modeled merely because it is discoverable

### Phase 4A: cUSE recovery run after publish-contract fix

- Do not rerun `cold-core` as the first recovery action.
- After the membership-authority refactor is deployed, run the smallest recovery lane that rebuilds serving payloads from the already rebuilt model artifacts.
- Preferred recovery path:
  - deploy control/serve changes
  - run `serve-refresh` against the current Neon-authoritative core artifacts
  - only escalate to a core lane again if `serve-refresh` demonstrates that the persisted core/projection artifacts themselves are stale or incomplete
- Validate explicitly that:
  - `serving_payload_current` advances off `model_run_20260414T213636Z`
  - live `universe_loadings` modeled counts recover
  - `SPY`, `QQQ`, `URA`, `XLE`, `SMH`, `AAL`, and `ASML` now resolve from the live served universe with the expected strategy/state

Success condition:
- cUSE recovers through publish-contract repair rather than another expensive bootstrap rebuild

### Phase 5: cPAR package/runtime/valuation split

- Refactor cPAR runtime coverage semantics so package-time price state is retained as package metadata, not as a blanket override on live valuation.
- Refactor cPAR coverage classification so `fit_missing` beats `missing_price` when both lookups miss.
- Introduce explicit cPAR coverage states such as:
  - `covered`
  - `fit_present_price_missing`
  - `fit_missing_price_present`
  - `fit_missing`
  - `insufficient_history`
  - `registry_only`
  - `identity_mismatch`
- Keep loadings visible for fit-present rows even when valuation is unavailable.
- Gate only weighted portfolio analytics and market-value-dependent contributions on valuation readiness.

Success condition:
- cPAR dashboard/snapshot views can still show package loadings for fit-covered names even when current valuation is missing

### Phase 6: cPAR portfolio/read-owner refactor

- Split cPAR portfolio assembly into explicit sub-steps inside the service layer:
  - identity resolution
  - support-row retrieval
  - package fit assembly
  - valuation assembly
  - coverage-state assembly
  - loadings aggregation
  - weighted contribution assembly
- Keep `cpar_outputs.py` authoritative for package truth.
- Keep `cpar_source_reads.py` authoritative for live-source decoration.
- Keep route owners thin.
- Do not let frontend code reinterpret package/runtime/source semantics on its own.

Success condition:
- the cPAR risk dashboard, account hedge payload, and what-if payload all derive their coverage semantics from the same normalized state object

### Phase 7: Diagnostics and operator surfaces

- Add cUSE diagnostics for:
  - live snapshot vs candidate snapshot
  - modeled count deltas
  - projection coverage deltas
  - degraded-date publish attempts
- Add cPAR diagnostics for:
  - fit-present vs fit-missing
  - package-price-present vs live-price-present
  - identity mismatch count
  - runtime-coverage / live-price conflict count
- Surface these diagnostics in operator/readiness payloads before removing compatibility fields.

Success condition:
- operators can tell whether a failure belongs to identity, source coverage, model outputs, publication, or UI compatibility

### Phase 8: Frontend contract cleanup

- Update cUSE frontend surfaces to render explicit readiness and strategy labels.
- Update cPAR frontend surfaces to distinguish:
  - model fit visibility
  - package-time price state
  - live valuation state
  - weighted contribution availability
- Remove UI assumptions that equate `missing_price` with `no model`.

Success condition:
- frontend labels and gating match backend semantics without route-local or component-local reinterpretation

### Phase 9: Compatibility cleanup and retirement

- Once the new states are stable, shrink or remove overloaded compatibility labels and dead fallback paths.
- Keep only compatibility fields that are still required by active consumers.
- Remove tests that were asserting the old compressed semantics after all consumers migrate.

Success condition:
- the final read path is simpler than the current one, not more layered

## Rollout Strategy

Implement in this order:

1. Phase 0 and Phase 1
2. Phase 2 for cPAR runtime-coverage persistence repair
3. Phase 3 for cUSE membership-authority and publish safety
4. Phase 4 for cUSE consumer/readiness semantics
5. Phase 4A cUSE recovery run using the repaired publish path
6. Phase 5 for cPAR consumer and coverage-state semantics
7. Phase 6 for cPAR portfolio/read-owner refactor
8. Phase 7 diagnostics
9. Phase 8 frontend contract cleanup
10. Phase 9 compatibility retirement

Reasoning:
- the live cPAR runtime-coverage persistence bug is an upstream data-contract failure and should be fixed before downstream snapshot semantics are cleaned up
- the live cUSE blocker is now specifically a membership-authority/publish-contract failure, not a missing-core failure
- publish safety and explicit membership-snapshot authority remain the highest-leverage cUSE fixes
- because the latest core artifacts are already in Neon, the next cUSE recovery attempt should use the repaired serving lane before spending more on a bootstrap lane
- cPAR coverage semantics must be corrected before frontend cleanup, otherwise the UI will simply keep rendering bad states more clearly
- diagnostics should land before compatibility retirement so the migration stays observable

## Adversarial Review Summary

The plan was challenged from three angles: cUSE serving/projection behavior, cPAR package/portfolio semantics, and cross-cutting authority/date-contract risks.

The most important objections were:

- A candidate payload can be internally consistent and still be the wrong snapshot.
  - response: add candidate-vs-live publish regression gates; do not rely on local non-emptiness

- cUSE membership history can be validly mixed-date, so “latest date in history” is not a safe publish authority.
  - response: keep history as history; promote an explicit active membership snapshot and consume the current run’s membership artifact during publish

- Search and staging are still conflated.
  - response: keep registry-first discovery but make stage readiness explicit and strict

- Projected outputs do not have a clean ownership boundary.
  - response: make projected ETF and non-core outputs first-class authoritative artifacts rather than fallback-shaped rows

- cPAR `missing_price` is hiding fit/identity failures.
  - response: separate fit visibility, package-time price semantics, live valuation readiness, and weighted contribution readiness

- Showing loadings when valuation is missing could hide a price bug.
  - response: keep those as separate dimensions and render the conflict explicitly instead of erasing one side

- The plan might still underweight an upstream persistence bug in cPAR runtime coverage.
  - response: the live Neon validation now promotes runtime-coverage persistence repair ahead of downstream cPAR service cleanup

## Acceptance Criteria

This plan is complete only when all of the following are true:

- cUSE cannot publish a materially narrower modeled universe over a healthier live snapshot without an explicit blocker or deliberate reuse decision.
- cUSE projected ETF names such as `SPY`, `QQQ`, `URA`, `XLE`, and `SMH` surface with the correct strategy and modeled state whenever authoritative projected outputs exist.
- cUSE non-core / foreign names either have a real served projection artifact path or are clearly labeled as admitted but not modeled.
- cUSE discovery and cUSE staging no longer imply the same readiness level.
- cPAR active-package runtime coverage no longer marks broad sets of fit-healthy names as `missing_price` when authoritative source reads show package-date prices.
- cPAR portfolio/dashboard paths preserve loadings visibility for fit-covered names even when valuation is unavailable.
- cPAR no longer uses `missing_price` as the default label for fit-missing or identifier-mismatch states.
- cPAR holdings/package joins are canonicalized and auditable.
- runtime coverage and live-source price conflicts are visible rather than silently resolved by overwriting one side.
- the resulting code paths are narrower and easier to reason about than the current ones.

## Working Rule For Implementation

If an implementation step makes the system appear to work by adding another late-stage override, compatibility branch, or UI-only reinterpretation, it is not following this plan.

The correct fix is to move the ambiguity earlier, make the state explicit, and let downstream read surfaces consume that explicit truth.
