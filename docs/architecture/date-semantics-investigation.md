# Date Semantics Investigation

## Summary

The system currently carries three distinct timelines, but the frontend collapses two of them into a vague `Model` date:

1. Core risk-state timeline
   - factor returns / covariance / specific risk
   - last core rebuild date
   - lagged exposure basis used for estimation
2. Daily serving timeline
   - current served loadings / projected portfolio outputs
   - holdings / prices
3. PIT source timeline
   - fundamentals
   - classifications

The main ambiguity is frontend labeling, not core model math.

## Current Fields

| Field | Current source | True meaning | Current display | Misleading? |
| --- | --- | --- | --- | --- |
| `risk_engine.factor_returns_latest_date` | `backend/analytics/refresh_metadata.py` via `risk_engine_meta` | Latest return date covered by the current core risk package | Exposures/Positions compact line as `Model`, Health page `Model As Of`, Operator Status `Factor Returns` | Yes. It is core-state-through, not a generic model date. |
| `risk_engine.last_recompute_date` | `backend/analytics/refresh_metadata.py` via `risk_engine_meta` | Date the core risk package was last rebuilt | Present in payload but not surfaced clearly in current UI | Underused / hidden. |
| `risk_engine.cross_section_min_age_days` | `risk_engine_meta` | Configured lag guard for estimation exposures | Health page subtitle | No, but it does not tell the user the actual selected estimation anchor date. |
| `model_sanity.coverage_date` | `backend/analytics/refresh_metadata.py` from refreshed eligibility summary | Current served loadings date used for dashboard-facing risk/exposure payloads | Health page fallback for `Model As Of`, Exposures truth summary fallback | Yes when used as a model/core date. It belongs to daily serving state. |
| `model_sanity.latest_available_date` | refreshed eligibility summary | Latest available loadings source date, even if the served snapshot uses an older well-covered date | Health/operator freshness logic | No, if labeled as latest loadings available. |
| `source_dates.exposures_served_asof` | `refresh_metadata.serving_source_dates(...)` | Current served loadings / served exposure snapshot date | Exposures/Positions compact line as `Loadings` | No. This is the right daily serving date. |
| `source_dates.exposures_latest_available_asof` | `refresh_metadata.serving_source_dates(...)` | Latest available loadings source date | update prompts / freshness logic | No. |
| `source_dates.prices_asof` | `core_reads.load_source_dates()` | Latest price source date | Operator Status / Positions source dates | No. |
| `source_dates.fundamentals_asof` | `core_reads.load_source_dates()` | Latest policy-compliant PIT fundamentals anchor | Operator Status / Positions source dates | No. |
| `source_dates.classification_asof` | `core_reads.load_source_dates()` | Latest policy-compliant PIT classification anchor | Operator Status / Positions source dates | No. |
| `eligibility_summary.exp_date` | `daily_universe_eligibility_summary.exp_date` | Actual lagged exposure snapshot used for the selected factor-return estimation date | Not surfaced in current UI | Yes, by omission. This is the missing date that explains the lagged core basis. |

## Where The UI Collapses Semantics

### `frontend/src/lib/analyticsTruth.ts`

- `modelAsOf` is currently derived from `risk_engine.factor_returns_latest_date`.
- The compact summary renders:
  - `Loadings = ...`
  - `Model = ...`
- This incorrectly implies one generic model date instead of:
  - current served loadings date
  - core-state-through date
  - core rebuild date

### `frontend/src/app/health/page.tsx`

- `modelAsOf` is currently:
  - `risk_engine.factor_returns_latest_date`
  - fallback to `model_sanity.coverage_date`
- The page then labels that as `Model As Of`.
- That mixes:
  - core-state-through
  - served loadings coverage

### `frontend/src/features/health/OperatorStatusSection.tsx`

- `Factor Returns` is currently shown inside `Authoritative Source Recency`.
- That is conceptually wrong:
  - factor returns are core risk-state outputs
  - not raw source recency

## Current Backend Meaning Is Mostly Sound

The backend already has the core pieces:

- `factor_returns_latest_date` = core state through date
- `last_recompute_date` = core rebuild date
- `exp_date` in eligibility summary = lagged estimation exposure anchor
- `exposures_served_asof` = daily served loadings date
- PIT source dates remain separate

What is missing is:

- one explicit core-state field for the estimation exposure anchor in the served risk payload
- consistent frontend labeling that respects the three-timeline model

## Conclusion

The fix should be narrow:

1. add an explicit `estimation_exposure_anchor_date` to the served core-risk state
2. add clearer aliases for the core package dates
3. relabel frontend summaries so they distinguish:
   - `Loadings`
   - `Core Through`
   - `Core Rebuilt`
   - `Estimation Exposure Anchor`

No core model math change is required.
