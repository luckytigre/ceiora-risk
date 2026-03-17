# Date Semantics Target

## Goal

Expose a small, durable date model that matches the operating design:

1. Weekly core risk state
2. Daily serving / projection state
3. Monthly PIT source state

This should correct semantics without changing core model math.

## Target Field Model

### A. Core Risk State

These belong on the served `risk_engine` payload and any operator-facing risk-state summary.

| Field | Meaning | Notes |
| --- | --- | --- |
| `core_state_through_date` | Latest return date covered by the current core package | Clear alias for `factor_returns_latest_date` |
| `core_rebuild_date` | Date the core package was last rebuilt | Clear alias for `last_recompute_date` |
| `estimation_exposure_anchor_date` | Actual lagged exposure snapshot used as the estimation anchor for the selected core state | Derived from `eligibility_summary.exp_date` |

Compatibility:

- keep `factor_returns_latest_date`
- keep `last_recompute_date`

### B. Daily Serving State

These remain on `source_dates` / serving payloads.

| Field | Meaning |
| --- | --- |
| `exposures_served_asof` | Current served loadings date |
| `exposures_latest_available_asof` | Latest available loadings source date |
| `prices_asof` | Latest prices source date |

Notes:

- `model_sanity.coverage_date` remains a serving/loadings coverage field, not a generic model date

### C. PIT Source State

These remain on `source_dates`.

| Field | Meaning |
| --- | --- |
| `fundamentals_asof` | Latest policy-compliant PIT fundamentals anchor |
| `classification_asof` | Latest policy-compliant PIT classification anchor |

## Recommended UI Labels

### Compact analytics summary

Use:

- `Loadings = YYYY-MM-DD`
- `Core Through = YYYY-MM-DD`
- `Rebuilt = YYYY-MM-DD`

Do not use:

- `Model = YYYY-MM-DD`

### Health page

Use separate cards / values for:

- `Core State Through`
- `Core Rebuilt`
- `Estimation Exposure Anchor`
- `Current Loadings As Of`

### Operator status

- Keep raw source dates under `Authoritative Source Recency`
- Move factor-return/core dates out of raw source recency language
- Use labels like:
  - `Core Through`
  - `Core Rebuilt`

## Deliberate Non-Goals

- no change to factor-return estimation logic
- no change to covariance / specific-risk math
- no new generalized date abstraction layer
- no attempt to expose every intermediate date in the model pipeline

## Tradeoff

The system does not currently persist a separate, durable `eligibility_anchor_date` distinct from the chosen exposure snapshot date. For this semantics cleanup, `estimation_exposure_anchor_date` should represent the actual selected exposure snapshot (`exp_date`), which is the user-meaningful anchor.
