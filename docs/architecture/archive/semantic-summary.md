# Semantic Summary

Date: 2026-03-17
Status: Completed semantic consistency pass
Owner: Codex

## What Changed

### Canonical names added

- `model_status_reason`
  - canonical replacement for the overloaded `eligibility_reason`
- `factor_coverage_asof`
  - canonical replacement for generic factor-row `coverage_date`
- `served_loadings_asof`
  - canonical replacement for `model_sanity.coverage_date`
- `latest_loadings_available_asof`
  - canonical replacement for `model_sanity.latest_available_date`

### Canonical names promoted in UI

Frontend truth helpers and labels now prefer:

- `core_state_through_date`
- `core_rebuild_date`
- `estimation_exposure_anchor_date`
- `exposures_served_asof`
- `exposures_latest_available_asof`
- `model_status_reason`
- `factor_coverage_asof`

### Compatibility aliases preserved

These remain available for compatibility, but they no longer drive primary semantics:

- `factor_returns_latest_date`
- `last_recompute_date`
- `eligibility_reason`
- `coverage_date`
- `latest_available_date`
- `exposures_asof`

## What Got Clearer

- model-status explanations are no longer mislabeled as narrow eligibility-only reasons
- factor-row dates now identify themselves as factor-coverage dates
- `model_sanity` now exposes explicit loadings dates instead of generic coverage names
- operator/health UI now treats `exposures_latest_available_asof` as the canonical latest-loadings date

## What Was Intentionally Not Renamed

- `source_dates`
  - stable container surface; field semantics were improved instead
- `model_status`
  - already the right canonical state field
- `model_warning`
  - still the correct freeform explanatory note

## What Future Contributors Should Avoid Reintroducing

- using `eligibility_reason` as the primary UI/developer-facing name
- using `coverage_date` or `latest_available_date` without subject context
- using `exposures_asof` when `exposures_latest_available_asof` or `exposures_served_asof` is available
- introducing new synonyms for the same timeline concept instead of extending the canonical set

## Validation Outcome

The pass preserved contract compatibility while making the primary semantics explicit. The key user-facing and developer-facing surfaces now use clearer names without changing the operating model or source-of-truth rules.
