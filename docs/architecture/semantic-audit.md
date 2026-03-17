# Semantic Audit

Date: 2026-03-17
Status: Completed audit
Owner: Codex

## Scope

This audit reviewed naming and contract semantics across:

- backend analytics payload contracts
- backend services that shape user-facing payloads
- frontend types and labels
- tests and architecture docs

The goal was not broad renaming. The goal was to identify names that were materially misleading, overloaded, or still driving the wrong semantics in UI and developer-facing contracts.

## Main Findings

### 1. `eligibility_reason` is overloaded

Current behavior:

- `eligibility_reason` is carried on positions, universe-by-ticker payloads, search results, and explore views.
- The values are used to explain `model_status`, not only strict structural eligibility.
- Examples include:
  - `missing_factor_exposures`
  - strict-model ineligible reasons

Problem:

- The name suggests a narrow eligibility classifier.
- In practice it explains broader model-state outcomes, including projected-only vs ineligible transitions.

Conclusion:

- `model_status_reason` should be the canonical name.
- `eligibility_reason` should remain only as a compatibility alias.

### 2. `coverage_date` is too vague in exposure payloads

Current behavior:

- factor rows on `/api/exposures` carry `coverage_date`
- the value is actually the factor-coverage / factor-cross-section date for those rows

Problem:

- `coverage_date` does not say what is being covered
- it is easy to confuse with:
  - model coverage
  - health coverage
  - serving snapshot date

Conclusion:

- `factor_coverage_asof` should be the canonical name for factor rows.
- `coverage_date` should remain as a compatibility alias.

### 3. `coverage_date` and `latest_available_date` are vague in `model_sanity`

Current behavior:

- `model_sanity.coverage_date` is the served loadings date
- `model_sanity.latest_available_date` is the latest available loadings source date

Problem:

- both names are generic and context-dependent
- frontend truth helpers still had to infer the meaning from the containing object

Conclusion:

- `served_loadings_asof` should be canonical
- `latest_loadings_available_asof` should be canonical
- legacy names should remain as aliases only

### 4. `exposures_asof` is still a compatibility alias, but it still leaks into meaning

Current behavior:

- `source_dates` already carries explicit `exposures_latest_available_asof`
- `exposures_asof` is still populated as a compatibility alias
- some code paths and labels still read `exposures_asof` directly

Problem:

- `exposures_asof` does not tell the reader whether it means:
  - latest available source cross-section date, or
  - current served loadings date

Conclusion:

- canonical readers should prefer:
  - `exposures_served_asof`
  - `exposures_latest_available_asof`
- `exposures_asof` should remain compatibility-only

### 5. Core risk-state names were mostly clean already

Current state:

- `core_state_through_date`
- `core_rebuild_date`
- `estimation_exposure_anchor_date`

These names already match the intended operating model.

Problem:

- compatibility aliases still exist:
  - `factor_returns_latest_date`
  - `last_recompute_date`

Conclusion:

- keep the aliases for compatibility
- do not let them drive frontend semantics or new documentation

## Areas Reviewed But Left Alone

### `source_dates`

`source_dates` still mixes PIT dates and serving/loadings recency in one object. That is acceptable for now because:

- the object is already a stable contract surface
- the field-level semantics are now explicit enough when canonical names are used
- renaming the container itself would create broad churn without enough clarity gain

### `model_status`

`model_status` remains the correct canonical field:

- `core_estimated`
- `projected_only`
- `ineligible`

No rename is needed there.

### `model_warning`

`model_warning` is still an appropriate user-facing freeform note. It does not conflict with the structured status fields.

## Canonical vs Compatibility Summary

| Current field | Canonical meaning | Action |
| --- | --- | --- |
| `factor_returns_latest_date` | `core_state_through_date` | keep as alias |
| `last_recompute_date` | `core_rebuild_date` | keep as alias |
| `eligibility_reason` | `model_status_reason` | canonical rename with alias preserved |
| `coverage_date` on factor rows | `factor_coverage_asof` | canonical rename with alias preserved |
| `coverage_date` in `model_sanity` | `served_loadings_asof` | canonical rename with alias preserved |
| `latest_available_date` in `model_sanity` | `latest_loadings_available_asof` | canonical rename with alias preserved |
| `exposures_asof` | `exposures_latest_available_asof` | compatibility alias only |

## Audit Conclusion

The repo did not need a sweeping rename campaign. The biggest remaining problems were a small set of overloaded contract fields still leaking into UI and developer meaning.

The high-value fix is:

1. add canonical names where semantics were still vague
2. keep compatibility aliases for existing payload readers
3. ensure UI and docs read only the canonical meanings
