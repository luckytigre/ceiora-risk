# Semantic Target

Date: 2026-03-17
Status: Canonical naming target
Owner: Codex

## Goal

Use a small set of canonical names that match the operating model, while preserving legacy aliases only where existing contracts still need them.

The target is clarity, not naming churn.

## Canonical Field Model

### Core risk-state fields

These fields define the stable weekly core package:

- `core_state_through_date`
  - latest return date covered by the current core package
- `core_rebuild_date`
  - date the current core package was rebuilt
- `estimation_exposure_anchor_date`
  - lagged exposure snapshot used as the basis for the current core package

Compatibility aliases:

- `factor_returns_latest_date`
- `last_recompute_date`

Rule:

- UI, docs, and new code should prefer the `core_*` names.

### Serving/loadings freshness fields

These fields define the daily serving/projection layer:

- `exposures_served_asof`
  - current served loadings date
- `exposures_latest_available_asof`
  - latest available loadings source date
- `prices_asof`
  - latest canonical price source date

Compatibility alias:

- `exposures_asof`

Rule:

- `exposures_asof` must not drive new UI semantics.
- It exists only so older readers still decode the payloads safely.

### PIT source fields

- `fundamentals_asof`
- `classification_asof`

These are already canonical and should remain unchanged.

### Status fields

- `model_status`
  - canonical structured state
- `model_status_reason`
  - canonical structured reason for the model-status outcome
- `model_warning`
  - user-facing explanatory note

Compatibility alias:

- `eligibility_reason`

Rule:

- new UI and docs should use `model_status_reason`
- `eligibility_reason` is compatibility-only

### Factor coverage fields

On exposure/factor rows:

- `factor_coverage_asof`
  - date for the factor cross-section / coverage metrics on that row

Compatibility alias:

- `coverage_date`

On `model_sanity`:

- `served_loadings_asof`
  - the loadings date currently being served
- `latest_loadings_available_asof`
  - latest available loadings source date

Compatibility aliases:

- `coverage_date`
- `latest_available_date`

Rule:

- generic `coverage_date` and `latest_available_date` must not drive UI meaning when the canonical fields are available.

## UI Label Rules

Use these labels:

- `Core State Through`
- `Core Rebuilt`
- `Estimation Anchor`
- `Loadings`
- `Loadings Available`
- `Fundamentals`
- `Classification`

Avoid these labels:

- `Model` for any generic date
- `Coverage Date` when the value is actually a loadings or factor-cross-section as-of date
- `Eligibility Reason` in user-facing surfaces when the field is actually explaining general model state

## Compatibility Policy

Compatibility aliases are allowed only when all of these are true:

1. the old field is already part of a stable payload contract
2. removing it would cause unnecessary churn
3. the canonical replacement is added alongside it
4. UI and docs stop depending on the old field for meaning

## Developer Rule

When reading or extending these contracts:

- prefer canonical names first
- use compatibility aliases only as fallback readers
- do not introduce a third synonym for an existing concept
- if a field name does not reveal the timeline or subject, rename it only when the new name materially reduces ambiguity
