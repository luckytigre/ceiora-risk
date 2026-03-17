# US-Core Market-Factor ADR

Date: 2026-03-15
Status: Accepted for implementation
Owner: Shaun + Codex

## Purpose

Freeze the core statistical, contract, and runtime decisions for the US-core one-stage migration so implementation does not drift.

This ADR governs:

- replacement of `Country: US` with `Market`
- migration from sequential two-phase WLS to one-stage constrained WLS
- separation of `core-estimated` names from `projected-only` names
- factor identity and payload contract direction
- local SQLite / Neon / cloud-serving responsibilities during the cutover

## Decisions

### 1. Core model scope

- The live core factor-return estimation universe becomes `US-only`.
- Non-US equities remain in coverage and portfolio analytics but do not participate in core factor-return estimation.
- This is a `US-core` model migration, not a global GEM-style model build.

### 2. Structural baseline factor

- `Market` replaces `Country: US` as the live structural baseline factor.
- `Country: US` is removed from:
  - live regression design
  - live factor catalog
  - live covariance factor set
- `Market` is the USE4-style baseline factor for the US-core model.

### 3. Intercept policy

- The live reported model does not carry a separate intercept factor.
- `Market` serves as the baseline common-move factor in the live one-stage regression.
- Any internal implementation detail needed for numerical stability must not surface as a public factor.

### 4. Regression form

- The live core estimator becomes `one-stage constrained WLS`.
- Market, industry, and style factors are estimated jointly.
- The constraint is a cap-weighted industry-sum-to-zero rule.
- Constraint residuals are persisted and monitored as a first-class diagnostic.

### 5. Normalization anchor

- Style standardization and orthogonalization for the live model are anchored on the `US core` universe only.
- Projected-only names are mapped onto the US-core normalized factor scale.
- Non-US names must not silently influence the normalization basis once they leave core estimation.

### 6. Non-US names

- Non-US names are `projected-only`.
- They receive:
  - exposures
  - fitted return projection
  - residual history
  - specific-risk forecast
- They do not influence factor-return estimation.
- No manual specific-risk uplift is introduced in this migration.

### 7. Security model status

The authoritative security status becomes:

- `core_estimated`
- `projected_only`
- `ineligible`

Active payloads and UI surfaces use `model_status` directly.
`eligible_for_model` is not part of the live contract.

### 8. Factor identity

- Stable `factor_id` becomes the canonical identity of each factor.
- Human-readable labels are presentation metadata, not system identity.
- Backend internals, ordering, persistence metadata, and diagnostics must use `factor_id`.

- public factor payloads move to factor-id keyed structures
- display name, short label, family, and ordering come from the factor catalog

### 9. Compatibility philosophy

- This is a single-user system.
- Migration scaffolding is removed once the cutover is complete.
- No active alias such as `country = market` remains in the live contracts.

### 10. Local / Neon / cloud-serving boundary

- Local SQLite plus local cache remain the heavy-compute authority.
- Local orchestrator runs the compute lanes that build the new model state.
- Durable SQLite analytics tables are the first persistence target for recomputed model outputs.
- Neon remains the bounded serving-oriented mirror and holdings authority when configured.
- Cloud-serving processes consume published serving payloads and mirrored bounded analytics state; they do not recompute the core model.

Cutover rule:

- new semantics land locally first
- then they persist durably in SQLite
- then they mirror to Neon
- then cloud-serving/Neon-backed readers are validated

## Required completion state

The migration is not complete until all of the following are true:

- no live `Country: US` factor remains
- no active payload field named `country` remains where the concept is actually `market`
- no active payload field named `eligible_for_model` remains
- public factor payload identity is based on stable `factor_id`
- operator and health surfaces are native to `market` and `model_status`
