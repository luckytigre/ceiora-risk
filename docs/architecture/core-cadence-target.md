# Core Cadence Target

Date: 2026-03-17
Status: Active operating-model target
Owner: Codex

## Goal

Make the operating model explicit and enforceable:

1. weekly stable core package
2. daily serving / projection layer
3. canonical historical ingest
4. PIT source timeline

This is an operating-model and semantics rule set, not a model-math redesign.

## A. Weekly Stable Core Package

The weekly stable core package is one coherent unit:

- factor returns
- covariance
- specific risk
- estimation basis metadata

### Rules

- The package advances **only** on core rebuild lanes.
- The package may contain a daily factor-return series internally.
- That series is frozen between rebuilds because the package itself is frozen between rebuilds.
- `serve-refresh` must never compute, persist, or advance any part of this package.

### Canonical Metadata

- `core_state_through_date`
  - latest return date covered by the current stable core package
- `core_rebuild_date`
  - date the current stable core package was last rebuilt
- `estimation_exposure_anchor_date`
  - lagged exposure/eligibility anchor date when available and meaningful
- `latest_r2`
  - latest persisted fit statistic for the current stable core package
  - if unavailable, operator-facing UI should render it as unavailable, not as `0`

Compatibility fields may remain:

- `factor_returns_latest_date`
- `last_recompute_date`

But UI semantics should prefer the canonical names above.

## B. Daily Serving / Projection Layer

The daily serving layer may move independently of the stable core package:

- holdings
- prices used for serving/projection
- current loadings
- portfolio outputs against the latest stable core package

### Rules

- It must not compute, persist, or advance:
  - factor returns
  - covariance
  - specific risk
  - `core_state_through_date`
- It may use fresher serving-time prices where available.
- Any such serving-time prices must be read-only for serving.
- Serving-time prices must never write into canonical model-estimation history tables such as:
  - `security_prices_eod`

### Canonical Serving Metadata

- `exposures_served_asof`
- `exposures_latest_available_asof`
- `prices_asof` when relevant to the surface

## C. Source-Daily Historical Ingest

Canonical historical EOD updates remain allowed through approved ingest/history paths only.

### Rules

- `source-daily` may update canonical `security_prices_eod`.
- historical backfill/repair scripts may update canonical `security_prices_eod`.
- serving-time logic may not update canonical `security_prices_eod`.

This preserves deterministic, reproducible model-estimation inputs.

## D. PIT Source Timeline

Separate cadence:

- `fundamentals_asof`
- `classification_asof`

These remain distinct from both the weekly stable core package and the daily serving layer.

## Explicit Enforcement Rules

1. `serve-refresh` never computes or persists:
   - factor returns
   - covariance
   - specific risk

2. `serve-refresh` never advances:
   - `core_state_through_date`
   - `core_rebuild_date`

3. If the stable core package is missing or stale, `serve-refresh` fails closed and instructs the operator to run a core lane.

4. Serving/orchestration/API layers must not write into:
   - `security_prices_eod`
   - other canonical model-estimation history tables

5. Canonical historical EOD writes remain restricted to approved ingest/history paths.
