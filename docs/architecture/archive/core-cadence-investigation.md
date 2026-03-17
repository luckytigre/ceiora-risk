# Core Cadence Investigation

Date: 2026-03-17
Status: Active investigation record
Owner: Codex

## Scope

This investigation checked whether any non-core path can:

- compute or persist factor returns
- compute or persist covariance or specific risk
- advance `core_state_through_date` / equivalent core metadata
- write serving-time prices into canonical historical model-estimation tables

The reviewed surfaces were:

- `backend/orchestration/*`
- `backend/analytics/pipeline.py`
- `backend/risk_model/daily_factor_returns.py`
- canonical source-ingest scripts
- current frontend/operator date semantics

## Findings Before This Pass

### 1. Core artifact computation is intended to live in core stages

Explicit core-artifact compute lives in:

- [stage_core.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/backend/orchestration/stage_core.py)
  - `factor_returns`
  - `risk_model`
- [daily_factor_returns.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/backend/risk_model/daily_factor_returns.py)
  - computes the factor-return series and eligibility summaries
- [model_outputs.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/backend/data/model_outputs.py)
  - persists durable factor returns / covariance / specific risk / run metadata

This already matched the intended core-package design.

### 2. A serving-only path could still fall through into core recompute

Before this pass, the orchestrated `serving_refresh` path called [run_refresh()](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/backend/analytics/pipeline.py) in light mode.

If `serving_refresh_skip_risk_engine(...)` returned `False` because:

- the current core package was due for rebuild
- covariance cache was missing
- specific-risk cache was missing
- risk-engine method/version was stale

then `run_refresh(mode="light")` could still recompute:

- factor returns
- covariance
- specific risk

That meant a `serve-refresh`-class path could effectively advance the stable core package. This violated the intended operating model.

### 3. `source-daily` updates canonical historical EOD source data, not serving-time prices

Canonical historical price updates are performed through approved ingest/history paths:

- [download_data_lseg.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/backend/scripts/download_data_lseg.py)
- [backfill_prices_range_lseg.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/backend/scripts/backfill_prices_range_lseg.py)
- [backfill_pit_history_lseg.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/backend/scripts/backfill_pit_history_lseg.py)

These are the approved writers to canonical `security_prices_eod`.

### 4. No distinct serving-time price writer was found

The serving/projection paths read prices from canonical reads such as:

- `core_reads.load_latest_prices(...)`
- `security_prices_eod`

But no serving/orchestration/API path was found that writes ad hoc, live, or serving-time prices into:

- `security_prices_eod`
- other core estimation-history tables

So the current repo does **not** show a separate live-pricing subsystem contaminating canonical history.

### 5. Current UI ambiguity was mostly semantics, not calculation drift

The frontend had already been moved away from one ambiguous `Model` date. The remaining timing confusion was mainly conceptual:

- the stable core package has its own dates
- served loadings have their own dates
- PIT source dates are separate again

The current compact summary and Health page were already close to the desired model, but the backend enforcement needed to match the semantics.

## Enforcement Landed In This Pass

### 1. `serve-refresh` now fails closed if the stable core package is not reusable

[stage_serving.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/backend/orchestration/stage_serving.py) now rejects a serving-only refresh when the current core package is:

- missing
- stale
- due for rebuild

instead of falling through into recompute.

### 2. Light refresh has an explicit stable-core guard

[pipeline.py](/Users/shaun/Library/CloudStorage/Dropbox/040%20-%20Creating/barra-dashboard/backend/analytics/pipeline.py) now supports an explicit `enforce_stable_core_package` guard so serving-only callers fail closed instead of silently recomputing core artifacts.

## Current Behavior After This Pass

### What changes only on core rebuild

- factor returns
- covariance
- specific risk
- durable core metadata:
  - `core_state_through_date`
  - `core_rebuild_date`
  - `estimation_exposure_anchor_date` when available

### What changes on `serve-refresh`

- holdings-driven projections
- current loadings / served exposures
- portfolio outputs
- serving payload snapshots

`serve-refresh` does **not** compute or persist new factor returns, covariance, or specific risk, and does **not** advance `core_state_through_date`.

### What changes on `source-daily`

- canonical `security_prices_eod` through approved ingest/history repair
- canonical PIT source tables through approved monthly/repair paths
- serving outputs may be refreshed afterward

This is canonical historical ingest, not serving-time pricing.

### Serving-time prices vs canonical history

No serving-time price writer was found in the serving/orchestration/API layers.

Canonical `security_prices_eod` writes remain confined to approved ingest/history scripts. That is now also guarded by a lightweight architecture test.
