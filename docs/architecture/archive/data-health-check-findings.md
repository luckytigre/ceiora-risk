# Data Health Check Findings

Date: 2026-03-16  
Mode: Read-only

## Scope

This check covered:

- local source archive and cache
- Neon authoritative source / model tables
- current serving snapshot and runtime state
- selected fresh LSEG point-in-time comparisons

No database writes were performed as part of this health check.

## Confirmed Findings

### 1. Neon is materially behind local for core model outputs

Evidence:

- Local `model_factor_returns_daily` latest date: `2026-03-13`, `45` rows on the latest date
- Neon `model_factor_returns_daily` latest date: `2026-03-03`, `1` row on the latest date
- Local `model_factor_covariance_daily` latest date: `2026-03-13`, `2025` rows on the latest date
- Neon `model_factor_covariance_daily` latest date: `2026-03-03`, `1` row on the latest date

Assessment:

- Systemic
- High confidence

Likely consequence:

- Any Neon-first read path that relies on durable model outputs can return stale or numerically broken risk inputs.

### 2. Neon raw cross-section history is missing seven repaired names on the latest date

Evidence:

- Local latest raw cross-section distinct `ric` count on `2026-03-13`: `3930`
- Neon latest raw cross-section distinct `ric` count on `2026-03-13`: `3923`
- Missing names in Neon:
  - `AAL.OQ`
  - `AAMI.K`
  - `AAMI.N`
  - `AAOI.OQ`
  - `AAON.OQ`
  - `AAP.N`
  - `AAPL.OQ`

Assessment:

- Isolated set, but operationally important
- High confidence

Likely consequence:

- Neon-backed rebuilds or serving reads can regress repaired eligibility and exposure coverage for valid equities.

### 3. Local source archive has intermittent row-count spikes tied to low-quality extra names

Evidence:

- `security_prices_eod` distinct `ric` counts jump from roughly `3670-3690` on most recent sessions to:
  - `3942` on `2026-03-04`
  - `3930` on `2026-03-13`
- `barra_raw_cross_section_history` mirrors the same spikes
- Extra names are heavily legacy / special / discontinuous listings such as:
  - `0US7.L`
  - `ACV^E11`
  - `ADCT.O^L10`
  - `ADVS.O^G15`
  - `AGN^E20`
  - `AHL.N`
  - `AKS^C20`

Assessment:

- Systemic ingestion / history-shaping issue
- High confidence

Likely consequence:

- unstable universe size
- many `missing_style` or otherwise ineligible names entering the current source archive
- noisy eligibility and loadings coverage metrics

### 4. ESTU membership has an obviously bad zero-membership day

Evidence:

- `estu_membership_daily` contains:
  - `2026-02-27`
  - `2026-03-03`
  - `2026-03-04`
- `2026-03-03` has `0` rows with `estu_flag = 1`
- `2026-03-04` returns to a normal `2799` rows with `estu_flag = 1`

Assessment:

- Systemic for that date, likely caused by an upstream data-shaping break
- High confidence

Likely consequence:

- broken or skipped factor-return production around that date
- misleading eligibility / regression-member history

### 5. Durable factor-return history has recent date gaps

Evidence:

- `model_factor_returns_daily` is present for:
  - `2026-03-03`
  - `2026-03-10`
  - `2026-03-11`
  - `2026-03-12`
  - `2026-03-13`
- Missing entirely for:
  - `2026-03-04`
  - `2026-03-05`
  - `2026-03-06`
  - `2026-03-09`

Assessment:

- Systemic within the recent window
- High confidence

Likely consequence:

- gaps in the durable model record
- increased risk of stale or misleading factor coverage metadata

### 6. Current exposure values are populated, but exposure metadata still comes from stale cache-era tables

Evidence:

- Current `/api/exposures` values are populated in all modes and non-Beta factors are non-zero.
- But current per-factor metadata still reports values such as:
  - `coverage_date = 2017-12-22`
  - `eligible_n = 2685`
  - `cross_section_n = 2669`
- Local `cache.db` tables show:
  - `daily_factor_returns` max(date) = `2017-12-22`
  - `daily_universe_eligibility_summary` max(date) = `2017-01-04`

Assessment:

- Systemic metadata drift
- High confidence

Likely consequence:

- UI or downstream consumers can display current factor values with obviously stale coverage context.

### 7. Data diagnostics are healthy only because fallback logic compensates for stale cache tables

Evidence:

- `/api/data/diagnostics` is currently populated and current.
- Underlying cache-era diagnostics tables remain stale.
- The service is falling back to newer durable truth instead of the old cache tables.

Assessment:

- Systemic stale cache issue, partially masked by service logic
- High confidence

Likely consequence:

- diagnostics can silently degrade if fallback logic changes or if a route starts trusting cache-era tables directly again

### 8. Neon `model_run_metadata` appears contaminated with test rows

Evidence:

- Top rows by `updated_at` in Neon include:
  - `run_without_value`
  - `run_with_value`
  - `run_2`
  - `run_1`
  - `test_run`
- These identifiers also exist in backend tests.

Assessment:

- Systemic environment / test-isolation problem
- High confidence

Likely consequence:

- operator views or audits that trust raw Neon metadata ordering can be polluted by test artifacts

### 9. Served snapshot alignment is currently good

Evidence:

- All `serving_payload_current` rows share snapshot `model_run_20260317T031226Z`
- `runtime_state_current.__cache_snapshot_active` points to the same snapshot

Assessment:

- Healthy current-state condition
- High confidence

Implication:

- The latest served snapshot itself is internally aligned even though several upstream durable and cache surfaces are not.

### 10. Previously reported current-value bugs are not currently reproduced at the API layer

Evidence:

- `/api/universe/ticker/AAPL` now returns `model_status = "core_estimated"` with populated exposures
- `/api/exposures` raw / sensitivity / risk-contribution modes all return non-zero non-Beta factors
- `/api/risk` returns plausible decomposition shares

Assessment:

- Current symptom resolved at the served-value layer
- High confidence

Important caveat:

- The underlying durability and metadata issues remain, especially Neon drift and stale cache-driven metadata.

## Hypotheses Needing Follow-Up

### A. Intermittent price-history spikes may come from a loose source filter or repair path

Confidence:

- Medium

Reason:

- The extra names cluster in legacy or special listings and appear only on selected dates.

### B. Factor-return gaps likely connect to the bad ESTU / source-history window

Confidence:

- Medium-high

Reason:

- The zero-membership ESTU date and factor-return gaps line up in the same recent period.

### C. Stale cache-era metadata is probably no longer suitable as a canonical source

Confidence:

- High

Reason:

- Current served analytics are correct only when newer durable truth overrides those tables.

## Top Areas To Investigate Next

1. Why local source ingestion intermittently admits the extra legacy / low-quality names on only selected dates.
2. Why factor returns and covariance were not durably mirrored to Neon after recent rebuilds.
3. Why factor-return history is missing `2026-03-04` through `2026-03-09` despite recent model activity.
4. Whether stale cache-era tables should be retired, rebuilt, or fully removed from live metadata paths.
5. How Neon test artifacts reached `model_run_metadata`, and how to isolate tests from production-like Neon state.
