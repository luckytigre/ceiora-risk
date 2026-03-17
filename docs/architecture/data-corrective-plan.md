# Data Corrective Plan

Date: 2026-03-17
Scope: Focused corrective pass only

## Area 1 — Neon factor-return / covariance publish drift

- Status:
  - completed on 2026-03-17

- Symptom:
  - Neon durable factor returns and covariance are behind local.
  - Recent Neon factor-return / covariance rows were contaminated by test-shaped runs.
- Likely root cause:
  - Publish-layer drift: factor returns use a separate Neon mirror path from stale `cache.db/daily_factor_returns` instead of the durable local `data.db/model_factor_returns_daily`.
  - Test isolation was weak enough that test-shaped model-output rows reached live Neon.
- Source of truth:
  - Local durable model-output tables in `backend/runtime/data.db`:
    - `model_factor_returns_daily`
    - `model_factor_covariance_daily`
    - `model_specific_risk_daily`
    - `model_run_metadata`
- Systems affected:
  - Neon durable model tables
  - bounded parity audit
  - broad Neon mirror cycle
  - tests that exercise model-output persistence
- Proposed fix:
  - Make `model_factor_returns_daily` part of the same canonical SQLite-to-Neon durable sync path as covariance / specific risk / run metadata.
  - Stop broad mirror from sourcing durable Neon factor returns from stale cache-era `daily_factor_returns`.
  - Add a test guard so normal test runs cannot write real model outputs into live Neon unless explicitly mocked / opted in.
  - Repair Neon by resyncing the authoritative local durable model tables.
- Validation method:
  - compare local vs Neon latest dates and latest-row counts for the four durable model tables
  - run targeted tests around canonical table coverage, bounded parity, and model-output test isolation
  - confirm live Neon no longer contains test-shaped latest rows
- Risk if left unfixed:
  - live Neon can be silently overwritten with stale or non-production factor data
  - risk pages and what-if analytics can regress even when the local rebuild is healthy

- Completed corrective result:
  - `model_factor_returns_daily` now syncs through the durable SQLite-to-Neon model-table path.
  - broad Neon mirror now reports factor-return sync from durable `model_factor_returns_daily`, not cache `daily_factor_returns`.
  - live Neon durable model tables were fully resynced from local authoritative state.
  - test-shaped live Neon rows were removed by the full durable-table reload.
  - default test runs now disable live Neon durable-model writes unless a test explicitly overrides the config with mocks.

- Validation note:
  - table freshness and row counts now match between local and Neon for factor returns, covariance, specific risk, and model run metadata.
  - the old stage-2 parity audit still reports a formatting-only timestamp mismatch for `model_run_metadata` (`T...+00:00` vs ` ...+00`), but not a data mismatch in the repaired model tables.

## Area 2 — Intermittent source-history spikes and missing-price holes

- Status:
  - completed on 2026-03-17

- Symptom:
  - local price and raw-cross-section counts spike on selected dates
  - some names show intermittent missing prices despite LSEG having values
- Likely root cause:
  - source-data-layer universe selection was too broad for default source refreshes, so repair / ingest runs kept pulling low-quality lineage or venue-alias rows into current source tables
  - downstream raw-history and current cross-section builds trusted all strict-flagged `security_master` rows instead of the canonical default source universe
- Source of truth:
  - local source archive tables:
    - `security_prices_eod`
    - `security_master`
    - `barra_raw_cross_section_history`
- Systems affected:
  - eligibility
  - ESTU
  - exposure coverage
  - served universe quality
- Proposed fix:
  - define one canonical default source universe for non-explicit refreshes:
    - keep pending seed rows so new names can still be enriched
    - keep only one preferred strict-eligible RIC per ticker
    - exclude obvious lineage / secondary-venue artifacts
    - exclude names with only degenerate recent spike-only price rows
  - use that canonical default source universe in:
    - default daily LSEG ingest
    - default price backfill
    - default PIT backfill completeness counting
    - raw cross-section rebuild
    - current cross-section snapshot build
  - repair the live local source tables by deleting only the objectively degenerate recent spike rows, then rebuild the latest raw cross-section and current cross-section snapshot
- Validation method:
  - row-count trend checks
  - targeted ticker/date gap checks
  - refreshed eligibility / raw-history sanity on affected names
- Risk if left unfixed:
  - recurring eligibility noise, `missing_style` inflation, and unstable row counts

- Completed corrective result:
  - canonical default source-universe selection now excludes the spike-only legacy / lineage rows while preserving pending seed rows and valid current equities
  - raw cross-section rebuild and current cross-section snapshot now use the same canonical default source-universe filter
  - live local source repair removed `484` degenerate recent price rows across `242` spike-only RICs and rebuilt the latest raw / snapshot surfaces
  - representative bad examples (`NBL.O^J20`, `VTRU.P`, `STR.N`) are now absent from current recent source rows and the latest raw cross-section, while valid names like `AAPL.OQ` remain current and eligible

- Validation note:
  - canonical default source universe shrank from `4,195` strict-eligible `security_master` rows to `3,273` current-source rows and excludes all `243` previously identified spike-only recent names
  - local `security_prices_eod` distinct RIC counts on the spike dates moved from `3942` / `3930` to `3700` / `3688`
  - local `barra_raw_cross_section_history` latest-date count moved from `3930` to `3085`
  - current `universe_cross_section_snapshot` latest-date count moved from `3930` to `3085`

## Area 3 — Stale cache-era metadata tables and misleading read surfaces

- Status:
  - completed on 2026-03-17

- Symptom:
  - exposure metadata still reported old coverage dates and stale cross-section counts
  - served payload source dates for fundamentals / classifications drifted from the month-end PIT policy
  - Data diagnostics still depended on cache-era tables that were frozen in 2017
- Likely root cause:
  - metadata/read-layer drift:
    - live factor coverage still read from legacy `cache.db/daily_factor_returns`
    - diagnostics still treated cache-era summary tables as normal current metadata sources
    - current serving payloads had to be republished after the PIT date fix so stale stored `source_dates` would stop winning in app reads
- Source of truth:
  - durable current serving payloads in `serving_payload_current`
  - durable model-output tables in `backend/runtime/data.db`, especially `model_factor_returns_daily`
  - authoritative source-date reads from `core_reads.load_source_dates()`
- Systems affected:
  - Risk / Exposures / Positions summary metadata
  - Data Pipeline Overview cards
  - diagnostics and metadata sanity checks
- Proposed fix:
  - retire cache-era factor coverage from live refresh reads and prefer durable `model_factor_returns_daily`
  - prefer durable serving/model metadata surfaces over legacy cache tables in diagnostics
  - republish the current serving snapshot so corrected PIT dates and coverage metadata replace stale stored payloads
- Validation method:
  - targeted tests for durable-factor-coverage preference, diagnostics-source selection, and staged payload PIT dates
  - live API checks on `/api/risk`, `/api/exposures`, and `/api/data/diagnostics`
  - direct inspection of `serving_payload_current`
- Risk if left unfixed:
  - UI drift, misleading diagnostics, and repeated reintroduction of stale cache-era values into live payloads

- Completed corrective result:
  - live factor coverage metadata now comes from durable `model_factor_returns_daily`, not legacy `daily_factor_returns`
  - diagnostics now prefer durable serving/model metadata surfaces and mark legacy cache tables as secondary compatibility state
  - the current serving snapshot was republished so `fundamentals_asof` / `classification_asof` now reflect the closed PIT anchor and factor coverage metadata now reflects the current model date
  - `daily_factor_returns` and `daily_universe_eligibility_summary` remain on disk for compatibility / forensics, but no longer win in the active metadata read paths addressed by this pass

- Validation note:
  - live `/api/risk` and `/api/exposures` now report `fundamentals_asof = 2026-02-27`, `classification_asof = 2026-02-27`, and factor coverage metadata on `2026-03-13`
  - live `/api/data/diagnostics` now reports current eligibility and factor-cross-section values from durable metadata surfaces after backend reload
  - the legacy cache tables remain frozen at `2017-12-22` and `2017-01-04`, but they are now compatibility-only for this corrective scope
