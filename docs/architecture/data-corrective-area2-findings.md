# Area 2 Findings — Source-History Spikes And Missing-Price Holes

Date: 2026-03-17
Scope: focused corrective pass only

## Symptom Summary

- `security_prices_eod` and `barra_raw_cross_section_history` showed obvious recent spikes on:
  - `2026-03-04`
  - `2026-03-13`
- Representative missing-price cases such as:
  - `NBL.O^J20`
  - `VTRU.P`
  - `STR.N`
  were not broad “LSEG missing data” failures.
- The current served snapshot was healthy, but these rows remained capable of re-polluting future raw-history and coverage surfaces.

## Confirmed Root-Cause Classes

### 1) Source-data problem

Default source refreshes were admitting too many current-source candidates from `security_master`.

Confirmed classes:
- lineage / corporate-action RICs with caret suffixes, e.g. `ABMD.O^L22`, `AKRO.OQ^L25`
- secondary / alias venues, e.g. PSX or “when trading” rows
- pink-sheet style rows
- duplicate ticker variants where one canonical primary-style RIC should win
- recent spike-only rows with:
  - exactly two recent observations
  - identical close values
  - zero recent volume

These rows were often still flagged `classification_ok = 1` and `is_equity_eligible = 1`, so strict-flagged selection alone was not enough.

### 2) Ingest / repair problem

The suspicious recent rows were being reintroduced by non-explicit source refreshes / repairs because the default source universe was too broad.

Evidence:
- `243` names had rows only on `2026-03-04` and `2026-03-13` across the recent session window.
- `242` of those had zero recent volume on both dates.
- all `243` had identical close values across the two dates.

This is not normal current daily price behavior.

### 3) Downstream read / serving symptom

Raw-history and current cross-section builds trusted broad strict-flagged `security_master` membership instead of the canonical default source universe.

That allowed the bad source rows to leak into:
- `barra_raw_cross_section_history`
- `universe_cross_section_snapshot`
- eligibility / missing-style counts

## Representative Examples

### Missing-price examples

- `NBL.O^J20`
  - recent source rows were only the degenerate spike rows
  - latest raw-cross-section row should not exist
  - correct treatment: exclude from canonical default source universe
- `VTRU.P`
  - same pattern: isolated recent rows, identical close, zero volume
  - correct treatment: exclude from canonical default source universe
- `STR.N`
  - same recent degenerate pattern even though the suffix looks normal
  - correct treatment: exclude via recent-price-quality filter rather than suffix-only rule

### Spike drivers

The spike set was not one homogeneous class. It contained:
- many caret lineage rows
- many exchange-alias or venue-alias rows
- some unique-ticker stale names that only the recent-price-quality rule catches

That is why one suffix rule alone would not have been enough.

## Correct Treatment By Issue Class

- lineage / consolidated / pink-sheet / obvious venue-alias rows:
  - source exclusion / quality filter
- duplicate ticker multi-RIC rows:
  - canonical one-RIC-per-ticker selection for default source refreshes
- spike-only identical-close zero-volume rows:
  - source exclusion / quality filter
  - plus one-time live repair delete for the recent bad rows already written
- explicit requested repairs:
  - still allowed
  - this pass did not block explicit `--rics` repair paths

## Implemented Fixes

Code-path fixes:
- added canonical default source-universe selection in `backend/universe/security_master_sync.py`
- wired that selection into:
  - `backend/scripts/download_data_lseg.py`
  - `backend/scripts/backfill_prices_range_lseg.py`
  - `backend/scripts/backfill_pit_history_lseg.py`
  - `backend/risk_model/raw_cross_section_history.py`
  - `backend/data/cross_section_snapshot_build.py`

Live data repair:
- deleted `484` objectively degenerate recent rows from `security_prices_eod`
- deleted matching recent rows from `barra_raw_cross_section_history`
- rebuilt latest raw cross-section and current cross-section snapshot

## Effect

Before:
- `security_prices_eod` distinct RIC count:
  - `2026-03-04`: `3942`
  - `2026-03-13`: `3930`
- `barra_raw_cross_section_history` distinct RIC count:
  - `2026-03-13`: `3930`

After:
- `security_prices_eod` distinct RIC count:
  - `2026-03-04`: `3700`
  - `2026-03-13`: `3688`
- `barra_raw_cross_section_history` distinct RIC count:
  - `2026-03-13`: `3085`
- `universe_cross_section_snapshot` latest-date rows:
  - `3085`

Examples after repair:
- `NBL.O^J20`: no recent price rows, no latest raw row
- `VTRU.P`: no recent price rows, no latest raw row
- `STR.N`: no recent price rows, no latest raw row
- `AAPL.OQ`: current recent price coverage intact, latest raw row present, API still eligible

## Intentionally Deferred

- stale served-payload metadata such as old coverage-date fields
- broader `security_master` registry curation / long-tail canonical-universe cleanup
- any deeper redesign of eligibility policy beyond the narrow default-source correction
