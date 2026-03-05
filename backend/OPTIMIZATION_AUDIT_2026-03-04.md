# Optimization Audit - 2026-03-04

## Scope
Professional optimization sweep across:
- organization/hygiene
- runtime speed
- storage efficiency
- data-flow cleanliness
- legacy path removal

## Findings and Remediation

### 1) Snapshot key mismatch (fixed)
Issue:
- `universe_cross_section_snapshot` was still keyed by `(ticker, as_of_date)` while runtime joins had already moved to RIC.

Fix:
- Migrated table key to `(ric, as_of_date)`.
- Added migration-safe rebuild path in `backend/db/cross_section_snapshot.py`.
- Added regression test `test_snapshot_schema_rekeys_to_ric`.

Validation:
- Live DB PK is now `ric, as_of_date`.
- Rebuild runtime (`mode=current`) ~8.9s with `rows_upserted=3019`.

### 2) Redundant high-cost indexes (fixed)
Issue:
- Duplicate indexes existed where PK already provided the same leading key.

Fix:
- Dropped redundant indexes via canonical schema ensure:
  - `idx_security_prices_eod_ric_date`
  - `idx_security_fundamentals_pit_ric_asof`
  - `idx_security_classification_pit_ric_asof`
  - `idx_barra_raw_cross_section_history_ric`

Validation:
- Verified all redundant index names are absent in live DB.

### 3) Security master migration residue and index ownership (fixed)
Issue:
- Legacy migration table/index ownership could remain after prior migrations.

Fix:
- Hardened `backend/cuse4/schema.py` to purge migration artifacts and re-attach active indexes to `security_master`.

Validation:
- `security_master__legacy_pre_ric_pk` absent.
- Active indexes present on `security_master` (`ticker`, `permid`, `sid`, PK on `ric`).

### 4) Snapshot build query inefficiency (fixed)
Issue:
- Prior snapshot path used case-normalized joins/windows that inflated scans.

Fix:
- Refactored snapshot builder to RIC-native joins and grouped max-date selection.
- Merged fundamentals/classification/prices by RIC as-of path.

Validation:
- `mode=current` rebuild consistently under ~10s in this environment.

### 5) Relational model output full-history rewrites (fixed)
Issue:
- Every refresh rewrote all factor-return/residual history into relational tables.

Fix:
- `backend/db/model_outputs.py` now writes incrementally from latest persisted date.

Validation:
- Regression test confirms second write only upserts latest-date slice.

### 6) Diagnostics endpoint legacy drift (fixed)
Issue:
- `/api/data/diagnostics` still probed legacy tables.

Fix:
- Diagnostics now reports canonical source tables only.

Validation:
- Endpoint logic references canonical-only table list in `backend/routes/data.py`.

### 7) Storage bloat from freelist pages (fixed)
Issue:
- `data.db` had large reclaimable freelist after heavy rebuild/migration activity.

Fix:
- Ran `backend/scripts/compact_sqlite_databases.py`.

Validation:
- `data.db`: `5,216,681,984 -> 3,745,165,312` bytes (`1,471,516,672` reclaimed).
- `cache.db`: `776,605,696 -> 776,597,504` bytes (`8,192` reclaimed).
- `PRAGMA quick_check`: `ok` on both DBs.

## Runtime Verification

### Weekly-core orchestration (`feature_build -> serving_refresh`)
Command:
- `python3 -m backend.scripts.run_model_pipeline --profile weekly-core --from-stage feature_build --to-stage serving_refresh --force-core`

Result:
- `status=ok`
- stage durations (from job rows):
  - `feature_build`: ~9.24s
  - `estu_audit`: ~7.35s
  - `factor_returns`: ~56.75s
  - `risk_model`: ~4.96s
  - `serving_refresh`: ~221.72s

### Daily-fast orchestration (`feature_build -> serving_refresh`)
Command:
- `python3 -m backend.scripts.run_model_pipeline --profile daily-fast --from-stage feature_build --to-stage serving_refresh`

Result:
- `status=ok`
- core stages intentionally skipped by profile policy
- total wall time ~45.4s

## Current Storage Profile (post-compaction)
- `backend/data.db`: ~3.57 GB
- `backend/cache.db`: ~0.74 GB
- largest objects are still expected model output/time-series tables:
  - `security_prices_eod`
  - `barra_raw_cross_section_history`
  - `model_factor_returns_daily` / `model_specific_risk_daily` (durable model outputs)
  - `cache.db.daily_specific_residuals` (compute cache, intentionally non-durable)

## Remaining Optional Optimizations
1. Add scheduled compaction cadence for local SQLite (e.g., weekly post-full-refresh).
2. Add TTL pruning for `cache.db.daily_specific_residuals` to bound cache growth.
3. Move model outputs to Postgres first during cloud transition to reduce local SQLite write amplification.
