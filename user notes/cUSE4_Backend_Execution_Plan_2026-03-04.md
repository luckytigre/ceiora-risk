# cUSE4 Backend Execution Plan (Pre-Cloud Cleanup)

Date: 2026-03-04
Owner: Shaun + Codex
Scope: Clean, modular backend refactor now; cloud DB cutover readiness in ~1 week.

## 0) Execution Status (2026-03-04, latest)

Completed in current refactor pass:
- Runtime readers moved to canonical-only paths (no legacy view SQL fallbacks in `analytics/barra/db/cuse4` runtime modules).
- Legacy bootstrap behavior retired; bootstrap is now canonical schema ensure + row-count report only.
- Compatibility views dropped from `backend/data.db`:
  - `ticker_ric_map`
  - `fundamental_snapshots`
  - `trbc_industry_history`
  - `prices_daily`
  - `fundamentals_history`
  - `trbc_industry_country_history`
- Legacy migration/resolver scripts moved to `backend/scripts/_archive/`.
- Ops hardening/cleanup scripts repointed to canonical tables.

Completed in latest optimization sweep:
- `universe_cross_section_snapshot` physical key migrated to `(ric, as_of_date)` and legacy `price_exchange` removed.
- Redundant large SQLite indexes dropped from canonical/source tables:
  - `idx_security_prices_eod_ric_date`
  - `idx_security_fundamentals_pit_ric_asof`
  - `idx_security_classification_pit_ric_asof`
  - `idx_barra_raw_cross_section_history_ric`
- `security_master` index ownership normalized on active table and migration artifact table removed.
- Cross-section snapshot builder refactored to RIC-native joins/filters (removed `UPPER(...)` join path and ticker-window partitions).
- Relational model output writes made incremental (write latest date slice instead of full-history rewrites each refresh).
- Removed duplicated durable residual-history table from Layer B outputs:
  - dropped `model_specific_residuals_daily` from schema + writer path
  - retained residual history only in `cache.db.daily_specific_residuals` (compute workspace)
- Data diagnostics endpoint reduced to canonical table set only (legacy table probes removed).
- Full DB compaction completed:
  - `data.db` reclaimed `1,471,516,672` bytes
  - `cache.db` reclaimed `8,192` bytes
- Universe baseline updated after coverage-universe augmentation:
  - `security_master` eligible RICs: `5,819`
  - distinct tickers in `security_master`: `4,956`
  - PIT monthly backfill for fundamentals/classification completed through `2026-02-27`.

Current remaining work (non-blocking):
- Optional archival cleanup under `backend/scripts/_archive/` if you want those files physically deleted instead of retained as history.
- Cloud migration preparation (Postgres DDL parity + repository wiring) when you are ready next week.

## 1) What "Clean and Organized" Means for This Project

This plan optimizes for:
- Single source of truth for inputs (no duplicate persisted datasets).
- Deterministic processing flow (same input -> same output).
- Clear boundary between ingest, model processing, and serving.
- Explicit lineage from LSEG pull to final risk outputs.
- Easy migration from local SQLite to Postgres (Neon/Aurora) with minimal logic rewrite.
- One canonical identity key in runtime data handling: `ric` (not `sid`).

Design terms that matter here:
- Separation of concerns: each module does one job.
- Idempotent jobs: safe to rerun without corrupting state.
- Contract-first tables: schemas are stable interfaces.
- Incremental compute: recompute only what changed where possible.

## 2) Current Canonical Model (Keep)

Canonical input tables (data.db):
- `security_master`
- `security_prices_eod`
- `security_fundamentals_pit`
- `security_classification_pit`
- `estu_membership_daily`

Canonical key policy:
- `security_master`: primary key is `ric` and remains the identity hub.
- `security_prices_eod`: store and key by `(ric, date)`.
- `security_fundamentals_pit`: store and key by `(ric, as_of_date, stat_date)`.
- `security_classification_pit`: store and key by `(ric, as_of_date)`.
- `estu_membership_daily`: store and key by `(date, ric)`.
- `sid` is deprecated for joins and storage in canonical time-series paths.

Derived/serving tables that can remain:
- `barra_raw_cross_section_history`
- `universe_cross_section_snapshot` (current-mode default)

Cache DB tables (cache.db):
- `daily_factor_returns`
- `daily_specific_residuals`
- `daily_universe_eligibility_summary`
- `cache`

## 3) Legacy Objects Status

Legacy compatibility views in `data.db` were removed:
- `ticker_ric_map`
- `fundamental_snapshots`
- `trbc_industry_history`
- `prices_daily`
- `fundamentals_history`
- `trbc_industry_country_history`

Current state: no compatibility views remain in active runtime DB.

## 4) Legacy Reader Refactor Matrix

### 4.1 Runtime-critical readers (P0)

1. `backend/barra/daily_factor_returns.py`
- Current legacy read: `prices_daily`
- Target read: `security_prices_eod` joined to `security_master` for ticker projection
- Outcome: remove dependence on `prices_daily` view in risk-engine core

2. `backend/analytics/health.py`
- Current legacy reads: `prices_daily`, `ticker_ric_map`, `fundamental_snapshots`, `trbc_industry_history`
- Target reads:
  - prices: `security_prices_eod`
  - universe list: `security_master`
  - fundamentals: `security_fundamentals_pit`
  - classification: `security_classification_pit`
- Outcome: diagnostics and health runs stay canonical-only
- Additional requirement: no `sid` joins in health diagnostics SQL.

3. `backend/cuse4/bootstrap.py`
- Current legacy reads: `ticker_ric_map`, `fundamental_snapshots`, `trbc_industry_history`, `prices_daily`
- Target:
  - replace bootstrap-from-legacy behavior with canonical no-op / canonical integrity checks
  - or gate legacy bootstrap behind explicit `CUSE4_LEGACY_BOOTSTRAP=1` and default it off
- Outcome: refresh pipeline cannot regress into legacy paths

4. `backend/db/postgres.py`
- Current status: canonical primary path, legacy fallback still present
- Target: remove fallback queries after P0/P1 migration complete
- Outcome: clean canonical data access layer with RIC-only keying in canonical repositories

### 4.2 Operational scripts (P1)

1. `backend/scripts/backfill_trbc_history_lseg.py`
- Legacy dependency: `ticker_ric_map`, `trbc_industry_history`
- Action: deprecate script or rewrite to canonical `security_classification_pit` and `security_master`

2. `backend/scripts/lseg_ric_resolver.py`
- Legacy dependency: `ticker_ric_map`
- Action: fold logic into `security_master` identity resolver or retire script

3. `backend/scripts/harden_source_tables.py`, `backend/db/fundamental_schema.py`, `backend/db/prices_schema.py`
- Legacy dependency: `fundamental_snapshots`, `prices_daily`
- Action: replace with canonical schema checks (`security_*`) or archive

4. `backend/scripts/purge_non_xnys_rows.py`
- Legacy targets mixed in
- Action: repoint to canonical tables only

5. `backend/scripts/sync_universe_lifecycle_dates.py`
- Reads `prices_daily`
- Action: repoint to `security_prices_eod`

### 4.3 Migration/one-time utilities (P2)

1. `backend/scripts/migrate_to_canonical_timeseries.py`
- Keep as historical migration tool but move to `scripts/_archive/` after cloud cutover

2. `backend/scripts/bootstrap_cuse4_source_tables.py`
- Update text and behavior to canonical names only

## 5) Target Processing Architecture (3-Layer)

## Layer A: Inputs (Canonical Source of Truth)

Owned by ingest/backfill jobs only.
- `security_master`
- `security_prices_eod`
- `security_fundamentals_pit`
- `security_classification_pit`
- `estu_membership_daily`

Rules:
- No analytics logic writes here (except ESTU audit table by ESTU job).
- All writes carry `source`, `job_run_id`, `updated_at`.
- All canonical time-series writes include `ric` and never require `sid`.

## Layer B: Model Processing (Derived Data Products)

Owned by feature/model jobs only.
- `barra_raw_cross_section_history`
- `universe_cross_section_snapshot` (`current` mode by default)
- New explicit output tables to add:
  - `model_factor_returns_daily`
  - `model_factor_covariance_daily`
  - `model_specific_risk_daily`
  - `model_run_metadata`

Rules:
- Computed only from canonical input tables.
- Versioned runs with reproducible parameters.
- No direct API writes.

## Layer C: Serving/API

Owned by API/read layer only.
- Reads from Layer B and selected Layer A metadata.
- Cache is a bounded compute workspace + acceleration layer, not source of truth.
- Prefer table-backed outputs over opaque serialized cache blobs where practical.

## 6) Efficient Output Strategy (for your "outputs and stuff")

Current outputs are split across `barra_raw_cross_section_history`, `cache.db` tables, and `cache` key-value blobs.

Proposed cleaner model:
1. Persist durable model outputs as relational tables (Layer B); keep residual history cache-only for specific-risk computation.
2. Use cache only as compute workspace + API acceleration, with short TTL and easy invalidation.
3. Keep one authoritative run manifest in `model_run_metadata`:
   - run_id
   - as_of_date
   - params hash
   - source table max dates
   - row counts
   - status/error
4. Incremental recompute policy:
   - Daily: factor returns for new dates (residual history remains in cache workspace)
   - Weekly: covariance/specific risk full or rolling refresh
   - On demand: full rebuild

This gives speed + traceability + easier cloud migration.

## 7) Folder/Module Organization Recommendation

Short answer: yes, it is good practice for this project.

Reason:
- You have an actual data platform workflow now. Making layers explicit in folders reduces accidental cross-layer coupling.

Recommended structure (incremental, not big-bang):

```text
backend/
  app/
    api/
    services/
  domain/
    ingest/
    universe/
    features/
    model/
    serving/
  infra/
    db/
      repositories/
      migrations/
    lseg/
  jobs/
    schedules/
    runners/
  observability/
    audits/
    qa/
  scripts/
    ops/
    _archive/
```

Migration approach:
- Keep old module paths as thin wrappers while moving logic.
- Move by domain, not by file extension.
- Avoid giant rename PRs.

## 8) Execution Phases (Detailed)

### Phase 0 - Freeze and Contract Baseline (0.5 day)
Deliverables:
- Canonical table contract doc (columns, PKs, indexes).
- Legacy object inventory + reader list frozen.
Acceptance:
- No new code references to legacy view names added.

### Phase 1 - Runtime P0 Refactor (1.5 days)
Tasks:
- Refactor `daily_factor_returns.py` off `prices_daily`.
- Refactor `analytics/health.py` off all legacy views.
- Disable/replace legacy path in `cuse4/bootstrap.py`.
- Keep `postgres.py` fallback temporarily.
Acceptance:
- Full refresh + health endpoints run with legacy views dropped in staging copy.

### Phase 2 - Output Table Normalization (1 day)
Tasks:
- Add `model_*` output tables and writer functions.
- Route pipeline outputs into tables (keep cache as secondary).
- Add `model_run_metadata`.
Acceptance:
- Model outputs queryable via SQL without cache blob dependence.

### Phase 3 - Orchestration and Connectors (1 day)
Tasks:
- Introduce explicit job runner sequence:
  1) ingest
  2) feature build
  3) ESTU audit
  4) factor returns
  5) covariance/specific risk
  6) serving refresh
- Add run-level status tracking.
Implemented:
- New orchestrator module: `backend/jobs/run_model_pipeline.py`
- Stage status table: `job_run_status` (in `backend/data.db`)
- CLI entrypoint:
  - `PYTHONPATH=backend python3 -m jobs.run_model_pipeline ...`
  - `python3 -m backend.scripts.run_model_pipeline ...`
- Profile model (single framework, multiple cadences):
  - `daily-fast`: no core recompute; serving refresh path
  - `daily-with-core-if-due`: core recompute only when interval/method gate says due
  - `weekly-core`: force core recompute then serving refresh
- Resume/selective execution flags:
  - `--resume-run-id`
  - `--from-stage`
  - `--to-stage`
  - `--force-core`
- Runtime/API cutover:
  - `backend/services/refresh_manager.py` now executes orchestrated runs instead of direct `run_refresh`.
  - `backend/routes/refresh.py` now accepts orchestrator params:
    - `profile`
    - `as_of_date`
    - `resume_run_id`
    - `from_stage`
    - `to_stage`
    - `force_core`
  - Legacy params still supported for compatibility:
    - `mode=full|light`
    - `force_risk_recompute` (mapped to `force_core`)
Acceptance:
- Single command can run end-to-end with resumable checkpoints.

### Phase 4 - Script Cleanup and Archival (0.5-1 day)
Tasks:
- Repoint or archive legacy scripts (`harden_source_tables`, `backfill_trbc_history_lseg`, `lseg_ric_resolver`, etc.).
- Move one-time migration scripts to `scripts/_archive/`.
Acceptance:
- `rg` on legacy view names returns only docs/archive (or intentional compatibility layer stubs).
- `rg` on `\\bsid\\b` across runtime paths returns only explicitly approved legacy-compatibility comments/docs.

### Phase 5 - Remove Legacy Views (0.5 day)
Tasks:
- Drop the six compatibility views in a controlled migration.
- Run full smoke test + selected analytics checks.
Acceptance:
- App and jobs run with zero legacy object dependency.

## 9) QA Gates (Must Pass Before Dropping Legacy Views)

1. Coverage parity:
- PIT fundamentals/classification coverage unchanged vs baseline.
- Price history coverage unchanged vs baseline.

2. Model parity (tolerance bands):
- factor return drift within agreed tolerance.
- covariance diagonal drift within agreed tolerance.
- portfolio risk decomposition stable within tolerance.

3. Operational parity:
- refresh runtime within acceptable window.
- no increase in failed jobs / lock errors.

4. Keying and refactor completeness:
- 100% non-null `ric` in `security_prices_eod`, `security_fundamentals_pit`, `security_classification_pit`, `estu_membership_daily`.
- No canonical table PK/unique index still anchored on `sid`.
- No runtime SQL joins keyed on `sid`.
- Referential coverage check passes: canonical table `ric` values map to `security_master.ric`.

## 10) Cloud-Readiness Checklist (Do Now, Migrate Later)

Do now:
- Move all SQL to repository layer functions.
- Add migration framework (Alembic-style SQL migrations for Postgres target).
- Stop using SQLite-specific assumptions (`PRAGMA`, implicit typing).
- Add explicit `NUMERIC/DOUBLE PRECISION/TIMESTAMP` typing conventions in schema docs.

Do at cutover week:
- Provision Neon or Aurora Serverless.
- Replay schema migrations.
- Bulk load canonical tables.
- Switch connection config and run validation suite.

## 11) Suggested Sequence for Next 5 Working Sessions

Session 1:
- Refactor `daily_factor_returns.py` + `analytics/health.py` legacy reads.

Session 2:
- Refactor/disable legacy bootstrap paths (`cuse4/bootstrap.py`).

Session 3:
- Introduce `model_*` output tables and write pipeline outputs there. (Done)

Session 4:
- Implement profile-driven orchestrator + run-stage checkpoints + CLI. (Done)

Session 5:
- Cut API/background refresh manager onto orchestrator profile defaults and run QA.
  - Status: cutover done, QA in progress.

Session 6:
- Execute final no-loose-ends sweep (schema/index/query references), then mark RIC-key cutover + orchestration cutover complete.

## 12) Decision Notes

### Should folder organization visibly reflect layers?
Yes. For this project, that is good practice.

Why:
- Your system is now a data platform + model engine, not a simple app.
- Folder boundaries reduce accidental coupling and make onboarding/debugging easier.

Caution:
- Do it incrementally. Avoid massive path churn in one PR.
- Preserve behavior first, then improve structure.

### Efficiency term you were looking for
The right framing is:
- "deterministic, idempotent, incremental data pipeline with clear data contracts and lineage."

## 13) Audit Closure Update (2026-03-04)

Audit remediation status: **all 8 findings closed**.

Closed items:
1. Style-score build defect fixed (`assemble_full_style_scores` call corrected; swallow removed).
2. False-success path removed (model-output quality gate + hard failure on empty writes).
3. Price ingest/backfill field quality upgraded (OHLCV/currency where vendor supplies).
4. Identifier consistency improved (`ric` physical keys in raw cross-section + residual/specific-risk relational outputs).
5. Synthetic `sid/permid` dependency removed (security master migrated to `ric` primary key; synthetic placeholders normalized to null).
6. Orchestrator ingest stage no longer hardcoded skip (`bootstrap_only` baseline, optional live ingest flag).
7. Automated regression suite added (`backend/tests`, 5 passing tests).
8. SQLite bloat remediation added and executed (compaction script + reclaimed space).

Current runtime quality snapshot:
- Raw style-score completeness (recent): ~99.9%.
- Regression coverage (recent): ~99.5%-99.8%.
- Core recompute succeeded with latest factor-return date: `2026-03-03`.

Operational notes:
- Keep `ORCHESTRATOR_ENABLE_INGEST=false` by default for controlled runs.
- Enable explicitly when live LSEG ingest is desired inside orchestrator.
- Continue periodic DB compaction via `backend/scripts/compact_sqlite_databases.py`.
