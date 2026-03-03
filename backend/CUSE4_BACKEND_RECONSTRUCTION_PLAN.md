# cUSE4 Backend Reconstruction Plan

Date: 2026-03-03  
Branch: refactor/cuse4-backend-master-plan

## 1) Context and Constraints

This plan replaces the backend data-collection + analytics engine while preserving:
- LSEG toolkit integration (`backend/vendor/lseg_toolkit`, `backend/scripts/download_data_lseg.py`)
- Trading-day calendarization (`backend/trading_calendar.py`)

The target state follows `cUSE4_engine_spec.md`.

## 2) Audit Summary (Current State)

### 2.1 Runtime Architecture
- FastAPI app routes are cache-driven (`backend/routes/*`) and call a single refresh manager (`backend/services/refresh_manager.py`).
- The refresh path (`backend/analytics/pipeline.py`) orchestrates:
  1. source snapshot rebuild,
  2. risk-engine recompute gating,
  3. portfolio/universe projections,
  4. cache writes.

### 2.2 Current Data Model
- Primary source DB: `backend/data.db`
- Cache DB: `backend/cache.db`
- Core source tables in active use:
  - `fundamental_snapshots` (283,492 rows)
  - `prices_daily` (7,342,315 rows)
  - `trbc_industry_history` (147,036 rows)
  - `barra_raw_cross_section_history` (146,913 rows)
  - `universe_cross_section_snapshot` (146,913 rows)
  - `universe_eligibility_summary` (4,160 rows)

Observed universe identity issue aligns with spec:
- `universe_eligibility_summary` synthetic IDs (`permid LIKE 'RIC::%'`): 1,311 rows.

### 2.3 Current Modeling Engine
- Factor returns: `backend/barra/daily_factor_returns.py`
- Covariance: `backend/barra/covariance.py` (EWMA + NW + shrinkage already present)
- Specific risk: `backend/barra/specific_risk.py`
- Eligibility: `backend/barra/eligibility.py`
- Style canonicalization and orthogonalization: `backend/barra/descriptors.py`

### 2.4 Gaps vs cUSE4 Spec
- Source-of-truth schemas do not yet match target (`security_master`, `fundamentals_history`, `trbc_industry_country_history`, `estu_membership_daily`).
- ESTU audit persistence is currently summary-level in cache, not per `(date, sid)` durable table.
- Fundamental history is keyed by `ticker/fetch_date` instead of canonical `sid/as_of_date/stat_date` contract.
- Identity layer is spread across `ticker_ric_map` and `universe_eligibility_summary`; no canonical `sid` table.
- Raw cross-section builder still depends on legacy field names and does not enforce all denominator/edge-case rules in spec.

## 3) Target Backend Design

### 3.1 New Canonical Source Layer (SQLite)
- `security_master` (canonical identity + equity eligibility flags)
- `fundamentals_history` (PIT-safe historical fundamentals)
- `trbc_industry_country_history` (PIT TRBC + country)
- `prices_daily` (retained)
- `estu_membership_daily` (daily ESTU membership audit)

### 3.2 New Engine Modules (`backend/cuse4/`)
- `settings.py`: fixed model policy/versioned knobs
- `schema.py`: DDL + schema migration/ensure utilities
- `bootstrap.py`: adapters that map legacy tables into canonical cUSE4 tables
- `estu.py`: ESTU construction + drop-reason audit persistence
- `descriptors.py`: descriptor transforms, denominator policy, winsorization stats
- `factor_returns.py`: constrained cross-sectional WLS on ESTU
- `covariance.py`: NW + shrinkage profile variants (`cUSE4-S`, `cUSE4-L`)
- `specific_risk.py`: residual-process specific risk
- `pipeline.py`: orchestration wrapper callable from API refresh flow

### 3.3 API Compatibility Strategy
- Preserve existing `/api/*` contracts for frontend continuity.
- Keep cache keys stable during transition (`portfolio`, `risk`, `exposures`, etc.).
- Introduce additive cUSE4 diagnostics keys/routes before cutover.

## 4) Phased Execution Plan

## Phase A: Foundation and Observability
1. Create plan + progress log docs.
2. Add `backend/cuse4/` package scaffolding + config/version policy.
3. Implement canonical schema creation for cUSE4 tables.
4. Add bootstrap script to populate cUSE4 source tables from existing tables.
5. Add ESTU audit table writer with explicit drop reasons.

Exit criteria:
- cUSE4 tables exist and are populated.
- ESTU audit rows are written for latest trading dates.
- Work log updated with reproducible commands + outputs.

## Phase B: Engine Core Replacement
1. Replace legacy eligibility path with ESTU-first path from `estu_membership_daily`.
2. Rebuild descriptor assembly under cUSE4 denominator/winsorization policy.
3. Replace daily factor-return compute with constrained WLS over ESTU.
4. Wire covariance + specific risk onto new factor/specific return streams.
5. Emit regression + orth diagnostics to durable tables.

Exit criteria:
- Full cUSE4 run completes end-to-end on local data.
- Diagnostics and QA gates persist and are queryable.

## Phase C: Cutover and Cleanup
1. Switch refresh pipeline to cUSE4 engine as default path.
2. Keep legacy engine behind fallback flag for rollback window.
3. Remove dead code paths and legacy schema dependencies.
4. Update operations playbook and runbook commands.

Exit criteria:
- `/api/refresh` uses cUSE4 path by default.
- Legacy path optional/disabled by config.

## 5) QA Gates (Mandatory)

- Schema checks:
  - unique keys, non-null identity fields, row-count sanity
- ESTU checks:
  - ESTU size trend, turnover, drop-reason distributions
- Descriptor checks:
  - missing %, clipped %, post-standardization moments
- Regression checks:
  - coverage, R², condition numbers, residual outliers
- Risk checks:
  - PSD covariance, volatility stability, specific-risk floor behavior

## 6) Commit Strategy

Commits are grouped by logical capability:
1. docs/audit/plan/log
2. cUSE4 schema + bootstrap
3. ESTU audit pipeline integration
4. descriptor/factor-return engine replacement
5. covariance/specific-risk cutover
6. API wiring + cleanup

## 7) Immediate Work Started

Implementation has started with Phase A in this branch.  
See `backend/CUSE4_WORK_PROGRESS_LOG.md` for step-by-step progress updates.
