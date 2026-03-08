# Barra USE4-Inspired Engine Specification

Date: 2026-03-04
Project: barra-dashboard

## 0) Purpose and Operating Intent

`cUSE4` is an adapted, USE4-inspired risk-model engine for a hobby-grade portfolio analytics platform, focused first on long/short equities. It is intended for non-institutional investing and portfolio management, so the design prioritizes interpretability, actionability, stability, and manageable compute/runtime over maximum factor granularity.

Key design tradeoff:
- Compared with full institutional Barra-style stacks (often 50+ factors), `cUSE4` intentionally uses a leaner factor set so exposures are easier to interpret and hedge in practice.

Operational intent:
- Data and analytics are stored in a relational database.
- Holdings are edited in Neon and serving outputs should read those holdings from Neon as the authoritative runtime store.
- Security-level loadings may refresh more frequently, but full model recomputation is intentionally slower (typically weekly, or up to twice weekly).
- Core cross-sectional estimation, factor returns, covariance, and specific-risk calculations run on lagged data only, with a minimum lag of 5 trading days (about one week).
- Over time, historical cross-sections can be backfilled forward toward `t-1`, but full model estimation remains restricted to a lagged cutoff (`t-5` or older) by policy.

Scope note:
- `cUSE4` is USE4-inspired in methodology and discipline, not a proprietary-identical implementation.

## 1) Key Decisions Locked

- Country factor is segmented as `US` vs `non-US` (binary structural block).
- Industry handling uses weighted sum-to-zero constraints (not dropped dummy baseline).
- Current runtime implementation carries country as a simple `Country: US` dummy in phase A.
- Factor covariance will include both:
  - Newey-West adjustment, and
  - shrinkage (toward a structured/shrunk target).
- No explicit market factor in the cross-sectional factor return regression block.

## 2) Current Universe and Data-Model State

Current implemented state:
- Universe definition is centralized in `security_master` (identity hub; canonical time-series joins are RIC-keyed).
- Holdings dirty / `RECALC needed` state is backend-persisted and visible in the operator UI.
- Data page is the primary operator cockpit, with lane controls, lane history, stage detail, and fast/deep diagnostics.
- Eligible investable set is controlled by:
  - `classification_ok = 1`
  - `is_equity_eligible = 1`
- As of this spec revision, the active eligible universe is 5,820 names (current DB run: 5,820 eligible RICs in `security_master`).
- Distinct tickers can be lower than distinct RICs due to share classes/listing aliases.

Data-model state:
- Canonical persisted source-of-truth tables are:
  - `security_fundamentals_pit`
  - `security_classification_pit`
  - `security_prices_eod`
- Compatibility views were removed; canonical readers/writers use `security_*` tables directly.

## 3) Universe and Ingestion Filters

### 3.1 Universe authority and maintenance policy

`security_master` is the only authoritative universe table.
- Universe updates are explicit (file-driven merge into `security_master`), not auto-regenerated from index constituent builders.
- Identity keys and mappings live in `security_master`; they are not duplicated into separate persisted mapping tables.
- The git-versioned universe artifact is `data/reference/security_master_seed.csv`; live DB state is runtime, not source control.

### 3.2 Equity-only ingest scope (all canonical time-series)

Use centralized eligibility rules from `security_master` when ingesting/backfilling canonical tables:
- `classification_ok = 1`
- `is_equity_eligible = 1`

### 3.3 PIT and daily backfill policy

- Fundamentals/classification are PIT-series with anchor-date backfills:
  - monthly baseline history by default.
- Prices are daily EOD history.
- Backfills run in manageable shards/chunks to avoid long stuck processes.
- For targeted repairs, subset backfills by RIC are supported.

### 3.4 ESTU construction and usage (explicit policy)

Build ESTU per date from eligible equities only.
- Require minimum data completeness for factor construction:
  - sufficient price history for return-based descriptors (beta/momentum/reversal/residual vol),
  - required PIT fundamentals for selected descriptors,
  - valid TRBC industry and country classification for structural blocks.
- Apply configurable liquidity, price-floor, and microcap guards.
- Exclude unresolved IDs and non-equity assets.

ESTU is the only set used for estimation steps.
- Use ESTU for descriptor standardization statistics.
- Use ESTU for orthogonalization regressions.
- Use ESTU for daily factor return WLS regression.
- Use ESTU-driven outputs for covariance and specific-risk estimation inputs.

Display and lookup scope remains broader than ESTU.
- Keep full universe for search/display/lookup.
- Non-ESTU names must never influence model coefficients.

Persist ESTU audit artifacts per `(date, ric)`.
- `estu_flag` (0/1)
- `drop_reason` (enumerated reason code)
- `drop_reason_detail` (optional text)
- This supports QA alerts for ESTU size drops and ESTU turnover spikes.

## 4) Implemented Source-of-Truth Schemas (Canonical)

## 4.1 `security_master` (canonical identity/classification)

Metadata columns:
- `ric`
- `ticker`
- `sid` (optional metadata, not the physical key)
- `permid` (optional metadata)
- `isin`
- `instrument_type`
- `asset_category_description`
- `exchange_name`
- `classification_ok`
- `is_equity_eligible`
- `source`
- `job_run_id`
- `updated_at`

LSEG fields:
- `TR.OrganizationID`
- `TR.RIC`
- `TR.TickerSymbol`
- `TR.ISIN`
- `TR.InstrumentType`
- `TR.AssetCategory`
- `TR.ExchangeName`

## 4.2 `security_fundamentals_pit`

Metadata columns:
- `ric`
- `as_of_date`
- `stat_date`
- `period_end_date`
- `fiscal_year`
- `period_type`
- `report_currency`
- `source`
- `job_run_id`
- `updated_at`

Metrics columns + LSEG mapping:
- `market_cap` <- `TR.CompanyMarketCap`
- `shares_outstanding` <- `TR.SharesOutstanding`
- `dividend_yield` <- `TR.DividendYield`
- `book_value_per_share` <- `TR.F.BookValuePerShr`
- `total_assets` <- `TR.F.TotAssets`
- `total_debt` <- `TR.TotalDebt`
- `cash_and_equivalents` <- `TR.CashAndEquivalents`
- `long_term_debt` <- `TR.LongTermDebt`
- `operating_cashflow` <- `TR.CashFromOperatingActivities`
- `capital_expenditures` <- `TR.CapitalExpenditures`
- `trailing_eps` <- `TR.EPSActValue`
- `forward_eps` <- `TR.EPSMean`
- `revenue` <- `TR.Revenue`
- `ebitda` <- `TR.EBITDA`
- `ebit` <- `TR.EBIT`
- `roe_pct` <- `TR.F.ReturnAvgTotEqPct`
- `operating_margin_pct` <- `TR.OperatingMarginPercent`
- `common_name` <- `TR.CommonName`

Notes:
- Gross profit is removed from profitability construction.
- For TR.F fields, ingestion must be historical/range-based and PIT-safe by `stat_date`.
- Primary key: `(ric, as_of_date, stat_date)`.

## 4.3 `security_classification_pit`

Metadata columns:
- `ric`
- `as_of_date`
- `source`
- `job_run_id`
- `updated_at`

Classification columns + LSEG mapping:
- `trbc_economic_sector` <- `TR.TRBCEconomicSector`
- `trbc_business_sector` <- `TR.TRBCBusinessSector`
- `trbc_industry_group` <- `TR.TRBCIndustryGroup`
- `trbc_industry` <- `TR.TRBCIndustry`
- `trbc_activity` <- `TR.TRBCActivity`
- `hq_country_code` <- `TR.HQCountryCode`

Primary key:
- `(ric, as_of_date)`

## 4.4 `security_prices_eod`

Metadata columns:
- `ric`
- `date`
- `source`
- `updated_at`

Metrics columns:
- `open`, `high`, `low`, `close`, `adj_close`, `volume`, `currency`

Policy:
- LSEG-only canonical history after migration validation.
- Standard daily ingest maps `volume` from `TR.Volume`.
- Targeted historical volume-repair mode uses the same metric
  (`backfill_prices_range_lseg.py --volume-only`).
- Primary key: `(ric, date)`.

## 4.5 `estu_membership_daily` (new audit table)

Purpose:
- Daily auditability and reproducibility of estimation-universe membership.

Columns:
- `date`
- `ric`
- `estu_flag`
- `drop_reason`
- `drop_reason_detail`
- `mcap`
- `price_close`
- `adv_20d`
- `has_required_price_history`
- `has_required_fundamentals`
- `has_required_trbc`
- `source`
- `job_run_id`
- `updated_at`

## 4.6 Compatibility Views and Deprecation Policy

- Compatibility views were removed from active runtime DB.
- Legacy migration/resolver scripts are archived under `backend/scripts/_archive/`.
- Canonical ingest/backfill scripts must write directly to `security_*` source-of-truth tables.

## 5) Barra Factors and Metric Roll-up

Structural blocks in regression:
- `country`: binary `US` dummy, emitted in runtime as `Country: US`
- `industry`: TRBC industry-group dummies

Style factors:
- Size: `ln(market_cap)`
- Nonlinear Size: stable nonlinear transform of standardized size
- Beta: EW rolling beta vs market proxy, shrunk toward 1.0
- Momentum: 12-1 return
- Short-term Reversal: 1M reversal proxy
- Residual Volatility: EWMA volatility of residual returns
- Liquidity: equal-weight composite of turnover horizons
  - built from daily `security_prices_eod.volume` (`TR.Volume`) via rolling turnover and ADV transforms
- Value: `book_to_price = BVPS / Price`
- Earnings Yield: 50/50 blend of trailing E/P and forward E/P
- Leverage: `debt_to_assets`
- Growth: 50/50 blend of sales growth and earnings growth
- Investment: asset growth (capex/assets optional later)
- Dividend Yield: `dividend_yield`
- Profitability: equal-weight composite:
  - 50% ROE
  - 50% Sales Margin (operating margin proxy)

## 6) Data Treatment and Orthogonalization Strategy

Per date on ESTU:
1. Transform raw descriptors (log/ratio construction).
2. Winsorize cross-sectionally (default p1/p99; configurable).
3. Standardize:
   - cap-weighted mean centering
   - equal-weighted std scaling
4. Structural neutralization (WLS, cap-based weights):
   - neutralize style descriptors to `country + industry` structural block as configured.
5. Build composites from standardized component descriptors.
6. Orthogonalize styles in fixed order (WLS):
   - Nonlinear Size ⟂ Size
   - Liquidity ⟂ Size
   - Residual Volatility ⟂ (Size, Beta)
   - Short-term Reversal ⟂ Momentum
   - optional fixed policy toggles: Growth ⟂ Value, Investment ⟂ Growth
7. Re-standardize after each orth step and final style pass.

Implementation note (current code path):
- Raw descriptor assembly is built from canonical source tables in `barra_raw_cross_section_history`.
- Cross-sectional z-scoring and orthogonalization are performed in descriptor assembly (`assemble_full_style_scores`).
- Daily factor-return regression consumes these processed exposures; z-scoring is not done during raw LSEG ingest.

Important design guardrail:
- Orth policy is fixed by config/version, not dynamically changing day-to-day based on transient correlations.

### 6.1 Descriptor edge-case and denominator policy

General ratio rule for any descriptor of the form `X / Y`:
- Require `|Y| > epsilon`.
- `epsilon` is defined as a configurable fraction (default 1%) of the cross-sectional median absolute `|Y|` on that date.
- If denominator test fails, set descriptor to missing (do not force-fill).
- Then apply winsorization (`p1/p99`) and standardization.
- Log and persist daily:
  - `% missing` by descriptor
  - `% clipped` by descriptor

Descriptor-specific rules:
- ROE:
  - If equity `<= 0`, set ROE descriptor to missing.
- Earnings Yield (E/P):
  - Allow negative earnings.
  - Use tighter winsorization than baseline if configured (to prevent distress-tail domination).
- Growth descriptors:
  - If base-period value is too small (below configured floor), set growth descriptor to missing.
  - Prefer multi-year growth definitions when history is available.

## 7) Factor Returns and Covariance Methodology

Daily cross-sectional model on ESTU:
- `r_i,t = CountryBlock_i,t + IndustryBlock_i,t + StyleBlock_i,t + eps_i,t`
- WLS weights: cap-based, stable policy (recommended `sqrt(mcap)`).
- Industry coefficients are estimated with weighted sum-to-zero constraints.
- Country currently enters as a simple structural dummy factor (`Country: US`) in the phase-A block.

Persist:
- factor returns by date/factor
- specific returns by date/security
- regression diagnostics (R², condition number, etc.)

Covariance:
- Base: EWMA factor covariance on factor returns.
- Newey-West adjustment: enabled.
- Shrinkage: enabled (toward structured target).

Specific risk:
- Stock-level forecast from residual return process (not `1 - R²` shortcut).

## 8) QA and Alerting

Persist and monitor:
- ESTU size and turnover
- descriptor/factor exposure moments (mean/std/skew/kurt/min/max)
- winsorized fraction per descriptor
- pre/post orth factor correlation diagnostics
- regression diagnostics and residual outlier signals

Alerts:
- factor exposure std outside [0.8, 1.2]
- key post-orth pairwise |corr| > 0.7
- ESTU day-over-day drop above threshold
- factor return volatility spike threshold

## 9) USE4 Alignment Assessment

This design is USE4-inspired and statistically disciplined, but not proprietary-identical (true USE4 internals are not fully public).

Where alignment is strong:
- Cross-sectional daily factor return estimation with structural + style blocks.
- Cap-aware weighting.
- Descriptor standardization discipline.
- Orthogonalization of selected style factors.
- Point-in-time joins for fundamentals/industry.
- Residual-specific risk framework.

Where this remains an approximation:
- Exact proprietary ESTU rules and exclusion lists.
- Exact proprietary descriptor definitions for all style legs.
- Exact proprietary covariance/specific-risk parameterization.

## 10) Implemented Data Workflow (Current)

1. Universe and identity layer:
   - Maintain `security_master` as the single universe authority (RIC primary key; SID/PermID as optional metadata).
   - Canonical time-series tables are physically RIC-keyed.
2. Canonical ingest/backfill from LSEG:
   - Write directly into:
     - `security_fundamentals_pit`
     - `security_classification_pit`
     - `security_prices_eod`
   - Run in shards/batches; monthly or quarterly PIT cadence depending on run mode.
3. Raw cross-section feature build:
   - Build `barra_raw_cross_section_history` from canonical source tables.
   - Compute descriptors, z-score/standardize, and apply orthogonalization policy.
4. Snapshot materialization for API convenience:
   - Build `universe_cross_section_snapshot` with `mode=current` by default (latest row per eligible ticker).
   - `mode=full` is available for historical materialization when needed.
5. ESTU audit persistence:
   - Build and persist per-date ESTU membership/drop reasons to `estu_membership_daily`.
6. Factor return and risk-engine stages:
   - Compute daily factor returns from lagged cross-sections on ESTU.
   - Build covariance (EWMA + Newey-West + shrinkage policy).
   - Build specific risk from residual history.
   - Residual/specific-risk persistence is keyed by `ric` with `ticker` retained as metadata.
7. Relational model output persistence:
   - Persist into:
     - `model_factor_returns_daily`
     - `model_factor_covariance_daily`
     - `model_specific_risk_daily`
     - `model_run_metadata`
   - Residual history remains cache-only in `cache.db.daily_specific_residuals` (compute workspace, not durable output).
   - Cache is limited to compute workspace + API acceleration; it is not a durable source of truth.
8. Orchestrated execution profiles:
   - Run via `run_model_pipeline` with profile-based cadence:
     - `serve-refresh`
     - `source-daily`
     - `source-daily-plus-core-if-due`
     - `core-weekly`
     - `cold-core`
     - `universe-add`
   - Stage checkpoints persist in `job_run_status`.
   - `ingest` stage always runs canonical bootstrap checks; optional live LSEG ingest is controlled by `ORCHESTRATOR_ENABLE_INGEST`.
   - Legacy profile names remain accepted as aliases during the transition:
     - `daily-fast` -> `serve-refresh`
     - `daily-with-core-if-due` -> `source-daily-plus-core-if-due`
     - `weekly-core` -> `core-weekly`
9. Downstream usage:
   - Portfolio exposures/risk attribution and API caches read from processed model outputs, not raw ingest tables.

## 11) Near-Term Operating Priorities

1. Complete final full-run parity QA under orchestrator profiles (`serve-refresh`, `source-daily-plus-core-if-due`, `core-weekly`).
2. Keep `universe_cross_section_snapshot` on `mode=current` unless a historical materialization run is explicitly requested.
3. Continue scheduled coverage audits (fundamentals/classification/prices) and ESTU drop-reason monitoring.
4. Optionally hard-delete archived deprecated scripts after cloud cutover readiness review.

## 12) cUSE4 Risk-Profile Variants (USE4-Inspired)

Naming:
- This adapted engine family is nicknamed `cUSE4`.

Default and rollout:
- Start with the long-horizon profile (`cUSE4-L`).
- `cUSE4-L` is the default model profile.

Profile parameter sets:

| Parameter | cUSE4-S (Short-Horizon) | cUSE4-L (Long-Horizon, Default) | Rationale |
|---|---:|---:|---|
| Factor volatility half-life | 84 | 252 | Short profile adapts faster to regime shifts; long profile is smoother. |
| Factor correlation half-life | 504 | 504 | Correlation structure is slower-moving and should remain stable in both profiles. |
| VRA half-life | 42 | 168 | Short profile rescales risk level faster; long profile avoids overreacting to noise. |

Comparison rule (all else equal):
- When comparing `cUSE4-S` vs `cUSE4-L`, only the horizon/decay knobs above may change.
- Keep all other components fixed:
  - same ESTU policy,
  - same factor definitions and mappings,
  - same orthogonalization order/rules,
  - same WLS weighting policy,
  - same constraints and QA gates.

## 13) Audit Remediation Snapshot (2026-03-04)

The following implementation defects were closed and verified:
- Style-score construction now executes correctly (no invalid argument path, no silent swallow).
- Refresh/model-output writes fail hard on empty required outputs.
- Price ingest/backfill requests richer OHLCV/currency fields, and volume-repair mode uses `TR.Volume`.
- Raw cross-section + residual/specific-risk relational persistence now use `ric` physical keys.
- `security_master` now uses `ric` as primary key; synthetic `sid/permid` placeholders are normalized out.
- Orchestrator ingest stage is active (`bootstrap_only` baseline, opt-in live ingest).
- Regression tests added for schema/key/quality-gate and ingest behavior.
- SQLite maintenance path added (`compact_sqlite_databases.py`) and operationalized.
