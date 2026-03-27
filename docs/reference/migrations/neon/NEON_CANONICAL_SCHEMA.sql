-- Neon canonical registry-first schema
-- Source parity target: backend/runtime/data.db canonical tables plus Neon-authored sync state.

CREATE TABLE IF NOT EXISTS security_registry (
    ric TEXT PRIMARY KEY,
    ticker TEXT,
    isin TEXT,
    exchange_name TEXT,
    tracking_status TEXT NOT NULL DEFAULT 'active',
    source TEXT,
    job_run_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS security_taxonomy_current (
    ric TEXT PRIMARY KEY,
    instrument_kind TEXT,
    vehicle_structure TEXT,
    issuer_country_code TEXT,
    listing_country_code TEXT,
    model_home_market_scope TEXT,
    is_single_name_equity SMALLINT NOT NULL DEFAULT 0 CHECK (is_single_name_equity IN (0, 1)),
    classification_ready SMALLINT NOT NULL DEFAULT 0 CHECK (classification_ready IN (0, 1)),
    source TEXT,
    job_run_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS security_policy_current (
    ric TEXT PRIMARY KEY,
    price_ingest_enabled SMALLINT NOT NULL DEFAULT 1 CHECK (price_ingest_enabled IN (0, 1)),
    pit_fundamentals_enabled SMALLINT NOT NULL DEFAULT 0 CHECK (pit_fundamentals_enabled IN (0, 1)),
    pit_classification_enabled SMALLINT NOT NULL DEFAULT 0 CHECK (pit_classification_enabled IN (0, 1)),
    allow_cuse_native_core SMALLINT NOT NULL DEFAULT 0 CHECK (allow_cuse_native_core IN (0, 1)),
    allow_cuse_fundamental_projection SMALLINT NOT NULL DEFAULT 0 CHECK (allow_cuse_fundamental_projection IN (0, 1)),
    allow_cuse_returns_projection SMALLINT NOT NULL DEFAULT 0 CHECK (allow_cuse_returns_projection IN (0, 1)),
    allow_cpar_core_target SMALLINT NOT NULL DEFAULT 0 CHECK (allow_cpar_core_target IN (0, 1)),
    allow_cpar_extended_target SMALLINT NOT NULL DEFAULT 0 CHECK (allow_cpar_extended_target IN (0, 1)),
    policy_source TEXT,
    job_run_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS security_source_observation_daily (
    as_of_date DATE NOT NULL,
    ric TEXT NOT NULL,
    classification_ready SMALLINT NOT NULL DEFAULT 0 CHECK (classification_ready IN (0, 1)),
    is_equity_eligible SMALLINT NOT NULL DEFAULT 0 CHECK (is_equity_eligible IN (0, 1)),
    price_ingest_enabled SMALLINT NOT NULL DEFAULT 0 CHECK (price_ingest_enabled IN (0, 1)),
    pit_fundamentals_enabled SMALLINT NOT NULL DEFAULT 0 CHECK (pit_fundamentals_enabled IN (0, 1)),
    pit_classification_enabled SMALLINT NOT NULL DEFAULT 0 CHECK (pit_classification_enabled IN (0, 1)),
    has_price_history_as_of_date SMALLINT NOT NULL DEFAULT 0 CHECK (has_price_history_as_of_date IN (0, 1)),
    has_fundamentals_history_as_of_date SMALLINT NOT NULL DEFAULT 0 CHECK (has_fundamentals_history_as_of_date IN (0, 1)),
    has_classification_history_as_of_date SMALLINT NOT NULL DEFAULT 0 CHECK (has_classification_history_as_of_date IN (0, 1)),
    latest_price_date DATE,
    latest_fundamentals_as_of_date DATE,
    latest_classification_as_of_date DATE,
    source TEXT,
    job_run_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (as_of_date, ric)
);

CREATE TABLE IF NOT EXISTS security_ingest_runs (
    job_run_id TEXT PRIMARY KEY,
    source TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS security_ingest_audit (
    job_run_id TEXT NOT NULL,
    ric TEXT NOT NULL,
    artifact_name TEXT NOT NULL,
    status TEXT NOT NULL,
    detail TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (job_run_id, ric, artifact_name)
);

CREATE TABLE IF NOT EXISTS security_master_compat_current (
    ric TEXT PRIMARY KEY,
    ticker TEXT,
    isin TEXT,
    exchange_name TEXT,
    classification_ok SMALLINT NOT NULL DEFAULT 0 CHECK (classification_ok IN (0, 1)),
    is_equity_eligible SMALLINT NOT NULL DEFAULT 0 CHECK (is_equity_eligible IN (0, 1)),
    coverage_role TEXT NOT NULL DEFAULT 'native_equity',
    source TEXT,
    job_run_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS source_sync_runs (
    sync_run_id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    sqlite_path TEXT,
    selected_tables_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    table_results_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    error_type TEXT,
    error_message TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS source_sync_watermarks (
    table_name TEXT PRIMARY KEY,
    sync_run_id TEXT NOT NULL,
    source_min_value TEXT,
    source_max_value TEXT,
    target_min_value TEXT,
    target_max_value TEXT,
    row_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS security_source_status_current (
    ric TEXT PRIMARY KEY,
    ticker TEXT,
    tracking_status TEXT NOT NULL,
    instrument_kind TEXT,
    vehicle_structure TEXT,
    model_home_market_scope TEXT,
    is_single_name_equity SMALLINT NOT NULL DEFAULT 0 CHECK (is_single_name_equity IN (0, 1)),
    classification_ready SMALLINT NOT NULL DEFAULT 0 CHECK (classification_ready IN (0, 1)),
    price_ingest_enabled SMALLINT NOT NULL DEFAULT 0 CHECK (price_ingest_enabled IN (0, 1)),
    pit_fundamentals_enabled SMALLINT NOT NULL DEFAULT 0 CHECK (pit_fundamentals_enabled IN (0, 1)),
    pit_classification_enabled SMALLINT NOT NULL DEFAULT 0 CHECK (pit_classification_enabled IN (0, 1)),
    allow_cuse_native_core SMALLINT NOT NULL DEFAULT 0 CHECK (allow_cuse_native_core IN (0, 1)),
    allow_cuse_fundamental_projection SMALLINT NOT NULL DEFAULT 0 CHECK (allow_cuse_fundamental_projection IN (0, 1)),
    allow_cuse_returns_projection SMALLINT NOT NULL DEFAULT 0 CHECK (allow_cuse_returns_projection IN (0, 1)),
    allow_cpar_core_target SMALLINT NOT NULL DEFAULT 0 CHECK (allow_cpar_core_target IN (0, 1)),
    allow_cpar_extended_target SMALLINT NOT NULL DEFAULT 0 CHECK (allow_cpar_extended_target IN (0, 1)),
    observation_as_of_date DATE,
    has_price_history_as_of_date SMALLINT NOT NULL DEFAULT 0 CHECK (has_price_history_as_of_date IN (0, 1)),
    has_fundamentals_history_as_of_date SMALLINT NOT NULL DEFAULT 0 CHECK (has_fundamentals_history_as_of_date IN (0, 1)),
    has_classification_history_as_of_date SMALLINT NOT NULL DEFAULT 0 CHECK (has_classification_history_as_of_date IN (0, 1)),
    latest_price_date DATE,
    latest_fundamentals_as_of_date DATE,
    latest_classification_as_of_date DATE,
    source_sync_run_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS security_prices_eod (
    ric TEXT NOT NULL,
    date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    adj_close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    currency TEXT,
    source TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (ric, date)
);

CREATE TABLE IF NOT EXISTS security_fundamentals_pit (
    ric TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    stat_date DATE NOT NULL,
    period_end_date DATE,
    fiscal_year INTEGER,
    period_type TEXT,
    report_currency TEXT,
    market_cap DOUBLE PRECISION,
    shares_outstanding DOUBLE PRECISION,
    dividend_yield DOUBLE PRECISION,
    book_value_per_share DOUBLE PRECISION,
    total_assets DOUBLE PRECISION,
    total_debt DOUBLE PRECISION,
    cash_and_equivalents DOUBLE PRECISION,
    long_term_debt DOUBLE PRECISION,
    operating_cashflow DOUBLE PRECISION,
    capital_expenditures DOUBLE PRECISION,
    trailing_eps DOUBLE PRECISION,
    forward_eps DOUBLE PRECISION,
    revenue DOUBLE PRECISION,
    ebitda DOUBLE PRECISION,
    ebit DOUBLE PRECISION,
    roe_pct DOUBLE PRECISION,
    operating_margin_pct DOUBLE PRECISION,
    common_name TEXT,
    source TEXT,
    job_run_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (ric, as_of_date, stat_date)
);

CREATE TABLE IF NOT EXISTS security_classification_pit (
    ric TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    trbc_economic_sector TEXT,
    trbc_business_sector TEXT,
    trbc_industry_group TEXT,
    trbc_industry TEXT,
    trbc_activity TEXT,
    hq_country_code TEXT,
    source TEXT,
    job_run_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (ric, as_of_date)
);

CREATE TABLE IF NOT EXISTS estu_membership_daily (
    date DATE NOT NULL,
    ric TEXT NOT NULL,
    estu_flag INTEGER NOT NULL DEFAULT 0,
    drop_reason TEXT,
    drop_reason_detail TEXT,
    mcap DOUBLE PRECISION,
    price_close DOUBLE PRECISION,
    adv_20d DOUBLE PRECISION,
    has_required_price_history INTEGER NOT NULL DEFAULT 0,
    has_required_fundamentals INTEGER NOT NULL DEFAULT 0,
    has_required_trbc INTEGER NOT NULL DEFAULT 0,
    source TEXT,
    job_run_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (date, ric)
);

CREATE TABLE IF NOT EXISTS universe_cross_section_snapshot (
    ric TEXT NOT NULL,
    ticker TEXT,
    as_of_date DATE NOT NULL,
    fundamental_fetch_date DATE,
    fundamental_period_end_date DATE,
    market_cap DOUBLE PRECISION,
    shares_outstanding DOUBLE PRECISION,
    dividend_yield DOUBLE PRECISION,
    common_name TEXT,
    book_value DOUBLE PRECISION,
    forward_eps DOUBLE PRECISION,
    trailing_eps DOUBLE PRECISION,
    total_debt DOUBLE PRECISION,
    cash_and_equivalents DOUBLE PRECISION,
    long_term_debt DOUBLE PRECISION,
    free_cash_flow DOUBLE PRECISION,
    gross_profit DOUBLE PRECISION,
    net_income DOUBLE PRECISION,
    operating_cashflow DOUBLE PRECISION,
    capital_expenditures DOUBLE PRECISION,
    shares_basic DOUBLE PRECISION,
    shares_diluted DOUBLE PRECISION,
    free_float_shares DOUBLE PRECISION,
    free_float_percent DOUBLE PRECISION,
    revenue DOUBLE PRECISION,
    ebitda DOUBLE PRECISION,
    ebit DOUBLE PRECISION,
    total_assets DOUBLE PRECISION,
    total_liabilities DOUBLE PRECISION,
    return_on_equity DOUBLE PRECISION,
    operating_margins DOUBLE PRECISION,
    report_currency TEXT,
    fiscal_year INTEGER,
    period_type TEXT,
    trbc_economic_sector_short TEXT,
    trbc_economic_sector TEXT,
    trbc_business_sector TEXT,
    trbc_industry_group TEXT,
    trbc_industry TEXT,
    trbc_activity TEXT,
    trbc_effective_date DATE,
    price_date DATE,
    price_close DOUBLE PRECISION,
    price_currency TEXT,
    fundamental_source TEXT,
    trbc_source TEXT,
    price_source TEXT,
    fundamental_job_run_id TEXT,
    trbc_job_run_id TEXT,
    snapshot_job_run_id TEXT,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (ric, as_of_date)
);

CREATE TABLE IF NOT EXISTS barra_raw_cross_section_history (
    ric TEXT NOT NULL,
    ticker TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    market_cap DOUBLE PRECISION,
    price_close DOUBLE PRECISION,
    price_volume DOUBLE PRECISION,
    shares_outstanding DOUBLE PRECISION,
    trbc_economic_sector_short TEXT,
    trbc_business_sector TEXT,
    beta_raw DOUBLE PRECISION,
    momentum_raw DOUBLE PRECISION,
    size_raw DOUBLE PRECISION,
    nonlinear_size_raw DOUBLE PRECISION,
    st_reversal_raw DOUBLE PRECISION,
    resid_vol_raw DOUBLE PRECISION,
    turnover_1m_raw DOUBLE PRECISION,
    turnover_12m_raw DOUBLE PRECISION,
    log_avg_dollar_volume_20d_raw DOUBLE PRECISION,
    book_to_price_raw DOUBLE PRECISION,
    forward_ep_raw DOUBLE PRECISION,
    cash_earnings_yield_raw DOUBLE PRECISION,
    trailing_ep_raw DOUBLE PRECISION,
    debt_to_equity_raw DOUBLE PRECISION,
    debt_to_assets_raw DOUBLE PRECISION,
    book_leverage_raw DOUBLE PRECISION,
    sales_growth_raw DOUBLE PRECISION,
    eps_growth_raw DOUBLE PRECISION,
    roe_raw DOUBLE PRECISION,
    gross_profitability_raw DOUBLE PRECISION,
    asset_growth_raw DOUBLE PRECISION,
    dividend_yield_raw DOUBLE PRECISION,
    beta_score DOUBLE PRECISION,
    momentum_score DOUBLE PRECISION,
    size_score DOUBLE PRECISION,
    nonlinear_size_score DOUBLE PRECISION,
    short_term_reversal_score DOUBLE PRECISION,
    resid_vol_score DOUBLE PRECISION,
    liquidity_score DOUBLE PRECISION,
    book_to_price_score DOUBLE PRECISION,
    earnings_yield_score DOUBLE PRECISION,
    leverage_score DOUBLE PRECISION,
    growth_score DOUBLE PRECISION,
    profitability_score DOUBLE PRECISION,
    investment_score DOUBLE PRECISION,
    dividend_yield_score DOUBLE PRECISION,
    confidence_band TEXT,
    fallback_depth TEXT,
    idio_var_daily DOUBLE PRECISION,
    coverage_degraded TEXT,
    barra_model_version TEXT,
    descriptor_schema_version TEXT,
    assumption_set_version TEXT,
    source TEXT,
    job_run_id TEXT,
    updated_at TIMESTAMPTZ,
    PRIMARY KEY (ric, as_of_date)
);

CREATE TABLE IF NOT EXISTS model_factor_returns_daily (
    date DATE NOT NULL,
    factor_name TEXT NOT NULL,
    factor_return DOUBLE PRECISION NOT NULL,
    robust_se DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    t_stat DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    r_squared DOUBLE PRECISION,
    residual_vol DOUBLE PRECISION,
    cross_section_n INTEGER,
    eligible_n INTEGER,
    coverage DOUBLE PRECISION,
    run_id TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date, factor_name)
);

ALTER TABLE model_factor_returns_daily
    ADD COLUMN IF NOT EXISTS run_id TEXT;

CREATE TABLE IF NOT EXISTS model_factor_covariance_daily (
    as_of_date DATE NOT NULL,
    factor_name TEXT NOT NULL,
    factor_name_2 TEXT NOT NULL,
    covariance DOUBLE PRECISION NOT NULL,
    run_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (as_of_date, factor_name, factor_name_2)
);

CREATE TABLE IF NOT EXISTS model_specific_risk_daily (
    as_of_date DATE NOT NULL,
    ric TEXT NOT NULL,
    ticker TEXT,
    specific_var DOUBLE PRECISION NOT NULL,
    specific_vol DOUBLE PRECISION NOT NULL,
    obs INTEGER NOT NULL DEFAULT 0,
    trbc_business_sector TEXT,
    run_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (as_of_date, ric)
);

CREATE TABLE IF NOT EXISTS model_run_metadata (
    run_id TEXT PRIMARY KEY,
    refresh_mode TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    factor_returns_asof DATE,
    source_dates_json TEXT NOT NULL,
    params_json TEXT NOT NULL,
    risk_engine_state_json TEXT NOT NULL,
    row_counts_json TEXT NOT NULL,
    error_type TEXT,
    error_message TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS projected_instrument_loadings (
    ric TEXT NOT NULL,
    ticker TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    factor_name TEXT NOT NULL,
    exposure DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (ric, as_of_date, factor_name)
);

CREATE TABLE IF NOT EXISTS projected_instrument_meta (
    ric TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    projection_method TEXT NOT NULL DEFAULT 'ols_returns_regression',
    lookback_days INTEGER NOT NULL,
    obs_count INTEGER NOT NULL,
    r_squared DOUBLE PRECISION NOT NULL,
    projected_specific_var DOUBLE PRECISION,
    projected_specific_vol DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (ric, as_of_date)
);

CREATE TABLE IF NOT EXISTS cuse_security_membership_daily (
    as_of_date DATE NOT NULL,
    ric TEXT,
    ticker TEXT NOT NULL,
    policy_path TEXT NOT NULL,
    realized_role TEXT NOT NULL,
    output_status TEXT NOT NULL,
    projection_candidate_status TEXT NOT NULL,
    projection_output_status TEXT NOT NULL,
    reason_code TEXT,
    quality_label TEXT NOT NULL,
    source_snapshot_status TEXT NOT NULL,
    projection_method TEXT,
    projection_basis_status TEXT NOT NULL,
    projection_source_package_date DATE,
    served_exposure_available INTEGER NOT NULL DEFAULT 0,
    run_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (as_of_date, ticker)
);

CREATE TABLE IF NOT EXISTS cuse_security_stage_results_daily (
    as_of_date DATE NOT NULL,
    ric TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    stage_state TEXT NOT NULL,
    reason_code TEXT,
    detail_json TEXT NOT NULL,
    run_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (as_of_date, ric, stage_name)
);

CREATE TABLE IF NOT EXISTS serving_payload_current (
    payload_name TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    refresh_mode TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS runtime_state_current (
    state_key TEXT PRIMARY KEY,
    value_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_security_registry_ticker ON security_registry (ticker);
CREATE INDEX IF NOT EXISTS idx_security_source_observation_daily_ric ON security_source_observation_daily (ric);
CREATE INDEX IF NOT EXISTS idx_security_ingest_audit_ric ON security_ingest_audit (ric);
CREATE INDEX IF NOT EXISTS idx_security_master_compat_current_ticker ON security_master_compat_current (ticker);
CREATE INDEX IF NOT EXISTS idx_source_sync_runs_started ON source_sync_runs (started_at);
CREATE INDEX IF NOT EXISTS idx_source_sync_runs_status ON source_sync_runs (status);
CREATE INDEX IF NOT EXISTS idx_source_sync_watermarks_updated ON source_sync_watermarks (updated_at);
CREATE INDEX IF NOT EXISTS idx_security_source_status_current_tracking ON security_source_status_current (tracking_status);
CREATE INDEX IF NOT EXISTS idx_security_source_status_current_observation ON security_source_status_current (observation_as_of_date);
CREATE INDEX IF NOT EXISTS idx_security_prices_eod_date ON security_prices_eod (date);
CREATE INDEX IF NOT EXISTS idx_security_fundamentals_pit_asof ON security_fundamentals_pit (as_of_date);
CREATE INDEX IF NOT EXISTS idx_security_classification_pit_asof ON security_classification_pit (as_of_date);
CREATE INDEX IF NOT EXISTS idx_estu_membership_daily_date_flag ON estu_membership_daily (date, estu_flag);
CREATE INDEX IF NOT EXISTS idx_estu_membership_daily_ric_date ON estu_membership_daily (ric, date);
CREATE INDEX IF NOT EXISTS idx_universe_cross_section_snapshot_asof ON universe_cross_section_snapshot (as_of_date);
CREATE INDEX IF NOT EXISTS idx_universe_cross_section_snapshot_ticker ON universe_cross_section_snapshot (ticker);
CREATE INDEX IF NOT EXISTS idx_barra_raw_cross_section_history_asof ON barra_raw_cross_section_history (as_of_date);
CREATE INDEX IF NOT EXISTS idx_barra_raw_cross_section_history_ticker ON barra_raw_cross_section_history (ticker);
CREATE INDEX IF NOT EXISTS idx_model_factor_returns_daily_date ON model_factor_returns_daily (date);
CREATE INDEX IF NOT EXISTS idx_model_factor_returns_daily_factor ON model_factor_returns_daily (factor_name);
CREATE INDEX IF NOT EXISTS idx_model_factor_covariance_daily_asof ON model_factor_covariance_daily (as_of_date);
CREATE INDEX IF NOT EXISTS idx_model_factor_covariance_daily_factor ON model_factor_covariance_daily (factor_name);
CREATE INDEX IF NOT EXISTS idx_model_specific_risk_daily_asof ON model_specific_risk_daily (as_of_date);
CREATE INDEX IF NOT EXISTS idx_model_specific_risk_daily_ric ON model_specific_risk_daily (ric);
CREATE INDEX IF NOT EXISTS idx_model_run_metadata_completed ON model_run_metadata (completed_at);
CREATE INDEX IF NOT EXISTS idx_model_run_metadata_status ON model_run_metadata (status);
CREATE INDEX IF NOT EXISTS idx_projected_instrument_loadings_asof ON projected_instrument_loadings (as_of_date);
CREATE INDEX IF NOT EXISTS idx_projected_instrument_loadings_factor ON projected_instrument_loadings (factor_name);
CREATE INDEX IF NOT EXISTS idx_projected_instrument_meta_asof ON projected_instrument_meta (as_of_date);
CREATE INDEX IF NOT EXISTS idx_cuse_security_membership_daily_date ON cuse_security_membership_daily (as_of_date);
CREATE INDEX IF NOT EXISTS idx_cuse_security_membership_daily_ric ON cuse_security_membership_daily (ric, as_of_date);
CREATE INDEX IF NOT EXISTS idx_cuse_security_stage_results_daily_date ON cuse_security_stage_results_daily (as_of_date, stage_name);
CREATE INDEX IF NOT EXISTS idx_cuse_security_stage_results_daily_ric ON cuse_security_stage_results_daily (ric, as_of_date);
CREATE INDEX IF NOT EXISTS idx_serving_payload_current_updated ON serving_payload_current (updated_at);
CREATE INDEX IF NOT EXISTS idx_runtime_state_current_updated ON runtime_state_current (updated_at);
