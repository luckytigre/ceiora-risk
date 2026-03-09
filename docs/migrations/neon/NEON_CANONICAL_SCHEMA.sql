-- Neon canonical source-of-truth schema (Stage 2)
-- Source parity target: backend/runtime/data.db canonical tables

CREATE TABLE IF NOT EXISTS security_master (
    ric TEXT PRIMARY KEY,
    ticker TEXT,
    sid TEXT,
    permid TEXT,
    isin TEXT,
    instrument_type TEXT,
    asset_category_description TEXT,
    exchange_name TEXT,
    classification_ok SMALLINT NOT NULL DEFAULT 0 CHECK (classification_ok IN (0, 1)),
    is_equity_eligible SMALLINT NOT NULL DEFAULT 0 CHECK (is_equity_eligible IN (0, 1)),
    source TEXT,
    job_run_id TEXT,
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

CREATE TABLE IF NOT EXISTS barra_raw_cross_section_history (
    ric TEXT NOT NULL,
    ticker TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    market_cap DOUBLE PRECISION,
    price_close DOUBLE PRECISION,
    price_volume DOUBLE PRECISION,
    shares_outstanding DOUBLE PRECISION,
    trbc_economic_sector_short TEXT,
    trbc_industry_group TEXT,
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
    value_score DOUBLE PRECISION,
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
    r_squared DOUBLE PRECISION,
    residual_vol DOUBLE PRECISION,
    cross_section_n INTEGER,
    eligible_n INTEGER,
    coverage DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date, factor_name)
);

CREATE TABLE IF NOT EXISTS serving_payload_current (
    payload_name TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    refresh_mode TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_security_master_ticker ON security_master (ticker);
CREATE INDEX IF NOT EXISTS idx_security_master_permid ON security_master (permid);
CREATE INDEX IF NOT EXISTS idx_security_master_sid ON security_master (sid);

CREATE INDEX IF NOT EXISTS idx_security_prices_eod_date ON security_prices_eod (date);
CREATE INDEX IF NOT EXISTS idx_security_fundamentals_pit_asof ON security_fundamentals_pit (as_of_date);
CREATE INDEX IF NOT EXISTS idx_security_classification_pit_asof ON security_classification_pit (as_of_date);
CREATE INDEX IF NOT EXISTS idx_barra_raw_cross_section_history_asof ON barra_raw_cross_section_history (as_of_date);
CREATE INDEX IF NOT EXISTS idx_barra_raw_cross_section_history_ticker ON barra_raw_cross_section_history (ticker);
CREATE INDEX IF NOT EXISTS idx_model_factor_returns_daily_factor ON model_factor_returns_daily (factor_name);
CREATE INDEX IF NOT EXISTS idx_serving_payload_current_updated ON serving_payload_current (updated_at);
