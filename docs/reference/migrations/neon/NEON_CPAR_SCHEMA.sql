CREATE TABLE IF NOT EXISTS cpar_package_runs (
    package_run_id TEXT PRIMARY KEY,
    package_date DATE NOT NULL,
    profile TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    method_version TEXT NOT NULL,
    factor_registry_version TEXT NOT NULL,
    lookback_weeks INTEGER NOT NULL,
    half_life_weeks INTEGER NOT NULL,
    min_observations INTEGER NOT NULL,
    proxy_price_rule TEXT NOT NULL,
    source_prices_asof DATE,
    classification_asof DATE,
    universe_count INTEGER NOT NULL DEFAULT 0,
    fit_ok_count INTEGER NOT NULL DEFAULT 0,
    fit_limited_count INTEGER NOT NULL DEFAULT 0,
    fit_insufficient_count INTEGER NOT NULL DEFAULT 0,
    data_authority TEXT NOT NULL,
    error_type TEXT,
    error_message TEXT,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS cpar_proxy_returns_weekly (
    package_run_id TEXT NOT NULL,
    package_date DATE NOT NULL,
    week_end DATE NOT NULL,
    factor_id TEXT NOT NULL,
    factor_group TEXT NOT NULL,
    proxy_ric TEXT NOT NULL,
    proxy_ticker TEXT NOT NULL,
    return_value DOUBLE PRECISION NOT NULL,
    weight_value DOUBLE PRECISION NOT NULL,
    price_field_used TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (package_run_id, week_end, factor_id)
);

CREATE TABLE IF NOT EXISTS cpar_proxy_transform_weekly (
    package_run_id TEXT NOT NULL,
    package_date DATE NOT NULL,
    factor_id TEXT NOT NULL,
    factor_group TEXT NOT NULL,
    proxy_ric TEXT NOT NULL,
    proxy_ticker TEXT NOT NULL,
    market_alpha DOUBLE PRECISION NOT NULL,
    market_beta DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (package_run_id, factor_id)
);

CREATE TABLE IF NOT EXISTS cpar_factor_covariance_weekly (
    package_run_id TEXT NOT NULL,
    package_date DATE NOT NULL,
    factor_id TEXT NOT NULL,
    factor_id_2 TEXT NOT NULL,
    covariance DOUBLE PRECISION NOT NULL,
    correlation DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (package_run_id, factor_id, factor_id_2)
);

CREATE TABLE IF NOT EXISTS cpar_instrument_fits_weekly (
    package_run_id TEXT NOT NULL,
    package_date DATE NOT NULL,
    ric TEXT NOT NULL,
    ticker TEXT,
    display_name TEXT,
    fit_status TEXT NOT NULL,
    warnings_json JSONB NOT NULL,
    observed_weeks INTEGER NOT NULL,
    lookback_weeks INTEGER NOT NULL,
    longest_gap_weeks INTEGER NOT NULL,
    price_field_used TEXT NOT NULL,
    hq_country_code TEXT,
    market_step_alpha DOUBLE PRECISION,
    market_step_beta DOUBLE PRECISION,
    block_alpha DOUBLE PRECISION,
    spy_trade_beta_raw DOUBLE PRECISION,
    raw_loadings_json JSONB NOT NULL,
    thresholded_loadings_json JSONB NOT NULL,
    factor_variance_proxy DOUBLE PRECISION,
    factor_volatility_proxy DOUBLE PRECISION,
    specific_variance_proxy DOUBLE PRECISION,
    specific_volatility_proxy DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (package_run_id, ric)
);

ALTER TABLE cpar_instrument_fits_weekly
    ADD COLUMN IF NOT EXISTS specific_variance_proxy DOUBLE PRECISION;

ALTER TABLE cpar_instrument_fits_weekly
    ADD COLUMN IF NOT EXISTS specific_volatility_proxy DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_cpar_package_runs_date_status
    ON cpar_package_runs (package_date, status);
CREATE INDEX IF NOT EXISTS idx_cpar_package_runs_completed
    ON cpar_package_runs (completed_at);
CREATE INDEX IF NOT EXISTS idx_cpar_proxy_returns_weekly_package_run
    ON cpar_proxy_returns_weekly (package_run_id);
CREATE INDEX IF NOT EXISTS idx_cpar_proxy_returns_weekly_package_date
    ON cpar_proxy_returns_weekly (package_date);
CREATE INDEX IF NOT EXISTS idx_cpar_proxy_returns_weekly_factor
    ON cpar_proxy_returns_weekly (factor_id);
CREATE INDEX IF NOT EXISTS idx_cpar_proxy_transform_weekly_package_run
    ON cpar_proxy_transform_weekly (package_run_id);
CREATE INDEX IF NOT EXISTS idx_cpar_proxy_transform_weekly_package_date
    ON cpar_proxy_transform_weekly (package_date);
CREATE INDEX IF NOT EXISTS idx_cpar_factor_covariance_weekly_package_run
    ON cpar_factor_covariance_weekly (package_run_id);
CREATE INDEX IF NOT EXISTS idx_cpar_factor_covariance_weekly_package_date
    ON cpar_factor_covariance_weekly (package_date);
CREATE INDEX IF NOT EXISTS idx_cpar_factor_covariance_weekly_factor
    ON cpar_factor_covariance_weekly (factor_id);
CREATE INDEX IF NOT EXISTS idx_cpar_instrument_fits_weekly_package_run
    ON cpar_instrument_fits_weekly (package_run_id);
CREATE INDEX IF NOT EXISTS idx_cpar_instrument_fits_weekly_package_date
    ON cpar_instrument_fits_weekly (package_date);
CREATE INDEX IF NOT EXISTS idx_cpar_instrument_fits_weekly_ticker
    ON cpar_instrument_fits_weekly (ticker);
CREATE INDEX IF NOT EXISTS idx_cpar_instrument_fits_weekly_status
    ON cpar_instrument_fits_weekly (fit_status);

CREATE TABLE IF NOT EXISTS cpar_package_universe_membership (
    package_run_id TEXT NOT NULL,
    package_date DATE NOT NULL,
    ric TEXT NOT NULL,
    ticker TEXT,
    universe_scope TEXT NOT NULL,
    target_scope TEXT NOT NULL,
    basis_role TEXT NOT NULL,
    build_reason_code TEXT,
    warnings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (package_run_id, ric)
);

CREATE INDEX IF NOT EXISTS idx_cpar_package_universe_membership_package_run
    ON cpar_package_universe_membership (package_run_id);
CREATE INDEX IF NOT EXISTS idx_cpar_package_universe_membership_package_date
    ON cpar_package_universe_membership (package_date);
CREATE INDEX IF NOT EXISTS idx_cpar_package_universe_membership_ticker
    ON cpar_package_universe_membership (ticker);

CREATE TABLE IF NOT EXISTS cpar_instrument_runtime_coverage_weekly (
    package_run_id TEXT NOT NULL,
    package_date DATE NOT NULL,
    ric TEXT NOT NULL,
    ticker TEXT,
    price_on_package_date_status TEXT NOT NULL,
    fit_row_status TEXT NOT NULL,
    fit_quality_status TEXT NOT NULL,
    portfolio_use_status TEXT NOT NULL,
    ticker_detail_use_status TEXT NOT NULL,
    hedge_use_status TEXT NOT NULL,
    fit_family TEXT NOT NULL,
    fit_status TEXT NOT NULL,
    reason_code TEXT,
    quality_label TEXT NOT NULL,
    warnings_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (package_run_id, ric)
);

CREATE INDEX IF NOT EXISTS idx_cpar_instrument_runtime_coverage_weekly_package_run
    ON cpar_instrument_runtime_coverage_weekly (package_run_id);
CREATE INDEX IF NOT EXISTS idx_cpar_instrument_runtime_coverage_weekly_package_date
    ON cpar_instrument_runtime_coverage_weekly (package_date);
CREATE INDEX IF NOT EXISTS idx_cpar_instrument_runtime_coverage_weekly_portfolio_use
    ON cpar_instrument_runtime_coverage_weekly (portfolio_use_status);
CREATE INDEX IF NOT EXISTS idx_cpar_instrument_runtime_coverage_weekly_ticker
    ON cpar_instrument_runtime_coverage_weekly (ticker);
