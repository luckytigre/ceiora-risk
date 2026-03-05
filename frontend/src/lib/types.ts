export interface Position {
  ticker: string;
  name: string;
  long_short: string;
  trbc_economic_sector_short: string;
  trbc_economic_sector_short_abbr: string;
  shares: number;
  price: number;
  market_value: number;
  weight: number;
  account: string;
  sleeve: string;
  source: string;
  trbc_industry_group: string;
  exposures: Record<string, number>;
  risk_contrib_pct: number;
  eligible_for_model?: boolean;
  eligibility_reason?: string;
  risk_mix?: {
    industry: number;
    style: number;
    idio: number;
  };
}

export interface PortfolioData {
  positions: Position[];
  total_value: number;
  position_count: number;
  _cached: boolean;
}

export interface FactorDrilldownItem {
  ticker: string;
  weight: number;
  exposure: number;
  sensitivity?: number;
  contribution: number;
}

export interface FactorExposure {
  factor: string;
  value: number;
  factor_vol?: number;
  coverage_pct?: number;
  cross_section_n?: number;
  eligible_n?: number;
  coverage_date?: string | null;
  drilldown: FactorDrilldownItem[];
}

export interface ExposuresData {
  mode: string;
  factors: FactorExposure[];
  _cached: boolean;
}

export interface FactorHistoryPoint {
  date: string;
  factor_return: number;
  cum_return: number;
}

export interface FactorHistoryData {
  factor: string;
  years: number;
  points: FactorHistoryPoint[];
  _cached: boolean;
}

export interface FactorDetail {
  factor: string;
  category: "industry" | "style";
  exposure: number;
  factor_vol: number;
  sensitivity: number;
  marginal_var_contrib: number;
  pct_of_total: number;
}

export interface RiskShares {
  industry: number;
  style: number;
  idio: number;
}

export interface CovMatrix {
  factors: string[];
  correlation: number[][];
}

export interface RiskData {
  risk_shares: RiskShares;
  component_shares: Omit<RiskShares, "idio">;
  factor_details: FactorDetail[];
  cov_matrix: CovMatrix;
  r_squared: number;
  condition_number: number;
  risk_engine?: {
    status?: string;
    method_version?: string;
    last_recompute_date?: string;
    factor_returns_latest_date?: string;
    cross_section_min_age_days?: number;
    recompute_interval_days?: number;
    lookback_days?: number;
    specific_risk_ticker_count?: number;
    recomputed_this_refresh?: boolean;
    recompute_reason?: string;
  };
  model_sanity?: {
    status?: string;
    warnings?: string[];
    checks?: Record<string, number>;
  };
  _cached: boolean;
}

export interface UniverseTickerItem {
  ticker: string;
  name: string;
  trbc_economic_sector_short: string;
  trbc_economic_sector_short_abbr: string;
  trbc_industry_group: string;
  market_cap: number | null;
  price: number;
  exposures: Record<string, number>;
  sensitivities: Record<string, number>;
  risk_loading: number | null;
  specific_var?: number | null;
  specific_vol?: number | null;
  eligible_for_model?: boolean;
  eligibility_reason?: string;
  model_warning?: string;
  as_of_date?: string;
}

export interface UniverseTickerData {
  item: UniverseTickerItem;
  _cached: boolean;
}

export interface WeeklyPricePoint {
  date: string;
  close: number;
}

export interface UniverseTickerHistoryData {
  ticker: string;
  ric: string;
  years: number;
  points: WeeklyPricePoint[];
  _cached: boolean;
}

export interface UniverseSearchItem {
  ticker: string;
  name: string;
  trbc_economic_sector_short: string;
  trbc_economic_sector_short_abbr: string;
  trbc_industry_group?: string;
  risk_loading: number | null;
  specific_vol?: number | null;
  eligible_for_model?: boolean;
  eligibility_reason?: string;
}

export interface UniverseSearchData {
  query: string;
  results: UniverseSearchItem[];
  total: number;
  _cached: boolean;
}

export interface UniverseFactorsData {
  factors: string[];
  factor_vols: Record<string, number>;
  r_squared?: number;
  condition_number?: number;
  ticker_count?: number;
  eligible_ticker_count?: number;
  _cached: boolean;
}

export interface SeriesPoint {
  date: string;
  value: number;
}

export interface HealthHistogram {
  centers: number[];
  counts: number[];
}

export interface HealthCorrelationMatrix {
  factors: string[];
  correlation: number[][];
}

export interface HealthR2Point {
  date: string;
  r2: number;
  roll60: number;
  roll252: number;
}

export interface HealthFactorPctRow {
  factor: string;
  value: number;
}

export interface HealthIncrementalBlockR2Point {
  date: string;
  r2_full: number;
  r2_industry: number;
  r2_style_incremental: number;
  roll60_full: number;
  roll60_industry: number;
  roll60_style_incremental: number;
}

export interface HealthBucketBreadthPoint {
  date: string;
  industry_mean_abs_t: number;
  style_mean_abs_t: number;
}

export interface HealthBucketBreadthSummary {
  industry_mean_abs_t: number;
  style_mean_abs_t: number;
}

export interface HealthPortfolioVarianceSplit {
  industry_pct_total: number;
  style_pct_total: number;
  idio_pct_total: number;
  industry_pct_factor_only: number;
  style_pct_factor_only: number;
}

export interface HealthExposureStats {
  factor: string;
  mean: number;
  std: number;
  p1: number;
  p99: number;
  max_abs: number;
}

export interface HealthTurnoverPoint {
  date: string;
  turnover: number;
  roll60: number;
}

export interface HealthForecastRealizedRow {
  name: string;
  forecast_vol: number;
  realized_vol_60d: number;
}

export interface HealthCoverageFieldRow {
  field: string;
  data_type: string;
  non_null_rows: number;
  total_rows: number;
  row_coverage_pct: number;
  avg_date_coverage_pct: number;
  worst_date: string | null;
  worst_date_coverage_pct: number;
  dates_below_80_pct_count: number;
  avg_ticker_lifecycle_coverage_pct: number;
  p10_ticker_lifecycle_coverage_pct: number;
  tickers_below_80_pct_count: number;
  coverage_score_pct: number;
}

export interface HealthCoverageTable {
  label: string;
  table: string;
  row_count: number;
  date_count: number;
  ticker_count: number;
  field_count: number;
  low_coverage_field_count: number;
  fields: HealthCoverageFieldRow[];
}

export interface HealthDiagnosticsData {
  status: string;
  as_of: string | null;
  notes: string[];
  section1: {
    sampling?: string;
    r2_series: HealthR2Point[];
    incremental_block_r2_series: HealthIncrementalBlockR2Point[];
    t_stat_hist: HealthHistogram;
    pct_days_abs_t_gt_2: HealthFactorPctRow[];
    bucket_breadth_series: HealthBucketBreadthPoint[];
    bucket_breadth_summary: HealthBucketBreadthSummary;
    portfolio_variance_split: HealthPortfolioVarianceSplit;
  };
  section2: {
    as_of: string | null;
    factor_stats: HealthExposureStats[];
    factor_histograms: Record<string, HealthHistogram>;
    exposure_corr: HealthCorrelationMatrix;
    turnover_series: HealthTurnoverPoint[];
  };
  section3: {
    factors: string[];
    cumulative_returns: Record<string, SeriesPoint[]>;
    rolling_vol_60d: Record<string, SeriesPoint[]>;
    return_corr: HealthCorrelationMatrix;
    return_dist: Record<string, HealthHistogram>;
  };
  section4: {
    eigenvalues: number[];
    condition_number: number;
    forecast_vs_realized: HealthForecastRealizedRow[];
    rolling_avg_factor_vol: SeriesPoint[];
  };
  section5: {
    fundamentals: HealthCoverageTable;
    trbc_history: HealthCoverageTable;
  };
  _cached: boolean;
}

export interface DataTableStats {
  table: string;
  exists: boolean;
  row_count?: number;
  ticker_count?: number | null;
  date_column?: string | null;
  min_date?: string | null;
  max_date?: string | null;
  last_updated_at?: string | null;
  last_job_run_id?: string | null;
}

export interface DataDiagnosticsData {
  status: string;
  database_path: string;
  cache_db_path: string;
  exposure_source_table: string;
  source_tables: {
    security_master: DataTableStats | null;
    security_fundamentals_pit: DataTableStats | null;
    security_classification_pit: DataTableStats | null;
    security_prices_eod: DataTableStats | null;
    estu_membership_daily: DataTableStats | null;
    barra_raw_cross_section_history: DataTableStats | null;
    universe_cross_section_snapshot: DataTableStats | null;
  };
  exposure_duplicates: {
    active_exposure_source: {
      table: string;
      exists: boolean;
      duplicate_groups: number;
      duplicate_extra_rows: number;
    };
  };
  cross_section_usage: {
    eligibility_summary: {
      available: boolean;
      latest?: {
        date: string;
        exp_date: string | null;
        exposure_n: number;
        structural_eligible_n: number;
        regression_member_n: number;
        structural_coverage_pct: number;
        regression_coverage_pct: number;
        alert_level: string;
      } | null;
      min_structural_eligible_n?: number | null;
      max_structural_eligible_n?: number | null;
      min_regression_member_n?: number | null;
      max_regression_member_n?: number | null;
    };
    factor_cross_section: {
      available: boolean;
      latest?: {
        date: string | null;
        cross_section_n_min: number;
        cross_section_n_max: number;
        eligible_n_min: number;
        eligible_n_max: number;
      } | null;
      min_cross_section_n?: number | null;
      max_cross_section_n?: number | null;
      min_eligible_n?: number | null;
      max_eligible_n?: number | null;
    };
  };
  risk_engine_meta: Record<string, unknown>;
  cache_outputs: Array<{
    key: string;
    updated_at_unix: number | null;
    updated_at_utc: string | null;
  }>;
}
