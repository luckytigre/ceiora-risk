export interface Position {
  ticker: string;
  name: string;
  long_short: string;
  trbc_sector: string;
  trbc_sector_abbr: string;
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
  trbc_sector: string;
  trbc_sector_abbr: string;
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

export interface UniverseSearchItem {
  ticker: string;
  name: string;
  trbc_sector: string;
  trbc_sector_abbr: string;
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
  _cached: boolean;
}
