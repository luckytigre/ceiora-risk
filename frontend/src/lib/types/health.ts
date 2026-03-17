import type { FactorCatalogEntry, SeriesPoint } from "@/lib/types/analytics";

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
  factor_id: string;
  value: number;
}

export interface HealthIncrementalBlockR2Point {
  date: string;
  r2_full: number;
  r2_structural: number;
  r2_style_incremental: number;
  roll60_full: number;
  roll60_structural: number;
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
  market_pct_total: number;
  industry_pct_total: number;
  style_pct_total: number;
  idio_pct_total: number;
  market_pct_factor_only: number;
  industry_pct_factor_only: number;
  style_pct_factor_only: number;
}

export interface HealthExposureStats {
  factor_id: string;
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
  diagnostics_refresh_state?: string;
  diagnostics_generated_from_run_id?: string | null;
  diagnostics_generated_from_snapshot_id?: string | null;
  factor_catalog?: FactorCatalogEntry[];
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
    forecast_vs_realized: HealthForecastRealizedRow[];
    rolling_avg_factor_vol: SeriesPoint[];
  };
  section5: {
    fundamentals: HealthCoverageTable;
    trbc_history: HealthCoverageTable;
  };
  _cached: boolean;
}
