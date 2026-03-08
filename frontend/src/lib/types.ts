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
    country: number;
    industry: number;
    style: number;
    idio: number;
  };
}

export interface PortfolioData {
  positions: Position[];
  total_value: number;
  position_count: number;
  source_dates?: {
    fundamentals_asof?: string | null;
    exposures_asof?: string | null;
    prices_asof?: string | null;
    classification_asof?: string | null;
  };
  _cached: boolean;
}

export type HoldingsImportMode = "replace_account" | "upsert_absolute" | "increment_delta";

export interface HoldingsModeData {
  modes: HoldingsImportMode[];
  default: HoldingsImportMode;
}

export interface HoldingsAccount {
  account_id: string;
  account_name: string;
  is_active: boolean;
  positions_count: number;
  gross_quantity: number;
  last_position_updated_at: string | null;
}

export interface HoldingsAccountsData {
  accounts: HoldingsAccount[];
}

export interface HoldingsPosition {
  account_id: string;
  ric: string;
  ticker: string;
  quantity: number;
  source: string;
  updated_at: string | null;
}

export interface HoldingsPositionsData {
  positions: HoldingsPosition[];
  account_id: string | null;
  count: number;
}

export interface HoldingsImportRowPayload {
  account_id?: string;
  ric?: string;
  ticker?: string;
  quantity: number;
  source?: string;
}

export interface HoldingsImportResponse {
  status: string;
  mode: HoldingsImportMode;
  account_id: string;
  import_batch_id: string;
  accepted_rows: number;
  rejected_rows: number;
  rejection_counts: Record<string, number>;
  warnings: string[];
  applied_upserts: number;
  applied_deletes: number;
  refresh?: {
    started: boolean;
    state: Record<string, unknown>;
  } | null;
  preview_rejections?: Array<Record<string, unknown>>;
}

export interface HoldingsPositionEditResponse {
  status: string;
  action: string;
  account_id: string;
  ric: string;
  ticker: string | null;
  quantity: number;
  import_batch_id: string;
  refresh?: {
    started: boolean;
    state: Record<string, unknown>;
  } | null;
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
  category: "country" | "industry" | "style";
  exposure: number;
  factor_vol: number;
  sensitivity: number;
  marginal_var_contrib: number;
  pct_of_total: number;
}

export interface RiskShares {
  country: number;
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
    coverage_date?: string | null;
    latest_available_date?: string | null;
    selection_mode?: string;
    update_available?: boolean;
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
  ric?: string | null;
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
  row_count_mode?: string | null;
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
  exposure_source?: {
    table: string;
    selection_mode: string;
    is_dynamic: boolean;
    latest_asof?: string | null;
    plain_english?: string | null;
  };
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
      duplicate_groups: number | null;
      duplicate_extra_rows: number | null;
      computed?: boolean;
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

export interface RefreshStatusState {
  status: string;
  job_id: string | null;
  pipeline_run_id: string | null;
  profile: string | null;
  requested_profile: string | null;
  mode: string | null;
  as_of_date: string | null;
  resume_run_id: string | null;
  from_stage: string | null;
  to_stage: string | null;
  force_core: boolean;
  force_risk_recompute: boolean;
  requested_at: string | null;
  started_at: string | null;
  finished_at: string | null;
  result: Record<string, unknown> | null;
  error: {
    type?: string;
    message?: string;
    traceback?: string;
  } | null;
}

export interface RefreshStatusData {
  status: string;
  refresh: RefreshStatusState;
}

export interface OperatorLaneLatestRun {
  run_id: string | null;
  profile: string | null;
  status: string;
  started_at: string | null;
  finished_at: string | null;
  updated_at: string | null;
  stage_count: number;
  completed_stage_count: number;
  failed_stage_count: number;
  running_stage_count: number;
  stages: Array<{
    stage_name: string;
    stage_order: number;
    status: string;
    started_at: string | null;
    completed_at: string | null;
    error_type: string | null;
    error_message: string | null;
  }>;
}

export interface OperatorLaneStatus {
  profile: string;
  label: string;
  description: string;
  core_policy: string;
  serving_mode: string;
  raw_history_policy: string;
  reset_core_cache: boolean;
  default_stages: string[];
  enable_ingest: boolean;
  aliases: string[];
  latest_run: OperatorLaneLatestRun;
  recent_runs?: OperatorLaneLatestRun[];
}

export interface OperatorStatusData {
  status: string;
  generated_at: string;
  lanes: OperatorLaneStatus[];
  source_dates: {
    fundamentals_asof?: string | null;
    classification_asof?: string | null;
    prices_asof?: string | null;
    exposures_asof?: string | null;
  };
  risk_engine: {
    status?: string;
    method_version?: string;
    last_recompute_date?: string;
    factor_returns_latest_date?: string;
    lookback_days?: number;
    cross_section_min_age_days?: number;
    recompute_interval_days?: number;
  };
  core_due: {
    due: boolean;
    reason: string;
  };
  refresh: RefreshStatusState;
  holdings_sync?: {
    pending?: boolean;
    pending_count?: number;
    dirty_since?: string | null;
    last_mutation_at?: string | null;
    last_mutation_kind?: string | null;
    last_mutation_summary?: string | null;
    last_mutation_account_id?: string | null;
    last_import_batch_id?: string | null;
    last_refresh_started_at?: string | null;
    last_refresh_finished_at?: string | null;
    last_refresh_status?: string | null;
    last_refresh_profile?: string | null;
    last_refresh_run_id?: string | null;
    last_refresh_message?: string | null;
  } | null;
  neon_sync_health?: {
    status?: string;
    message?: string;
    updated_at?: string;
    artifact_path?: string | null;
    mirror_status?: string | null;
    sync_status?: string | null;
    parity_status?: string | null;
    parity_issue_count?: number;
  } | null;
  active_snapshot?: {
    snapshot_id?: string;
    published_at?: number;
  } | null;
  latest_parity_artifact?: string | null;
  runtime?: {
    data_backend?: string;
    neon_database_configured?: boolean;
    neon_auto_sync_enabled?: boolean;
    neon_auto_parity_enabled?: boolean;
    neon_auto_prune_enabled?: boolean;
    neon_read_surfaces?: string[];
    warnings?: string[];
  } | null;
}
