export type CparFactorGroup = "market" | "sector" | "style";
export type CparFitStatus = "ok" | "limited_history" | "insufficient_history";
export type CparWarning = "continuity_gap" | "ex_us_caution";
export type CparHedgeStatus = "hedge_ok" | "hedge_degraded" | "hedge_unavailable";
export type CparHedgeMode = "factor_neutral" | "market_neutral";
export type CparFactorHistoryMode = "residual" | "market_adjusted";
export type CparRiskExposureMode = "raw" | "sensitivity" | "risk_contribution";
export type CparPortfolioStatus = "ok" | "partial" | "empty" | "unavailable";
export type CparPortfolioCoverage = "covered" | "missing_price" | "missing_cpar_fit" | "insufficient_history";
export type CparRiskScope = "all_accounts";
export type CparExploreScope = "all_accounts" | "restricted_accounts";
export type CparSourceContextStatus = "ok" | "partial" | "missing" | "unavailable";
export type CparSourceContextReason = "missing_rows" | "shared_source_unavailable" | "mixed";

export interface CparPackageMeta {
  package_run_id: string;
  package_date: string;
  profile: string;
  started_at?: string | null;
  completed_at?: string | null;
  method_version: string;
  factor_registry_version: string;
  data_authority: string;
  lookback_weeks: number;
  half_life_weeks: number;
  min_observations: number;
  source_prices_asof?: string | null;
  classification_asof?: string | null;
  universe_count: number;
  fit_ok_count: number;
  fit_limited_count: number;
  fit_insufficient_count: number;
}

export interface CparFactorSpec {
  factor_id: string;
  ticker: string;
  label: string;
  group: CparFactorGroup;
  display_order: number;
  method_version: string;
  factor_registry_version: string;
}

export interface CparSearchItem {
  ticker: string | null;
  ric: string;
  display_name: string | null;
  fit_status?: CparFitStatus | null;
  warnings: CparWarning[];
  hq_country_code?: string | null;
  target_scope?: string | null;
  fit_family?: string | null;
  price_on_package_date_status?: string | null;
  fit_row_status?: string | null;
  fit_quality_status?: string | null;
  portfolio_use_status?: string | null;
  ticker_detail_use_status?: string | null;
  hedge_use_status?: string | null;
  reason_code?: string | null;
  quality_label?: string | null;
  risk_tier?: string | null;
  risk_tier_label?: string | null;
  risk_tier_detail?: string | null;
  quote_source?: string | null;
  quote_source_label?: string | null;
  quote_source_detail?: string | null;
  scenario_stage_supported?: boolean;
  scenario_stage_detail?: string | null;
}

export interface CparSearchData extends CparPackageMeta {
  query: string;
  limit: number;
  total: number;
  results: CparSearchItem[];
}

export interface CparLoading {
  factor_id: string;
  label: string;
  group: CparFactorGroup;
  display_order: number;
  beta: number;
}

export interface CparFactorVarianceContribution {
  factor_id: string;
  label: string;
  group: CparFactorGroup;
  display_order: number;
  beta: number;
  variance_contribution: number;
  variance_share: number | null;
}

export interface CparRiskShares {
  market: number;
  industry: number;
  style: number;
  idio: number;
}

export interface CparCoverageBucket {
  positions_count: number;
  gross_market_value: number;
}

export interface CparCoverageBreakdown {
  covered: CparCoverageBucket;
  missing_price: CparCoverageBucket;
  missing_cpar_fit: CparCoverageBucket;
  insufficient_history: CparCoverageBucket;
}

export interface CparLatestCommonName {
  value: string;
  as_of_date: string;
}

export interface CparClassificationSnapshot {
  as_of_date: string;
  trbc_economic_sector: string | null;
  trbc_business_sector: string | null;
  trbc_industry_group: string | null;
  trbc_industry: string | null;
  trbc_activity: string | null;
}

export interface CparLatestPriceContext {
  price: number;
  price_date: string;
  price_field_used: string | null;
  currency: string | null;
}

export interface CparSourceContext {
  status: CparSourceContextStatus;
  reason: CparSourceContextReason | null;
  latest_common_name: CparLatestCommonName | null;
  classification_snapshot: CparClassificationSnapshot | null;
  latest_price_context: CparLatestPriceContext | null;
}

export interface CparTickerDetailData extends CparPackageMeta {
  ticker: string | null;
  ric: string;
  display_name: string | null;
  fit_status?: CparFitStatus | null;
  warnings: CparWarning[];
  observed_weeks: number;
  lookback_weeks: number;
  longest_gap_weeks: number;
  price_field_used?: string | null;
  hq_country_code?: string | null;
  market_step_alpha?: number | null;
  beta_market_step1?: number | null;
  block_alpha?: number | null;
  beta_spy_trade?: number | null;
  display_loadings: CparLoading[];
  raw_loadings: CparLoading[];
  thresholded_loadings: CparLoading[];
  pre_hedge_factor_variance_proxy?: number | null;
  pre_hedge_factor_volatility_proxy?: number | null;
  source_context: CparSourceContext;
  target_scope?: string | null;
  fit_family?: string | null;
  price_on_package_date_status?: string | null;
  fit_row_status?: string | null;
  fit_quality_status?: string | null;
  portfolio_use_status?: string | null;
  ticker_detail_use_status?: string | null;
  hedge_use_status?: string | null;
  reason_code?: string | null;
  quality_label?: string | null;
  risk_tier?: string | null;
  risk_tier_label?: string | null;
  risk_tier_detail?: string | null;
  quote_source?: string | null;
  quote_source_label?: string | null;
  quote_source_detail?: string | null;
  scenario_stage_supported?: boolean;
  scenario_stage_detail?: string | null;
}

export interface CparHedgeLeg {
  factor_id: string;
  label?: string | null;
  group?: CparFactorGroup | null;
  display_order?: number | null;
  weight: number;
}

export interface CparPostHedgeExposure {
  factor_id: string;
  label?: string | null;
  group?: CparFactorGroup | null;
  display_order?: number | null;
  pre_beta: number;
  hedge_leg: number;
  post_beta: number;
}

export interface CparHedgePreviewData extends CparPackageMeta {
  ticker: string | null;
  ric: string;
  display_name: string | null;
  fit_status: CparFitStatus;
  warnings: CparWarning[];
  mode: CparHedgeMode;
  hedge_status: CparHedgeStatus;
  hedge_reason?: string | null;
  hedge_legs: CparHedgeLeg[];
  post_hedge_exposures: CparPostHedgeExposure[];
  pre_hedge_factor_variance_proxy: number;
  post_hedge_factor_variance_proxy: number;
  gross_hedge_notional: number;
  net_hedge_notional: number;
  non_market_reduction_ratio?: number | null;
  stability: {
    leg_overlap_ratio?: number | null;
    gross_hedge_notional_change?: number | null;
    net_hedge_notional_change?: number | null;
  };
}

export interface CparMetaData extends CparPackageMeta {
  factor_count: number;
  factors: CparFactorSpec[];
}

export interface CparPortfolioPositionRow {
  account_id: string;
  ric: string;
  ticker: string | null;
  display_name: string | null;
  trbc_industry_group: string | null;
  quantity: number;
  price: number | null;
  price_date: string | null;
  price_field_used: string | null;
  market_value: number | null;
  portfolio_weight: number | null;
  fit_status: CparFitStatus | null;
  warnings: CparWarning[];
  beta_spy_trade: number | null;
  specific_variance_proxy?: number | null;
  specific_volatility_proxy?: number | null;
  coverage: CparPortfolioCoverage;
  coverage_reason: string | null;
  display_contributions: CparLoading[];
  thresholded_contributions: CparLoading[];
  risk_mix?: CparRiskShares | null;
}

export interface CparFactorDrilldownRow {
  ric: string;
  ticker: string | null;
  display_name: string | null;
  market_value: number | null;
  portfolio_weight: number | null;
  fit_status: CparFitStatus | null;
  warnings: CparWarning[];
  coverage: CparPortfolioCoverage;
  coverage_reason: string | null;
  factor_beta: number | null;
  contribution_beta: number;
  vol_scaled_loading: number;
  vol_scaled_contribution: number;
  covariance_adjusted_loading: number;
  risk_contribution_pct: number;
}

export interface CparFactorChartRow {
  factor_id: string;
  label: string;
  group: CparFactorGroup;
  display_order: number;
  beta: number;
  aggregate_beta: number;
  factor_volatility: number;
  covariance_adjustment: number;
  sensitivity_beta: number;
  risk_contribution_pct: number;
  positive_contribution_beta: number;
  negative_contribution_beta: number;
  variance_contribution: number | null;
  variance_share: number | null;
  drilldown: CparFactorDrilldownRow[];
}

export interface CparCovMatrix {
  factors: string[];
  correlation: number[][];
}

export interface CparFactorHistoryPoint {
  date: string;
  factor_return: number;
  cum_return: number;
}

export interface CparFactorHistoryData {
  factor_id: string;
  factor_name: string;
  history_mode: CparFactorHistoryMode;
  years: number;
  points: CparFactorHistoryPoint[];
  _cached: boolean;
}

export interface CparTickerHistoryPoint {
  date: string;
  close: number;
}

export interface CparTickerHistoryData {
  ticker: string;
  ric: string;
  years: number;
  points: CparTickerHistoryPoint[];
  _cached: boolean;
}

export interface CparExploreHeldPosition {
  ric: string;
  ticker: string | null;
  quantity: number;
  price: number | null;
  market_value: number | null;
  portfolio_weight: number | null;
  long_short: "LONG" | "SHORT";
  fit_status: CparFitStatus | null;
  coverage: CparPortfolioCoverage;
}

export interface CparExploreContextData extends CparPackageMeta {
  scope: CparExploreScope;
  accounts_count: number;
  positions_count: number;
  covered_positions_count: number;
  excluded_positions_count: number;
  gross_market_value: number;
  net_market_value: number;
  covered_gross_market_value: number;
  coverage_ratio: number | null;
  portfolio_status: CparPortfolioStatus;
  portfolio_reason: string | null;
  held_positions: CparExploreHeldPosition[];
}

export interface CparRiskData extends CparPackageMeta {
  factors: CparFactorSpec[];
  scope: CparRiskScope;
  accounts_count: number;
  portfolio_status: CparPortfolioStatus;
  portfolio_reason: string | null;
  positions_count: number;
  covered_positions_count: number;
  excluded_positions_count: number;
  gross_market_value: number;
  net_market_value: number;
  covered_gross_market_value: number;
  coverage_ratio: number | null;
  coverage_breakdown: CparCoverageBreakdown;
  aggregate_display_loadings: CparLoading[];
  aggregate_thresholded_loadings: CparLoading[];
  risk_shares: CparRiskShares;
  vol_scaled_shares?: CparRiskShares;
  display_factor_variance_contributions: CparFactorVarianceContribution[];
  factor_variance_contributions: CparFactorVarianceContribution[];
  display_factor_chart: CparFactorChartRow[];
  factor_chart: CparFactorChartRow[];
  display_cov_matrix?: CparCovMatrix;
  cov_matrix: CparCovMatrix;
  factor_variance_proxy: number;
  idio_variance_proxy: number;
  total_variance_proxy: number;
  pre_hedge_factor_variance_proxy?: number | null;
  positions: CparPortfolioPositionRow[];
}

export interface CparPortfolioHedgeData extends CparPackageMeta {
  account_id: string;
  account_name: string | null;
  mode: CparHedgeMode;
  portfolio_status: CparPortfolioStatus;
  portfolio_reason: string | null;
  positions_count: number;
  covered_positions_count: number;
  excluded_positions_count: number;
  gross_market_value: number;
  net_market_value: number;
  covered_gross_market_value: number;
  coverage_ratio: number | null;
  coverage_breakdown: CparCoverageBreakdown;
  aggregate_display_loadings: CparLoading[];
  aggregate_thresholded_loadings: CparLoading[];
  risk_shares: CparRiskShares;
  vol_scaled_shares?: CparRiskShares;
  display_factor_variance_contributions: CparFactorVarianceContribution[];
  factor_variance_contributions: CparFactorVarianceContribution[];
  display_factor_chart: CparFactorChartRow[];
  factor_chart: CparFactorChartRow[];
  display_cov_matrix?: CparCovMatrix;
  cov_matrix: CparCovMatrix;
  factor_variance_proxy: number;
  idio_variance_proxy: number;
  total_variance_proxy: number;
  hedge_status: CparHedgeStatus | null;
  hedge_reason: string | null;
  hedge_legs: CparHedgeLeg[];
  post_hedge_exposures: CparPostHedgeExposure[];
  pre_hedge_factor_variance_proxy: number | null;
  post_hedge_factor_variance_proxy: number | null;
  gross_hedge_notional: number | null;
  net_hedge_notional: number | null;
  non_market_reduction_ratio: number | null;
  positions: CparPortfolioPositionRow[];
}

export interface CparPortfolioWhatIfScenarioRow {
  ric: string;
  ticker: string | null;
  display_name: string | null;
  quantity_delta: number;
  current_quantity: number;
  hypothetical_quantity: number;
  price: number | null;
  price_date: string | null;
  price_field_used: string | null;
  market_value_delta: number | null;
  fit_status: CparFitStatus | null;
  warnings: CparWarning[];
  coverage: CparPortfolioCoverage;
  coverage_reason: string | null;
}

export interface CparPortfolioWhatIfData extends CparPackageMeta {
  account_id: string;
  account_name: string | null;
  mode: CparHedgeMode;
  scenario_row_count: number;
  changed_positions_count: number;
  scenario_rows: CparPortfolioWhatIfScenarioRow[];
  current: CparPortfolioHedgeData;
  hypothetical: CparPortfolioHedgeData;
  _preview_only: boolean;
}

export interface CparExploreScenarioRow {
  account_id: string;
  ticker: string | null;
  ric: string;
  quantity: number;
  source?: string | null;
}

export interface CparExploreHoldingDelta {
  account_id: string;
  ticker: string | null;
  ric: string;
  current_quantity: number;
  hypothetical_quantity: number;
  delta_quantity: number;
}

export type CparExploreRiskShares = CparRiskShares;

export interface CparExploreExposureDrilldownRow {
  ric: string | null;
  ticker: string | null;
  display_name: string | null;
  weight: number;
  exposure: number;
  sensitivity: number;
  contribution: number;
  fit_status: CparFitStatus | null;
  coverage: CparPortfolioCoverage | null;
}

export interface CparExploreExposureRow {
  factor_id: string;
  label: string | null;
  group: CparFactorGroup | null;
  display_order: number;
  value: number;
  factor_volatility: number;
  drilldown: CparExploreExposureDrilldownRow[];
}

export interface CparExplorePreviewSide {
  scope: CparExploreScope;
  positions: CparPortfolioPositionRow[];
  total_value: number;
  position_count: number;
  risk_shares: CparExploreRiskShares;
  exposure_modes: {
    raw: CparExploreExposureRow[];
    sensitivity: CparExploreExposureRow[];
    risk_contribution: CparExploreExposureRow[];
  };
  display_exposure_modes: {
    raw: CparExploreExposureRow[];
    sensitivity: CparExploreExposureRow[];
    risk_contribution: CparExploreExposureRow[];
  };
  factor_catalog: CparFactorSpec[];
  portfolio_status: CparPortfolioStatus;
  portfolio_reason: string | null;
}

export interface CparExploreFactorDeltaRow {
  factor_id: string;
  current: number;
  hypothetical: number;
  delta: number;
}

export interface CparExplorePreviewScope {
  kind: "staged_accounts";
  account_ids: string[];
  accounts_count: number;
}

export interface CparExploreWhatIfData extends CparPackageMeta {
  scenario_rows: CparExploreScenarioRow[];
  holding_deltas: CparExploreHoldingDelta[];
  current: CparExplorePreviewSide;
  hypothetical: CparExplorePreviewSide;
  diff: {
    total_value: number;
    position_count: number;
    risk_shares: CparExploreRiskShares;
    factor_deltas: {
      raw: CparExploreFactorDeltaRow[];
      sensitivity: CparExploreFactorDeltaRow[];
      risk_contribution: CparExploreFactorDeltaRow[];
    };
    display_factor_deltas: {
      raw: CparExploreFactorDeltaRow[];
      sensitivity: CparExploreFactorDeltaRow[];
      risk_contribution: CparExploreFactorDeltaRow[];
    };
  };
  source_dates?: {
    prices_asof?: string | null;
    classification_asof?: string | null;
    exposures_asof?: string | null;
    exposures_served_asof?: string | null;
  };
  truth_surface?: string | null;
  preview_scope?: CparExplorePreviewScope;
  _preview_only: boolean;
}
