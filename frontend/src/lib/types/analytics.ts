export type ModelStatus = "core_estimated" | "projected_only" | "ineligible";
export type FactorFamily = "market" | "industry" | "style";
export type ExposureOrigin =
  | "native"
  | "projected_fundamental"
  | "projected_returns"
  | "projected";

export interface FactorCatalogEntry {
  factor_id: string;
  factor_name: string;
  short_label: string;
  family: FactorFamily;
  block: string;
  source_column?: string | null;
  display_order?: number;
  covariance_display?: boolean;
  exposure_publish?: boolean;
  active?: boolean;
  method_version?: string;
}

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
  specific_var?: number | null;
  specific_vol?: number | null;
  risk_contrib_pct: number;
  model_status?: ModelStatus;
  model_status_reason?: string;
  eligibility_reason?: string;
  exposure_origin?: ExposureOrigin;
  projection_method?: string | null;
  projection_r_squared?: number | null;
  projection_obs_count?: number | null;
  projection_asof?: string | null;
  projection_basis_status?: string | null;
  projection_candidate_status?: string | null;
  projection_output_status?: string | null;
  served_exposure_available?: boolean;
  risk_mix?: {
    market: number;
    industry: number;
    style: number;
    idio: number;
  };
}

export interface SourceDates {
  fundamentals_asof?: string | null;
  exposures_asof?: string | null;
  exposures_latest_available_asof?: string | null;
  exposures_served_asof?: string | null;
  prices_asof?: string | null;
  classification_asof?: string | null;
}

export interface ServingSnapshotMeta {
  run_id?: string | null;
  snapshot_id?: string | null;
  refresh_started_at?: string | null;
}

export interface PortfolioData extends ServingSnapshotMeta {
  positions: Position[];
  total_value: number;
  position_count: number;
  source_dates?: SourceDates;
  _cached: boolean;
  _account_scoped?: boolean;
  account_id?: string | null;
}

export interface CuseExploreHeldPosition {
  ticker: string;
  shares: number;
  weight: number;
  market_value: number;
  long_short: string;
  price: number;
}

export interface CuseExploreContextData extends ServingSnapshotMeta {
  held_positions: CuseExploreHeldPosition[];
  source_dates?: SourceDates;
  _cached: boolean;
  _account_scoped?: boolean;
  account_id?: string | null;
}

export interface FactorDrilldownItem {
  ticker: string;
  weight: number;
  exposure: number;
  sensitivity?: number;
  contribution: number;
  model_status?: ModelStatus;
  exposure_origin?: ExposureOrigin;
}

export interface FactorExposure {
  factor_id: string;
  value: number;
  factor_vol?: number;
  coverage_pct?: number;
  cross_section_n?: number;
  eligible_n?: number;
  factor_coverage_asof?: string | null;
  coverage_date?: string | null;
  drilldown: FactorDrilldownItem[];
}

export interface ExposuresData extends ServingSnapshotMeta {
  mode: string;
  factors: FactorExposure[];
  source_dates?: SourceDates;
  _cached: boolean;
  _account_scoped?: boolean;
  account_id?: string | null;
}

export interface FactorHistoryPoint {
  date: string;
  factor_return: number;
  cum_return: number;
}

export interface FactorHistoryData {
  factor_id: string;
  factor_name: string;
  years: number;
  points: FactorHistoryPoint[];
  _cached: boolean;
}

export interface FactorDetail {
  factor_id: string;
  category: FactorFamily;
  exposure: number;
  factor_vol: number;
  sensitivity: number;
  marginal_var_contrib: number;
  pct_of_total: number;
  pct_of_systematic?: number;
}

export interface RiskShares {
  market: number;
  industry: number;
  style: number;
  idio: number;
}

export interface CovMatrix {
  factors: string[];
  correlation?: number[][];
  matrix?: number[][];
}

export interface RiskData extends ServingSnapshotMeta {
  risk_shares: RiskShares;
  vol_scaled_shares?: RiskShares;
  component_shares: Omit<RiskShares, "idio">;
  factor_details: FactorDetail[];
  factor_catalog?: FactorCatalogEntry[];
  cov_matrix: CovMatrix;
  r_squared: number | null;
  source_dates?: SourceDates;
  risk_engine?: {
    status?: string;
    method_version?: string;
    last_recompute_date?: string;
    factor_returns_latest_date?: string;
    core_rebuild_date?: string;
    core_state_through_date?: string;
    estimation_exposure_anchor_date?: string | null;
    latest_r2?: number | null;
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
    served_loadings_asof?: string | null;
    latest_loadings_available_asof?: string | null;
    coverage_date?: string | null;
    latest_available_date?: string | null;
    selection_mode?: string;
    update_available?: boolean;
  };
  _cached: boolean;
  _account_scoped?: boolean;
  account_id?: string | null;
}

export interface CuseRiskPageSummaryRiskData extends ServingSnapshotMeta {
  risk_shares: RiskShares;
  vol_scaled_shares?: RiskShares;
  factor_details: FactorDetail[];
  factor_catalog?: FactorCatalogEntry[];
  source_dates?: SourceDates;
  risk_engine?: RiskData["risk_engine"];
  model_sanity?: RiskData["model_sanity"];
  _cached: boolean;
  _account_scoped?: boolean;
  account_id?: string | null;
}

export interface CuseRiskPageExposureModeData extends ExposuresData {}

export interface CuseRiskPageCovarianceData extends ServingSnapshotMeta {
  cov_matrix: CovMatrix;
  _cached: boolean;
  _account_scoped?: boolean;
  account_id?: string | null;
}

export interface CuseRiskPageSummaryData {
  portfolio: PortfolioData;
  risk: CuseRiskPageSummaryRiskData;
  exposures: {
    raw: CuseRiskPageExposureModeData;
  };
  _cached: boolean;
  _account_scoped?: boolean;
  account_id?: string | null;
  truth_surface?: string | null;
}

export interface UniverseTickerItem {
  ticker: string;
  name: string;
  trbc_economic_sector_short: string;
  trbc_economic_sector_short_abbr: string;
  trbc_industry_group: string;
  market_cap: number | null;
  price: number | null;
  exposures: Record<string, number>;
  sensitivities: Record<string, number>;
  risk_loading: number | null;
  specific_var?: number | null;
  specific_vol?: number | null;
  model_status?: ModelStatus;
  model_status_reason?: string;
  eligibility_reason?: string;
  model_warning?: string;
  as_of_date?: string;
  exposure_origin?: ExposureOrigin;
  projection_method?: string | null;
  projection_r_squared?: number | null;
  projection_obs_count?: number | null;
  projection_asof?: string | null;
  projection_basis_status?: string | null;
  projection_candidate_status?: string | null;
  projection_output_status?: string | null;
  served_exposure_available?: boolean;
  risk_tier?: string | null;
  risk_tier_label?: string | null;
  risk_tier_detail?: string | null;
  quote_source?: string | null;
  quote_source_label?: string | null;
  quote_source_detail?: string | null;
  whatif_ready?: boolean;
  whatif_ready_label?: string | null;
  whatif_ready_detail?: string | null;
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
  model_status?: ModelStatus;
  model_status_reason?: string;
  eligibility_reason?: string;
  exposure_origin?: ExposureOrigin;
  risk_tier?: string | null;
  risk_tier_label?: string | null;
  risk_tier_detail?: string | null;
  quote_source?: string | null;
  quote_source_label?: string | null;
  quote_source_detail?: string | null;
  whatif_ready?: boolean;
  whatif_ready_label?: string | null;
  whatif_ready_detail?: string | null;
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
  factor_catalog?: FactorCatalogEntry[];
  r_squared?: number | null;
  ticker_count?: number;
  eligible_ticker_count?: number;
  core_estimated_ticker_count?: number;
  projected_only_ticker_count?: number;
  ineligible_ticker_count?: number;
  _cached: boolean;
}

export interface SeriesPoint {
  date: string;
  value: number;
}
