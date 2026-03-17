export type ModelStatus = "core_estimated" | "projected_only" | "ineligible";
export type FactorFamily = "market" | "industry" | "style";

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
  risk_contrib_pct: number;
  model_status?: ModelStatus;
  eligibility_reason?: string;
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
}

export interface FactorDrilldownItem {
  ticker: string;
  weight: number;
  exposure: number;
  sensitivity?: number;
  contribution: number;
}

export interface FactorExposure {
  factor_id: string;
  value: number;
  factor_vol?: number;
  coverage_pct?: number;
  cross_section_n?: number;
  eligible_n?: number;
  coverage_date?: string | null;
  drilldown: FactorDrilldownItem[];
}

export interface ExposuresData extends ServingSnapshotMeta {
  mode: string;
  factors: FactorExposure[];
  source_dates?: SourceDates;
  _cached: boolean;
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
  component_shares: Omit<RiskShares, "idio">;
  factor_details: FactorDetail[];
  factor_catalog?: FactorCatalogEntry[];
  cov_matrix: CovMatrix;
  r_squared: number;
  source_dates?: SourceDates;
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
  model_status?: ModelStatus;
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
  model_status?: ModelStatus;
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
  factor_catalog?: FactorCatalogEntry[];
  r_squared?: number;
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
