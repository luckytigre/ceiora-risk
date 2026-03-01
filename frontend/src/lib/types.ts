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
  _cached: boolean;
}

export interface UniverseTickerItem {
  ticker: string;
  name: string;
  trbc_sector: string;
  trbc_sector_abbr: string;
  trbc_industry_group: string;
  market_cap: number;
  price: number;
  exposures: Record<string, number>;
  sensitivities: Record<string, number>;
  risk_loading: number;
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
  risk_loading: number;
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
  _cached: boolean;
}
