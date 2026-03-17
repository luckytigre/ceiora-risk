import type { FactorCatalogEntry, FactorFamily } from "@/lib/types";

/** Short display names for Barra factor labels (style + industry). */
const SHORT_LABELS: Record<string, string> = {
  Market: "Market",
  "Book-to-Price": "B/P",
  "Earnings Yield": "Earn Yld",
  "Dividend Yield": "Div Yld",
  "Nonlinear Size": "NL Size",
  "Short-Term Reversal": "ST Rev",
  "Residual Volatility": "Resid Vol",
  "Software & Services": "Software",
  "Diversified Financials": "Div Fin",
  "Health Care Equipment & Services": "Healthcare",
  "Consumer Discretionary Distribution": "Cons Disc",
  "Consumer Staples Distribution": "Cons Staples",
  "Capital Goods": "Cap Goods",
  "Equity REITs": "REITs",
  "Media & Entertainment": "Media",
  "Semiconductors & Semiconductor Equipment": "Semis",
  "Aerospace & Defense": "Aero/Def",
  "Automobiles & Auto Parts": "Autos",
  "Academic & Educational Services": "Edu Svcs",
  "Applied Resources": "App Res",
  "Banking Services": "Banks",
  "Banking & Investment Services": "Bank/Inv",
  Beverages: "Bev",
  "Biotechnology & Medical Research": "Biotech",
  Chemicals: "Chem",
  Coal: "Coal",
  "Collective Investments": "Coll Inv",
  "Communications & Networking": "Comm/Net",
  "Construction & Engineering": "Constr Eng",
  "Construction Materials": "Constr Mat",
  "Computers, Phones & Household Electronics": "Comp/Phone",
  "Containers & Packaging": "Containers",
  "Consumer Goods Conglomerates": "Cons Congl",
  "Cyclical Consumer Products": "Cyc Prod",
  "Cyclical Consumer Services": "Cyc Svcs",
  "Diversified Retail": "Div Retail",
  "Diversified Industrial Goods Wholesale": "Ind Whsl",
  "Electric Utilities & IPPs": "Elec Utils",
  "Energy - Fossil Fuels": "Fossil En",
  "Electronic Equipment & Parts": "Elec Parts",
  "Financial Technology (Fintech) & Infrastructure": "FinTech",
  "Food & Beverages": "Food/Bev",
  "Food & Drug Retailing": "Food/Drug",
  "Food & Tobacco": "Food/Tob",
  "Freight & Logistics Services": "Freight",
  "Healthcare Equipment & Supplies": "HC Equip",
  "Healthcare Services & Equipment": "HC Svc/Eq",
  "Healthcare Providers & Services": "HC Prov",
  "Homebuilding & Construction Supplies": "Home Const",
  "Hotels & Entertainment Services": "Hotels/Ent",
  "Household Goods": "HH Goods",
  "Industrial & Commercial Services": "Ind/Com",
  "Industrial Goods": "Ind Goods",
  Insurance: "Insur",
  "Integrated Hardware & Software": "HW/SW",
  "Investment Banking & Investment Services": "IB & Inv",
  "Investment Holding Companies": "Inv Hold",
  "Leisure Products": "Leisure",
  "Machinery, Tools, Heavy Vehicles, Trains & Ships": "Mach/Heavy",
  "Media & Publishing": "Media/Pub",
  "Metals & Mining": "Metals",
  "Mineral Resources": "Min Res",
  "Miscellaneous Educational Service Providers": "Edu Svcs",
  "Multiline Utilities": "Multi Utils",
  "Natural Gas Utilities": "Gas Utils",
  "Office Equipment": "Office Eq",
  "Oil & Gas": "Oil & Gas",
  "Oil & Gas Related Equipment and Services": "O&G Equip",
  "Paper & Forest Products": "Paper/For",
  "Passenger Transportation Services": "Passenger",
  "Personal & Household Products & Services": "Pers/HH",
  Pharmaceuticals: "Pharma",
  "Pharmaceuticals & Medical Research": "Pharma/Med",
  "Professional & Business Education": "Prof Edu",
  "Professional & Commercial Services": "Prof Svcs",
  "Real Estate Operations": "RE Ops",
  "Residential & Commercial REITs": "REITs",
  "Renewable Energy": "Renewables",
  Retailers: "Retail",
  "Schools, Colleges & Universities": "Schools",
  "Software & IT Services": "Software",
  "Specialty Retailers": "Spec Retail",
  "Technology Equipment": "Tech Equip",
  "Telecommunications Services": "Telecom",
  "Textiles & Apparel": "Textiles",
  Transportation: "Transport",
  "Transport Infrastructure": "Transport",
  Uranium: "Uranium",
  "Water & Related Utilities": "Water Utils",
  "Basic Materials": "Matls",
  "Consumer Cyclicals": "ConsCyc",
  "Consumer Non-Cyclicals": "ConsDef",
  Energy: "Energy",
  Financials: "Fins",
  Healthcare: "Health",
  Industrials: "Inds",
  "Real Estate": "RealEst",
  Technology: "Tech",
  "Telecommunication Services": "Telco",
  Utilities: "Utils",
  "Digital Assets": "Crypto",
  "Commodity Derivatives": "Cmdty Deriv",
  "FX Derivatives": "FX Deriv",
};

const STYLE_FACTOR_NAME_BY_ID: Record<string, string> = {
  style_beta_score: "Beta",
  style_momentum_score: "Momentum",
  style_size_score: "Size",
  style_nonlinear_size_score: "Nonlinear Size",
  style_short_term_reversal_score: "Short-Term Reversal",
  style_resid_vol_score: "Residual Volatility",
  style_liquidity_score: "Liquidity",
  style_book_to_price_score: "Book-to-Price",
  style_earnings_yield_score: "Earnings Yield",
  style_leverage_score: "Leverage",
  style_growth_score: "Growth",
  style_profitability_score: "Profitability",
  style_investment_score: "Investment",
  style_dividend_yield_score: "Dividend Yield",
};

export const STYLE_FACTORS = new Set(Object.values(STYLE_FACTOR_NAME_BY_ID));
export const STYLE_FACTOR_IDS = new Set(Object.keys(STYLE_FACTOR_NAME_BY_ID));

function lookupCatalogEntry(
  factorIdOrName: string,
  factorCatalog?: FactorCatalogEntry[],
): FactorCatalogEntry | null {
  const key = String(factorIdOrName || "").trim();
  if (!key || !factorCatalog?.length) return null;
  return factorCatalog.find((entry) => entry.factor_id === key || entry.factor_name === key) ?? null;
}

function fallbackFactorName(factorIdOrName: string): string {
  const key = String(factorIdOrName || "").trim();
  if (!key) return "";
  if (STYLE_FACTOR_NAME_BY_ID[key]) return STYLE_FACTOR_NAME_BY_ID[key];
  if (key === "market") return "Market";
  return key;
}

export function factorDisplayName(
  factorIdOrName: string,
  factorCatalog?: FactorCatalogEntry[],
): string {
  return lookupCatalogEntry(factorIdOrName, factorCatalog)?.factor_name ?? fallbackFactorName(factorIdOrName);
}

export function factorFamily(
  factorIdOrName: string,
  factorCatalog?: FactorCatalogEntry[],
): FactorFamily {
  const fromCatalog = lookupCatalogEntry(factorIdOrName, factorCatalog)?.family;
  if (fromCatalog) return fromCatalog;
  const key = String(factorIdOrName || "").trim();
  if (key === "market") return "market";
  if (key.startsWith("industry_")) return "industry";
  if (key.startsWith("style_") || STYLE_FACTORS.has(key)) return "style";
  return "industry";
}

/** Return a shortened factor name suitable for chart axes. */
export function shortFactorLabel(
  factorIdOrName: string,
  factorCatalog?: FactorCatalogEntry[],
): string {
  const name = factorDisplayName(factorIdOrName, factorCatalog);
  const direct = SHORT_LABELS[name];
  if (direct) return direct;

  const normalized = name
    .replace(/\band\b/gi, "&")
    .replace(/\s*&\s*/g, " & ")
    .replace(/\s+/g, " ")
    .trim();
  return SHORT_LABELS[normalized] ?? name;
}

type FactorTier = 1 | 2 | 3;

/**
 * Sort key for regression hierarchy:
 *  1 = market
 *  2 = industry
 *  3 = style
 */
export function factorTier(
  factorIdOrName: string,
  factorCatalog?: FactorCatalogEntry[],
): FactorTier {
  const family = factorFamily(factorIdOrName, factorCatalog);
  if (family === "market") return 1;
  if (family === "industry") return 2;
  return 3;
}
