/** Short display names for Barra factor labels (style + industry). */
const SHORT_LABELS: Record<string, string> = {
  "Country: US vs Non-US": "Country",
  // Style factors
  "Book-to-Price": "B/P",
  "Earnings Yield": "Earn Yld",
  "Dividend Yield": "Div Yld",
  "Nonlinear Size": "NL Size",
  "Short-Term Reversal": "ST Rev",
  "Residual Volatility": "Resid Vol",
  // Industry factors
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
  "Beverages": "Bev",
  "Biotechnology & Medical Research": "Biotech",
  "Chemicals": "Chem",
  "Coal": "Coal",
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
  "Insurance": "Insur",
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
  "Pharmaceuticals": "Pharma",
  "Pharmaceuticals & Medical Research": "Pharma/Med",
  "Professional & Business Education": "Prof Edu",
  "Professional & Commercial Services": "Prof Svcs",
  "Real Estate Operations": "RE Ops",
  "Residential & Commercial REITs": "REITs",
  "Renewable Energy": "Renewables",
  "Retailers": "Retail",
  "Schools, Colleges & Universities": "Schools",
  "Software & IT Services": "Software",
  "Specialty Retailers": "Spec Retail",
  "Technology Equipment": "Tech Equip",
  "Telecommunications Services": "Telecom",
  "Textiles & Apparel": "Textiles",
  "Transportation": "Transport",
  "Transport Infrastructure": "Transport",
  "Uranium": "Uranium",
  "Water & Related Utilities": "Water Utils",
  // TRBC sectors (for any sector-level chart categories)
  "Basic Materials": "Matls",
  "Consumer Cyclicals": "ConsCyc",
  "Consumer Non-Cyclicals": "ConsDef",
  "Energy": "Energy",
  "Financials": "Fins",
  "Healthcare": "Health",
  "Industrials": "Inds",
  "Real Estate": "RealEst",
  "Technology": "Tech",
  "Telecommunication Services": "Telco",
  "Utilities": "Utils",
  "Digital Assets": "Crypto",
  "Commodity Derivatives": "Cmdty Deriv",
  "FX Derivatives": "FX Deriv",
};

/** Return a shortened factor name suitable for chart axes. */
export function shortFactorLabel(name: string): string {
  const direct = SHORT_LABELS[name];
  if (direct) return direct;

  const normalized = name
    .replace(/\band\b/gi, "&")
    .replace(/\s*&\s*/g, " & ")
    .replace(/\s+/g, " ")
    .trim();
  return SHORT_LABELS[normalized] ?? name;
}

/*
 * Regression hierarchy tier for Toraniko-style ordering.
 * Phase A: intercept + industry dummies, estimated first.
 * Phase B: all style factors, estimated on Phase A residuals.
 * Size is not orthogonalised itself — other factors are orthogonalised *to* it.
 */
type FactorTier = 1 | 2 | 3;

export const STYLE_FACTORS = new Set([
  "Size", "Nonlinear Size", "Liquidity", "Beta",
  "Book-to-Price", "Earnings Yield", "Value", "Leverage",
  "Growth", "Profitability", "Investment", "Dividend Yield",
  "Momentum", "Short-Term Reversal", "Residual Volatility",
]);

export function isCountryFactor(name: string): boolean {
  return String(name || "").startsWith("Country:");
}

/**
 * Sort key for regression hierarchy:
 *  1 = industry (Phase A alongside intercept)
 *  2 = style (Phase B, on Phase A residuals)
 */
export function factorTier(name: string): FactorTier {
  if (isCountryFactor(name)) return 1;
  if (STYLE_FACTORS.has(name)) return 3;
  return 2; // industry
}
