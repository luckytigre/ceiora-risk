/** Short display names for Barra factor labels (style + industry). */
const SHORT_LABELS: Record<string, string> = {
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
  "Digital Assets": "Crypto",
  "Commodity Derivatives": "Cmdty Deriv",
  "FX Derivatives": "FX Deriv",
};

/** Return a shortened factor name suitable for chart axes. */
export function shortFactorLabel(name: string): string {
  return SHORT_LABELS[name] ?? name;
}

/*
 * Regression hierarchy tier for Toraniko-style ordering.
 * Phase A: intercept + industry dummies, estimated first.
 * Phase B: all style factors, estimated on Phase A residuals.
 * Size is not orthogonalised itself — other factors are orthogonalised *to* it.
 */
type FactorTier = 1 | 2;

export const STYLE_FACTORS = new Set([
  "Size", "Nonlinear Size", "Liquidity", "Beta",
  "Book-to-Price", "Earnings Yield", "Value", "Leverage",
  "Growth", "Profitability", "Investment", "Dividend Yield",
  "Momentum", "Short-Term Reversal", "Residual Volatility",
]);

/**
 * Sort key for regression hierarchy:
 *  1 = industry (Phase A alongside intercept)
 *  2 = style (Phase B, on Phase A residuals)
 */
export function factorTier(name: string): FactorTier {
  if (STYLE_FACTORS.has(name)) return 2;
  return 1; // industry
}
