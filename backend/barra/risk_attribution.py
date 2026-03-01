"""Portfolio risk decomposition using factor covariance."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from barra.descriptors import FULL_STYLE_FACTORS

# Column name → human label for style factors from barra_exposures table
STYLE_COLUMN_TO_LABEL: dict[str, str] = {
    "beta_score": "Beta",
    "momentum_score": "Momentum",
    "size_score": "Size",
    "nonlinear_size_score": "Nonlinear Size",
    "short_term_reversal_score": "Short-Term Reversal",
    "resid_vol_score": "Residual Volatility",
    "liquidity_score": "Liquidity",
    "book_to_price_score": "Book-to-Price",
    "earnings_yield_score": "Earnings Yield",
    "value_score": "Value",
    "leverage_score": "Leverage",
    "growth_score": "Growth",
    "profitability_score": "Profitability",
    "investment_score": "Investment",
    "dividend_yield_score": "Dividend Yield",
}

STYLE_FACTOR_NAMES = set(FULL_STYLE_FACTORS.keys())


def portfolio_factor_exposure(
    positions: list[dict[str, Any]],
    factor: str,
) -> float:
    """Compute portfolio-weighted exposure to a single factor."""
    total_weight = 0.0
    weighted_exposure = 0.0
    for pos in positions:
        w = float(pos.get("weight", 0.0) or 0.0)
        # Look up exposure by factor label in the exposures dict
        exposures = pos.get("exposures", {})
        exp = float(exposures.get(factor, 0.0) or 0.0)
        weighted_exposure += w * exp
        total_weight += abs(w)
    return weighted_exposure


def risk_decomposition(
    *,
    cov: pd.DataFrame,
    positions: list[dict[str, Any]],
    specific_risk_by_ticker: dict[str, dict[str, float | int | str]] | None = None,
) -> tuple[dict[str, float], dict[str, float], list[dict[str, Any]]]:
    """Decompose portfolio risk into style/industry/idiosyncratic.

    Returns:
        (risk_shares, component_shares, factor_details)
    """
    if cov.empty:
        return (
            {"industry": 0.0, "style": 0.0, "idio": 100.0},
            {"industry": 0.0, "style": 0.0},
            [],
        )

    # The intercept is part of phase-A fit but not modeled as a factor.
    # Drop any stale "market" column defensively if present in a legacy cache.
    factors = [str(c) for c in cov.columns if str(c).lower() != "market"]
    exposure_map = {f: portfolio_factor_exposure(positions, f) for f in factors}
    h = np.array([exposure_map[f] for f in factors], dtype=float)
    f_mat = cov.reindex(index=factors, columns=factors).to_numpy(dtype=float)

    if h.size == 0 or f_mat.size == 0:
        return (
            {"industry": 0.0, "style": 0.0, "idio": 100.0},
            {"industry": 0.0, "style": 0.0},
            [],
        )

    # Classify factors
    style_factors = [f for f in factors if f in STYLE_FACTOR_NAMES]
    industry_factors = [f for f in factors if f not in set(style_factors)]

    idx = {name: i for i, name in enumerate(factors)}

    def _component_variance(names: list[str]) -> float:
        if not names:
            return 0.0
        idv = [idx[n] for n in names if n in idx]
        if not idv:
            return 0.0
        vec = h[idv]
        sub = f_mat[np.ix_(idv, idv)]
        raw = float(vec.T @ sub @ vec)
        return max(0.0, raw if np.isfinite(raw) else 0.0)

    raw_industry = _component_variance(industry_factors)
    raw_style = _component_variance(style_factors)
    raw_systematic = raw_industry + raw_style

    spec_map = specific_risk_by_ticker or {}
    raw_specific = 0.0
    for pos in positions:
        ticker = str(pos.get("ticker", "")).upper()
        w_i = float(pos.get("weight", 0.0) or 0.0)
        spec_row = spec_map.get(ticker, {})
        spec_var = float(spec_row.get("specific_var", 0.0) or 0.0)
        if not np.isfinite(spec_var) or spec_var < 0:
            spec_var = 0.0
        raw_specific += (w_i ** 2) * spec_var
    raw_specific = max(0.0, raw_specific)
    raw_total = raw_systematic + raw_specific

    if raw_systematic <= 0:
        shares = {"industry": 0.5, "style": 0.5}
    else:
        shares = {
            "industry": raw_industry / raw_systematic,
            "style": raw_style / raw_systematic,
        }

    if raw_total <= 0:
        systematic_pct = 0.0
        idio_pct = 100.0
    else:
        systematic_pct = max(0.0, min(100.0, 100.0 * raw_systematic / raw_total))
        idio_pct = max(0.0, min(100.0, 100.0 * raw_specific / raw_total))

    risk_shares = {
        "industry": round(systematic_pct * shares["industry"], 2),
        "style": round(systematic_pct * shares["style"], 2),
        "idio": round(idio_pct, 2),
    }

    # Per-factor details
    total_portfolio_var = float(raw_total)
    systematic_var = float(raw_systematic)
    factor_details = []
    for f_name in factors:
        i = idx[f_name]
        exp = exposure_map[f_name]
        factor_vol = float(np.sqrt(max(0.0, f_mat[i, i])))
        # Marginal contribution: h_f * (F·h)_f
        fh = f_mat @ h
        marginal = h[i] * fh[i]
        pct_of_total = (marginal / total_portfolio_var * 100.0) if total_portfolio_var > 0 else 0.0
        pct_of_systematic = (marginal / systematic_var * 100.0) if systematic_var > 0 else 0.0

        category = "style" if f_name in STYLE_FACTOR_NAMES else "industry"

        factor_details.append({
            "factor": f_name,
            "category": category,
            "exposure": round(exp, 6),
            "factor_vol": round(factor_vol, 6),
            "sensitivity": round(exp * factor_vol, 6),
            "marginal_var_contrib": round(marginal, 8),
            "pct_of_total": round(pct_of_total, 2),
            "pct_of_systematic": round(pct_of_systematic, 2),
        })

    return risk_shares, shares, factor_details
