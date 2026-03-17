"""Portfolio risk decomposition using factor covariance."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from backend.risk_model.factor_catalog import (
    STYLE_COLUMN_TO_LABEL,
    infer_factor_family,
)

SYSTEMATIC_CATEGORIES: tuple[str, ...] = ("market", "industry", "style")


def is_market_factor(name: str) -> bool:
    return infer_factor_family(str(name or "").strip()) == "market"


def factor_category(name: str) -> str:
    family = infer_factor_family(str(name or "").strip())
    if family == "style":
        return "style"
    if family == "market":
        return "market"
    return "industry"


def systematic_variance_by_category(
    *,
    factors: list[str],
    exposures: np.ndarray,
    covariance: np.ndarray,
) -> dict[str, float]:
    """Allocate systematic variance into market/industry/style buckets."""
    if not factors:
        return {category: 0.0 for category in SYSTEMATIC_CATEGORIES}

    x = np.asarray(exposures, dtype=float).reshape(-1)
    f_mat = np.asarray(covariance, dtype=float)
    if x.size != len(factors) or f_mat.shape != (len(factors), len(factors)):
        return {category: 0.0 for category in SYSTEMATIC_CATEGORIES}

    idx_by_category: dict[str, list[int]] = {category: [] for category in SYSTEMATIC_CATEGORIES}
    for i, factor in enumerate(factors):
        idx_by_category[factor_category(factor)].append(i)

    base: dict[str, float] = {}
    allocated: dict[str, float] = {}
    for category, idv in idx_by_category.items():
        if not idv:
            base[category] = 0.0
            allocated[category] = 0.0
            continue
        vec = x[idv]
        sub = f_mat[np.ix_(idv, idv)]
        raw = float(vec.T @ sub @ vec)
        clean = raw if np.isfinite(raw) else 0.0
        clean = max(0.0, clean)
        base[category] = clean
        allocated[category] = clean

    ordered_categories = list(SYSTEMATIC_CATEGORIES)
    for i, left in enumerate(ordered_categories):
        left_idx = idx_by_category[left]
        if not left_idx:
            continue
        for right in ordered_categories[i + 1:]:
            right_idx = idx_by_category[right]
            if not right_idx:
                continue
            x_left = x[left_idx]
            x_right = x[right_idx]
            cross = float(2.0 * x_left.T @ f_mat[np.ix_(left_idx, right_idx)] @ x_right)
            if not np.isfinite(cross) or abs(cross) <= 1e-12:
                continue
            denom = base[left] + base[right]
            if denom <= 1e-12:
                allocated[left] += 0.5 * cross
                allocated[right] += 0.5 * cross
            else:
                allocated[left] += cross * (base[left] / denom)
                allocated[right] += cross * (base[right] / denom)

    return {
        category: max(0.0, float(allocated.get(category, 0.0)))
        for category in SYSTEMATIC_CATEGORIES
    }


def portfolio_factor_exposure(
    positions: list[dict[str, Any]],
    factor: str,
) -> float:
    """Compute portfolio-weighted exposure to a single factor ID."""
    total_weight = 0.0
    weighted_exposure = 0.0
    for pos in positions:
        w = float(pos.get("weight", 0.0) or 0.0)
        # Look up exposure by factor ID in the exposures dict
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
    """Decompose portfolio risk into market/industry/style/idiosyncratic.

    Returns:
        (risk_shares, component_shares, factor_details)
    """
    if cov.empty:
        return (
            {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 100.0},
            {"market": 0.0, "industry": 0.0, "style": 0.0},
            [],
        )

    # The solver does not publish an intercept as a modeled factor.
    factors = [str(c) for c in cov.columns]
    exposure_map = {f: portfolio_factor_exposure(positions, f) for f in factors}
    h = np.array([exposure_map[f] for f in factors], dtype=float)
    f_mat = cov.reindex(index=factors, columns=factors).to_numpy(dtype=float)

    if h.size == 0 or f_mat.size == 0:
        return (
            {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 100.0},
            {"market": 0.0, "industry": 0.0, "style": 0.0},
            [],
        )

    idx = {name: i for i, name in enumerate(factors)}
    systematic_by_category = systematic_variance_by_category(
        factors=factors,
        exposures=h,
        covariance=f_mat,
    )
    raw_systematic = float(sum(systematic_by_category.values()))

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
        shares = {category: 0.0 for category in SYSTEMATIC_CATEGORIES}
    else:
        shares = {
            category: float(systematic_by_category.get(category, 0.0)) / raw_systematic
            for category in SYSTEMATIC_CATEGORIES
        }

    if raw_total <= 0:
        systematic_pct = 0.0
        idio_pct = 100.0
    else:
        systematic_pct = max(0.0, min(100.0, 100.0 * raw_systematic / raw_total))
        idio_pct = max(0.0, min(100.0, 100.0 * raw_specific / raw_total))

    risk_shares = {
        "market": round(systematic_pct * shares["market"], 2),
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

        category = factor_category(f_name)

        factor_details.append({
            "factor_id": f_name,
            "category": category,
            "exposure": round(exp, 6),
            "factor_vol": round(factor_vol, 6),
            "sensitivity": round(exp * factor_vol, 6),
            "marginal_var_contrib": round(marginal, 8),
            "pct_of_total": round(pct_of_total, 2),
            "pct_of_systematic": round(pct_of_systematic, 2),
        })

    return risk_shares, shares, factor_details
