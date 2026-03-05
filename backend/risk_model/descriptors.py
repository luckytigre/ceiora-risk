"""Descriptor normalization and style factor assembly."""

from __future__ import annotations

import numpy as np
import pandas as pd

from backend.risk_model.math_utils import standardize_cap_weighted

STYLE_SCORE_ABS_CAP = 5.0

MVP_STYLE_FACTORS: dict[str, dict[str, float]] = {
    "Beta": {"beta_raw": 1.0},
    "Momentum": {"momentum_raw": 1.0},
    "Size": {"size_raw": 1.0},
    "Residual Volatility": {"resid_vol_raw": 1.0},
    "Book-to-Price": {"book_to_price_raw": 1.0},
    "Earnings Yield": {
        "forward_ep_raw": 0.68,
        "cash_earnings_yield_raw": 0.21,
        "trailing_ep_raw": 0.11,
    },
    "Short-Term Reversal": {"st_reversal_raw": 1.0},
}

FULL_STYLE_FACTORS: dict[str, dict[str, float]] = {
    "Beta": {"beta_raw": 1.0},
    "Momentum": {"momentum_raw": 1.0},
    "Size": {"size_raw": 1.0},
    "Nonlinear Size": {"nonlinear_size_raw": 1.0},
    "Short-Term Reversal": {"st_reversal_raw": 1.0},
    "Residual Volatility": {"resid_vol_raw": 1.0},
    "Liquidity": {
        "turnover_1m_raw": 0.35,
        "turnover_12m_raw": 0.35,
        "log_avg_dollar_volume_20d_raw": 0.30,
    },
    "Book-to-Price": {"book_to_price_raw": 1.0},
    "Earnings Yield": {
        "forward_ep_raw": 0.68,
        "cash_earnings_yield_raw": 0.21,
        "trailing_ep_raw": 0.11,
    },
    "Value": {
        "book_to_price_raw": 0.4,
        "forward_ep_raw": 0.3,
        "cash_earnings_yield_raw": 0.2,
        "trailing_ep_raw": 0.1,
    },
    "Leverage": {
        "debt_to_equity_raw": 0.38,
        "debt_to_assets_raw": 0.35,
        "book_leverage_raw": 0.27,
    },
    "Growth": {
        "sales_growth_raw": 0.5,
        "eps_growth_raw": 0.5,
    },
    "Profitability": {
        "roe_raw": 0.5,
        "gross_profitability_raw": 0.5,
    },
    "Investment": {"asset_growth_raw": 1.0},
    "Dividend Yield": {"dividend_yield_raw": 1.0},
}

_ORTH_NONE = "none"
_ORTH_INDUSTRY = "industry"
_ORTH_SIZE = "size"
_ORTH_INDUSTRY_SIZE = "industry_size"

MVP_STYLE_ORTH_RULES: dict[str, str] = {
    "Size": _ORTH_NONE,
    "Book-to-Price": _ORTH_INDUSTRY,
    "Earnings Yield": _ORTH_INDUSTRY,
    "Beta": _ORTH_INDUSTRY,
    "Momentum": _ORTH_INDUSTRY_SIZE,
    "Residual Volatility": _ORTH_INDUSTRY_SIZE,
    "Short-Term Reversal": _ORTH_INDUSTRY_SIZE,
}

FULL_STYLE_ORTH_RULES: dict[str, str] = {
    "Size": _ORTH_NONE,
    "Nonlinear Size": _ORTH_SIZE,
    "Book-to-Price": _ORTH_INDUSTRY,
    "Earnings Yield": _ORTH_INDUSTRY,
    "Value": _ORTH_INDUSTRY,
    "Leverage": _ORTH_INDUSTRY,
    "Growth": _ORTH_INDUSTRY,
    "Profitability": _ORTH_INDUSTRY,
    "Investment": _ORTH_INDUSTRY,
    "Dividend Yield": _ORTH_INDUSTRY,
    "Beta": _ORTH_INDUSTRY,
    "Momentum": _ORTH_INDUSTRY_SIZE,
    "Short-Term Reversal": _ORTH_INDUSTRY_SIZE,
    "Residual Volatility": _ORTH_INDUSTRY_SIZE,
    "Liquidity": _ORTH_SIZE,
}


def _z(
    descriptor: pd.Series,
    market_caps: pd.Series,
    *,
    abs_cap: float = STYLE_SCORE_ABS_CAP,
) -> pd.Series:
    vals = descriptor.to_numpy(dtype=float)
    caps = market_caps.to_numpy(dtype=float)
    z = standardize_cap_weighted(vals, caps)
    try:
        cap = float(abs_cap)
    except (TypeError, ValueError):
        cap = 0.0
    if np.isfinite(cap) and cap > 0:
        z = np.clip(z, -cap, cap)
    return pd.Series(z, index=descriptor.index, dtype=float)


def _weighted_residualize(
    target: pd.Series,
    controls: pd.DataFrame,
    market_caps: pd.Series,
) -> pd.Series:
    if controls is None or controls.empty:
        return target.astype(float)
    aligned = pd.concat(
        [
            target.rename("target").astype(float),
            controls.astype(float),
            market_caps.rename("market_cap").astype(float),
        ],
        axis=1,
        join="inner",
    ).dropna(subset=["target", "market_cap"])
    if aligned.empty:
        return target.astype(float)
    y = aligned["target"].to_numpy(dtype=float)
    x = aligned[controls.columns].to_numpy(dtype=float)
    caps = aligned["market_cap"].to_numpy(dtype=float)
    w = np.sqrt(np.clip(caps, 0.0, None))
    if x.size == 0 or np.allclose(x, 0.0):
        return target.astype(float)
    try:
        beta, *_ = np.linalg.lstsq(x * w[:, None], y * w, rcond=None)
        resid = y - x @ beta
    except np.linalg.LinAlgError:
        resid = y
    out = pd.Series(resid, index=aligned.index, dtype=float)
    return out.reindex(target.index).combine_first(target.astype(float))


def _industry_controls(
    *,
    index: pd.Index,
    industry_exposures: pd.DataFrame | None,
) -> pd.DataFrame:
    if industry_exposures is None or industry_exposures.empty:
        return pd.DataFrame(index=index)
    out = industry_exposures.reindex(index).fillna(0.0).astype(float)
    keep = [c for c in out.columns if float(np.abs(out[c]).sum()) > 0.0]
    if not keep:
        return pd.DataFrame(index=index)
    return out[keep]


def _orthogonalize_scores(
    *,
    raw_scores: pd.DataFrame,
    market_caps: pd.Series,
    orth_rules: dict[str, str] | None = None,
    industry_exposures: pd.DataFrame | None = None,
) -> pd.DataFrame:
    rules = orth_rules or {}
    out = raw_scores.copy()
    industry_ctrl = _industry_controls(
        index=out.index,
        industry_exposures=industry_exposures,
    )
    for factor in out.columns:
        rule = str(rules.get(factor, _ORTH_NONE)).strip().lower()
        control_parts: list[pd.DataFrame] = []
        if rule in {_ORTH_INDUSTRY, _ORTH_INDUSTRY_SIZE} and not industry_ctrl.empty:
            control_parts.append(industry_ctrl)
        if rule in {_ORTH_SIZE, _ORTH_INDUSTRY_SIZE} and "Size" in out.columns and factor != "Size":
            control_parts.append(out[["Size"]].astype(float))
        if not control_parts:
            out[factor] = _z(out[factor], market_caps)
            continue
        controls = pd.concat(control_parts, axis=1)
        controls = controls.loc[:, ~controls.columns.duplicated()].copy()
        resid = _weighted_residualize(out[factor], controls, market_caps)
        out[factor] = _z(resid, market_caps)
    return out


def canonicalize_style_scores(
    *,
    style_scores: pd.DataFrame,
    market_caps: pd.Series,
    orth_rules: dict[str, str] | None = None,
    industry_exposures: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Clean and canonicalize already-constructed style scores cross-sectionally.

    This is useful when style scores are loaded from storage (for example
    `barra_raw_cross_section_history`) and we want a consistent preprocessing/orthogonalization
    pass before factor-return estimation.
    """
    if style_scores is None or style_scores.empty:
        return pd.DataFrame(index=style_scores.index if style_scores is not None else None)

    scores = style_scores.copy()
    for col in scores.columns:
        scores[col] = pd.to_numeric(scores[col], errors="coerce")
    scores = scores.replace([np.inf, -np.inf], np.nan)
    scores = scores.fillna(scores.median(numeric_only=True)).fillna(0.0)

    caps = pd.to_numeric(market_caps, errors="coerce").reindex(scores.index)
    cap_vals = caps.to_numpy(dtype=float)
    finite_pos = cap_vals[np.isfinite(cap_vals) & (cap_vals > 0)]
    cap_fallback = float(np.nanmedian(finite_pos)) if finite_pos.size > 0 else 1.0
    if not np.isfinite(cap_fallback) or cap_fallback <= 0:
        cap_fallback = 1.0
    caps = caps.where(np.isfinite(caps) & (caps > 0), cap_fallback).astype(float)

    return _orthogonalize_scores(
        raw_scores=scores.astype(float),
        market_caps=caps,
        orth_rules=orth_rules,
        industry_exposures=industry_exposures,
    )


def assemble_style_scores(
    raw_descriptors: pd.DataFrame,
    *,
    factor_schema: dict[str, dict[str, float]],
    market_cap_col: str = "market_cap",
    orth_rules: dict[str, str] | None = None,
    industry_exposures: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build cap-weighted standardized style factor scores for a schema."""
    if market_cap_col not in raw_descriptors.columns:
        raise KeyError(f"Missing required market cap column: {market_cap_col}")
    caps = raw_descriptors[market_cap_col].astype(float)
    base = pd.DataFrame(index=raw_descriptors.index)

    for factor_name, weights in factor_schema.items():
        parts: list[pd.Series] = []
        for descriptor_col, weight in weights.items():
            if descriptor_col not in raw_descriptors.columns:
                raise KeyError(f"Missing descriptor column for {factor_name}: {descriptor_col}")
            z = _z(raw_descriptors[descriptor_col].astype(float), caps)
            parts.append(float(weight) * z)
        composite = sum(parts) if parts else pd.Series(0.0, index=raw_descriptors.index)
        base[factor_name] = _z(composite, caps)

    return _orthogonalize_scores(
        raw_scores=base,
        market_caps=caps,
        orth_rules=orth_rules,
        industry_exposures=industry_exposures,
    )


def assemble_mvp_style_scores(
    raw_descriptors: pd.DataFrame,
    *,
    market_cap_col: str = "market_cap",
    industry_exposures: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build cap-weighted standardized MVP style factor scores."""
    return assemble_style_scores(
        raw_descriptors,
        factor_schema=MVP_STYLE_FACTORS,
        market_cap_col=market_cap_col,
        orth_rules=MVP_STYLE_ORTH_RULES,
        industry_exposures=industry_exposures,
    )


def assemble_full_style_scores(
    raw_descriptors: pd.DataFrame,
    *,
    market_cap_col: str = "market_cap",
    industry_exposures: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build cap-weighted standardized full style factor scores."""
    return assemble_style_scores(
        raw_descriptors,
        factor_schema=FULL_STYLE_FACTORS,
        market_cap_col=market_cap_col,
        orth_rules=FULL_STYLE_ORTH_RULES,
        industry_exposures=industry_exposures,
    )


def build_nonlinear_size(size_scores: pd.Series, market_caps: pd.Series) -> pd.Series:
    """Compute cube(size_z) orthogonalized to size_z, then re-standardized."""
    x = size_scores.to_numpy(dtype=float)
    y = x**3
    denom = float(np.dot(x, x))
    if denom > 0:
        beta = float(np.dot(x, y) / denom)
        resid = y - beta * x
    else:
        resid = np.zeros_like(y)
    return pd.Series(
        standardize_cap_weighted(resid, market_caps.to_numpy(dtype=float)),
        index=size_scores.index,
        dtype=float,
    )
