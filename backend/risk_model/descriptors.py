"""Descriptor normalization and style factor assembly."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from backend.risk_model.math_utils import standardize_cap_weighted, weighted_mean_std

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
_ORTH_SIZE_BETA = "size_beta"
_ORTH_MOMENTUM = "momentum"

MVP_STYLE_ORTH_RULES: dict[str, str] = {
    "Size": _ORTH_NONE,
    "Book-to-Price": _ORTH_INDUSTRY,
    "Earnings Yield": _ORTH_INDUSTRY,
    "Beta": _ORTH_INDUSTRY,
    "Momentum": _ORTH_INDUSTRY_SIZE,
    "Residual Volatility": _ORTH_SIZE_BETA,
    "Short-Term Reversal": _ORTH_MOMENTUM,
}

FULL_STYLE_ORTH_RULES: dict[str, str] = {
    "Size": _ORTH_NONE,
    "Nonlinear Size": _ORTH_SIZE,
    "Book-to-Price": _ORTH_INDUSTRY,
    "Earnings Yield": _ORTH_INDUSTRY,
    "Leverage": _ORTH_INDUSTRY,
    "Growth": _ORTH_INDUSTRY,
    "Profitability": _ORTH_INDUSTRY,
    "Investment": _ORTH_INDUSTRY,
    "Dividend Yield": _ORTH_INDUSTRY,
    "Beta": _ORTH_INDUSTRY,
    "Momentum": _ORTH_INDUSTRY_SIZE,
    "Short-Term Reversal": _ORTH_MOMENTUM,
    "Residual Volatility": _ORTH_SIZE_BETA,
    "Liquidity": _ORTH_SIZE,
}


@dataclass(frozen=True)
class ZScoreTransform:
    median: float
    mad: float
    weighted_mean: float
    std: float
    n_mad: float = 3.0
    abs_cap: float = STYLE_SCORE_ABS_CAP


@dataclass(frozen=True)
class ResidualizationTransform:
    control_names: tuple[str, ...]
    coefficients: tuple[float, ...]


@dataclass(frozen=True)
class StyleFactorTransform:
    factor_name: str
    rule: str
    zscore: ZScoreTransform
    residualization: ResidualizationTransform | None = None


@dataclass(frozen=True)
class StyleCanonicalizationModel:
    transforms: dict[str, StyleFactorTransform]
    fill_values: dict[str, float]


def _prepare_style_scores(
    *,
    style_scores: pd.DataFrame,
    fill_values: dict[str, float] | None = None,
) -> tuple[pd.DataFrame, dict[str, float]]:
    scores = style_scores.copy()
    for col in scores.columns:
        scores[col] = pd.to_numeric(scores[col], errors="coerce")
    scores = scores.replace([np.inf, -np.inf], np.nan)

    resolved_fill: dict[str, float] = {}
    if fill_values is None:
        for col in scores.columns:
            median = float(scores[col].median(skipna=True))
            if not np.isfinite(median):
                median = 0.0
            resolved_fill[col] = median
    else:
        for col in scores.columns:
            raw = fill_values.get(col, 0.0)
            try:
                fill = float(raw)
            except (TypeError, ValueError):
                fill = 0.0
            resolved_fill[col] = fill if np.isfinite(fill) else 0.0

    for col, fill in resolved_fill.items():
        if col in scores.columns:
            scores[col] = scores[col].fillna(fill)
    scores = scores.fillna(0.0)
    return scores.astype(float), resolved_fill


def _sanitize_market_caps(market_caps: pd.Series) -> pd.Series:
    caps = pd.to_numeric(market_caps, errors="coerce")
    cap_vals = caps.to_numpy(dtype=float)
    finite_pos = cap_vals[np.isfinite(cap_vals) & (cap_vals > 0)]
    cap_fallback = float(np.nanmedian(finite_pos)) if finite_pos.size > 0 else 1.0
    if not np.isfinite(cap_fallback) or cap_fallback <= 0:
        cap_fallback = 1.0
    return caps.where(np.isfinite(caps) & (caps > 0), cap_fallback).astype(float)


def _apply_zscore_transform(
    descriptor: pd.Series,
    transform: ZScoreTransform,
) -> pd.Series:
    vals = pd.to_numeric(descriptor, errors="coerce").to_numpy(dtype=float)
    safe_vals = np.where(np.isfinite(vals), vals, float(transform.median))
    if np.isfinite(transform.mad) and float(transform.mad) > 0:
        lo = float(transform.median) - (float(transform.n_mad) * float(transform.mad))
        hi = float(transform.median) + (float(transform.n_mad) * float(transform.mad))
        safe_vals = np.clip(safe_vals, lo, hi)
    sigma = float(transform.std)
    if not np.isfinite(sigma) or sigma <= 0:
        z = np.zeros_like(safe_vals, dtype=float)
    else:
        z = (safe_vals - float(transform.weighted_mean)) / sigma
    cap = float(transform.abs_cap)
    if np.isfinite(cap) and cap > 0:
        z = np.clip(z, -cap, cap)
    return pd.Series(z, index=descriptor.index, dtype=float)


def _fit_zscore_transform(
    descriptor: pd.Series,
    market_caps: pd.Series,
    *,
    abs_cap: float = STYLE_SCORE_ABS_CAP,
    n_mad: float = 3.0,
) -> tuple[pd.Series, ZScoreTransform]:
    vals = pd.to_numeric(descriptor, errors="coerce").to_numpy(dtype=float)
    if vals.size == 0:
        transform = ZScoreTransform(
            median=0.0,
            mad=0.0,
            weighted_mean=0.0,
            std=0.0,
            n_mad=float(n_mad),
            abs_cap=float(abs_cap),
        )
        return pd.Series(dtype=float, index=descriptor.index), transform
    median = float(np.nanmedian(vals))
    if not np.isfinite(median):
        median = 0.0
    mad = float(np.nanmedian(np.abs(vals - median)))
    safe_vals = np.where(np.isfinite(vals), vals, median)
    if np.isfinite(mad) and mad > 0:
        lo = median - (float(n_mad) * mad)
        hi = median + (float(n_mad) * mad)
        clipped = np.clip(safe_vals, lo, hi)
    else:
        clipped = safe_vals
    caps = _sanitize_market_caps(market_caps).to_numpy(dtype=float)
    mean, _ = weighted_mean_std(clipped, caps)
    finite = np.isfinite(clipped)
    if int(finite.sum()) < 2:
        sigma = 0.0
        z = np.zeros_like(clipped, dtype=float)
    else:
        sigma = float(np.nanstd(clipped[finite], ddof=0))
        if not np.isfinite(sigma) or sigma <= 0:
            sigma = 0.0
            z = np.zeros_like(clipped, dtype=float)
        else:
            z = (clipped - mean) / sigma
    cap = float(abs_cap)
    if np.isfinite(cap) and cap > 0:
        z = np.clip(z, -cap, cap)
    transform = ZScoreTransform(
        median=median,
        mad=mad if np.isfinite(mad) else 0.0,
        weighted_mean=mean if np.isfinite(mean) else 0.0,
        std=sigma,
        n_mad=float(n_mad),
        abs_cap=float(abs_cap),
    )
    return pd.Series(z, index=descriptor.index, dtype=float), transform


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


def _fit_weighted_residualization(
    target: pd.Series,
    controls: pd.DataFrame,
    market_caps: pd.Series,
) -> tuple[pd.Series, ResidualizationTransform | None]:
    if controls is None or controls.empty:
        return target.astype(float), None
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
        return target.astype(float), None
    y = aligned["target"].to_numpy(dtype=float)
    x = aligned[controls.columns].to_numpy(dtype=float)
    caps = aligned["market_cap"].to_numpy(dtype=float)
    w = np.sqrt(np.clip(caps, 0.0, None))
    if x.size == 0 or np.allclose(x, 0.0):
        return target.astype(float), None
    try:
        beta, *_ = np.linalg.lstsq(x * w[:, None], y * w, rcond=None)
        resid = y - x @ beta
    except np.linalg.LinAlgError:
        return target.astype(float), None
    out = pd.Series(resid, index=aligned.index, dtype=float)
    transform = ResidualizationTransform(
        control_names=tuple(str(col) for col in controls.columns),
        coefficients=tuple(float(v) for v in beta.reshape(-1)),
    )
    return out.reindex(target.index).combine_first(target.astype(float)), transform


def _apply_weighted_residualization(
    target: pd.Series,
    controls: pd.DataFrame,
    transform: ResidualizationTransform | None,
) -> pd.Series:
    if transform is None or not transform.control_names:
        return pd.to_numeric(target, errors="coerce").fillna(0.0).astype(float)
    base = pd.to_numeric(target, errors="coerce").fillna(0.0).astype(float)
    ctrl = controls.reindex(index=base.index).fillna(0.0).astype(float)
    ctrl = ctrl.reindex(columns=list(transform.control_names), fill_value=0.0)
    beta = np.asarray(transform.coefficients, dtype=float).reshape(-1)
    if ctrl.empty or beta.size == 0:
        return base
    fitted = ctrl.to_numpy(dtype=float) @ beta
    return pd.Series(base.to_numpy(dtype=float) - fitted, index=base.index, dtype=float)


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


def _rule_factor_dependencies(
    *,
    factor: str,
    rule: str,
) -> tuple[str, ...]:
    deps: list[str] = []
    if rule in {_ORTH_SIZE, _ORTH_INDUSTRY_SIZE} and factor != "Size":
        deps.append("Size")
    if rule == _ORTH_SIZE_BETA:
        for control_name in ("Size", "Beta"):
            if control_name != factor:
                deps.append(control_name)
    if rule == _ORTH_MOMENTUM and factor != "Momentum":
        deps.append("Momentum")
    return tuple(deps)


def _resolve_style_processing_order(
    factors: list[str],
    *,
    orth_rules: dict[str, str] | None = None,
) -> list[str]:
    ordered_factors = list(dict.fromkeys(str(factor) for factor in factors))
    rules = orth_rules or {}
    factor_set = set(ordered_factors)
    dependencies = {
        factor: tuple(
            dep
            for dep in _rule_factor_dependencies(
                factor=factor,
                rule=str(rules.get(factor, _ORTH_NONE)).strip().lower(),
            )
            if dep in factor_set
        )
        for factor in ordered_factors
    }

    resolved: list[str] = []
    remaining = set(ordered_factors)
    while remaining:
        progressed = False
        for factor in ordered_factors:
            if factor not in remaining:
                continue
            if all(dep in resolved for dep in dependencies[factor]):
                resolved.append(factor)
                remaining.remove(factor)
                progressed = True
        if progressed:
            continue
        unresolved = {
            factor: tuple(dep for dep in dependencies[factor] if dep in remaining)
            for factor in ordered_factors
            if factor in remaining
        }
        raise ValueError(f"Unable to resolve style orthogonalization order: {unresolved}")
    return resolved


def _control_parts_for_factor(
    *,
    factor: str,
    rule: str,
    out: pd.DataFrame,
    industry_ctrl: pd.DataFrame,
) -> list[pd.DataFrame]:
    control_parts: list[pd.DataFrame] = []
    if rule in {_ORTH_INDUSTRY, _ORTH_INDUSTRY_SIZE} and not industry_ctrl.empty:
        control_parts.append(industry_ctrl)
    for control_name in _rule_factor_dependencies(factor=factor, rule=rule):
        if control_name in out.columns and control_name != factor:
            control_parts.append(out[[control_name]].astype(float))
    return control_parts


def fit_and_apply_style_canonicalization(
    *,
    style_scores: pd.DataFrame,
    market_caps: pd.Series,
    orth_rules: dict[str, str] | None = None,
    industry_exposures: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, StyleCanonicalizationModel]:
    rules = orth_rules or {}
    scores, fill_values = _prepare_style_scores(style_scores=style_scores)
    caps = _sanitize_market_caps(market_caps).reindex(scores.index)
    out = pd.DataFrame(index=scores.index)
    transforms: dict[str, StyleFactorTransform] = {}
    industry_ctrl = _industry_controls(
        index=out.index,
        industry_exposures=industry_exposures,
    )
    processing_order = _resolve_style_processing_order(
        list(scores.columns),
        orth_rules=rules,
    )
    for factor in processing_order:
        rule = str(rules.get(factor, _ORTH_NONE)).strip().lower()
        control_parts = _control_parts_for_factor(
            factor=factor,
            rule=rule,
            out=out,
            industry_ctrl=industry_ctrl,
        )
        if not control_parts:
            standardized, zscore_transform = _fit_zscore_transform(scores[factor], caps)
            out[factor] = standardized
            transforms[factor] = StyleFactorTransform(
                factor_name=factor,
                rule=rule,
                zscore=zscore_transform,
            )
            continue
        controls = pd.concat(control_parts, axis=1)
        controls = controls.loc[:, ~controls.columns.duplicated()].copy()
        resid, residualization = _fit_weighted_residualization(scores[factor], controls, caps)
        standardized, zscore_transform = _fit_zscore_transform(resid, caps)
        out[factor] = standardized
        transforms[factor] = StyleFactorTransform(
            factor_name=factor,
            rule=rule,
            zscore=zscore_transform,
            residualization=residualization,
        )
    return out.reindex(columns=list(scores.columns), fill_value=0.0), StyleCanonicalizationModel(
        transforms=transforms,
        fill_values=fill_values,
    )


def apply_style_canonicalization(
    *,
    style_scores: pd.DataFrame,
    model: StyleCanonicalizationModel,
    industry_exposures: pd.DataFrame | None = None,
) -> pd.DataFrame:
    scores, _ = _prepare_style_scores(
        style_scores=style_scores,
        fill_values=model.fill_values,
    )
    out = pd.DataFrame(index=scores.index)
    industry_ctrl = _industry_controls(
        index=scores.index,
        industry_exposures=industry_exposures,
    )
    model_rules = {
        factor: str(transform.rule).strip().lower()
        for factor, transform in model.transforms.items()
    }
    processing_order = _resolve_style_processing_order(
        list(scores.columns),
        orth_rules=model_rules,
    )
    for factor in processing_order:
        transform = model.transforms.get(factor)
        if transform is None:
            out[factor] = 0.0
            continue
        rule = str(transform.rule).strip().lower()
        control_parts = _control_parts_for_factor(
            factor=factor,
            rule=rule,
            out=out,
            industry_ctrl=industry_ctrl,
        )

        target = scores[factor]
        if control_parts:
            controls = pd.concat(control_parts, axis=1)
            controls = controls.loc[:, ~controls.columns.duplicated()].copy()
            target = _apply_weighted_residualization(
                target,
                controls,
                transform.residualization,
            )
        out[factor] = _apply_zscore_transform(target, transform.zscore)
    return out.reindex(columns=list(scores.columns), fill_value=0.0)


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
    canonical, _ = fit_and_apply_style_canonicalization(
        style_scores=style_scores,
        market_caps=market_caps,
        orth_rules=orth_rules,
        industry_exposures=industry_exposures,
    )
    return canonical


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
            z, _ = _fit_zscore_transform(raw_descriptors[descriptor_col].astype(float), caps)
            parts.append(float(weight) * z)
        composite = sum(parts) if parts else pd.Series(0.0, index=raw_descriptors.index)
        base[factor_name], _ = _fit_zscore_transform(composite, caps)

    canonical, _ = fit_and_apply_style_canonicalization(
        style_scores=base,
        market_caps=caps,
        orth_rules=orth_rules,
        industry_exposures=industry_exposures,
    )
    return canonical


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
