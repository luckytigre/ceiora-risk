"""Two-phase cross-sectional WLS factor return estimation."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class WlsTwoPhaseResult:
    factor_returns: dict[str, float]
    residuals: np.ndarray
    r_squared: float
    condition_number: float
    phase_a_condition_number: float
    phase_b_condition_number: float
    residual_vol: float


def _weighted_lstsq(x: np.ndarray, y: np.ndarray, weights: np.ndarray) -> np.ndarray:
    w = np.clip(np.asarray(weights, dtype=float), 0.0, None)
    xw = x * w[:, None]
    yw = y * w
    beta, *_ = np.linalg.lstsq(xw, yw, rcond=None)
    return beta


def _safe_weighted_r2(y: np.ndarray, y_hat: np.ndarray, w: np.ndarray) -> float:
    ww = np.clip(np.asarray(w, dtype=float), 0.0, None)
    denom = float(np.sum(ww))
    if denom <= 0:
        return 0.0
    ww = ww / denom
    mu = float(np.sum(ww * y))
    sst = float(np.sum(ww * (y - mu) ** 2))
    if sst <= 0:
        return 0.0
    sse = float(np.sum(ww * (y - y_hat) ** 2))
    return max(0.0, min(1.0, 1.0 - sse / sst))


def _safe_condition_number(x: np.ndarray, w: np.ndarray) -> float:
    if x.size == 0:
        return 0.0
    ww = np.clip(np.asarray(w, dtype=float), 0.0, None).reshape(-1)
    if ww.size != x.shape[0]:
        return 0.0
    if float(np.sum(ww)) <= 0:
        return 0.0
    try:
        cond = float(np.linalg.cond(x * ww[:, None]))
    except np.linalg.LinAlgError:
        return float("inf")
    if not np.isfinite(cond):
        return float("inf")
    return max(0.0, cond)


def estimate_factor_returns_two_phase(
    *,
    returns: np.ndarray,
    market_caps: np.ndarray,
    industry_exposures: np.ndarray | None,
    style_exposures: np.ndarray | None,
    industry_names: list[str],
    style_names: list[str],
    residualize_styles: bool = True,
) -> WlsTwoPhaseResult:
    """Estimate industry then style returns with Barra-like two-phase WLS.

    Phase A mirrors Toraniko's core setup:
    - Intercept + industry block estimated first
    - Industry block constrained so industry returns sum to 0
    - Cap-weighted WLS using sqrt(market_cap)

    Phase B estimates style returns either on phase-A residuals (default) or
    directly on raw returns (optional), then forms final residuals as:
        residual = phase_a_residual - style_fit
    """
    y = np.asarray(returns, dtype=float).reshape(-1)
    w = np.sqrt(np.clip(np.asarray(market_caps, dtype=float).reshape(-1), 0.0, None))
    n = y.shape[0]

    intercept = np.ones((n, 1), dtype=float)
    factor_returns: dict[str, float] = {}

    # Phase A: intercept + industry with sum-to-zero industry constraint.
    if industry_exposures is not None and industry_exposures.size:
        z = np.asarray(industry_exposures, dtype=float)
        m = z.shape[1]
        beta_sector = np.hstack([intercept, z])  # [intercept | industries]

        # Constraint matrix (Toraniko-style): industry returns sum to zero.
        a = np.concatenate([np.array([0.0]), -1.0 * np.ones(max(0, m - 1), dtype=float)])
        r_sector = np.vstack([np.identity(m, dtype=float), a])  # (m+1) x m
        b_sector = beta_sector @ r_sector

        phase_a_condition = _safe_condition_number(b_sector, w)
        g = _weighted_lstsq(b_sector, y, w)
        fac_ret_sector = (r_sector @ g.reshape(-1, 1)).reshape(-1)

        # The first element is the intercept. We model it for fit/residuals but
        # do not expose it as a factor return in downstream analytics caches.
        for idx, name in enumerate(industry_names, start=1):
            if idx < fac_ret_sector.shape[0]:
                factor_returns[name] = float(fac_ret_sector[idx])
        y_a_hat = b_sector @ g
    else:
        x_a = intercept
        phase_a_condition = _safe_condition_number(x_a, w)
        beta_a = _weighted_lstsq(x_a, y, w)
        y_a_hat = x_a @ beta_a
        # Intercept-only fit contributes no explicit factor return.

    residual_a = y - y_a_hat

    residual_final = residual_a
    y_hat_final = y_a_hat
    phase_b_condition = 0.0
    if style_exposures is not None and style_exposures.size:
        x_b = np.asarray(style_exposures, dtype=float)
        phase_b_condition = _safe_condition_number(x_b, w)
        style_target = residual_a if residualize_styles else y
        beta_b = _weighted_lstsq(x_b, style_target, w)
        y_b_hat = x_b @ beta_b
        residual_final = residual_a - y_b_hat
        y_hat_final = y_a_hat + y_b_hat
        for idx, name in enumerate(style_names):
            factor_returns[name] = float(beta_b[idx])

    r2 = _safe_weighted_r2(y, y_hat_final, w)
    residual_vol = float(np.std(residual_final, ddof=1)) if residual_final.size > 1 else 0.0
    if not np.isfinite(residual_vol):
        residual_vol = 0.0
    condition_number = max(phase_a_condition, phase_b_condition)
    return WlsTwoPhaseResult(
        factor_returns=factor_returns,
        residuals=residual_final,
        r_squared=r2,
        condition_number=condition_number,
        phase_a_condition_number=phase_a_condition,
        phase_b_condition_number=phase_b_condition,
        residual_vol=residual_vol,
    )
