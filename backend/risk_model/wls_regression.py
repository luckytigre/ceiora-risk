"""Cross-sectional WLS factor return estimation."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class WlsOneStageResult:
    factor_returns: dict[str, float]
    robust_se: dict[str, float]
    t_stats: dict[str, float]
    residuals: np.ndarray
    raw_residuals: np.ndarray
    r_squared: float
    condition_number: float
    residual_vol: float
    constraint_residual: float


def fitted_returns_one_stage(
    result: WlsOneStageResult,
    *,
    n_obs: int | None = None,
    market_exposures: np.ndarray | None,
    industry_exposures: np.ndarray | None,
    style_exposures: np.ndarray | None,
    market_name: str,
    industry_names: list[str],
    style_names: list[str],
) -> np.ndarray:
    """Project a fitted one-stage model onto an arbitrary exposure matrix."""
    base_n = 0
    for matrix in (market_exposures, industry_exposures, style_exposures):
        if matrix is not None and np.asarray(matrix).size:
            base_n = int(np.asarray(matrix).shape[0])
            break
    if base_n <= 0 and n_obs is not None:
        base_n = max(0, int(n_obs))
    if base_n <= 0:
        return np.zeros(0, dtype=float)

    out = np.zeros(base_n, dtype=float)
    if market_exposures is not None and np.asarray(market_exposures).size:
        x = np.asarray(market_exposures, dtype=float)
        if x.ndim == 1:
            x = x.reshape(-1, 1)
        beta = float(result.factor_returns.get(market_name, 0.0))
        out += x.reshape(base_n, -1)[:, 0] * beta
    if industry_exposures is not None and np.asarray(industry_exposures).size:
        x = np.asarray(industry_exposures, dtype=float)
        beta = np.array([float(result.factor_returns.get(name, 0.0)) for name in industry_names[: x.shape[1]]], dtype=float)
        if beta.size:
            out += x @ beta
    if style_exposures is not None and np.asarray(style_exposures).size:
        x = np.asarray(style_exposures, dtype=float)
        beta = np.array([float(result.factor_returns.get(name, 0.0)) for name in style_names[: x.shape[1]]], dtype=float)
        if beta.size:
            out += x @ beta
    return out


def _weighted_lstsq(x: np.ndarray, y: np.ndarray, omega_sqrt: np.ndarray) -> np.ndarray:
    w = np.clip(np.asarray(omega_sqrt, dtype=float), 0.0, None)
    xw = x * w[:, None]
    yw = y * w
    beta, *_ = np.linalg.lstsq(xw, yw, rcond=None)
    return beta


def _safe_weighted_r2(y: np.ndarray, y_hat: np.ndarray, omega: np.ndarray) -> float:
    ww = np.clip(np.asarray(omega, dtype=float), 0.0, None)
    denom = float(np.sum(ww))
    if denom <= 0:
        return 0.0
    ww = ww / denom
    mu = float(np.sum(ww * y))
    sst = float(np.sum(ww * (y - mu) ** 2))
    if sst <= 0:
        return 0.0
    sse = float(np.sum(ww * (y - y_hat) ** 2))
    return 1.0 - (sse / sst)


def _safe_condition_number(x: np.ndarray, omega_sqrt: np.ndarray) -> float:
    if x.size == 0:
        return 0.0
    ww = np.clip(np.asarray(omega_sqrt, dtype=float), 0.0, None).reshape(-1)
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


def _industry_constraint_basis(m: int) -> np.ndarray:
    if m <= 1:
        return np.zeros((m, 0), dtype=float)
    return np.vstack(
        [
            np.eye(m - 1, dtype=float),
            -1.0 * np.ones((1, m - 1), dtype=float),
        ]
    )


def _hc_scale(n: int, p: int) -> float:
    dof = max(1, n - p)
    return float(n / dof)


def _linear_map_hc_cov(
    linear_map: np.ndarray,
    residuals: np.ndarray,
    *,
    scale: float,
) -> np.ndarray:
    if linear_map.size == 0:
        return np.zeros((0, 0), dtype=float)
    u2 = np.square(np.asarray(residuals, dtype=float)).reshape(-1)
    weighted = linear_map * u2[None, :]
    cov = weighted @ linear_map.T
    cov *= float(scale)
    cov = 0.5 * (cov + cov.T)
    return cov


def _safe_diag_sqrt(matrix: np.ndarray) -> np.ndarray:
    if matrix.size == 0:
        return np.zeros(0, dtype=float)
    diag = np.diag(matrix).astype(float, copy=False)
    diag = np.clip(diag, 0.0, None)
    diag = np.where(np.isfinite(diag), diag, 0.0)
    return np.sqrt(diag)


def _pinv(matrix: np.ndarray) -> np.ndarray:
    try:
        return np.linalg.pinv(matrix)
    except np.linalg.LinAlgError:
        return np.zeros_like(matrix, dtype=float)


def _solve_constrained_wls(
    *,
    design: np.ndarray,
    returns: np.ndarray,
    omega: np.ndarray,
    constraints: np.ndarray | None,
) -> tuple[np.ndarray, np.ndarray]:
    a = design.T @ (omega[:, None] * design)
    rhs = design.T @ (omega * returns)
    if constraints is None or constraints.size == 0:
        a_inv = _pinv(a)
        beta = a_inv @ rhs
        linear_map = a_inv @ (design.T * omega[None, :])
        return beta, linear_map

    c = np.asarray(constraints, dtype=float)
    if c.ndim == 1:
        c = c.reshape(1, -1)
    zeros = np.zeros((c.shape[0], c.shape[0]), dtype=float)
    kkt = np.block([
        [a, c.T],
        [c, zeros],
    ])
    rhs_aug = np.concatenate([rhs, np.zeros(c.shape[0], dtype=float)])
    kkt_inv = _pinv(kkt)
    sol = kkt_inv @ rhs_aug
    beta = sol[: design.shape[1]]
    linear_map = kkt_inv[: design.shape[1], : design.shape[1]] @ (design.T * omega[None, :])
    return beta, linear_map


def estimate_factor_returns_one_stage(
    *,
    returns: np.ndarray,
    raw_returns: np.ndarray | None,
    market_caps: np.ndarray,
    market_exposures: np.ndarray | None,
    industry_exposures: np.ndarray | None,
    style_exposures: np.ndarray | None,
    market_name: str,
    industry_names: list[str],
    style_names: list[str],
) -> WlsOneStageResult:
    """Estimate market, industry, and style returns jointly via constrained WLS."""
    y = np.asarray(returns, dtype=float).reshape(-1)
    raw_y = np.asarray(raw_returns, dtype=float).reshape(-1) if raw_returns is not None else y
    omega = np.clip(np.asarray(market_caps, dtype=float).reshape(-1), 0.0, None)
    w = np.sqrt(omega)
    n = y.shape[0]

    market_x = (
        np.asarray(market_exposures, dtype=float)
        if market_exposures is not None and np.asarray(market_exposures).size
        else np.ones((n, 1), dtype=float)
    )
    if market_x.ndim == 1:
        market_x = market_x.reshape(-1, 1)
    industry_x = (
        np.asarray(industry_exposures, dtype=float)
        if industry_exposures is not None and np.asarray(industry_exposures).size
        else np.zeros((n, 0), dtype=float)
    )
    style_x = (
        np.asarray(style_exposures, dtype=float)
        if style_exposures is not None and np.asarray(style_exposures).size
        else np.zeros((n, 0), dtype=float)
    )

    design = np.hstack([market_x, industry_x, style_x])
    condition_number = _safe_condition_number(design, w)

    constraints = None
    if industry_x.shape[1]:
        total_cap = float(np.sum(omega))
        industry_caps = industry_x.T @ omega
        if total_cap > 0:
            industry_weights = industry_caps / total_cap
        else:
            industry_weights = np.zeros(industry_x.shape[1], dtype=float)
        constraints = np.zeros((1, design.shape[1]), dtype=float)
        constraints[0, 1 : 1 + industry_x.shape[1]] = industry_weights

    beta, linear_map = _solve_constrained_wls(
        design=design,
        returns=y,
        omega=omega,
        constraints=constraints,
    )
    y_hat = design @ beta
    residuals = y - y_hat
    raw_residuals = raw_y - y_hat

    names = [str(market_name)] + list(industry_names[: industry_x.shape[1]]) + list(style_names[: style_x.shape[1]])
    factor_returns = {
        names[idx]: float(beta[idx])
        for idx in range(min(len(names), beta.shape[0]))
    }

    scale = _hc_scale(n=n, p=design.shape[1])
    cov = _linear_map_hc_cov(linear_map, residuals, scale=scale)
    se = _safe_diag_sqrt(cov)
    robust_se: dict[str, float] = {}
    t_stats: dict[str, float] = {}
    for idx, name in enumerate(names[: se.shape[0]]):
        value = float(factor_returns.get(name, 0.0))
        sigma = float(se[idx])
        robust_se[name] = sigma
        t_stats[name] = float(value / sigma) if sigma > 0 else 0.0

    r2 = _safe_weighted_r2(y, y_hat, omega)
    residual_vol = float(np.std(residuals, ddof=1)) if residuals.size > 1 else 0.0
    if not np.isfinite(residual_vol):
        residual_vol = 0.0
    constraint_residual = 0.0
    if constraints is not None and constraints.size:
        constraint_residual = float(np.max(np.abs(constraints @ beta)))
        if not np.isfinite(constraint_residual):
            constraint_residual = 0.0

    return WlsOneStageResult(
        factor_returns=factor_returns,
        robust_se=robust_se,
        t_stats=t_stats,
        residuals=residuals,
        raw_residuals=raw_residuals,
        r_squared=r2,
        condition_number=condition_number,
        residual_vol=residual_vol,
        constraint_residual=constraint_residual,
    )
