"""Forecast factor covariance matrix from daily factor return history."""

from __future__ import annotations

import numpy as np
import pandas as pd

from barra.math_utils import exponential_weights


ANNUALIZATION = 252.0


def _symmetrize(x: np.ndarray) -> np.ndarray:
    return 0.5 * (x + x.T)


def _ensure_psd(x: np.ndarray, *, floor: float = 1e-8) -> np.ndarray:
    sym = _symmetrize(x)
    try:
        eigvals, eigvecs = np.linalg.eigh(sym)
    except np.linalg.LinAlgError:
        return np.diag(np.clip(np.diag(sym), floor, None))
    eigvals = np.clip(eigvals, floor, None)
    out = eigvecs @ np.diag(eigvals) @ eigvecs.T
    return _symmetrize(out)


def _weighted_covariance(x: np.ndarray, weights: np.ndarray) -> np.ndarray:
    w = np.asarray(weights, dtype=float).reshape(-1)
    xx = np.asarray(x, dtype=float)
    if xx.ndim != 2 or xx.shape[0] == 0:
        return np.zeros((0, 0), dtype=float)
    s = float(np.sum(w))
    if s <= 0:
        return np.cov(xx, rowvar=False, ddof=0)
    w = w / s
    mu = np.sum(xx * w[:, None], axis=0)
    xc = xx - mu
    return (xc * w[:, None]).T @ xc


def _newey_west_covariance(x: np.ndarray, max_lag: int = 3) -> np.ndarray:
    xx = np.asarray(x, dtype=float)
    if xx.ndim != 2 or xx.shape[0] < 2:
        return np.zeros((xx.shape[1], xx.shape[1]), dtype=float) if xx.ndim == 2 else np.zeros((0, 0), dtype=float)
    t, n = xx.shape
    xc = xx - np.mean(xx, axis=0, keepdims=True)
    denom = float(t)
    cov = (xc.T @ xc) / denom
    lag_cap = max(0, min(int(max_lag), t - 1))
    for lag in range(1, lag_cap + 1):
        weight = 1.0 - lag / float(lag_cap + 1)
        gamma = (xc[lag:].T @ xc[:-lag]) / denom
        cov += weight * (gamma + gamma.T)
    return _symmetrize(cov)


def _ewma_correlation(returns_panel: np.ndarray, half_life: float) -> np.ndarray:
    t, n = returns_panel.shape
    w = exponential_weights(t, half_life=half_life)
    cov = _weighted_covariance(returns_panel, w)
    vol = np.sqrt(np.clip(np.diag(cov), 1e-12, None))
    z = returns_panel / vol[None, :]
    corr_cov = _weighted_covariance(z, w)
    diag = np.sqrt(np.clip(np.diag(corr_cov), 1e-12, None))
    corr = corr_cov / np.outer(diag, diag)
    corr = np.clip(corr, -1.0, 1.0)
    np.fill_diagonal(corr, 1.0)
    if corr.shape != (n, n):
        return np.eye(n, dtype=float)
    return _symmetrize(corr)


def build_factor_covariance(
    factor_history_rows: list[dict],
    lookback_days: int = 504,
) -> tuple[pd.DataFrame, float]:
    """Build annualized factor covariance matrix from factor return history.

    Args:
        factor_history_rows: List of dicts with date, factor_name, factor_return, r_squared.
        lookback_days: Number of most recent trading days to use.  0 = all available.

    Returns (cov_matrix, latest_r_squared).
    """
    if not factor_history_rows:
        return pd.DataFrame(), 0.0

    hist_df = pd.DataFrame(factor_history_rows)
    if hist_df.empty or "date" not in hist_df or "factor_name" not in hist_df:
        return pd.DataFrame(), 0.0

    hist_df["date"] = pd.to_datetime(hist_df["date"], errors="coerce")
    hist_df = hist_df.dropna(subset=["date"])
    if hist_df.empty:
        return pd.DataFrame(), 0.0

    pivot = hist_df.pivot_table(
        index="date",
        columns="factor_name",
        values="factor_return",
        aggfunc="last",
    ).sort_index()
    pivot = pivot.dropna(axis=1, how="all")

    # Apply lookback window
    if lookback_days > 0 and pivot.shape[0] > lookback_days:
        pivot = pivot.iloc[-lookback_days:]

    if pivot.shape[0] < 2 or pivot.shape[1] == 0:
        return pd.DataFrame(), 0.0

    # Preserve both style + industry factors in the panel. Missing entries
    # happen on sparse dates and are treated as neutral (0) for stability.
    pivot = pivot.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    factors = list(pivot.columns)
    x = pivot.to_numpy(dtype=float)
    n = x.shape[1]

    # 1) EWMA volatility and correlation forecasts (different half-lives).
    w_vol = exponential_weights(x.shape[0], half_life=84.0)
    vol_cov = _weighted_covariance(x, w_vol)
    factor_vols = np.sqrt(np.clip(np.diag(vol_cov), 1e-12, None))
    corr = _ewma_correlation(x, half_life=252.0)
    ewma_cov = np.outer(factor_vols, factor_vols) * corr

    # 2) Newey-West adjustment for autocorrelation in factor returns.
    nw_cov = _newey_west_covariance(x, max_lag=3)

    # 3) Blend forecasts, then shrink toward diagonal target.
    blended = 0.8 * ewma_cov + 0.2 * nw_cov
    diag_target = np.diag(np.clip(np.diag(blended), 1e-10, None))
    shrunk = 0.9 * blended + 0.1 * diag_target

    # 4) PSD enforcement and annualization.
    cov_daily = _ensure_psd(shrunk, floor=1e-10)
    if cov_daily.shape != (n, n):
        cov_daily = np.diag(np.clip(np.diag(cov_daily), 1e-10, None))
    cov = pd.DataFrame(cov_daily * ANNUALIZATION, index=factors, columns=factors).fillna(0.0)

    # Extract latest R-squared
    latest_date = pivot.index.max()
    latest_r2_rows = hist_df.loc[hist_df["date"] == latest_date, "r_squared"]
    latest_r2 = float(latest_r2_rows.dropna().mean()) if not latest_r2_rows.empty else 0.0
    latest_r2 = max(0.0, min(1.0, latest_r2 if np.isfinite(latest_r2) else 0.0))

    return cov, latest_r2


def build_factor_covariance_from_cache(
    cache_db,
    lookback_days: int = 504,
) -> tuple[pd.DataFrame, float]:
    """Build covariance matrix directly from the daily_factor_returns cache table.

    Convenience wrapper that loads from the cache and calls build_factor_covariance.
    """
    from barra.daily_factor_returns import load_daily_factor_returns

    rows = load_daily_factor_returns(cache_db, lookback_days=lookback_days)
    return build_factor_covariance(rows, lookback_days=lookback_days)
