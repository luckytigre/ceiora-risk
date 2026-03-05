"""Numerical utilities for Barra-style cross-sectional modeling."""

from __future__ import annotations

import numpy as np


def winsorize_mad(values: np.ndarray, *, n_mad: float = 3.0) -> np.ndarray:
    """Clip outliers using median +/- n_mad * MAD."""
    arr = np.asarray(values, dtype=float)
    if arr.size == 0:
        return arr.copy()
    median = np.nanmedian(arr)
    mad = np.nanmedian(np.abs(arr - median))
    if not np.isfinite(mad) or mad <= 0:
        return np.where(np.isfinite(arr), arr, median)
    lo = median - n_mad * mad
    hi = median + n_mad * mad
    clipped = np.clip(arr, lo, hi)
    return np.where(np.isfinite(clipped), clipped, median)


def weighted_mean_std(values: np.ndarray, weights: np.ndarray) -> tuple[float, float]:
    """Return weighted mean and std with defensive normalization."""
    v = np.asarray(values, dtype=float)
    w = np.asarray(weights, dtype=float)
    if v.size == 0:
        return 0.0, 0.0
    w = np.where(np.isfinite(w) & (w > 0), w, 0.0)
    denom = float(np.sum(w))
    if denom <= 0:
        mu = float(np.nanmean(v))
        sigma = float(np.nanstd(v))
        return mu if np.isfinite(mu) else 0.0, sigma if np.isfinite(sigma) else 0.0
    w = w / denom
    safe_v = np.where(np.isfinite(v), v, 0.0)
    mu = float(np.sum(w * safe_v))
    var = float(np.sum(w * (safe_v - mu) ** 2))
    return mu, float(np.sqrt(max(var, 0.0)))


def standardize_cap_weighted(
    values: np.ndarray,
    market_caps: np.ndarray,
    *,
    n_mad: float = 3.0,
) -> np.ndarray:
    """Winsorize then return z-score with cap-weighted mean and equal-weighted std."""
    clipped = winsorize_mad(values, n_mad=n_mad)
    weights = np.asarray(market_caps, dtype=float)
    mu, _ = weighted_mean_std(clipped, weights)
    finite = np.isfinite(clipped)
    if int(finite.sum()) < 2:
        return np.zeros_like(clipped, dtype=float)
    sigma = float(np.nanstd(clipped[finite], ddof=0))
    if not np.isfinite(sigma) or sigma <= 0:
        return np.zeros_like(clipped, dtype=float)
    return (clipped - mu) / sigma


def exponential_weights(length: int, *, half_life: float) -> np.ndarray:
    """Return normalized newest-to-oldest EW weights."""
    n = max(0, int(length))
    if n == 0:
        return np.zeros(0, dtype=float)
    hl = max(float(half_life), 1e-6)
    lam = np.log(2.0) / hl
    steps_from_latest = np.arange(n - 1, -1, -1, dtype=float)
    raw = np.exp(-lam * steps_from_latest)
    s = float(np.sum(raw))
    if s <= 0:
        return np.full(n, 1.0 / n, dtype=float)
    return raw / s
