"""Stock-level specific-risk forecasts from residual return history."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from barra.daily_factor_returns import load_specific_residuals
from barra.math_utils import exponential_weights

ANNUALIZATION = 252.0


def _ewma_variance(values: np.ndarray, *, half_life: float) -> float:
    x = np.asarray(values, dtype=float)
    finite = np.isfinite(x)
    x = x[finite]
    if x.size < 2:
        return 0.0
    w = exponential_weights(int(x.size), half_life=half_life)
    mu = float(np.sum(w * x))
    var = float(np.sum(w * (x - mu) ** 2))
    return max(0.0, var if np.isfinite(var) else 0.0)


def build_specific_risk_from_cache(
    cache_db: Path,
    *,
    lookback_days: int = 504,
    half_life: float = 126.0,
    min_obs: int = 40,
) -> dict[str, dict[str, float | int | str]]:
    """Build annualized specific variances from cached stock residual returns."""
    resid_df = load_specific_residuals(cache_db, lookback_days=lookback_days)
    if resid_df.empty:
        return {}

    resid_df["ticker"] = resid_df["ticker"].astype(str).str.upper()
    resid_df["residual"] = pd.to_numeric(resid_df["residual"], errors="coerce")
    resid_df["industry_group"] = resid_df["industry_group"].fillna("Unmapped").astype(str)
    resid_df = resid_df.dropna(subset=["ticker", "residual"])
    if resid_df.empty:
        return {}

    rows: list[dict[str, float | int | str]] = []
    for ticker, grp in resid_df.groupby("ticker", sort=False):
        g = grp.sort_values("date")
        values = g["residual"].to_numpy(dtype=float)
        obs = int(np.isfinite(values).sum())
        if obs < 2:
            continue
        raw_daily_var = _ewma_variance(values, half_life=half_life)
        raw_var = max(0.0, raw_daily_var * ANNUALIZATION)
        industry = str(g["industry_group"].dropna().iloc[-1]) if not g["industry_group"].dropna().empty else "Unmapped"
        rows.append({
            "ticker": str(ticker),
            "industry_group": industry,
            "obs": obs,
            "raw_specific_var": raw_var,
        })

    if not rows:
        return {}

    stats = pd.DataFrame(rows)
    positive = stats.loc[stats["raw_specific_var"] > 0, "raw_specific_var"]
    global_target = float(positive.median()) if not positive.empty else 0.0
    if not np.isfinite(global_target) or global_target <= 0:
        global_target = 1e-6

    industry_targets = (
        stats.groupby("industry_group")["raw_specific_var"]
        .median()
        .replace([np.inf, -np.inf], np.nan)
        .fillna(global_target)
        .to_dict()
    )

    out: dict[str, dict[str, float | int | str]] = {}
    for _, row in stats.iterrows():
        ticker = str(row["ticker"])
        industry = str(row["industry_group"])
        obs = int(row["obs"])
        raw_var = float(row["raw_specific_var"])

        industry_target = float(industry_targets.get(industry, global_target))
        target = 0.7 * industry_target + 0.3 * global_target

        # More observations => less shrinkage toward structural target.
        confidence = max(0.0, min(1.0, obs / max(1.0, 2.0 * float(min_obs))))
        specific_var = confidence * raw_var + (1.0 - confidence) * target
        specific_var = max(1e-8, specific_var if np.isfinite(specific_var) else target)

        out[ticker] = {
            "specific_var": float(specific_var),
            "specific_vol": float(np.sqrt(specific_var)),
            "obs": obs,
            "industry_group": industry,
        }

    return out
