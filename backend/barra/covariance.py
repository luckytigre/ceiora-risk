"""Factor covariance matrix from return time series.

Supports both the old pre-computed quarterly factor returns and the new
daily cross-sectional WLS factor returns. The interface is the same:
pass a list of dicts with date/factor_name/factor_return/r_squared.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


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

    # Annualized covariance
    cov = pivot.cov().fillna(0.0) * 252.0

    # Extract latest R-squared
    latest_date = pivot.index.max()
    latest_r2_rows = hist_df.loc[hist_df["date"] == latest_date, "r_squared"]
    latest_r2 = float(latest_r2_rows.dropna().mean()) if not latest_r2_rows.empty else 0.0
    latest_r2 = max(0.0, min(1.0, latest_r2 if np.isfinite(latest_r2) else 0.0))

    return cov, latest_r2


def build_factor_covariance_from_cache(
    cache_db: Path,
    lookback_days: int = 504,
) -> tuple[pd.DataFrame, float]:
    """Build covariance matrix directly from the daily_factor_returns cache table.

    Convenience wrapper that loads from the cache and calls build_factor_covariance.
    """
    from barra.daily_factor_returns import load_daily_factor_returns

    rows = load_daily_factor_returns(cache_db, lookback_days=lookback_days)
    return build_factor_covariance(rows, lookback_days=lookback_days)
