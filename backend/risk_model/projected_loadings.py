"""Returns-based projection of factor loadings for projection-only instruments.

Projection-only instruments (e.g., ETFs) get factor exposures via time-series OLS
regression of their returns on the durable cUSE4 core factor return series.
They never affect the native core model (factor returns, covariance, specific risk).
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ProjectedLoadingResult:
    ric: str
    ticker: str
    exposures: dict[str, float] = field(default_factory=dict)
    specific_var: float = 0.0
    specific_vol: float = 0.0
    r_squared: float = 0.0
    obs_count: int = 0
    lookback_days: int = 252
    projection_asof: str = ""
    status: str = "ok"


_LOADINGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS projected_instrument_loadings (
    ric TEXT NOT NULL,
    ticker TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    factor_name TEXT NOT NULL,
    exposure REAL NOT NULL,
    PRIMARY KEY (ric, as_of_date, factor_name)
);
"""

_META_SCHEMA = """
CREATE TABLE IF NOT EXISTS projected_instrument_meta (
    ric TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    projection_method TEXT NOT NULL DEFAULT 'ols_returns_regression',
    lookback_days INTEGER NOT NULL,
    obs_count INTEGER NOT NULL,
    r_squared REAL NOT NULL,
    projected_specific_var REAL,
    projected_specific_vol REAL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (ric, as_of_date)
);
"""


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(_LOADINGS_SCHEMA)
    conn.execute(_META_SCHEMA)
    conn.commit()


def _load_factor_returns_wide(
    data_db: Path,
    *,
    core_state_through_date: str,
) -> pd.DataFrame:
    """Load durable core-package factor returns and pivot to wide format (date × factor)."""
    conn = sqlite3.connect(str(data_db))
    try:
        df = pd.read_sql(
            """
            SELECT date, factor_name, factor_return
            FROM model_factor_returns_daily
            WHERE date IS NOT NULL
              AND date <= ?
            ORDER BY date, factor_name
            """,
            conn,
            params=(str(core_state_through_date),),
        )
    finally:
        conn.close()
    if df.empty:
        return pd.DataFrame()
    df["date"] = df["date"].astype(str).str.strip()
    df["factor_return"] = pd.to_numeric(df["factor_return"], errors="coerce")
    wide = df.pivot(index="date", columns="factor_name", values="factor_return")
    wide = wide.sort_index()
    # Drop factors that have no data at all (would waste observations in the NaN filter).
    wide = wide.dropna(axis=1, how="all")
    return wide


def _load_instrument_returns(data_db: Path, ric: str) -> pd.Series:
    """Load daily close prices and compute arithmetic returns for a single instrument."""
    conn = sqlite3.connect(str(data_db))
    try:
        df = pd.read_sql(
            "SELECT date, close FROM security_prices_eod WHERE ric = ? ORDER BY date",
            conn,
            params=(ric,),
        )
    finally:
        conn.close()
    if df.empty or len(df) < 2:
        return pd.Series(dtype=float)
    df["date"] = df["date"].astype(str).str.strip()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"])
    df = df.set_index("date").sort_index()
    returns = df["close"].pct_change().dropna()
    return returns


def _run_ols(
    instrument_returns: pd.Series,
    factor_returns_wide: pd.DataFrame,
    *,
    core_state_through_date: str,
    lookback_days: int,
    min_obs: int,
) -> ProjectedLoadingResult | None:
    """Run OLS regression of instrument returns on factor returns.

    Returns None if insufficient data.
    """
    # Inner join on date
    core_state_through_date = str(core_state_through_date or "").strip()
    common_dates = instrument_returns.index.intersection(factor_returns_wide.index)
    if len(common_dates) == 0:
        return None

    # Trim to trailing lookback_days
    common_dates_sorted = sorted(str(dt) for dt in common_dates if str(dt) <= core_state_through_date)
    if not common_dates_sorted:
        return None
    if core_state_through_date not in common_dates_sorted:
        return None
    if len(common_dates_sorted) > lookback_days:
        common_dates_sorted = common_dates_sorted[-lookback_days:]

    y = instrument_returns.loc[common_dates_sorted].to_numpy(dtype=float)
    X = factor_returns_wide.loc[common_dates_sorted].to_numpy(dtype=float)

    # Drop rows with NaN in either y or X
    valid_mask = np.isfinite(y) & np.isfinite(X).all(axis=1)
    y = y[valid_mask]
    X = X[valid_mask]
    obs_count = len(y)

    if obs_count < min_obs:
        return None

    # OLS via numpy.linalg.lstsq
    beta, residuals_sum, rank, singular_values = np.linalg.lstsq(X, y, rcond=None)

    # Compute residuals
    y_hat = X @ beta
    epsilon = y - y_hat

    # R-squared
    ss_res = float(np.sum(epsilon**2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

    # Annualized specific variance
    specific_var = float(np.var(epsilon, ddof=1) * 252) if obs_count > 1 else 0.0
    specific_vol = float(np.sqrt(max(0.0, specific_var)))

    factor_names = list(factor_returns_wide.columns)
    exposures = {
        str(factor_names[i]): round(float(beta[i]), 8)
        for i in range(len(factor_names))
    }

    result = ProjectedLoadingResult(
        ric="",
        ticker="",
        exposures=exposures,
        specific_var=round(specific_var, 8),
        specific_vol=round(specific_vol, 6),
        r_squared=round(r_squared, 6),
        obs_count=obs_count,
        lookback_days=lookback_days,
        projection_asof=core_state_through_date,
        status="ok",
    )
    return result


def compute_projected_loadings(
    *,
    data_db: Path,
    projection_rics: list[dict[str, str]],
    core_state_through_date: str,
    lookback_days: int = 252,
    min_obs: int = 60,
) -> dict[str, ProjectedLoadingResult]:
    """Compute projected factor loadings via time-series OLS on cUSE4 factor returns.

    Args:
        data_db: Path to data.db (has security_prices_eod and model_factor_returns_daily)
        projection_rics: List of {"ric": ..., "ticker": ...} for projection-only instruments
        core_state_through_date: Active durable core-package date used for factor returns
        lookback_days: Trailing observation window (default 252 = 1 year)
        min_obs: Minimum observations required (default 60)

    Returns:
        Dict keyed by ticker → ProjectedLoadingResult
    """
    if not projection_rics:
        return {}

    core_state_through_date = str(core_state_through_date or "").strip()
    if not core_state_through_date:
        logger.warning("No active core_state_through_date available for projection; skipping.")
        return {}

    factor_returns_wide = _load_factor_returns_wide(
        data_db,
        core_state_through_date=core_state_through_date,
    )
    if factor_returns_wide.empty:
        logger.warning(
            "No durable model_factor_returns_daily rows available through %s for projection; skipping.",
            core_state_through_date,
        )
        return {}

    results: dict[str, ProjectedLoadingResult] = {}
    for entry in projection_rics:
        ric = str(entry.get("ric", "")).strip().upper()
        ticker = str(entry.get("ticker", "")).strip().upper()
        if not ric or not ticker:
            continue

        instrument_returns = _load_instrument_returns(data_db, ric)
        if instrument_returns.empty:
            results[ticker] = ProjectedLoadingResult(
                ric=ric,
                ticker=ticker,
                status="insufficient_data",
            )
            continue

        try:
            ols_result = _run_ols(
                instrument_returns,
                factor_returns_wide,
                core_state_through_date=core_state_through_date,
                lookback_days=lookback_days,
                min_obs=min_obs,
            )
        except Exception as exc:
            logger.warning("Projection OLS failed for %s (%s): %s", ticker, ric, exc)
            results[ticker] = ProjectedLoadingResult(
                ric=ric,
                ticker=ticker,
                status="error",
            )
            continue

        if ols_result is None:
            results[ticker] = ProjectedLoadingResult(
                ric=ric,
                ticker=ticker,
                status="insufficient_data",
            )
            continue

        ols_result.ric = ric
        ols_result.ticker = ticker
        results[ticker] = ols_result

    ok_count = sum(1 for r in results.values() if r.status == "ok")
    logger.info(
        "Projected loadings: %d/%d instruments computed successfully (lookback=%d, min_obs=%d)",
        ok_count,
        len(projection_rics),
        lookback_days,
        min_obs,
    )

    # Persist results to data.db
    _persist_projected_loadings(
        data_db,
        results,
        as_of_date=core_state_through_date,
    )

    return results


def load_persisted_projected_loadings(
    *,
    data_db: Path,
    projection_rics: list[dict[str, str]],
    as_of_date: str,
) -> dict[str, ProjectedLoadingResult]:
    """Load persisted projected loadings for one core-package date."""
    as_of_date = str(as_of_date or "").strip()
    if not projection_rics or not as_of_date:
        return {}

    requested_rics = sorted(
        {
            str(row.get("ric") or "").strip().upper()
            for row in projection_rics
            if str(row.get("ric") or "").strip()
        }
    )
    if not requested_rics:
        return {}

    conn = sqlite3.connect(str(data_db))
    try:
        _ensure_schema(conn)
        placeholders = ",".join("?" for _ in requested_rics)
        rows = conn.execute(
            f"""
            SELECT
                l.ric,
                l.ticker,
                l.factor_name,
                l.exposure,
                m.lookback_days,
                m.obs_count,
                m.r_squared,
                m.projected_specific_var,
                m.projected_specific_vol
            FROM projected_instrument_loadings l
            JOIN projected_instrument_meta m
              ON m.ric = l.ric
             AND m.as_of_date = l.as_of_date
            WHERE l.as_of_date = ?
              AND l.ric IN ({placeholders})
            ORDER BY l.ticker, l.factor_name
            """,
            (as_of_date, *requested_rics),
        ).fetchall()
    finally:
        conn.close()

    out: dict[str, ProjectedLoadingResult] = {}
    for ric, ticker, factor_name, exposure, lookback_days, obs_count, r_squared, projected_specific_var, projected_specific_vol in rows:
        ticker_txt = str(ticker or "").strip().upper()
        ric_txt = str(ric or "").strip().upper()
        factor_txt = str(factor_name or "").strip()
        if not ticker_txt or not ric_txt or not factor_txt:
            continue
        existing = out.get(ticker_txt)
        if existing is None:
            existing = ProjectedLoadingResult(
                ric=ric_txt,
                ticker=ticker_txt,
                exposures={},
                specific_var=float(projected_specific_var or 0.0),
                specific_vol=float(projected_specific_vol or 0.0),
                r_squared=float(r_squared or 0.0),
                obs_count=int(obs_count or 0),
                lookback_days=int(lookback_days or 0),
                projection_asof=as_of_date,
                status="ok",
            )
            out[ticker_txt] = existing
        existing.exposures[factor_txt] = round(float(exposure or 0.0), 8)
    return out


def latest_persisted_projection_asof(
    *,
    data_db: Path,
    projection_rics: list[dict[str, str]],
) -> str | None:
    """Return the latest persisted projection date for the requested RICs."""
    requested_rics = sorted(
        {
            str(row.get("ric") or "").strip().upper()
            for row in projection_rics
            if str(row.get("ric") or "").strip()
        }
    )
    if not requested_rics:
        return None

    conn = sqlite3.connect(str(data_db))
    try:
        _ensure_schema(conn)
        placeholders = ",".join("?" for _ in requested_rics)
        row = conn.execute(
            f"""
            SELECT MAX(as_of_date)
            FROM projected_instrument_meta
            WHERE ric IN ({placeholders})
            """,
            requested_rics,
        ).fetchone()
    finally:
        conn.close()
    latest = str((row or [None])[0] or "").strip()
    return latest or None


def _persist_projected_loadings(
    data_db: Path,
    results: dict[str, ProjectedLoadingResult],
    *,
    as_of_date: str,
) -> None:
    """Write projected loadings and metadata to data.db."""
    conn = sqlite3.connect(str(data_db))
    try:
        _ensure_schema(conn)
        now_iso = datetime.now(timezone.utc).isoformat()
        as_of_date = str(as_of_date or "").strip()

        loading_rows: list[tuple[Any, ...]] = []
        meta_rows: list[tuple[Any, ...]] = []
        touched_rics = sorted(
            {
                str(result.ric or "").strip().upper()
                for result in results.values()
                if str(result.ric or "").strip()
            }
        )

        if touched_rics and as_of_date:
            placeholders = ",".join("?" for _ in touched_rics)
            conn.execute(
                f"DELETE FROM projected_instrument_loadings WHERE as_of_date = ? AND ric IN ({placeholders})",
                (as_of_date, *touched_rics),
            )
            conn.execute(
                f"DELETE FROM projected_instrument_meta WHERE as_of_date = ? AND ric IN ({placeholders})",
                (as_of_date, *touched_rics),
            )

        for ticker, result in results.items():
            if result.status != "ok":
                continue
            as_of = str(result.projection_asof or as_of_date or "").strip()
            for factor_name, exposure in result.exposures.items():
                loading_rows.append((
                    result.ric,
                    result.ticker,
                    as_of,
                    factor_name,
                    exposure,
                ))
            meta_rows.append((
                result.ric,
                as_of,
                "ols_returns_regression",
                result.lookback_days,
                result.obs_count,
                result.r_squared,
                result.specific_var,
                result.specific_vol,
                now_iso,
            ))

        if loading_rows:
            conn.executemany(
                """
                INSERT OR REPLACE INTO projected_instrument_loadings
                    (ric, ticker, as_of_date, factor_name, exposure)
                VALUES (?, ?, ?, ?, ?)
                """,
                loading_rows,
            )
        if meta_rows:
            conn.executemany(
                """
                INSERT OR REPLACE INTO projected_instrument_meta
                    (ric, as_of_date, projection_method, lookback_days, obs_count,
                     r_squared, projected_specific_var, projected_specific_vol, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                meta_rows,
            )
        conn.commit()
    finally:
        conn.close()
