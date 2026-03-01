"""Daily cross-sectional Barra factor returns via two-phase WLS.

Runs WLS regression for each trading day:
  1. Carry-forward quarterly exposures to each trading day
  2. Compute daily stock returns from prices
  3. Carry-forward market caps per ticker from fundamental snapshots
  4. Inner-join returns + exposures + market_caps
  5. Two-phase WLS: intercept+industry (phase A), style scores (phase B)
  6. Cache results incrementally in SQLite
"""

from __future__ import annotations

import logging
import sqlite3
import time
from pathlib import Path

import numpy as np
import pandas as pd

from barra.descriptors import FULL_STYLE_ORTH_RULES, canonicalize_style_scores
from barra.risk_attribution import STYLE_COLUMN_TO_LABEL
from barra.wls_regression import estimate_factor_returns_two_phase

logger = logging.getLogger(__name__)

# Style score columns in the barra_exposures table
STYLE_SCORE_COLS = list(STYLE_COLUMN_TO_LABEL.keys())
STYLE_FACTOR_NAMES = list(STYLE_COLUMN_TO_LABEL.values())
RETURNS_WINSOR_PCT = 0.05
MIN_CROSS_SECTION_SIZE = 30
MIN_ELIGIBLE_COVERAGE = 0.60
CACHE_METHOD_VERSION = "v6_industry_history_2026_03_01"

_DAILY_FR_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_factor_returns (
    date        TEXT NOT NULL,
    factor_name TEXT NOT NULL,
    factor_return REAL NOT NULL,
    r_squared   REAL NOT NULL,
    residual_vol REAL NOT NULL,
    cross_section_n INTEGER NOT NULL DEFAULT 0,
    eligible_n  INTEGER NOT NULL DEFAULT 0,
    coverage    REAL NOT NULL DEFAULT 0,
    PRIMARY KEY (date, factor_name)
);
"""

_DAILY_FR_META_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_factor_returns_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

_DAILY_RESIDUALS_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_specific_residuals (
    date TEXT NOT NULL,
    ticker TEXT NOT NULL,
    residual REAL NOT NULL,
    market_cap REAL NOT NULL DEFAULT 0.0,
    industry_group TEXT,
    PRIMARY KEY (date, ticker)
);
"""

_GICS_HISTORY_TABLE = "gics_industry_history"


def _winsorize_cross_section(values: np.ndarray, pct: float) -> np.ndarray:
    """Symmetric percentile winsorization for a 1D cross-section."""
    out = np.asarray(values, dtype=float).copy()
    if not (0.0 < pct < 0.5):
        return out
    finite = np.isfinite(out)
    if int(finite.sum()) < 10:
        return out
    lo = float(np.nanpercentile(out[finite], pct * 100.0))
    hi = float(np.nanpercentile(out[finite], (1.0 - pct) * 100.0))
    out[finite] = np.clip(out[finite], lo, hi)
    return out


def _load_exposures(data_db: Path) -> pd.DataFrame:
    """Load all barra_exposures snapshots, sorted by date."""
    conn = sqlite3.connect(str(data_db))
    df = pd.read_sql_query(
        """SELECT ticker, as_of_date,
                  beta_score, momentum_score, size_score, nonlinear_size_score,
                  short_term_reversal_score, resid_vol_score, liquidity_score,
                  book_to_price_score, earnings_yield_score, value_score,
                  leverage_score, growth_score, profitability_score,
                  investment_score, dividend_yield_score,
                  gics_industry_group
           FROM barra_exposures
           ORDER BY as_of_date, ticker""",
        conn,
    )
    conn.close()
    # Cast score columns to float
    for col in STYLE_SCORE_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _load_industry_history(data_db: Path) -> pd.DataFrame:
    """Load historical industry-group classifications if table exists."""
    conn = sqlite3.connect(str(data_db))
    try:
        row = conn.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name=?
            """,
            (_GICS_HISTORY_TABLE,),
        ).fetchone()
        if not row:
            return pd.DataFrame(columns=["ticker", "as_of_date", "gics_industry_group"])
        df = pd.read_sql_query(
            f"""
            SELECT ticker, as_of_date, gics_industry_group
            FROM {_GICS_HISTORY_TABLE}
            ORDER BY as_of_date, ticker
            """,
            conn,
        )
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame(columns=["ticker", "as_of_date", "gics_industry_group"])
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["as_of_date"] = df["as_of_date"].astype(str)
    df["gics_industry_group"] = (
        df["gics_industry_group"]
        .astype(str)
        .str.strip()
        .replace({"": "Unmapped", "None": "Unmapped", "nan": "Unmapped"})
    )
    return df


def _load_prices(data_db: Path) -> pd.DataFrame:
    """Load all daily close prices."""
    conn = sqlite3.connect(str(data_db))
    df = pd.read_sql_query(
        "SELECT ticker, date, close FROM prices_daily ORDER BY ticker, date",
        conn,
    )
    conn.close()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    return df


def _load_market_caps(data_db: Path) -> pd.DataFrame:
    """Load all fundamental snapshots (ticker, fetch_date, market_cap)."""
    conn = sqlite3.connect(str(data_db))
    df = pd.read_sql_query(
        "SELECT ticker, fetch_date, market_cap FROM fundamental_snapshots ORDER BY ticker, fetch_date",
        conn,
    )
    conn.close()
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
    return df


def _load_cached_dates(cache_db: Path) -> set[str]:
    """Return set of dates already computed in the cache."""
    conn = sqlite3.connect(str(cache_db))
    conn.execute(_DAILY_FR_SCHEMA)
    conn.execute(_DAILY_FR_META_SCHEMA)
    conn.execute(_DAILY_RESIDUALS_SCHEMA)
    conn.commit()
    cur = conn.execute("SELECT DISTINCT date FROM daily_factor_returns")
    dates = {row[0] for row in cur.fetchall()}
    conn.close()
    return dates


def _ensure_cache_version(cache_db: Path):
    """Invalidate cache rows if the daily-factor-returns methodology changed."""
    conn = sqlite3.connect(str(cache_db))
    conn.execute(_DAILY_FR_SCHEMA)
    conn.execute(_DAILY_FR_META_SCHEMA)
    conn.execute(_DAILY_RESIDUALS_SCHEMA)
    row = conn.execute(
        "SELECT value FROM daily_factor_returns_meta WHERE key = ?",
        ("method_version",),
    ).fetchone()
    current_version = row[0] if row else None
    if current_version != CACHE_METHOD_VERSION:
        conn.execute("DROP TABLE IF EXISTS daily_factor_returns")
        conn.execute("DROP TABLE IF EXISTS daily_specific_residuals")
        conn.execute(_DAILY_FR_SCHEMA)
        conn.execute(_DAILY_RESIDUALS_SCHEMA)
        conn.execute(
            """
            INSERT INTO daily_factor_returns_meta(key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            ("method_version", CACHE_METHOD_VERSION),
        )
        conn.commit()
        logger.info(
            "Cleared cached daily_factor_returns due to method version change: %s -> %s",
            current_version,
            CACHE_METHOD_VERSION,
        )
    conn.close()


def _save_daily_results(cache_db: Path, results: list[dict]):
    """Batch-insert daily factor return rows into the cache."""
    if not results:
        return
    conn = sqlite3.connect(str(cache_db))
    conn.execute(_DAILY_FR_SCHEMA)
    conn.execute(_DAILY_FR_META_SCHEMA)
    conn.executemany(
        """INSERT OR REPLACE INTO daily_factor_returns
           (date, factor_name, factor_return, r_squared, residual_vol, cross_section_n, eligible_n, coverage)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (
                r["date"],
                r["factor_name"],
                r["factor_return"],
                r["r_squared"],
                r["residual_vol"],
                r["cross_section_n"],
                r["eligible_n"],
                r["coverage"],
            )
            for r in results
        ],
    )
    conn.commit()
    conn.close()


def _save_daily_residuals(cache_db: Path, rows: list[dict]):
    """Batch-insert stock residual rows into cache."""
    if not rows:
        return
    conn = sqlite3.connect(str(cache_db))
    conn.execute(_DAILY_RESIDUALS_SCHEMA)
    conn.executemany(
        """INSERT OR REPLACE INTO daily_specific_residuals
           (date, ticker, residual, market_cap, industry_group)
           VALUES (?, ?, ?, ?, ?)""",
        [
            (
                r["date"],
                r["ticker"],
                r["residual"],
                r["market_cap"],
                r["industry_group"],
            )
            for r in rows
        ],
    )
    conn.commit()
    conn.close()


def compute_daily_factor_returns(
    data_db: Path,
    cache_db: Path,
    lookback_days: int = 0,
) -> pd.DataFrame:
    """Compute daily factor returns via cross-sectional WLS for every trading day.

    Args:
        data_db: Path to data.db with prices, exposures, fundamentals.
        cache_db: Path to cache.db for incremental storage.
        lookback_days: If >0, only compute the last N trading days. 0 = all.

    Returns:
        DataFrame with columns: date, factor_name, factor_return, r_squared, residual_vol
    """
    t0 = time.time()
    logger.info("Loading data for daily factor returns...")

    # Load source data
    exposures_df = _load_exposures(data_db)
    industry_hist_df = _load_industry_history(data_db)
    prices_df = _load_prices(data_db)
    mcap_df = _load_market_caps(data_db)

    if exposures_df.empty or prices_df.empty:
        logger.warning("No exposures or prices data — cannot compute daily factor returns")
        return pd.DataFrame(columns=["date", "factor_name", "factor_return", "r_squared", "residual_vol"])

    # Build exposure snapshots keyed by as_of_date
    exposure_dates = sorted(exposures_df["as_of_date"].unique())

    # Build daily returns: pivot prices to wide, then compute pct_change
    prices_wide = prices_df.pivot(index="date", columns="ticker", values="close")
    prices_wide = prices_wide.sort_index()
    daily_returns = prices_wide.pct_change(fill_method=None)  # r_i(t) = close(t)/close(t-1) - 1
    daily_returns = daily_returns.iloc[1:]  # drop first NaN row
    trading_dates = sorted(daily_returns.index.tolist())

    if lookback_days > 0:
        trading_dates = trading_dates[-lookback_days:]

    # Build per-ticker carry-forward market-cap panel on trading dates.
    # This avoids shrinking the universe on dates where only a partial
    # fundamentals snapshot was ingested.
    if not mcap_df.empty:
        mcap_wide = (
            mcap_df.dropna(subset=["ticker", "fetch_date"])
            .drop_duplicates(subset=["ticker", "fetch_date"], keep="last")
            .pivot(index="fetch_date", columns="ticker", values="market_cap")
            .sort_index()
        )
        mcap_wide = mcap_wide.reindex(trading_dates).ffill()
    else:
        mcap_wide = pd.DataFrame(index=trading_dates)

    # Invalidate stale cache rows if methodology changed, then check cached dates.
    _ensure_cache_version(cache_db)
    cached_dates = _load_cached_dates(cache_db)
    dates_to_compute = [d for d in trading_dates if d not in cached_dates]

    if not dates_to_compute:
        logger.info("All dates already cached, loading from cache")
        return _load_all_from_cache(cache_db, lookback_days)

    logger.info(
        f"Computing daily factor returns: {len(dates_to_compute)} new dates "
        f"({len(cached_dates)} already cached)"
    )

    # Pre-build exposure lookup: for each exposure date, a DataFrame indexed by ticker
    exposure_snapshots: dict[str, pd.DataFrame] = {}
    industry_hist_by_date: dict[str, pd.Series] = {}
    if not industry_hist_df.empty:
        for as_of, grp in industry_hist_df.groupby("as_of_date", sort=False):
            s = (
                grp.drop_duplicates(subset=["ticker"], keep="last")
                .set_index("ticker")["gics_industry_group"]
                .astype(str)
            )
            industry_hist_by_date[str(as_of)] = s

    for as_of in exposure_dates:
        snap = exposures_df[exposures_df["as_of_date"] == as_of].copy()
        snap = snap.drop_duplicates(subset=["ticker"], keep="last")
        snap = snap.set_index("ticker")
        hist_groups = industry_hist_by_date.get(str(as_of))
        if hist_groups is not None:
            merged_group = (
                hist_groups.reindex(snap.index)
                .combine_first(snap.get("gics_industry_group"))
                .fillna("Unmapped")
            )
            snap["gics_industry_group"] = merged_group.astype(str)
        elif "gics_industry_group" in snap.columns:
            snap["gics_industry_group"] = snap["gics_industry_group"].fillna("Unmapped").astype(str)
        else:
            snap["gics_industry_group"] = "Unmapped"
        exposure_snapshots[as_of] = snap

    # Process each trading day
    batch_results: list[dict] = []
    batch_residuals: list[dict] = []
    n_computed = 0
    for i, date in enumerate(dates_to_compute):
        # 1. Carry-forward exposures: find most recent snapshot <= date
        exp_date = _find_most_recent(exposure_dates, date)
        if exp_date is None:
            continue
        exp_snap = exposure_snapshots[exp_date]

        # 2. Daily stock returns for this date
        if date not in daily_returns.index:
            continue
        ret_row = daily_returns.loc[date].dropna()

        # 3. Carry-forward market caps per ticker
        if date in mcap_wide.index:
            mcap_series = mcap_wide.loc[date].dropna()
        else:
            mcap_series = pd.Series(dtype=float)

        # 4. Eligible universe for this date:
        #    all names with fundamentals + exposures; returns are intersected from prices.
        eligible = exp_snap.index.intersection(mcap_series.index)
        if len(eligible) < MIN_CROSS_SECTION_SIZE:
            continue
        common = ret_row.index.intersection(eligible)
        if len(common) < MIN_CROSS_SECTION_SIZE:
            # Need a reasonable cross-section for regression.
            continue

        returns_series = ret_row.loc[common].astype(float)
        market_cap_series = pd.to_numeric(mcap_series.loc[common], errors="coerce").astype(float)

        # 5. Prepare valid cross-section (finite returns + positive finite caps)
        valid = (
            np.isfinite(returns_series.to_numpy(dtype=float))
            & np.isfinite(market_cap_series.to_numpy(dtype=float))
            & (market_cap_series.to_numpy(dtype=float) > 0)
        )
        valid_n = int(valid.sum())
        if valid_n < MIN_CROSS_SECTION_SIZE:
            continue
        coverage = valid_n / max(1, len(eligible))
        if coverage < MIN_ELIGIBLE_COVERAGE:
            # Skip sparse days to keep factor returns representative of the eligible universe.
            continue

        valid_idx = returns_series.index[valid]
        returns = returns_series.loc[valid_idx].to_numpy(dtype=float)
        returns = _winsorize_cross_section(returns, RETURNS_WINSOR_PCT)
        market_caps = market_cap_series.loc[valid_idx].to_numpy(dtype=float)

        # Style exposures (canonicalized cross-sectionally to keep model inputs consistent)
        style_cols_present = [c for c in STYLE_SCORE_COLS if c in exp_snap.columns]
        style_names = [STYLE_COLUMN_TO_LABEL[c] for c in style_cols_present]
        style_scores = exp_snap.loc[valid_idx, style_cols_present].copy()
        style_scores.columns = style_names

        # Industry exposures (one-hot from gics_industry_group)
        industry_series = pd.Series("Unmapped", index=valid_idx, dtype="object")
        if "gics_industry_group" in exp_snap.columns:
            industry_series = exp_snap.loc[valid_idx, "gics_industry_group"].fillna("Unmapped")
            industry_dummies = pd.get_dummies(industry_series, dtype=float)
            ind_x = industry_dummies.to_numpy(dtype=float)
            ind_names = list(industry_dummies.columns)
        else:
            industry_dummies = pd.DataFrame(index=valid_idx)
            ind_x = None
            ind_names = []

        style_canonical = canonicalize_style_scores(
            style_scores=style_scores,
            market_caps=market_cap_series.loc[valid_idx],
            orth_rules=FULL_STYLE_ORTH_RULES,
            industry_exposures=industry_dummies,
        )
        style_x = style_canonical[style_names].to_numpy(dtype=float)

        # 6. Run two-phase WLS
        result = estimate_factor_returns_two_phase(
            returns=returns,
            market_caps=market_caps,
            industry_exposures=ind_x if ind_x is not None else None,
            style_exposures=style_x if style_x is not None else None,
            industry_names=ind_names,
            style_names=style_names,
        )

        # 7. Store factor returns
        for factor_name, factor_return in result.factor_returns.items():
            if not np.isfinite(factor_return):
                factor_return = 0.0
            batch_results.append({
                "date": date,
                "factor_name": factor_name,
                "factor_return": factor_return,
                "r_squared": result.r_squared if np.isfinite(result.r_squared) else 0.0,
                "residual_vol": result.residual_vol if np.isfinite(result.residual_vol) else 0.0,
                "cross_section_n": int(valid_n),
                "eligible_n": int(len(eligible)),
                "coverage": float(coverage) if np.isfinite(coverage) else 0.0,
            })

        # 8. Store per-stock residual history for specific risk forecasting
        residuals = np.asarray(result.residuals, dtype=float)
        for idx, ticker in enumerate(valid_idx):
            if idx >= residuals.shape[0]:
                break
            resid_val = float(residuals[idx])
            if not np.isfinite(resid_val):
                continue
            mcap_val = float(market_cap_series.loc[ticker])
            if not np.isfinite(mcap_val) or mcap_val <= 0:
                mcap_val = 0.0
            industry = str(industry_series.loc[ticker]) if ticker in industry_series.index else "Unmapped"
            batch_residuals.append({
                "date": date,
                "ticker": str(ticker),
                "residual": resid_val,
                "market_cap": mcap_val,
                "industry_group": industry,
            })

        n_computed += 1

        # Batch save every 100 dates
        if len(batch_results) > 3000:
            _save_daily_results(cache_db, batch_results)
            batch_results = []
        if len(batch_residuals) > 25000:
            _save_daily_residuals(cache_db, batch_residuals)
            batch_residuals = []

        if (i + 1) % 200 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            logger.info(f"  {i + 1}/{len(dates_to_compute)} dates ({rate:.0f} dates/s)")

    # Save remaining
    _save_daily_results(cache_db, batch_results)
    _save_daily_residuals(cache_db, batch_residuals)

    elapsed = time.time() - t0
    logger.info(f"Computed {n_computed} daily cross-sections in {elapsed:.1f}s")

    return _load_all_from_cache(cache_db, lookback_days)


def _find_most_recent(sorted_dates: list[str], target: str) -> str | None:
    """Binary search for the most recent date <= target in a sorted list."""
    if not sorted_dates:
        return None
    lo, hi = 0, len(sorted_dates) - 1
    result = None
    while lo <= hi:
        mid = (lo + hi) // 2
        if sorted_dates[mid] <= target:
            result = sorted_dates[mid]
            lo = mid + 1
        else:
            hi = mid - 1
    return result


def _load_all_from_cache(cache_db: Path, lookback_days: int = 0) -> pd.DataFrame:
    """Load all daily factor returns from the cache."""
    conn = sqlite3.connect(str(cache_db))
    conn.execute(_DAILY_FR_SCHEMA)
    conn.execute(_DAILY_FR_META_SCHEMA)
    if lookback_days > 0:
        df = pd.read_sql_query(
            """SELECT date, factor_name, factor_return, r_squared, residual_vol
               FROM daily_factor_returns
               ORDER BY date DESC""",
            conn,
        )
        # Get the last N unique dates
        unique_dates = df["date"].unique()
        if len(unique_dates) > lookback_days:
            cutoff = sorted(unique_dates)[-lookback_days]
            df = df[df["date"] >= cutoff]
        df = df.sort_values(["date", "factor_name"]).reset_index(drop=True)
    else:
        df = pd.read_sql_query(
            """SELECT date, factor_name, factor_return, r_squared, residual_vol
               FROM daily_factor_returns
               ORDER BY date, factor_name""",
            conn,
        )
    conn.close()
    return df


def load_daily_factor_returns(
    cache_db: Path,
    lookback_days: int = 504,
) -> list[dict]:
    """Load cached daily factor returns as list of dicts (for covariance builder)."""
    df = _load_all_from_cache(cache_db, lookback_days)
    return df.to_dict("records")


def load_specific_residuals(
    cache_db: Path,
    lookback_days: int = 504,
) -> pd.DataFrame:
    """Load per-stock daily residual returns from cache for specific-risk modeling."""
    conn = sqlite3.connect(str(cache_db))
    conn.execute(_DAILY_RESIDUALS_SCHEMA)
    if lookback_days > 0:
        df = pd.read_sql_query(
            """
            SELECT date, ticker, residual, market_cap, industry_group
            FROM daily_specific_residuals
            WHERE date IN (
                SELECT date
                FROM (
                    SELECT DISTINCT date
                    FROM daily_specific_residuals
                    ORDER BY date DESC
                    LIMIT ?
                )
            )
            ORDER BY date, ticker
            """,
            conn,
            params=(int(lookback_days),),
        )
    else:
        df = pd.read_sql_query(
            """
            SELECT date, ticker, residual, market_cap, industry_group
            FROM daily_specific_residuals
            ORDER BY date, ticker
            """,
            conn,
        )
    conn.close()
    return df
