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

from backend.data.trbc_schema import pick_trbc_business_sector_column, pick_trbc_industry_column
from backend.risk_model.eligibility import (
    build_eligibility_context,
    structural_eligibility_for_date,
)
from backend.risk_model.descriptors import FULL_STYLE_ORTH_RULES, canonicalize_style_scores
from backend.risk_model.risk_attribution import COUNTRY_FACTOR, STYLE_COLUMN_TO_LABEL
from backend.risk_model.wls_regression import estimate_factor_returns_two_phase
from backend.trading_calendar import filter_xnys_sessions, non_xnys_dates, previous_or_same_xnys_session

logger = logging.getLogger(__name__)

# Style score columns in the raw cross-section table.
STYLE_SCORE_COLS = list(STYLE_COLUMN_TO_LABEL.keys())
RETURNS_WINSOR_PCT = 0.05
MIN_CROSS_SECTION_SIZE = 30
MIN_ELIGIBLE_COVERAGE = 0.60
CACHE_METHOD_VERSION = "v14_trbc_l2_country_us_dummy_2026_03_08"

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
    ric TEXT NOT NULL,
    ticker TEXT NOT NULL,
    residual REAL NOT NULL,
    market_cap REAL NOT NULL DEFAULT 0.0,
    trbc_industry_group TEXT,
    PRIMARY KEY (date, ric)
);
"""

_DAILY_ELIGIBILITY_SUMMARY_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_universe_eligibility_summary (
    date TEXT PRIMARY KEY,
    exp_date TEXT,
    exposure_n INTEGER NOT NULL DEFAULT 0,
    structural_eligible_n INTEGER NOT NULL DEFAULT 0,
    regression_member_n INTEGER NOT NULL DEFAULT 0,
    structural_coverage REAL NOT NULL DEFAULT 0.0,
    regression_coverage REAL NOT NULL DEFAULT 0.0,
    drop_pct_from_prev REAL NOT NULL DEFAULT 0.0,
    alert_level TEXT NOT NULL DEFAULT '',
    missing_style_n INTEGER NOT NULL DEFAULT 0,
    missing_market_cap_n INTEGER NOT NULL DEFAULT 0,
    missing_trbc_economic_sector_short_n INTEGER NOT NULL DEFAULT 0,
    missing_trbc_industry_n INTEGER NOT NULL DEFAULT 0,
    non_equity_n INTEGER NOT NULL DEFAULT 0,
    missing_return_n INTEGER NOT NULL DEFAULT 0
);
"""

def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


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


def _shift_date_by_days(date_key: str, days: int) -> str:
    shift = max(0, int(days))
    ts = pd.to_datetime(str(date_key), errors="coerce")
    if pd.isna(ts):
        return str(date_key)
    shifted = ts if shift <= 0 else (ts - pd.Timedelta(days=shift))
    return previous_or_same_xnys_session(shifted)


def _purge_non_session_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str,
) -> int:
    rows = conn.execute(
        f"SELECT DISTINCT {date_col} FROM {table} WHERE {date_col} IS NOT NULL"
    ).fetchall()
    dates = [str(r[0]) for r in rows if r and r[0] is not None]
    invalid = non_xnys_dates(dates)
    if not invalid:
        return 0

    deleted = 0
    chunk = 500
    for i in range(0, len(invalid), chunk):
        part = invalid[i : i + chunk]
        placeholders = ",".join("?" for _ in part)
        cur = conn.execute(
            f"DELETE FROM {table} WHERE {date_col} IN ({placeholders})",
            part,
        )
        deleted += int(cur.rowcount or 0)
    return deleted


def _load_prices(data_db: Path) -> pd.DataFrame:
    """Load daily close prices keyed by RIC for an optional bounded date window."""
    return _load_prices_for_window(data_db)


def _load_prices_for_window(
    data_db: Path,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Load daily close prices keyed by RIC for an optional bounded date window."""
    conn = sqlite3.connect(str(data_db))
    predicates: list[str] = []
    params: list[str] = []
    if start_date:
        predicates.append("p.date >= ?")
        params.append(str(start_date))
    if end_date:
        predicates.append("p.date <= ?")
        params.append(str(end_date))
    where_clause = f"WHERE {' AND '.join(predicates)}" if predicates else ""
    df = pd.read_sql_query(
        f"""
        SELECT UPPER(p.ric) AS ric, UPPER(sm.ticker) AS ticker, p.date, p.close, p.source, p.updated_at
        FROM security_prices_eod p
        JOIN security_master sm
          ON sm.ric = p.ric
        {where_clause}
        ORDER BY UPPER(p.ric), p.date
        """,
        conn,
        params=params,
    )
    conn.close()
    df["ric"] = df["ric"].astype(str).str.upper()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype("string")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["source"] = df.get("source", pd.Series(index=df.index, dtype="string")).astype("string")
    df["updated_at"] = pd.to_datetime(df.get("updated_at"), errors="coerce")
    df = df.dropna(subset=["ric", "date", "close"])
    # Deduplicate repeated ingests by taking preferred source + latest update per (RIC, date).
    df["source_priority"] = np.where(df["source"].str.lower() == "lseg_toolkit", 1, 0)
    df = df.sort_values(["ric", "date", "source_priority", "updated_at"], ascending=[True, True, False, False])
    df = df.drop_duplicates(subset=["ric", "date"], keep="first")
    return df[["ric", "ticker", "date", "close"]]


def _load_trading_dates(data_db: Path) -> list[str]:
    """Load distinct trading dates from the price table without materializing the full panel."""
    conn = sqlite3.connect(str(data_db))
    rows = conn.execute(
        """
        SELECT DISTINCT date
        FROM security_prices_eod
        WHERE date IS NOT NULL
        ORDER BY date
        """
    ).fetchall()
    conn.close()
    trading_dates = [str(row[0]) for row in rows if row and row[0]]
    return filter_xnys_sessions(trading_dates)


def _load_cached_dates(cache_db: Path) -> set[str]:
    """Return dates that are complete in cache for factors, residuals, and eligibility."""
    conn = sqlite3.connect(str(cache_db))
    conn.execute(_DAILY_FR_SCHEMA)
    conn.execute(_DAILY_FR_META_SCHEMA)
    conn.execute(_DAILY_RESIDUALS_SCHEMA)
    conn.execute(_DAILY_ELIGIBILITY_SUMMARY_SCHEMA)
    conn.commit()
    factor_dates = {str(row[0]) for row in conn.execute("SELECT DISTINCT date FROM daily_factor_returns").fetchall()}
    residual_dates = {str(row[0]) for row in conn.execute("SELECT DISTINCT date FROM daily_specific_residuals").fetchall()}
    eligibility_dates = {
        str(row[0])
        for row in conn.execute("SELECT DISTINCT date FROM daily_universe_eligibility_summary").fetchall()
    }
    dates = factor_dates & residual_dates & eligibility_dates
    conn.close()
    return dates


def _factor_return_cache_signature(*, min_cross_section_age_days: int) -> dict[str, str]:
    return {
        "method_version": str(CACHE_METHOD_VERSION),
        "cross_section_min_age_days": str(max(0, int(min_cross_section_age_days))),
    }


def _ensure_cache_version(cache_db: Path, *, min_cross_section_age_days: int):
    """Invalidate cache rows if the daily-factor-returns methodology changed."""
    conn = sqlite3.connect(str(cache_db))
    conn.execute(_DAILY_FR_SCHEMA)
    conn.execute(_DAILY_FR_META_SCHEMA)
    conn.execute(_DAILY_RESIDUALS_SCHEMA)
    conn.execute(_DAILY_ELIGIBILITY_SUMMARY_SCHEMA)
    required_meta = _factor_return_cache_signature(
        min_cross_section_age_days=min_cross_section_age_days
    )
    current_meta = {
        str(row[0]): str(row[1])
        for row in conn.execute(
            "SELECT key, value FROM daily_factor_returns_meta WHERE key IN (?, ?)",
            tuple(required_meta.keys()),
        ).fetchall()
    }
    if current_meta != required_meta:
        conn.execute("DROP TABLE IF EXISTS daily_factor_returns")
        conn.execute("DROP TABLE IF EXISTS daily_specific_residuals")
        conn.execute("DROP TABLE IF EXISTS daily_universe_eligibility_summary")
        conn.execute(_DAILY_FR_SCHEMA)
        conn.execute(_DAILY_RESIDUALS_SCHEMA)
        conn.execute(_DAILY_ELIGIBILITY_SUMMARY_SCHEMA)
        conn.executemany(
            """
            INSERT INTO daily_factor_returns_meta(key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            [(key, value) for key, value in required_meta.items()],
        )
        conn.commit()
        logger.info(
            "Cleared cached daily_factor_returns due to cache signature change: %s -> %s",
            current_meta,
            required_meta,
        )
    pruned = {
        "daily_factor_returns": _purge_non_session_rows(
            conn, table="daily_factor_returns", date_col="date"
        ),
        "daily_specific_residuals": _purge_non_session_rows(
            conn, table="daily_specific_residuals", date_col="date"
        ),
        "daily_universe_eligibility_summary": _purge_non_session_rows(
            conn, table="daily_universe_eligibility_summary", date_col="date"
        ),
    }
    if any(pruned.values()):
        conn.commit()
        logger.info(
            "Pruned non-session rows from cache: factor_returns=%s residuals=%s eligibility=%s",
            pruned["daily_factor_returns"],
            pruned["daily_specific_residuals"],
            pruned["daily_universe_eligibility_summary"],
        )
    conn.close()


def _save_daily_results_and_residuals(
    cache_db: Path,
    results: list[dict],
    residuals: list[dict],
) -> None:
    """Atomically batch-insert factor returns and residual history into cache."""
    if not results and not residuals:
        return
    conn = sqlite3.connect(str(cache_db))
    try:
        conn.execute(_DAILY_FR_SCHEMA)
        conn.execute(_DAILY_FR_META_SCHEMA)
        conn.execute(_DAILY_RESIDUALS_SCHEMA)
        if results:
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
        if residuals:
            conn.executemany(
                """INSERT OR REPLACE INTO daily_specific_residuals
                   (date, ric, ticker, residual, market_cap, trbc_industry_group)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [
                    (
                        r["date"],
                        r["ric"],
                        r["ticker"],
                        r["residual"],
                        r["market_cap"],
                        r["trbc_industry_group"],
                    )
                    for r in residuals
                ],
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _save_daily_eligibility_summary(cache_db: Path, rows: list[dict]) -> None:
    if not rows:
        return
    conn = sqlite3.connect(str(cache_db))
    conn.execute(_DAILY_ELIGIBILITY_SUMMARY_SCHEMA)
    conn.executemany(
        """
        INSERT OR REPLACE INTO daily_universe_eligibility_summary (
            date, exp_date, exposure_n, structural_eligible_n, regression_member_n,
            structural_coverage, regression_coverage, drop_pct_from_prev, alert_level,
            missing_style_n, missing_market_cap_n, missing_trbc_economic_sector_short_n,
            missing_trbc_industry_n, non_equity_n, missing_return_n
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r["date"],
                r.get("exp_date"),
                int(r.get("exposure_n", 0)),
                int(r.get("structural_eligible_n", 0)),
                int(r.get("regression_member_n", 0)),
                float(r.get("structural_coverage", 0.0)),
                float(r.get("regression_coverage", 0.0)),
                float(r.get("drop_pct_from_prev", 0.0)),
                str(r.get("alert_level") or ""),
                int(r.get("missing_style_n", 0)),
                int(r.get("missing_market_cap_n", 0)),
                int(r.get("missing_trbc_economic_sector_short_n", 0)),
                int(r.get("missing_trbc_industry_n", 0)),
                int(r.get("non_equity_n", 0)),
                int(r.get("missing_return_n", 0)),
            )
            for r in rows
        ],
    )
    conn.commit()
    conn.close()


def _load_structural_counts(cache_db: Path) -> dict[str, int]:
    conn = sqlite3.connect(str(cache_db))
    try:
        conn.execute(_DAILY_ELIGIBILITY_SUMMARY_SCHEMA)
        rows = conn.execute(
            """
            SELECT date, structural_eligible_n
            FROM daily_universe_eligibility_summary
            WHERE date IS NOT NULL
            ORDER BY date
            """
        ).fetchall()
    finally:
        conn.close()
    out: dict[str, int] = {}
    for row in rows:
        try:
            date_key = str(row[0])
            out[date_key] = int(row[1])
        except (TypeError, ValueError):
            continue
    return out


def _previous_structural_count(structural_counts: dict[str, int], date: str) -> int | None:
    prev_dates = [key for key in structural_counts.keys() if key < str(date)]
    if not prev_dates:
        return None
    return structural_counts[max(prev_dates)]


def compute_daily_factor_returns(
    data_db: Path,
    cache_db: Path,
    lookback_days: int = 0,
    *,
    min_cross_section_age_days: int = 7,
) -> pd.DataFrame:
    """Compute daily factor returns via cross-sectional WLS for every trading day.

    Args:
        data_db: Path to data.db with prices, exposures, fundamentals.
        cache_db: Path to cache.db for incremental storage.
        lookback_days: If >0, only compute the last N trading days. 0 = all.
        min_cross_section_age_days: Minimum age (calendar days) of exposure
            snapshots used for each regression date.

    Returns:
        DataFrame with columns: date, factor_name, factor_return, r_squared, residual_vol
    """
    t0 = time.time()
    logger.info("Loading data for daily factor returns...")

    # Invalidate stale cache rows if methodology changed, then check cached dates.
    _ensure_cache_version(
        cache_db,
        min_cross_section_age_days=min_cross_section_age_days,
    )
    trading_dates = _load_trading_dates(data_db)
    if lookback_days > 0:
        trading_dates = trading_dates[-lookback_days:]
    logger.info("Resolved trading date window: total_dates=%s lookback_days=%s", len(trading_dates), lookback_days)
    cached_dates = _load_cached_dates(cache_db)
    dates_to_compute = [d for d in trading_dates if d not in cached_dates]

    if not dates_to_compute:
        logger.info("All dates already cached, loading from cache")
        return _load_all_from_cache(cache_db, lookback_days)

    logger.info(
        f"Computing daily factor returns: {len(dates_to_compute)} new dates "
        f"({len(cached_dates)} already cached)"
    )

    first_compute_idx = trading_dates.index(dates_to_compute[0])
    price_start_date = trading_dates[max(0, first_compute_idx - 1)]
    price_end_date = dates_to_compute[-1]
    prices_df = _load_prices_for_window(
        data_db,
        start_date=price_start_date,
        end_date=price_end_date,
    )
    if prices_df.empty:
        logger.warning("No prices data in resolved window — cannot compute daily factor returns")
        return pd.DataFrame(columns=["date", "factor_name", "factor_return", "r_squared", "residual_vol"])
    logger.info(
        "Loaded bounded prices for factor returns: rows=%s rics=%s trading_dates=%s start=%s end=%s",
        len(prices_df),
        prices_df["ric"].nunique(),
        prices_df["date"].nunique(),
        price_start_date,
        price_end_date,
    )

    prices_wide = prices_df.pivot(index="date", columns="ric", values="close")
    prices_wide = prices_wide.sort_index()
    daily_returns = prices_wide.pct_change(fill_method=None)
    daily_returns = daily_returns.iloc[1:]
    daily_returns.index = daily_returns.index.astype(str)
    ric_ticker_map = (
        prices_df.sort_values(["ric", "date"])
        .dropna(subset=["ric"])
        .drop_duplicates(subset=["ric"], keep="last")
        .set_index("ric")["ticker"]
        .astype(str)
        .str.upper()
        .to_dict()
    )

    # Centralized structural-eligibility context for all daily cross-sections.
    lag_days = max(0, int(min_cross_section_age_days))
    eligibility_dates = sorted({
        _shift_date_by_days(d, lag_days)
        for d in dates_to_compute
    })
    eligibility_ctx = build_eligibility_context(data_db, dates=eligibility_dates)
    if not eligibility_ctx.exposure_dates:
        logger.warning("No exposure snapshots available — cannot compute daily factor returns")
        return pd.DataFrame(columns=["date", "factor_name", "factor_return", "r_squared", "residual_vol"])
    logger.info(
        "Eligibility context ready: snapshots=%s first_snapshot=%s last_snapshot=%s",
        len(eligibility_ctx.exposure_dates),
        eligibility_ctx.exposure_dates[0] if eligibility_ctx.exposure_dates else None,
        eligibility_ctx.exposure_dates[-1] if eligibility_ctx.exposure_dates else None,
    )

    # Process each trading day
    batch_results: list[dict] = []
    batch_residuals: list[dict] = []
    batch_eligibility: list[dict] = []
    n_computed = 0
    skip_counts = {
        "missing_return_row": 0,
        "missing_eligibility": 0,
        "small_cross_section": 0,
        "low_coverage": 0,
        "missing_l2_sector": 0,
        "missing_country": 0,
        "empty_dummies": 0,
    }
    structural_counts = _load_structural_counts(cache_db)

    for i, date in enumerate(dates_to_compute):
        # 1. Daily stock returns for this date
        if date not in daily_returns.index:
            skip_counts["missing_return_row"] += 1
            continue
        ret_row = daily_returns.loc[date]
        ret_row = pd.to_numeric(ret_row, errors="coerce")
        ret_row = ret_row[np.isfinite(ret_row.to_numpy(dtype=float))]

        # 2. Structural eligibility for this date from centralized context.
        # Enforce minimum cross-section age by resolving eligibility at (date - lag_days).
        eligibility_date = _shift_date_by_days(date, lag_days)
        exp_date, eligibility = structural_eligibility_for_date(eligibility_ctx, eligibility_date)
        if exp_date is None or eligibility.empty:
            skip_counts["missing_eligibility"] += 1
            batch_eligibility.append({
                "date": date,
                "exp_date": exp_date,
                "exposure_n": 0,
                "structural_eligible_n": 0,
                "regression_member_n": 0,
                "structural_coverage": 0.0,
                "regression_coverage": 0.0,
                "drop_pct_from_prev": 0.0,
                "alert_level": "",
                "missing_style_n": 0,
                "missing_market_cap_n": 0,
                "missing_trbc_economic_sector_short_n": 0,
                "missing_trbc_industry_n": 0,
                "non_equity_n": 0,
                "missing_return_n": 0,
            })
            continue

        exp_snap = eligibility_ctx.exposure_snapshots[exp_date]
        exposure_n = int(len(eligibility))
        structural_mask = eligibility["is_structural_eligible"].astype(bool)
        structural_n = int(structural_mask.sum())
        has_return = eligibility.index.isin(ret_row.index)
        regression_mask = structural_mask & has_return
        regression_n = int(regression_mask.sum())

        no_struct = eligibility.loc[~structural_mask, "exclusion_reason"].astype(str)
        exploded = no_struct.str.split("|").explode()
        missing_style_n = int((exploded == "missing_style").sum())
        missing_market_cap_n = int((exploded == "missing_market_cap").sum())
        missing_trbc_economic_sector_short_n = int((exploded == "missing_trbc_economic_sector_short").sum())
        missing_trbc_industry_n = int((exploded == "missing_trbc_industry").sum())
        non_equity_n = int((exploded == "non_equity").sum())
        missing_return_n = int((structural_mask & ~has_return).sum())

        structural_coverage = float(structural_n / max(1, exposure_n))
        regression_coverage = float(regression_n / max(1, structural_n))
        prev_structural_n = _previous_structural_count(structural_counts, date)
        if prev_structural_n is None or prev_structural_n <= 0:
            drop_pct_from_prev = 0.0
        else:
            drop_pct_from_prev = float((prev_structural_n - structural_n) / prev_structural_n)
        alert_level = ""
        if drop_pct_from_prev > 0.20:
            alert_level = "critical"
        elif drop_pct_from_prev > 0.10:
            alert_level = "warn"
        if alert_level:
            logger.warning(
                "Eligibility drop on %s: structural %s -> %s (%.2f%%)",
                date,
                prev_structural_n,
                structural_n,
                drop_pct_from_prev * 100.0,
            )
        structural_counts[date] = structural_n
        batch_eligibility.append({
            "date": date,
            "exp_date": exp_date,
            "exposure_n": exposure_n,
            "structural_eligible_n": structural_n,
            "regression_member_n": regression_n,
            "structural_coverage": structural_coverage,
            "regression_coverage": regression_coverage,
            "drop_pct_from_prev": drop_pct_from_prev,
            "alert_level": alert_level,
            "missing_style_n": missing_style_n,
            "missing_market_cap_n": missing_market_cap_n,
            "missing_trbc_economic_sector_short_n": missing_trbc_economic_sector_short_n,
            "missing_trbc_industry_n": missing_trbc_industry_n,
            "non_equity_n": non_equity_n,
            "missing_return_n": missing_return_n,
        })

        if structural_n < MIN_CROSS_SECTION_SIZE or regression_n < MIN_CROSS_SECTION_SIZE:
            skip_counts["small_cross_section"] += 1
            continue
        if regression_coverage < MIN_ELIGIBLE_COVERAGE:
            skip_counts["low_coverage"] += 1
            continue

        valid_idx = eligibility.index[regression_mask]
        returns_series = ret_row.loc[valid_idx].astype(float)
        market_cap_series = pd.to_numeric(eligibility.loc[valid_idx, "market_cap"], errors="coerce").astype(float)
        industry_series = (
            eligibility.loc[valid_idx, "trbc_business_sector"]
            .fillna("")
            .astype(str)
            .str.strip()
        )
        country_series = (
            eligibility.loc[valid_idx, "hq_country_code"]
            .fillna("")
            .astype(str)
            .str.upper()
            .str.strip()
        )
        if industry_series.eq("").all():
            skip_counts["missing_l2_sector"] += 1
            continue
        if country_series.eq("").all():
            skip_counts["missing_country"] += 1
            continue

        returns = returns_series.to_numpy(dtype=float)
        returns = _winsorize_cross_section(returns, RETURNS_WINSOR_PCT)
        market_caps = market_cap_series.to_numpy(dtype=float)

        # Style exposures (canonicalized cross-sectionally using only structurally eligible names)
        style_cols_present = [c for c in STYLE_SCORE_COLS if c in exp_snap.columns]
        style_names = [STYLE_COLUMN_TO_LABEL[c] for c in style_cols_present]
        style_scores = exp_snap.loc[valid_idx, style_cols_present].copy()
        style_scores.columns = style_names

        # Structural exposures: US country dummy plus TRBC L2 business-sector groups.
        structural_dummies = pd.get_dummies(industry_series, dtype=float)
        if structural_dummies.empty:
            skip_counts["empty_dummies"] += 1
            continue
        country_exposure = np.where(country_series.eq("US"), 1.0, 0.0)
        country_exposure = pd.Series(country_exposure, index=country_series.index, dtype=float)
        if country_series.eq("US").any() and country_series.ne("US").any():
            structural_dummies = pd.concat(
                [country_exposure.rename(COUNTRY_FACTOR), structural_dummies],
                axis=1,
            )
        ind_x = structural_dummies.to_numpy(dtype=float)
        ind_names = list(structural_dummies.columns)

        style_canonical = canonicalize_style_scores(
            style_scores=style_scores,
            market_caps=market_cap_series.loc[valid_idx],
            orth_rules=FULL_STYLE_ORTH_RULES,
            industry_exposures=structural_dummies,
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
                "cross_section_n": int(regression_n),
                "eligible_n": int(structural_n),
                "coverage": float(regression_coverage) if np.isfinite(regression_coverage) else 0.0,
            })

        # 8. Store per-stock residual history for specific risk forecasting.
        # Column name remains legacy `trbc_industry_group` for cache compatibility,
        # but values now carry TRBC L2 business-sector buckets.
        residuals = np.asarray(result.residuals, dtype=float)
        for idx, ric in enumerate(valid_idx):
            if idx >= residuals.shape[0]:
                break
            resid_val = float(residuals[idx])
            if not np.isfinite(resid_val):
                continue
            mcap_val = float(market_cap_series.loc[ric])
            if not np.isfinite(mcap_val) or mcap_val <= 0:
                continue
            industry = str(industry_series.loc[ric]) if ric in industry_series.index else ""
            ric_key = str(ric).upper()
            batch_residuals.append({
                "date": date,
                "ric": ric_key,
                "ticker": str(ric_ticker_map.get(ric_key, "")),
                "residual": resid_val,
                "market_cap": mcap_val,
                "trbc_industry_group": industry,
            })

        n_computed += 1

        # Batch save every 100 dates
        if len(batch_results) > 3000 or len(batch_residuals) > 25000:
            pending_results = len(batch_results)
            pending_residuals = len(batch_residuals)
            _save_daily_results_and_residuals(cache_db, batch_results, batch_residuals)
            batch_results = []
            batch_residuals = []
            logger.info(
                "Flushed factor-return cache batch: through_date=%s factor_rows=%s residual_rows=%s",
                date,
                pending_results,
                pending_residuals,
            )
        if len(batch_eligibility) > 500:
            pending_eligibility = len(batch_eligibility)
            _save_daily_eligibility_summary(cache_db, batch_eligibility)
            batch_eligibility = []
            logger.info(
                "Flushed eligibility summary batch: through_date=%s rows=%s",
                date,
                pending_eligibility,
            )

        if (i + 1) % 200 == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            logger.info(
                "Progress %s/%s dates (%.0f dates/s, computed=%s, skipped_small=%s, skipped_low_cov=%s)",
                i + 1,
                len(dates_to_compute),
                rate,
                n_computed,
                skip_counts["small_cross_section"],
                skip_counts["low_coverage"],
            )

    # Save remaining
    pending_results = len(batch_results)
    pending_residuals = len(batch_residuals)
    pending_eligibility = len(batch_eligibility)
    _save_daily_results_and_residuals(cache_db, batch_results, batch_residuals)
    _save_daily_eligibility_summary(cache_db, batch_eligibility)
    if pending_results or pending_residuals or pending_eligibility:
        logger.info(
            "Flushed final cache batch: factor_rows=%s residual_rows=%s eligibility_rows=%s",
            pending_results,
            pending_residuals,
            pending_eligibility,
        )

    elapsed = time.time() - t0
    logger.info(
        "Computed %s daily cross-sections in %.1fs (skip_counts=%s)",
        n_computed,
        elapsed,
        skip_counts,
    )

    return _load_all_from_cache(cache_db, lookback_days)


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
    cols = _table_columns(conn, "daily_specific_residuals")
    industry_col = pick_trbc_business_sector_column(cols) or pick_trbc_industry_column(cols)
    if industry_col is None:
        conn.close()
        return pd.DataFrame(columns=["date", "ric", "ticker", "residual", "market_cap", "trbc_industry_group"])
    ric_expr = "UPPER(ric)" if "ric" in cols else "UPPER(ticker)"
    ticker_expr = "UPPER(ticker)" if "ticker" in cols else "UPPER(ric)"
    if lookback_days > 0:
        df = pd.read_sql_query(
            f"""
            SELECT date, {ric_expr} AS ric, {ticker_expr} AS ticker, residual, market_cap, {industry_col} AS trbc_industry_group
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
            f"""
            SELECT date, {ric_expr} AS ric, {ticker_expr} AS ticker, residual, market_cap, {industry_col} AS trbc_industry_group
            FROM daily_specific_residuals
            ORDER BY date, ric
            """,
            conn,
        )
    conn.close()
    if df.empty:
        return df
    df["ric"] = df["ric"].astype(str).str.upper()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    return df
