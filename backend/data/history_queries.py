"""Read-only historical query helpers used by API routes."""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path


def load_factor_return_history(
    cache_db: Path,
    *,
    factor: str,
    years: int,
) -> tuple[str | None, list[tuple[str, float]]]:
    """Return latest factor-return date and historical rows for a factor."""
    conn = sqlite3.connect(str(cache_db))
    try:
        latest_row = conn.execute("SELECT MAX(date) FROM daily_factor_returns").fetchone()
        latest = str(latest_row[0]).strip() if latest_row and latest_row[0] is not None else ""
        if not latest:
            return None, []
        latest_dt = date.fromisoformat(latest)
        start_dt = latest_dt - timedelta(days=365 * max(1, int(years)))
        rows = conn.execute(
            """
            SELECT date, factor_return
            FROM daily_factor_returns
            WHERE factor_name = ?
              AND date >= ?
            ORDER BY date
            """,
            (str(factor), start_dt.isoformat()),
        ).fetchall()
        out = [(str(dt), float(raw_ret or 0.0)) for dt, raw_ret in rows]
        return latest, out
    finally:
        conn.close()


def load_price_history_rows(
    data_db: Path,
    *,
    ric: str,
    years: int,
) -> tuple[str | None, list[tuple[str, float]]]:
    """Return latest price date and historical daily closes for a RIC."""
    conn = sqlite3.connect(str(data_db))
    try:
        latest_row = conn.execute(
            """
            SELECT MAX(date)
            FROM security_prices_eod
            WHERE ric = ?
            """,
            (str(ric),),
        ).fetchone()
        latest = str(latest_row[0]).strip() if latest_row and latest_row[0] is not None else ""
        if not latest:
            return None, []

        end_dt = date.fromisoformat(latest)
        start_dt = end_dt - timedelta(days=(366 * max(1, int(years))))
        rows = conn.execute(
            """
            SELECT date, CAST(close AS REAL) AS close
            FROM security_prices_eod
            WHERE ric = ?
              AND date >= ?
              AND date <= ?
              AND close IS NOT NULL
            ORDER BY date ASC
            """,
            (str(ric), start_dt.isoformat(), end_dt.isoformat()),
        ).fetchall()
        out = [(str(dt), float(close)) for dt, close in rows if dt is not None and close is not None]
        return latest, out
    finally:
        conn.close()
