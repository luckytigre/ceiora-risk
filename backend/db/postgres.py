"""Data queries for Barra dashboard.

Reads from local data.db maintained by scripts/download_data.py
(LSEG gatherer by default; legacy Postgres snapshot optional).
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

DATA_DB = Path(__file__).resolve().parent.parent / "data.db"


def _fetch_rows(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(DATA_DB))
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql, params or [])
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def _resolve_latest_barra_tuple() -> dict[str, str] | None:
    rows = _fetch_rows(
        """
        SELECT as_of_date, barra_model_version, descriptor_schema_version, assumption_set_version
        FROM barra_exposures
        ORDER BY as_of_date DESC, updated_at DESC
        LIMIT 1
        """,
    )
    if not rows:
        return None
    row = rows[0]
    return {
        "as_of_date": str(row.get("as_of_date") or ""),
        "barra_model_version": str(row.get("barra_model_version") or ""),
        "descriptor_schema_version": str(row.get("descriptor_schema_version") or ""),
        "assumption_set_version": str(row.get("assumption_set_version") or ""),
    }


def load_barra_exposures(tickers: list[str] | None = None) -> pd.DataFrame:
    params: list[Any] = []
    ticker_clause = ""
    if tickers:
        clean = [t.upper() for t in tickers if t.strip()]
        if clean:
            placeholders = ",".join("?" for _ in clean)
            ticker_clause = f" WHERE ticker IN ({placeholders})"
            params.extend(clean)

    rows = _fetch_rows(
        f"""
        WITH ranked AS (
            SELECT
                e.*,
                ROW_NUMBER() OVER (
                    PARTITION BY e.ticker
                    ORDER BY e.as_of_date DESC, e.updated_at DESC
                ) AS rn
            FROM barra_exposures e
            {ticker_clause}
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        ORDER BY ticker ASC
        """,
        params,
    )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def load_fundamental_snapshots(tickers: list[str] | None = None) -> pd.DataFrame:
    clean = [t.upper() for t in (tickers or []) if t.strip()]
    as_of = datetime.now(timezone.utc).date().isoformat()
    ticker_filter = ""
    params: list[Any] = [as_of]
    if clean:
        placeholders = ",".join("?" for _ in clean)
        ticker_filter = f" AND ticker IN ({placeholders})"
        params.extend(clean)

    rows = _fetch_rows(
        f"""
        WITH latest AS (
            SELECT ticker, MAX(fetch_date) AS fetch_date
            FROM fundamental_snapshots
            WHERE fetch_date <= ?
              {ticker_filter}
            GROUP BY ticker
        )
        SELECT f.*
        FROM fundamental_snapshots f
        JOIN latest l
          ON f.ticker = l.ticker
         AND f.fetch_date = l.fetch_date
        ORDER BY f.ticker ASC
        """,
        params,
    )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def load_latest_prices(tickers: list[str] | None = None) -> pd.DataFrame:
    clean = [t.upper() for t in (tickers or []) if t.strip()]
    ticker_filter = ""
    params: list[Any] = []
    if clean:
        placeholders = ",".join("?" for _ in clean)
        ticker_filter = f" WHERE ticker IN ({placeholders})"
        params = clean
    rows = _fetch_rows(
        f"""
        WITH latest AS (
            SELECT ticker, MAX(date) AS date
            FROM prices_daily
            {ticker_filter}
            GROUP BY ticker
        )
        SELECT p.ticker, p.date, CAST(p.close AS REAL) as close
        FROM prices_daily p
        JOIN latest l
          ON p.ticker = l.ticker
         AND p.date = l.date
        ORDER BY p.ticker ASC
        """,
        params,
    )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def load_source_dates() -> dict[str, str | None]:
    def _max_val(sql: str) -> str | None:
        rows = _fetch_rows(sql)
        if not rows:
            return None
        val = rows[0].get("latest")
        return str(val) if val else None

    return {
        "fundamentals_asof": _max_val("SELECT MAX(fetch_date) AS latest FROM fundamental_snapshots"),
        "exposures_asof": _max_val("SELECT MAX(as_of_date) AS latest FROM barra_exposures"),
    }
