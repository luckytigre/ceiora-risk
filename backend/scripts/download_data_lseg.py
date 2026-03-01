"""Update local SQLite market/fundamental data using jl-lseg-toolkit.

This script is intentionally schema-compatible with the existing dashboard DB:
  - updates `fundamental_snapshots` (market_cap/sector + basic fields)
  - updates `prices_daily` (latest close snapshot)

It does NOT overwrite `barra_exposures`. Factor exposures are handled separately.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "vendor"))

from barra.gics_mapping import map_to_industry_group
from portfolio.mock_portfolio import get_tickers

DEFAULT_DB = Path(__file__).resolve().parent.parent / "data.db"
LSEG_BATCH_SIZE = 500


def _to_ric(symbol: str, ric_suffix: str) -> str:
    s = symbol.strip().upper()
    if not s:
        return s
    if "." in s:
        return s
    return f"{s}{ric_suffix}"


def _to_local_ticker(ric: str) -> str:
    base = str(ric or "").strip().upper()
    if not base:
        return base
    return base.split(".", 1)[0]


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        got = cols.get(c.lower())
        if got:
            return got
    return None


def _ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fundamental_snapshots (
            ticker TEXT,
            fetch_date TEXT,
            market_cap TEXT,
            shares_outstanding TEXT,
            dividend_yield TEXT,
            sector TEXT,
            industry TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prices_daily (
            ticker TEXT,
            date TEXT,
            open TEXT,
            high TEXT,
            low TEXT,
            close TEXT,
            adj_close TEXT,
            volume TEXT,
            currency TEXT,
            exchange TEXT,
            source TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gics_industry_history (
            ticker TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            gics_industry_group TEXT,
            trbc_economic_sector TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT,
            PRIMARY KEY (ticker, as_of_date)
        )
        """
    )


def _existing_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [str(r[1]) for r in cur.fetchall()]


def _insert_rows(
    conn: sqlite3.Connection,
    table: str,
    rows: list[dict[str, Any]],
    *,
    replace: bool = False,
) -> int:
    if not rows:
        return 0
    cols = _existing_cols(conn, table)
    use_cols = [c for c in cols if c in rows[0]]
    if not use_cols:
        return 0
    placeholders = ",".join("?" for _ in use_cols)
    insert_kw = "INSERT OR REPLACE" if replace else "INSERT"
    sql = f'{insert_kw} INTO {table} ({",".join(use_cols)}) VALUES ({placeholders})'
    conn.executemany(sql, [tuple(r.get(c) for c in use_cols) for r in rows])
    return len(rows)


def _resolve_universe(
    *,
    db_path: Path,
    index: str | None,
    tickers_csv: str | None,
    ric_suffix: str,
) -> list[str]:
    if tickers_csv:
        tickers = [t.strip().upper() for t in tickers_csv.split(",") if t.strip()]
        return [_to_ric(t, ric_suffix) for t in tickers]
    if index:
        LsegClient = _load_lseg_client()

        with LsegClient() as client:
            return client.get_index_constituents(index=index)
    # Default: use the local barra universe if available, else fallback to mock portfolio.
    try:
        conn = sqlite3.connect(str(db_path))
        try:
            rows = conn.execute("SELECT DISTINCT ticker FROM barra_exposures ORDER BY ticker").fetchall()
        finally:
            conn.close()
        if rows:
            tickers = [str(r[0]).strip().upper() for r in rows if r and str(r[0]).strip()]
            if tickers:
                return [_to_ric(t, ric_suffix) for t in tickers]
    except Exception:
        pass
    return [_to_ric(t, ric_suffix) for t in get_tickers()]


def _load_lseg_client():
    try:
        from lseg_toolkit import LsegClient
    except Exception as exc:
        raise RuntimeError(
            "Unable to import lseg_toolkit/LSEG runtime. "
            "Ensure vendored toolkit is present and `lseg-data` is installed."
        ) from exc
    return LsegClient


def download_from_lseg(
    *,
    db_path: Path = DEFAULT_DB,
    index: str | None = None,
    tickers_csv: str | None = None,
    ric_suffix: str = ".O",
) -> dict[str, Any]:
    LsegClient = _load_lseg_client()

    as_of = datetime.now(timezone.utc).date().isoformat()
    updated_at = datetime.now(timezone.utc).isoformat()
    universe = _resolve_universe(
        db_path=db_path,
        index=index,
        tickers_csv=tickers_csv,
        ric_suffix=ric_suffix,
    )
    if not universe:
        return {"status": "no-universe", "as_of": as_of}

    print(f"Fetching LSEG data for {len(universe)} instruments...")
    company_parts: list[pd.DataFrame] = []
    with LsegClient() as client:
        for i in range(0, len(universe), LSEG_BATCH_SIZE):
            batch = universe[i : i + LSEG_BATCH_SIZE]
            part = client.get_company_data(
                batch,
                fields=[
                    "TR.CommonName",
                    "TR.TRBCEconomicSector",
                    "TR.TRBCIndustryGroup",
                    "TR.PriceClose",
                    "TR.CompanyMarketCap",
                    "TR.SharesOutstanding",
                    "TR.DividendYield",
                ],
                as_of_date=as_of,
            )
            if part is not None and not part.empty:
                company_parts.append(part)
            done = min(i + LSEG_BATCH_SIZE, len(universe))
            print(f"  company_data: {done:,}/{len(universe):,}")

    company = pd.concat(company_parts, ignore_index=True) if company_parts else pd.DataFrame()

    if company is None or company.empty:
        return {"status": "no-data", "as_of": as_of, "universe": len(universe)}

    instrument_col = _pick_col(company, ["Instrument"])
    price_col = _pick_col(company, ["Price Close"])
    mcap_col = _pick_col(company, ["Company Market Cap"])
    sector_col = _pick_col(company, ["TRBC Economic Sector Name", "TRBC Economic Sector"])
    industry_col = _pick_col(company, ["TRBC Industry Group Name", "TRBC Industry Group"])
    shares_col = _pick_col(company, ["Shares Outstanding", "Shares Outstanding - Common Stock"])
    divy_col = _pick_col(company, ["Dividend Yield"])
    if not instrument_col:
        raise RuntimeError("LSEG response missing Instrument column")

    job_run_id = f"lseg_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    fundamentals_rows: list[dict[str, Any]] = []
    prices_rows: list[dict[str, Any]] = []
    gics_history_rows: list[dict[str, Any]] = []

    for _, row in company.iterrows():
        ric = str(row.get(instrument_col) or "").strip()
        ticker = _to_local_ticker(ric)
        if not ticker:
            continue
        close = row.get(price_col) if price_col else None
        market_cap = row.get(mcap_col) if mcap_col else None
        sector = row.get(sector_col) if sector_col else None
        industry = row.get(industry_col) if industry_col else None
        shares_outstanding = row.get(shares_col) if shares_col else None
        dividend_yield = row.get(divy_col) if divy_col else None

        industry_group = map_to_industry_group(
            sector=None if pd.isna(sector) else str(sector),
            industry=None if pd.isna(industry) else str(industry),
            ticker=ticker,
        )

        fundamentals_rows.append(
            {
                "ticker": ticker,
                "fetch_date": as_of,
                "market_cap": None if pd.isna(market_cap) else str(market_cap),
                "shares_outstanding": None if pd.isna(shares_outstanding) else str(shares_outstanding),
                "dividend_yield": None if pd.isna(dividend_yield) else str(dividend_yield),
                "sector": None if pd.isna(sector) else str(sector),
                "industry": None if pd.isna(industry) else str(industry),
                "source": "lseg_toolkit",
                "job_run_id": job_run_id,
                "updated_at": updated_at,
            }
        )
        gics_history_rows.append(
            {
                "ticker": ticker,
                "as_of_date": as_of,
                "gics_industry_group": industry_group,
                "trbc_economic_sector": None if pd.isna(sector) else str(sector),
                "source": "lseg_toolkit",
                "job_run_id": job_run_id,
                "updated_at": updated_at,
            }
        )

        prices_rows.append(
            {
                "ticker": ticker,
                "date": as_of,
                "open": None if pd.isna(close) else str(close),
                "high": None if pd.isna(close) else str(close),
                "low": None if pd.isna(close) else str(close),
                "close": None if pd.isna(close) else str(close),
                "adj_close": None if pd.isna(close) else str(close),
                "volume": None,
                "currency": None,
                "exchange": None,
                "source": "lseg_toolkit",
                "updated_at": updated_at,
            }
        )

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        _ensure_tables(conn)
        n_f = _insert_rows(conn, "fundamental_snapshots", fundamentals_rows)
        n_p = _insert_rows(conn, "prices_daily", prices_rows)
        n_g = _insert_rows(conn, "gics_industry_history", gics_history_rows, replace=True)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fund_ticker ON fundamental_snapshots(ticker)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fund_date ON fundamental_snapshots(fetch_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_ticker ON prices_daily(ticker)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON prices_daily(date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices_daily(ticker, date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gics_hist_date ON gics_industry_history(as_of_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gics_hist_ticker ON gics_industry_history(ticker)")
        conn.commit()
    finally:
        conn.close()

    out = {
        "status": "ok",
        "as_of": as_of,
        "universe": len(universe),
        "fundamental_rows_inserted": n_f,
        "price_rows_inserted": n_p,
        "gics_rows_inserted": n_g,
        "db_path": str(db_path),
    }
    print(out)
    return out


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Update local SQLite data using jl-lseg-toolkit.")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Path to target SQLite DB")
    p.add_argument("--index", default=None, help="Index code (e.g. SPX, NDX). If set, uses index constituents")
    p.add_argument("--tickers", default=None, help="Comma-separated plain tickers to fetch")
    p.add_argument("--ric-suffix", default=".O", help="Suffix when converting plain tickers to RICs")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    download_from_lseg(
        db_path=Path(args.db_path),
        index=args.index,
        tickers_csv=args.tickers,
        ric_suffix=args.ric_suffix,
    )
