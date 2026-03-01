"""Backfill historical GICS industry groups from LSEG for all exposure dates.

This script creates and populates `gics_industry_history` and then syncs
`barra_exposures.gics_industry_group` so each (ticker, as_of_date) cross-section
has a point-in-time industry classification.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "vendor"))

from barra.gics_mapping import map_to_industry_group

DEFAULT_DB = Path(__file__).resolve().parent.parent / "data.db"
LSEG_BATCH_SIZE = 500
TABLE = "gics_industry_history"


def _load_lseg_client():
    try:
        from lseg_toolkit import LsegClient
    except Exception as exc:
        raise RuntimeError(
            "Unable to import lseg_toolkit/LSEG runtime. "
            "Ensure vendored toolkit is present and `lseg-data` is installed."
        ) from exc
    return LsegClient


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        got = cols.get(c.lower())
        if got:
            return got
    return None


def _to_ric(symbol: str, ric_suffix: str) -> str:
    s = symbol.strip().upper()
    if "." in s:
        return s
    return f"{s}{ric_suffix}"


def _to_local_ticker(ric: str) -> str:
    base = str(ric or "").strip().upper()
    return base.split(".", 1)[0] if base else ""


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
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
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_date ON {TABLE}(as_of_date)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_ticker ON {TABLE}(ticker)")


def _resolve_dates(conn: sqlite3.Connection, start_date: str | None, end_date: str | None) -> list[str]:
    sql = "SELECT DISTINCT as_of_date FROM barra_exposures"
    clauses: list[str] = []
    params: list[Any] = []
    if start_date:
        clauses.append("as_of_date >= ?")
        params.append(start_date)
    if end_date:
        clauses.append("as_of_date <= ?")
        params.append(end_date)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY as_of_date"
    rows = conn.execute(sql, params).fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def _resolve_tickers(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT DISTINCT ticker FROM barra_exposures ORDER BY ticker").fetchall()
    return [str(r[0]).strip().upper() for r in rows if r and str(r[0]).strip()]


def _insert_history_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    conn.executemany(
        f"""
        INSERT OR REPLACE INTO {TABLE}
        (ticker, as_of_date, gics_industry_group, trbc_economic_sector, source, job_run_id, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r["ticker"],
                r["as_of_date"],
                r["gics_industry_group"],
                r["trbc_economic_sector"],
                r["source"],
                r["job_run_id"],
                r["updated_at"],
            )
            for r in rows
        ],
    )
    return len(rows)


def _sync_barra_exposures(conn: sqlite3.Connection) -> dict[str, int]:
    """Update barra_exposures.gics_industry_group from history with robust fill."""
    exp_df = pd.read_sql_query(
        """
        SELECT rowid, ticker, as_of_date, gics_industry_group
        FROM barra_exposures
        ORDER BY ticker, as_of_date
        """,
        conn,
    )
    if exp_df.empty:
        return {"updated_rows": 0}

    hist_df = pd.read_sql_query(
        f"""
        SELECT ticker, as_of_date, gics_industry_group
        FROM {TABLE}
        WHERE gics_industry_group IS NOT NULL
          AND TRIM(gics_industry_group) <> ''
        ORDER BY ticker, as_of_date
        """,
        conn,
    )
    if hist_df.empty:
        return {"updated_rows": 0}

    exp_df["ticker"] = exp_df["ticker"].astype(str).str.upper()
    exp_df["as_of_date"] = exp_df["as_of_date"].astype(str)
    exp_df["existing"] = (
        exp_df["gics_industry_group"]
        .astype(str)
        .str.strip()
        .replace({"": np.nan, "None": np.nan, "nan": np.nan})
    )
    hist_df["ticker"] = hist_df["ticker"].astype(str).str.upper()
    hist_df["as_of_date"] = hist_df["as_of_date"].astype(str)
    hist_df["gics_industry_group"] = (
        hist_df["gics_industry_group"]
        .astype(str)
        .str.strip()
        .replace({"": np.nan, "None": np.nan, "nan": np.nan})
    )
    hist_df = hist_df.dropna(subset=["gics_industry_group"])

    exact = exp_df.merge(
        hist_df,
        on=["ticker", "as_of_date"],
        how="left",
        suffixes=("", "_hist"),
    )
    exact["resolved"] = exact["gics_industry_group_hist"].where(
        exact["gics_industry_group_hist"].notna(),
        exact["existing"],
    )
    exact["resolved"] = exact["resolved"].astype("object")

    # Fill unresolved points by nearest available history within ticker chronology.
    for ticker, idx in exact.groupby("ticker", sort=False).groups.items():
        loc = list(idx)
        grp = exact.loc[loc].sort_values("as_of_date")
        filled = grp["resolved"].replace({"": np.nan}).ffill().bfill()
        exact.loc[grp.index, "resolved"] = filled.to_numpy()

    exact["resolved"] = exact["resolved"].fillna("Unmapped").astype(str)
    exact["current"] = exact["existing"].fillna("Unmapped").astype(str)
    to_update = exact.loc[exact["resolved"] != exact["current"], ["rowid", "resolved"]]
    if to_update.empty:
        return {"updated_rows": 0}

    conn.executemany(
        "UPDATE barra_exposures SET gics_industry_group = ? WHERE rowid = ?",
        [(str(r["resolved"]), int(r["rowid"])) for _, r in to_update.iterrows()],
    )
    return {"updated_rows": int(len(to_update))}


def run_backfill(
    *,
    db_path: Path,
    ric_suffix: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    LsegClient = _load_lseg_client()
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        _ensure_table(conn)
        dates = _resolve_dates(conn, start_date=start_date, end_date=end_date)
        tickers = _resolve_tickers(conn)
        if not dates or not tickers:
            return {"status": "no-op", "dates": 0, "tickers": 0}
        universe = [_to_ric(t, ric_suffix) for t in tickers]
    finally:
        conn.close()

    job_run_id = f"lseg_gics_backfill_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    updated_at = datetime.now(timezone.utc).isoformat()

    total_rows = 0
    print(f"Backfilling historical industry groups for {len(tickers)} tickers across {len(dates)} dates...")
    with LsegClient() as client:
        conn = sqlite3.connect(str(db_path))
        try:
            _ensure_table(conn)
            for date_idx, as_of in enumerate(dates, start=1):
                rows: list[dict[str, Any]] = []
                for i in range(0, len(universe), LSEG_BATCH_SIZE):
                    batch = universe[i : i + LSEG_BATCH_SIZE]
                    part = client.get_company_data(
                        batch,
                        fields=[
                            "TR.TRBCEconomicSector",
                            "TR.TRBCIndustryGroup",
                        ],
                        as_of_date=as_of,
                    )
                    if part is None or part.empty:
                        continue

                    instrument_col = _pick_col(part, ["Instrument"])
                    sector_col = _pick_col(part, ["TRBC Economic Sector Name", "TRBC Economic Sector"])
                    industry_col = _pick_col(part, ["TRBC Industry Group Name", "TRBC Industry Group"])
                    if not instrument_col:
                        continue
                    for _, row in part.iterrows():
                        ticker = _to_local_ticker(str(row.get(instrument_col) or ""))
                        if not ticker:
                            continue
                        industry = row.get(industry_col) if industry_col else None
                        sector = row.get(sector_col) if sector_col else None
                        industry_group = map_to_industry_group(
                            sector=None if pd.isna(sector) else str(sector),
                            industry=None if pd.isna(industry) else str(industry),
                            ticker=ticker,
                        )
                        rows.append(
                            {
                                "ticker": ticker,
                                "as_of_date": as_of,
                                "gics_industry_group": industry_group,
                                "trbc_economic_sector": None if pd.isna(sector) else str(sector),
                                "source": "lseg_toolkit_backfill",
                                "job_run_id": job_run_id,
                                "updated_at": updated_at,
                            }
                        )
                inserted = _insert_history_rows(conn, rows)
                total_rows += inserted
                conn.commit()
                print(f"  {date_idx:>3}/{len(dates)} {as_of}: upserted {inserted:,} rows")

            sync_stats = _sync_barra_exposures(conn)
            conn.commit()
        finally:
            conn.close()

    out = {
        "status": "ok",
        "dates": len(dates),
        "tickers": len(tickers),
        "history_rows_upserted": total_rows,
        "barra_exposures_updates": int(sync_stats.get("updated_rows", 0)),
        "db_path": str(db_path),
        "job_run_id": job_run_id,
    }
    print(out)
    return out


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill historical GICS industry groups from LSEG.")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Path to target SQLite DB")
    p.add_argument("--ric-suffix", default=".O", help="Suffix when converting plain tickers to RICs")
    p.add_argument("--start-date", default=None, help="Optional YYYY-MM-DD lower bound for as_of_date")
    p.add_argument("--end-date", default=None, help="Optional YYYY-MM-DD upper bound for as_of_date")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_backfill(
        db_path=Path(args.db_path),
        ric_suffix=args.ric_suffix,
        start_date=args.start_date,
        end_date=args.end_date,
    )
