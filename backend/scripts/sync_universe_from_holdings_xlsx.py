"""Sync universe tables from user-provided Derived Holdings XLSX files.

This script enforces a strict universe scope from the workbook RIC lists:
- Writes raw holdings to universe_candidate_holdings
- Replaces ticker_ric_map with candidate tickers/RICs
- Replaces universe_eligibility_summary and universe_constituent_snapshots
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

from db.universe_schema import ensure_universe_tables

CANDIDATE_TABLE = "universe_candidate_holdings"


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _norm_text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s or s.lower() in {"nan", "none", "null", "<na>"}:
        return ""
    return s


def _to_iso_date(v: Any) -> str:
    if v is None:
        return ""
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
        return ""
    return str(ts.date())


def _to_float(v: Any) -> float | None:
    try:
        out = float(v)
    except (TypeError, ValueError):
        return None
    if pd.isna(out):
        return None
    return out


def _parse_holdings_file(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Derived Holdings", header=None)
    if df.empty:
        return pd.DataFrame()

    as_of = _to_iso_date(df.iat[1, 1] if len(df) > 1 and df.shape[1] > 1 else None)

    header_idx = None
    for i in range(min(30, len(df))):
        if _norm_text(df.iat[i, 0]).upper() == "RIC":
            header_idx = i
            break
    if header_idx is None:
        return pd.DataFrame()

    cols = [
        _norm_text(x) or f"col_{j}"
        for j, x in enumerate(df.iloc[header_idx].tolist())
    ]
    body = df.iloc[header_idx + 1 :].copy()
    body.columns = cols
    if "RIC" not in body.columns:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["ric"] = body["RIC"].map(lambda x: _norm_text(x).upper())
    out["name"] = body["Name"].map(_norm_text) if "Name" in body.columns else ""
    out["country"] = body["Country"].map(_norm_text) if "Country" in body.columns else ""
    out["weight"] = body["Weight"].map(_to_float) if "Weight" in body.columns else None
    out["shares"] = body["No. Shares"].map(_to_float) if "No. Shares" in body.columns else None
    out["change"] = body["Change"].map(_to_float) if "Change" in body.columns else None
    out["source_file"] = path.name
    out["as_of_date"] = as_of

    # Treat only dotted values as canonical RICs.
    out = out[out["ric"].str.contains(".", regex=False)]
    out = out[out["ric"] != ""]
    out["ticker"] = out["ric"].str.split(".", n=1, expand=True)[0].str.upper().str.strip()
    out = out[out["ticker"] != ""]
    return out.reset_index(drop=True)


def _ensure_candidate_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {CANDIDATE_TABLE} (
            ticker TEXT NOT NULL,
            ric TEXT NOT NULL,
            as_of_date TEXT,
            name TEXT,
            country TEXT,
            weight REAL,
            shares REAL,
            change REAL,
            source_file TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT,
            PRIMARY KEY (ticker, ric, as_of_date, source_file)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{CANDIDATE_TABLE}_ticker ON {CANDIDATE_TABLE}(ticker)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{CANDIDATE_TABLE}_ric ON {CANDIDATE_TABLE}(ric)")


def _latest_universe_rows_by_ticker(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    if not _table_exists(conn, "universe_eligibility_summary"):
        return {}
    rows = conn.execute(
        """
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY UPPER(ticker)
                       ORDER BY COALESCE(current_snapshot_date, '') DESC,
                                COALESCE(updated_at, '') DESC,
                                rowid DESC
                   ) AS rn
            FROM universe_eligibility_summary
            WHERE ticker IS NOT NULL AND TRIM(ticker) <> ''
        )
        SELECT UPPER(ticker), permid, common_name, exchange_name
        FROM ranked
        WHERE rn = 1
        """
    ).fetchall()
    return {
        str(t): {
            "permid": permid,
            "common_name": common_name,
            "exchange_name": exchange_name,
        }
        for t, permid, common_name, exchange_name in rows
    }


def _pick_latest_candidates(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["as_of_sort"] = pd.to_datetime(x["as_of_date"], errors="coerce")
    x["weight_abs"] = x["weight"].abs().fillna(0.0)
    x = x.sort_values(["ticker", "as_of_sort", "weight_abs", "source_file"], ascending=[True, False, False, True])
    x = x.drop_duplicates(subset=["ticker"], keep="first")
    return x.drop(columns=["as_of_sort", "weight_abs"])


def sync_universe(*, db_path: Path, holdings_dir: Path) -> dict[str, Any]:
    files = sorted(holdings_dir.glob("*.xlsx"))
    if not files:
        raise RuntimeError(f"No .xlsx files found in {holdings_dir}")

    frames: list[pd.DataFrame] = []
    for p in files:
        frame = _parse_holdings_file(p)
        if not frame.empty:
            frames.append(frame)

    if not frames:
        raise RuntimeError("No parseable holdings rows found in provided xlsx files")

    holdings = pd.concat(frames, ignore_index=True)
    holdings = holdings.drop_duplicates(subset=["ticker", "ric", "as_of_date", "source_file"], keep="last")
    latest = _pick_latest_candidates(holdings)

    now_iso = datetime.now(timezone.utc).isoformat()
    job_run_id = f"universe_xlsx_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

    as_of_dates = sorted(d for d in holdings["as_of_date"].dropna().astype(str).unique().tolist() if d)
    min_asof = as_of_dates[0] if as_of_dates else datetime.now(timezone.utc).date().isoformat()
    max_asof = as_of_dates[-1] if as_of_dates else datetime.now(timezone.utc).date().isoformat()

    conn = sqlite3.connect(str(db_path), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")
    try:
        ensure_universe_tables(conn)
        _ensure_candidate_table(conn)
        existing_by_ticker = _latest_universe_rows_by_ticker(conn)

        # 1) Candidate raw table
        conn.execute(f"DELETE FROM {CANDIDATE_TABLE}")
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {CANDIDATE_TABLE} (
                ticker, ric, as_of_date, name, country, weight, shares, change,
                source_file, source, job_run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    str(r.ticker),
                    str(r.ric),
                    _norm_text(r.as_of_date) or max_asof,
                    _norm_text(r.name) or None,
                    _norm_text(r.country) or None,
                    _to_float(r.weight),
                    _to_float(r.shares),
                    _to_float(r.change),
                    str(r.source_file),
                    "user_holdings_xlsx",
                    job_run_id,
                    now_iso,
                )
                for r in holdings.itertuples(index=False)
            ],
        )

        # 2) ticker_ric_map strict replace
        conn.execute("DELETE FROM ticker_ric_map")
        conn.executemany(
            """
            INSERT OR REPLACE INTO ticker_ric_map (
                ticker, ric, resolution_method, classification_ok, as_of_date, source, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    str(r.ticker),
                    str(r.ric),
                    "user_holdings_xlsx",
                    1,
                    _norm_text(r.as_of_date) or max_asof,
                    "user_holdings_xlsx",
                    now_iso,
                )
                for r in latest.itertuples(index=False)
            ],
        )

        # 3) universe_eligibility_summary strict replace
        conn.execute("DELETE FROM universe_eligibility_summary")
        universe_rows = []
        for r in latest.itertuples(index=False):
            ticker = str(r.ticker)
            ric = str(r.ric)
            prev = existing_by_ticker.get(ticker, {})
            permid = _norm_text(prev.get("permid")) or f"RIC::{ric}"
            common_name = _norm_text(r.name) or _norm_text(prev.get("common_name"))
            exchange_name = _norm_text(prev.get("exchange_name"))
            universe_rows.append(
                (
                    permid,
                    ric,
                    ticker,
                    common_name or None,
                    exchange_name or None,
                    1,
                    max_asof,
                    None,
                    "eligible",
                    min_asof,
                    "9999-12-31",
                    1,
                    1,
                    max_asof,
                    min_asof,
                    1,
                    1,
                    "user_holdings_xlsx",
                    job_run_id,
                    now_iso,
                )
            )
        conn.executemany(
            """
            INSERT OR REPLACE INTO universe_eligibility_summary (
                permid, current_ric, ticker, common_name, exchange_name,
                instrument_is_active, last_quote_date, delisting_reason,
                eligibility_state, start_date, end_date,
                in_current_snapshot, in_historical_snapshot,
                current_snapshot_date, historical_snapshot_date,
                is_trading_day_active, is_eligible,
                source, job_run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            universe_rows,
        )

        # 4) universe_constituent_snapshots strict replace
        conn.execute("DELETE FROM universe_constituent_snapshots")
        snapshot_rows = []
        for r in holdings.itertuples(index=False):
            ticker = str(r.ticker)
            ric = str(r.ric)
            prev = existing_by_ticker.get(ticker, {})
            permid = _norm_text(prev.get("permid")) or f"RIC::{ric}"
            snapshot_rows.append(
                (
                    str(r.source_file),
                    _norm_text(r.as_of_date) or max_asof,
                    str(r.source_file),
                    "user_holdings_xlsx",
                    ric,
                    ric,
                    permid,
                    ticker,
                    _norm_text(r.name) or None,
                    _norm_text(prev.get("exchange_name")) or None,
                    1,
                    "user_holdings_xlsx",
                    job_run_id,
                    now_iso,
                )
            )
        conn.executemany(
            """
            INSERT OR REPLACE INTO universe_constituent_snapshots (
                snapshot_label, snapshot_date, input_identifier, retrieval_method,
                input_ric, resolved_ric, permid, ticker, common_name, exchange_name,
                instrument_is_active, source, job_run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            snapshot_rows,
        )

        conn.commit()

        return {
            "status": "ok",
            "job_run_id": job_run_id,
            "files": [p.name for p in files],
            "parsed_rows": int(len(holdings)),
            "distinct_tickers": int(holdings["ticker"].nunique()),
            "distinct_rics": int(holdings["ric"].nunique()),
            "latest_universe_tickers": int(latest["ticker"].nunique()),
            "as_of_min": min_asof,
            "as_of_max": max_asof,
            "table": CANDIDATE_TABLE,
        }
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync strict universe from holdings xlsx files")
    parser.add_argument("--db-path", default="backend/data.db", help="SQLite DB path")
    parser.add_argument(
        "--holdings-dir",
        default="Universe Candidates",
        help="Directory containing Derived Holdings *.xlsx files",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    out = sync_universe(
        db_path=Path(args.db_path).expanduser(),
        holdings_dir=Path(args.holdings_dir).expanduser(),
    )
    print(out)
