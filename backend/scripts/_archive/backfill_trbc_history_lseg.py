"""Backfill historical TRBC classifications from LSEG for raw cross-section dates."""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


from backend.data.trbc_schema import ensure_trbc_naming
from lseg_ric_resolver import ensure_ric_map_table, load_ric_map, resolve_ric_map
from backend.trading_calendar import filter_xnys_sessions, previous_or_same_xnys_session

_DB_RAW = Path(os.getenv("DATA_DB_PATH", "data.db")).expanduser()
DEFAULT_DB = _DB_RAW if _DB_RAW.is_absolute() else (Path(__file__).resolve().parent.parent / _DB_RAW)
LSEG_BATCH_SIZE = 500
TABLE = "trbc_industry_history"
SQLITE_TIMEOUT_SECONDS = 120
SQLITE_BUSY_TIMEOUT_MS = 300000
SQLITE_MAX_RETRIES = 60
SQLITE_RETRY_SLEEP_SECONDS = 0.5


def _load_lseg_client():
    try:
        from backend.vendor.lseg_toolkit import LsegClient
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


def _to_local_ticker(ric: str) -> str:
    base = str(ric or "").strip().upper()
    return base.split(".", 1)[0] if base else ""


def _connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    return conn


def _existing_cols(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(r[1]) for r in rows}


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = _existing_cols(conn, table)
    if column in cols:
        return
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
    except sqlite3.OperationalError as exc:
        if "duplicate column name" not in str(exc).lower():
            raise


def _ensure_table(conn: sqlite3.Connection) -> None:
    ensure_trbc_naming(conn)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            ticker TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            trbc_industry_group TEXT,
            trbc_economic_sector TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT,
            PRIMARY KEY (ticker, as_of_date)
        )
        """
    )
    for col in ["trbc_business_sector", "trbc_industry", "trbc_activity"]:
        _ensure_column(conn, TABLE, col, "TEXT")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_date ON {TABLE}(as_of_date)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_ticker ON {TABLE}(ticker)")


def _resolve_dates(conn: sqlite3.Connection, start_date: str | None, end_date: str | None) -> list[str]:
    sql = "SELECT DISTINCT as_of_date FROM barra_raw_cross_section_history"
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
    out = [str(r[0]) for r in rows if r and r[0]]
    return filter_xnys_sessions(out)


def _resolve_tickers(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT DISTINCT ticker FROM barra_raw_cross_section_history ORDER BY ticker").fetchall()
    return [str(r[0]).strip().upper() for r in rows if r and str(r[0]).strip()]


def _insert_history_rows(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    payload = [
        (
            r["ticker"],
            r["as_of_date"],
            r.get("trbc_economic_sector"),
            r.get("trbc_business_sector"),
            r.get("trbc_industry_group"),
            r.get("trbc_industry"),
            r.get("trbc_activity"),
            r["source"],
            r["job_run_id"],
            r["updated_at"],
        )
        for r in rows
    ]
    sql = f"""
        INSERT OR REPLACE INTO {TABLE}
        (ticker, as_of_date, trbc_economic_sector, trbc_business_sector, trbc_industry_group, trbc_industry, trbc_activity, source, job_run_id, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    for attempt in range(SQLITE_MAX_RETRIES):
        try:
            conn.executemany(sql, payload)
            break
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt + 1 >= SQLITE_MAX_RETRIES:
                raise
            backoff = SQLITE_RETRY_SLEEP_SECONDS * (1 + (attempt % 6))
            time.sleep(backoff + random.uniform(0.0, 0.25))
    return len(rows)


def run_backfill(
    *,
    db_path: Path,
    ric_suffix: str,
    start_date: str | None = None,
    end_date: str | None = None,
    shard_count: int = 1,
    shard_index: int = 0,
    skip_sync: bool = False,
) -> dict[str, Any]:
    LsegClient = _load_lseg_client()

    conn = _connect_db(db_path)
    try:
        _ensure_table(conn)
        ensure_ric_map_table(conn)
        norm_start = previous_or_same_xnys_session(start_date) if start_date else None
        norm_end = previous_or_same_xnys_session(end_date) if end_date else None
        dates = _resolve_dates(conn, start_date=norm_start, end_date=norm_end)
        tickers = _resolve_tickers(conn)
        if not dates or not tickers:
            return {"status": "no-op", "dates": 0, "tickers": 0}
        _ = load_ric_map(conn)
    finally:
        conn.close()

    shard_count = max(1, int(shard_count))
    shard_index = int(shard_index)
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError(f"shard_index must be in [0, {shard_count - 1}]")
    if shard_count > 1:
        tickers = [
            t for t in tickers
            if int(hashlib.md5(t.encode("utf-8")).hexdigest(), 16) % shard_count == shard_index
        ]
        if not tickers:
            return {
                "status": "no-op",
                "dates": len(dates),
                "tickers": 0,
                "shard_index": shard_index,
                "shard_count": shard_count,
            }

    job_run_id = f"lseg_trbc_backfill_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    updated_at = datetime.now(timezone.utc).isoformat()

    total_rows = 0
    print(f"Backfilling historical TRBC for {len(tickers)} tickers across {len(dates)} dates...")
    with LsegClient() as client:
        conn = _connect_db(db_path)
        try:
            _ensure_table(conn)
            ensure_ric_map_table(conn)
            probe_date = dates[-1]
            ticker_to_ric = resolve_ric_map(
                client=client,
                conn=conn,
                tickers=tickers,
                as_of_date=probe_date,
                source="lseg_backfill",
                suffixes=[ric_suffix, ".N", ".O", ".A", ".K", ".P", ".PK"],
                batch_size=LSEG_BATCH_SIZE,
            )
            universe = [ticker_to_ric[t] for t in tickers if t in ticker_to_ric]
            ric_to_ticker = {v.upper(): k.upper() for k, v in ticker_to_ric.items() if v}

            for date_idx, as_of in enumerate(dates, start=1):
                rows: list[dict[str, Any]] = []
                for i in range(0, len(universe), LSEG_BATCH_SIZE):
                    batch = universe[i : i + LSEG_BATCH_SIZE]
                    part = client.get_company_data(
                        batch,
                        fields=[
                            "TR.TRBCEconomicSector",
                            "TR.TRBCBusinessSector",
                            "TR.TRBCIndustryGroup",
                            "TR.TRBCIndustry",
                            "TR.TRBCActivity",
                        ],
                        as_of_date=as_of,
                    )
                    if part is None or part.empty:
                        continue

                    instrument_col = _pick_col(part, ["Instrument"])
                    sector_col = _pick_col(part, ["TRBC Economic Sector Name", "TRBC Economic Sector"])
                    biz_col = _pick_col(part, ["TRBC Business Sector Name", "TRBC Business Sector"])
                    group_col = _pick_col(part, ["TRBC Industry Group Name", "TRBC Industry Group"])
                    industry_col = _pick_col(part, ["TRBC Industry Name", "TRBC Industry"])
                    activity_col = _pick_col(part, ["TRBC Activity Name", "TRBC Activity"])
                    if not instrument_col:
                        continue

                    for _, row in part.iterrows():
                        instrument = str(row.get(instrument_col) or "").strip().upper()
                        ticker = ric_to_ticker.get(instrument) or _to_local_ticker(instrument)
                        if not ticker:
                            continue

                        def _txt(v: Any) -> str | None:
                            if v is None or pd.isna(v):
                                return None
                            s = str(v).strip()
                            if not s or s.lower() in {"nan", "none"}:
                                return None
                            return s

                        rows.append(
                            {
                                "ticker": ticker,
                                "as_of_date": as_of,
                                "trbc_economic_sector": _txt(row.get(sector_col) if sector_col else None),
                                "trbc_business_sector": _txt(row.get(biz_col) if biz_col else None),
                                "trbc_industry_group": _txt(row.get(group_col) if group_col else None),
                                "trbc_industry": _txt(row.get(industry_col) if industry_col else None),
                                "trbc_activity": _txt(row.get(activity_col) if activity_col else None),
                                "source": "lseg_toolkit_backfill",
                                "job_run_id": job_run_id,
                                "updated_at": updated_at,
                            }
                        )

                inserted = _insert_history_rows(conn, rows)
                total_rows += inserted
                conn.commit()
                print(f"  {date_idx:>3}/{len(dates)} {as_of}: upserted {inserted:,} rows")

            ric_map_size = conn.execute("SELECT COUNT(*) FROM ticker_ric_map").fetchone()[0]
        finally:
            conn.close()

    out = {
        "status": "ok",
        "dates": len(dates),
        "tickers": len(tickers),
        "history_rows_upserted": total_rows,
        "ticker_ric_map_size": int(ric_map_size or 0),
        "db_path": str(db_path),
        "job_run_id": job_run_id,
        "shard_index": int(shard_index),
        "shard_count": int(shard_count),
        "skip_sync": bool(skip_sync),
    }
    print(out)
    return out


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill historical TRBC from LSEG.")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Path to target SQLite DB")
    p.add_argument("--ric-suffix", default=".O", help="Suffix when converting plain tickers to RICs")
    p.add_argument("--start-date", default=None, help="Optional YYYY-MM-DD lower bound for as_of_date")
    p.add_argument("--end-date", default=None, help="Optional YYYY-MM-DD upper bound for as_of_date")
    p.add_argument("--shard-count", type=int, default=1, help="Total number of ticker shards")
    p.add_argument("--shard-index", type=int, default=0, help="Zero-based shard index to process")
    p.add_argument("--skip-sync", action="store_true", help="Deprecated no-op (kept for backward CLI compatibility).")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_backfill(
        db_path=Path(args.db_path),
        ric_suffix=args.ric_suffix,
        start_date=args.start_date,
        end_date=args.end_date,
        shard_count=args.shard_count,
        shard_index=args.shard_index,
        skip_sync=bool(args.skip_sync),
    )
