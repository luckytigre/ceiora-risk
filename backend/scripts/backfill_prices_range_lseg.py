#!/usr/bin/env python3
"""Extend canonical security_prices_eod backward with LSEG range pulls in manageable batches."""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "vendor"))

import lseg.data as rd

from cuse4.schema import PRICES_TABLE, SECURITY_MASTER_TABLE, ensure_cuse4_schema
from trading_calendar import filter_xnys_sessions, previous_or_same_xnys_session

SQLITE_TIMEOUT_SECONDS = 120
SQLITE_BUSY_TIMEOUT_MS = 120000
SQLITE_MAX_RETRIES = 6
SQLITE_RETRY_SLEEP_SECONDS = 1.5


def _connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    return conn


def _existing_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [str(r[1]) for r in cur.fetchall()]


def _insert_rows(conn: sqlite3.Connection, table: str, rows: list[dict[str, Any]], *, replace: bool) -> int:
    if not rows:
        return 0
    cols = _existing_cols(conn, table)
    use_cols = [c for c in cols if c in rows[0]]
    placeholders = ",".join("?" for _ in use_cols)
    insert_kw = "INSERT OR REPLACE" if replace else "INSERT"
    sql = f'{insert_kw} INTO {table} ({",".join(use_cols)}) VALUES ({placeholders})'
    payload = [tuple(r.get(c) for c in use_cols) for r in rows]
    for attempt in range(SQLITE_MAX_RETRIES):
        try:
            conn.executemany(sql, payload)
            return len(rows)
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt + 1 >= SQLITE_MAX_RETRIES:
                raise
            time.sleep(SQLITE_RETRY_SLEEP_SECONDS * (attempt + 1))
    return len(rows)


def _iter_date_windows(start_date: str, end_date: str, days_per_window: int) -> list[tuple[str, str]]:
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    windows: list[tuple[str, str]] = []
    cur = start
    step = pd.Timedelta(days=max(1, int(days_per_window)))
    while cur <= end:
        w_end = min(cur + step - pd.Timedelta(days=1), end)
        windows.append((cur.date().isoformat(), w_end.date().isoformat()))
        cur = w_end + pd.Timedelta(days=1)
    return windows


def backfill_prices(
    *,
    db_path: Path,
    start_date: str,
    end_date: str,
    ticker_batch_size: int,
    days_per_window: int,
    max_retries: int,
    sleep_seconds: float,
    rics_csv: str | None = None,
) -> dict[str, int | str]:
    conn = _connect_db(db_path)
    ensure_cuse4_schema(conn)

    try:
        where = [
            "COALESCE(classification_ok, 0) = 1",
            "COALESCE(is_equity_eligible, 0) = 1",
            "ric IS NOT NULL",
            "TRIM(ric) <> ''",
        ]
        params: list[Any] = []
        if rics_csv:
            req = [str(r).strip().upper() for r in str(rics_csv).split(",") if str(r).strip()]
            if req:
                placeholders = ",".join("?" for _ in req)
                where.append(f"UPPER(TRIM(ric)) IN ({placeholders})")
                params.extend(req)

        universe = conn.execute(
            f"""
            SELECT sid, UPPER(TRIM(ric)) AS ric
            FROM {SECURITY_MASTER_TABLE}
            WHERE {' AND '.join(where)}
            ORDER BY ric
            """,
            params,
        ).fetchall()
        ric_to_sid = {str(r[1]): str(r[0]) for r in universe if r and r[0] and r[1]}
        rics = sorted(ric_to_sid.keys())

        if not rics:
            return {"status": "no-universe"}

        windows = _iter_date_windows(start_date=start_date, end_date=end_date, days_per_window=days_per_window)
        if not windows:
            return {"status": "no-date-windows"}

        start_bound = str(pd.Timestamp(start_date).date())
        end_bound = str(pd.Timestamp(end_date).date())

        rows_upserted = 0
        batch_calls = 0
        failed_batches = 0
        now_iso = datetime.now(timezone.utc).isoformat()

        rd.open_session()
        try:
            for w_start, w_end in windows:
                print({"window_start": w_start, "window_end": w_end}, flush=True)
                for i in range(0, len(rics), max(1, int(ticker_batch_size))):
                    batch = rics[i : i + max(1, int(ticker_batch_size))]
                    ok = False
                    for attempt in range(max(1, int(max_retries)) + 1):
                        try:
                            df = rd.get_data(
                                universe=batch,
                                fields=["TR.PriceClose.date", "TR.PriceClose"],
                                parameters={"SDate": w_start, "EDate": w_end},
                            )
                            if df is None or df.empty:
                                ok = True
                                break

                            inst_col = next((c for c in df.columns if str(c).lower() == "instrument"), None)
                            date_col = next((c for c in df.columns if str(c).lower() in {"date", "calc date"}), None)
                            price_col = next((c for c in df.columns if str(c).lower() == "price close"), None)
                            if not inst_col or not date_col or not price_col:
                                raise RuntimeError(f"unexpected columns: {list(df.columns)}")

                            rows: list[dict[str, Any]] = []
                            for _, row in df.iterrows():
                                ric = str(row.get(inst_col) or "").strip().upper()
                                sid = ric_to_sid.get(ric)
                                if not sid:
                                    continue
                                d = row.get(date_col)
                                if pd.isna(d) or d is None:
                                    continue
                                ds = str(pd.Timestamp(d).date())
                                if ds < start_bound or ds > end_bound:
                                    continue
                                close = row.get(price_col)
                                close_val = None if pd.isna(close) else float(close)
                                rows.append(
                                    {
                                        "sid": sid,
                                        "date": ds,
                                        "open": close_val,
                                        "high": close_val,
                                        "low": close_val,
                                        "close": close_val,
                                        "adj_close": close_val,
                                        "volume": None,
                                        "currency": None,
                                        "exchange": None,
                                        "source": "lseg_toolkit_history",
                                        "updated_at": now_iso,
                                    }
                                )

                            if rows:
                                # Keep only XNYS sessions.
                                valid_dates = set(filter_xnys_sessions([r["date"] for r in rows]))
                                rows = [r for r in rows if r["date"] in valid_dates]
                                rows_upserted += _insert_rows(conn, PRICES_TABLE, rows, replace=True)
                                conn.commit()

                            ok = True
                            break
                        except Exception as exc:
                            print(
                                {
                                    "window_start": w_start,
                                    "window_end": w_end,
                                    "batch_start": i,
                                    "batch_size": len(batch),
                                    "attempt": attempt + 1,
                                    "error": str(exc),
                                },
                                flush=True,
                            )
                            if attempt < int(max_retries):
                                time.sleep(float(sleep_seconds))

                    batch_calls += 1
                    if not ok:
                        failed_batches += 1
                        continue

                print({"window_start": w_start, "window_end": w_end, "rows_upserted_so_far": rows_upserted}, flush=True)
        finally:
            rd.close_session()

        norm_end = previous_or_same_xnys_session(end_date)

        return {
            "status": "ok",
            "rows_upserted": int(rows_upserted),
            "batch_calls": int(batch_calls),
            "failed_batches": int(failed_batches),
            "start_date": start_bound,
            "end_date": norm_end,
        }
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", type=Path, default=Path("backend/data.db"), help="Path to SQLite DB")
    p.add_argument("--start-date", default="2012-01-01", help="Backfill start date")
    p.add_argument("--end-date", default="2016-02-15", help="Backfill end date")
    p.add_argument("--ticker-batch-size", type=int, default=180, help="RIC batch size")
    p.add_argument("--days-per-window", type=int, default=180, help="Date window size in days")
    p.add_argument("--max-retries", type=int, default=1, help="Retries per failed batch")
    p.add_argument("--sleep-seconds", type=float, default=2.0, help="Sleep between retries")
    p.add_argument("--rics", default=None, help="Optional comma-separated RIC subset")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    out = backfill_prices(
        db_path=args.db_path,
        start_date=str(args.start_date),
        end_date=str(args.end_date),
        ticker_batch_size=max(1, int(args.ticker_batch_size)),
        days_per_window=max(1, int(args.days_per_window)),
        max_retries=max(0, int(args.max_retries)),
        sleep_seconds=max(0.0, float(args.sleep_seconds)),
        rics_csv=(str(args.rics).strip() if args.rics else None),
    )
    print(out)
