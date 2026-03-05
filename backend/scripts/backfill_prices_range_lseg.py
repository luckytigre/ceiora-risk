#!/usr/bin/env python3
"""Extend canonical security_prices_eod backward with LSEG range pulls in manageable batches.

Volume policy:
- Standard OHLCV pulls use `TR.Volume` when available.
- Volume-repair mode (`--volume-only`) writes `TR.AvgDailyVolume3Month` into
  `security_prices_eod.volume` for higher historical coverage in LSEG range pulls.
"""

from __future__ import annotations

import argparse
import sqlite3
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


import lseg.data as rd

from backend.cuse4.schema import PRICES_TABLE, SECURITY_MASTER_TABLE, ensure_cuse4_schema
from backend.trading_calendar import filter_xnys_sessions, previous_or_same_xnys_session

warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    module=r"lseg\.data\._tools\._dataframe",
)

SQLITE_TIMEOUT_SECONDS = 120
SQLITE_BUSY_TIMEOUT_MS = 120000
SQLITE_MAX_RETRIES = 6
SQLITE_RETRY_SLEEP_SECONDS = 1.5
VOLUME_REPAIR_FIELD = "TR.AvgDailyVolume3Month"


def _pick_col(df: pd.DataFrame, names: list[str]) -> str | None:
    lookup = {str(c).strip().lower(): str(c) for c in df.columns}
    for name in names:
        key = str(name).strip().lower()
        if key in lookup:
            return lookup[key]
    return None


def _float_or_none(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str) and not value.strip():
        return None
    try:
        return float(value)
    except Exception:
        return None


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


def _update_volume_rows(
    conn: sqlite3.Connection,
    rows: list[tuple[float, str, str, str]],
    *,
    only_null_volume: bool,
) -> int:
    if not rows:
        return 0
    where_clause = "AND volume IS NULL" if only_null_volume else ""
    sql = f"""
        UPDATE {PRICES_TABLE}
        SET volume = ?, updated_at = ?, source = COALESCE(source, 'lseg_toolkit_history')
        WHERE ric = ? AND date = ? {where_clause}
    """
    before = conn.total_changes
    for attempt in range(SQLITE_MAX_RETRIES):
        try:
            conn.executemany(sql, rows)
            break
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt + 1 >= SQLITE_MAX_RETRIES:
                raise
            time.sleep(SQLITE_RETRY_SLEEP_SECONDS * (attempt + 1))
    return int(conn.total_changes - before)


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


def _load_missing_volume_pairs(
    conn: sqlite3.Connection,
    *,
    start_bound: str,
    end_bound: str,
    rics_csv: str | None,
) -> pd.DataFrame:
    where_mv = [
        "date >= ?",
        "date <= ?",
        "volume IS NULL",
    ]
    mv_params: list[Any] = [start_bound, end_bound]
    if rics_csv:
        req = [str(r).strip().upper() for r in str(rics_csv).split(",") if str(r).strip()]
        if req:
            placeholders = ",".join("?" for _ in req)
            where_mv.append(f"UPPER(TRIM(ric)) IN ({placeholders})")
            mv_params.extend(req)
    out = pd.read_sql_query(
        f"""
        SELECT UPPER(TRIM(ric)) AS ric, date
        FROM {PRICES_TABLE}
        WHERE {' AND '.join(where_mv)}
        ORDER BY date, ric
        """,
        conn,
        params=mv_params,
    )
    if not out.empty:
        out["date"] = out["date"].astype(str)
    return out


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
    volume_only: bool = False,
    only_null_volume: bool = False,
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
            SELECT UPPER(TRIM(ric)) AS ric
            FROM {SECURITY_MASTER_TABLE}
            WHERE {' AND '.join(where)}
            ORDER BY ric
            """,
            params,
        ).fetchall()
        rics = sorted({str(r[0]) for r in universe if r and r[0]})
        ric_set = set(rics)

        if not rics:
            return {"status": "no-universe"}

        windows = _iter_date_windows(start_date=start_date, end_date=end_date, days_per_window=days_per_window)
        if not windows:
            return {"status": "no-date-windows"}

        start_bound = str(pd.Timestamp(start_date).date())
        end_bound = str(pd.Timestamp(end_date).date())

        rows_upserted = 0
        volume_rows_updated = 0
        batch_calls = 0
        failed_batches = 0
        now_iso = datetime.now(timezone.utc).isoformat()
        missing_volume_df = pd.DataFrame()

        if volume_only and only_null_volume:
            missing_volume_df = _load_missing_volume_pairs(
                conn,
                start_bound=start_bound,
                end_bound=end_bound,
                rics_csv=rics_csv,
            )
            if missing_volume_df.empty:
                return {
                    "status": "no-null-volume",
                    "start_date": start_bound,
                    "end_date": previous_or_same_xnys_session(end_bound),
                }
            # `TR.AvgDailyVolume3Month` behaves reliably on single-day pulls; force one-day windows.
            unique_dates = sorted({str(d) for d in missing_volume_df["date"].tolist() if str(d).strip()})
            windows = [(d, d) for d in unique_dates]

        rd.open_session()
        try:
            for w_start, w_end in windows:
                window_pairs: set[tuple[str, str]] | None = None
                window_rics = rics
                if volume_only and only_null_volume and not missing_volume_df.empty:
                    wmask = (missing_volume_df["date"] >= w_start) & (missing_volume_df["date"] <= w_end)
                    subset = missing_volume_df.loc[wmask, ["ric", "date"]]
                    if subset.empty:
                        continue
                    window_pairs = set((str(r.ric), str(r.date)) for r in subset.itertuples(index=False))
                    window_rics = sorted({str(r.ric) for r in subset.itertuples(index=False)})

                print({"window_start": w_start, "window_end": w_end}, flush=True)
                for i in range(0, len(window_rics), max(1, int(ticker_batch_size))):
                    batch = window_rics[i : i + max(1, int(ticker_batch_size))]
                    ok = False
                    for attempt in range(max(1, int(max_retries)) + 1):
                        try:
                            if volume_only:
                                field_sets = [
                                    ["TR.PriceClose.date", VOLUME_REPAIR_FIELD],
                                ]
                            else:
                                field_sets = [
                                    [
                                        "TR.PriceClose.date",
                                        "TR.PriceOpen",
                                        "TR.PriceHigh",
                                        "TR.PriceLow",
                                        "TR.PriceClose",
                                        "TR.Volume",
                                        "TR.PriceClose.currency",
                                    ],
                                    ["TR.PriceClose.date", "TR.PriceClose", "TR.Volume"],
                                    ["TR.PriceClose.date", "TR.PriceClose"],
                                ]
                            df = None
                            last_field_error: Exception | None = None
                            for fields in field_sets:
                                try:
                                    df = rd.get_data(
                                        universe=batch,
                                        fields=fields,
                                        parameters={"SDate": w_start, "EDate": w_end},
                                    )
                                    break
                                except Exception as exc:
                                    last_field_error = exc
                                    continue
                            if df is None and last_field_error is not None:
                                raise last_field_error
                            if df is None or df.empty:
                                ok = True
                                break

                            inst_col = _pick_col(df, ["Instrument"])
                            date_col = _pick_col(df, ["Date", "Calc Date"])
                            if volume_only:
                                volume_col = _pick_col(
                                    df,
                                    [
                                        "Average Daily Volume, Three Month",
                                        "Average Daily Volume, 3 Month",
                                        "Average Daily Volume Three Month",
                                        "Average Daily Volume 3 Month",
                                        VOLUME_REPAIR_FIELD,
                                    ],
                                )
                                if not inst_col or not date_col:
                                    raise RuntimeError(f"unexpected columns: {list(df.columns)}")
                            else:
                                volume_col = _pick_col(df, ["Volume"])
                                price_col = _pick_col(df, ["Price Close"])
                                open_col = _pick_col(df, ["Price Open"])
                                high_col = _pick_col(df, ["Price High"])
                                low_col = _pick_col(df, ["Price Low"])
                                ccy_col = _pick_col(df, ["Price Close Currency", "Currency"])
                                if not inst_col or not date_col or not price_col:
                                    raise RuntimeError(f"unexpected columns: {list(df.columns)}")

                            if volume_only:
                                updates: list[tuple[float, str, str, str]] = []
                                for _, row in df.iterrows():
                                    ric = str(row.get(inst_col) or "").strip().upper()
                                    if ric not in ric_set:
                                        continue
                                    d = row.get(date_col)
                                    if pd.isna(d) or d is None:
                                        continue
                                    ds = str(pd.Timestamp(d).date())
                                    if ds < start_bound or ds > end_bound:
                                        continue
                                    if window_pairs is not None and (ric, ds) not in window_pairs:
                                        continue
                                    volume_val = _float_or_none(row.get(volume_col)) if volume_col else None
                                    if volume_val is None:
                                        continue
                                    updates.append((volume_val, now_iso, ric, ds))
                                if updates:
                                    volume_rows_updated += _update_volume_rows(
                                        conn,
                                        updates,
                                        only_null_volume=bool(only_null_volume),
                                    )
                                    conn.commit()
                                ok = True
                                break

                            rows: list[dict[str, Any]] = []
                            for _, row in df.iterrows():
                                ric = str(row.get(inst_col) or "").strip().upper()
                                if ric not in ric_set:
                                    continue
                                d = row.get(date_col)
                                if pd.isna(d) or d is None:
                                    continue
                                ds = str(pd.Timestamp(d).date())
                                if ds < start_bound or ds > end_bound:
                                    continue
                                close = row.get(price_col)
                                close_val = None if pd.isna(close) else float(close)
                                open_val = row.get(open_col) if open_col else None
                                high_val = row.get(high_col) if high_col else None
                                low_val = row.get(low_col) if low_col else None
                                volume_val = _float_or_none(row.get(volume_col)) if volume_col else None
                                ccy_val = str(row.get(ccy_col)).strip() if ccy_col and not pd.isna(row.get(ccy_col)) else None
                                rows.append(
                                    {
                                        "ric": ric,
                                        "date": ds,
                                        "open": close_val if pd.isna(open_val) or open_val is None else float(open_val),
                                        "high": close_val if pd.isna(high_val) or high_val is None else float(high_val),
                                        "low": close_val if pd.isna(low_val) or low_val is None else float(low_val),
                                        "close": close_val,
                                        "adj_close": close_val,
                                        "volume": volume_val,
                                        "currency": ccy_val or None,
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

                if volume_only:
                    print(
                        {
                            "window_start": w_start,
                            "window_end": w_end,
                            "volume_rows_updated_so_far": int(volume_rows_updated),
                        },
                        flush=True,
                    )
                else:
                    print({"window_start": w_start, "window_end": w_end, "rows_upserted_so_far": rows_upserted}, flush=True)
        finally:
            rd.close_session()

        norm_end = previous_or_same_xnys_session(end_date)

        if volume_only:
            out: dict[str, int | str] = {
                "status": "ok",
                "mode": "volume-only",
                "volume_metric": VOLUME_REPAIR_FIELD,
                "volume_rows_updated": int(volume_rows_updated),
                "batch_calls": int(batch_calls),
                "failed_batches": int(failed_batches),
                "start_date": start_bound,
                "end_date": norm_end,
            }
            if only_null_volume:
                remaining_nulls = int(
                    conn.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM {PRICES_TABLE}
                        WHERE date >= ? AND date <= ? AND volume IS NULL
                        """,
                        (start_bound, end_bound),
                    ).fetchone()[0]
                )
                out["remaining_null_volume_rows"] = int(remaining_nulls)
            return out

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
    p.add_argument(
        "--volume-only",
        action="store_true",
        help=(
            "Only update volume field for existing price rows (no OHLC rewrite); "
            f"uses {VOLUME_REPAIR_FIELD}."
        ),
    )
    p.add_argument(
        "--only-null-volume",
        action="store_true",
        help="With --volume-only, only process rows where volume is currently NULL.",
    )
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
        volume_only=bool(args.volume_only),
        only_null_volume=bool(args.only_null_volume),
    )
    print(out)
