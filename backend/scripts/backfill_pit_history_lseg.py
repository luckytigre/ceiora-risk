#!/usr/bin/env python3
"""Backfill canonical PIT fundamentals/classification to earlier dates via batched LSEG pulls."""

from __future__ import annotations

import argparse
import time
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd


from backend.scripts.download_data_lseg import download_from_lseg
from backend.trading_calendar import previous_or_same_xnys_session


def _pit_dates(start_date: str, end_date: str, *, frequency: str) -> list[str]:
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    freq = "QE" if str(frequency).strip().lower() == "quarterly" else "ME"
    anchors = pd.date_range(start=start, end=end, freq=freq)
    out = sorted({previous_or_same_xnys_session(d.date().isoformat()) for d in anchors})
    return out


def _eligible_universe_count(db_path: Path) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM security_master
            WHERE COALESCE(classification_ok, 0) = 1
              AND COALESCE(is_equity_eligible, 0) = 1
            """
        ).fetchone()
        return int(row[0] or 0) if row else 0
    finally:
        conn.close()


def _ric_count_for_date(
    conn: sqlite3.Connection,
    *,
    table: str,
    date_col: str,
    as_of_date: str,
) -> int:
    row = conn.execute(
        f"""
        SELECT COUNT(DISTINCT ric)
        FROM {table}
        WHERE {date_col} = ?
        """,
        (as_of_date,),
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _is_date_complete(
    conn: sqlite3.Connection,
    *,
    as_of_date: str,
    universe_n: int,
    write_fundamentals: bool,
    write_prices: bool,
    write_classification: bool,
) -> bool:
    checks: list[bool] = []
    if write_fundamentals:
        checks.append(
            _ric_count_for_date(
                conn,
                table="security_fundamentals_pit",
                date_col="as_of_date",
                as_of_date=as_of_date,
            )
            >= universe_n
        )
    if write_prices:
        checks.append(
            _ric_count_for_date(
                conn,
                table="security_prices_eod",
                date_col="date",
                as_of_date=as_of_date,
            )
            >= universe_n
        )
    if write_classification:
        checks.append(
            _ric_count_for_date(
                conn,
                table="security_classification_pit",
                date_col="as_of_date",
                as_of_date=as_of_date,
            )
            >= universe_n
        )
    return bool(checks) and all(checks)


def _filter_incomplete_dates(
    *,
    db_path: Path,
    dates: list[str],
    write_fundamentals: bool,
    write_prices: bool,
    write_classification: bool,
    skip_complete_dates: bool,
) -> tuple[list[str], int]:
    if not skip_complete_dates:
        return dates, 0
    universe_n = _eligible_universe_count(db_path)
    if universe_n <= 0:
        return dates, 0
    conn = sqlite3.connect(str(db_path))
    skipped = 0
    try:
        out: list[str] = []
        for d in dates:
            if _is_date_complete(
                conn,
                as_of_date=str(d),
                universe_n=universe_n,
                write_fundamentals=bool(write_fundamentals),
                write_prices=bool(write_prices),
                write_classification=bool(write_classification),
            ):
                skipped += 1
            else:
                out.append(str(d))
        return out, skipped
    finally:
        conn.close()


def _run_shard_with_retries(
    *,
    db_path: Path,
    date: str,
    shard_count: int,
    shard_idx: int,
    max_retries: int,
    sleep_seconds: float,
    rics_csv: str | None,
    write_fundamentals: bool,
    write_prices: bool,
    write_classification: bool,
) -> tuple[bool, dict[str, Any]]:
    for attempt in range(max(1, int(max_retries)) + 1):
        try:
            out = download_from_lseg(
                db_path=db_path,
                as_of_date=date,
                rics_csv=rics_csv,
                shard_count=int(shard_count),
                shard_index=int(shard_idx),
                write_fundamentals=bool(write_fundamentals),
                write_prices=bool(write_prices),
                write_classification=bool(write_classification),
            )
            payload = {
                "date": str(date),
                "shard_index": int(shard_idx),
                "attempt": int(attempt + 1),
                "status": out.get("status"),
                "fundamental_rows_inserted": out.get("fundamental_rows_inserted"),
                "classification_rows_inserted": out.get("classification_rows_inserted"),
                "price_rows_inserted": out.get("price_rows_inserted"),
            }
            return True, payload
        except Exception as exc:
            payload = {
                "date": str(date),
                "shard_index": int(shard_idx),
                "attempt": int(attempt + 1),
                "error": str(exc),
            }
            if attempt < int(max_retries):
                time.sleep(float(sleep_seconds))
            else:
                return False, payload
    return False, {
        "date": str(date),
        "shard_index": int(shard_idx),
        "error": "unknown_failure",
    }


def run_backfill(
    *,
    db_path: Path,
    start_date: str,
    end_date: str,
    shard_count: int,
    max_retries: int,
    sleep_seconds: float,
    rics_csv: str | None = None,
    frequency: str = "monthly",
    write_fundamentals: bool = True,
    write_prices: bool = True,
    write_classification: bool = True,
    parallel_shards: bool = False,
    max_workers: int = 4,
    skip_complete_dates: bool = True,
) -> dict[str, int | str]:
    dates = _pit_dates(start_date, end_date, frequency=frequency)
    dates, skipped_dates = _filter_incomplete_dates(
        db_path=db_path,
        dates=dates,
        write_fundamentals=bool(write_fundamentals),
        write_prices=bool(write_prices),
        write_classification=bool(write_classification),
        skip_complete_dates=bool(skip_complete_dates),
    )
    if not dates:
        return {
            "status": "no-dates",
            "start_date": start_date,
            "end_date": end_date,
            "dates_skipped_as_complete": int(skipped_dates),
        }

    dates_done = 0
    shards_done = 0
    failed = 0

    for d in dates:
        print(f"=== PIT DATE {d} ===", flush=True)
        date_ok = True
        shard_indices = list(range(max(1, int(shard_count))))
        if bool(parallel_shards) and len(shard_indices) > 1:
            workers = max(1, min(int(max_workers), len(shard_indices)))
            with ThreadPoolExecutor(max_workers=workers) as ex:
                fut_map = {
                    ex.submit(
                        _run_shard_with_retries,
                        db_path=db_path,
                        date=str(d),
                        shard_count=int(shard_count),
                        shard_idx=int(shard_idx),
                        max_retries=int(max_retries),
                        sleep_seconds=float(sleep_seconds),
                        rics_csv=rics_csv,
                        write_fundamentals=bool(write_fundamentals),
                        write_prices=bool(write_prices),
                        write_classification=bool(write_classification),
                    ): shard_idx
                    for shard_idx in shard_indices
                }
                for fut in as_completed(fut_map):
                    ok, payload = fut.result()
                    print(payload, flush=True)
                    if ok:
                        shards_done += 1
                    else:
                        failed += 1
                        date_ok = False
        else:
            for shard_idx in shard_indices:
                ok, payload = _run_shard_with_retries(
                    db_path=db_path,
                    date=str(d),
                    shard_count=int(shard_count),
                    shard_idx=int(shard_idx),
                    max_retries=int(max_retries),
                    sleep_seconds=float(sleep_seconds),
                    rics_csv=rics_csv,
                    write_fundamentals=bool(write_fundamentals),
                    write_prices=bool(write_prices),
                    write_classification=bool(write_classification),
                )
                print(payload, flush=True)
                if ok:
                    shards_done += 1
                else:
                    failed += 1
                    date_ok = False
        if date_ok:
            dates_done += 1

    return {
        "status": "ok",
        "dates_requested": int(len(dates)),
        "dates_completed": int(dates_done),
        "shards_completed": int(shards_done),
        "shard_failures": int(failed),
        "dates_skipped_as_complete": int(skipped_dates),
        "parallel_shards": bool(parallel_shards),
        "max_workers": int(max_workers),
        "frequency": str(frequency),
        "start_date": start_date,
        "end_date": end_date,
    }


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-path", type=Path, default=Path("backend/data.db"), help="Path to SQLite DB")
    p.add_argument("--start-date", default="2012-01-01", help="Backfill start date (YYYY-MM-DD)")
    p.add_argument("--end-date", default="2016-12-29", help="Backfill end date (YYYY-MM-DD)")
    p.add_argument("--shard-count", type=int, default=6, help="Number of shards per date")
    p.add_argument("--max-retries", type=int, default=1, help="Retries per shard on failure")
    p.add_argument("--sleep-seconds", type=float, default=2.0, help="Sleep between retries")
    p.add_argument("--rics", default=None, help="Optional comma-separated RIC subset")
    p.add_argument("--frequency", choices=["quarterly", "monthly"], default="monthly", help="PIT schedule frequency")
    p.add_argument("--skip-fundamentals", action="store_true", help="Skip fundamentals writes")
    p.add_argument("--skip-prices", action="store_true", help="Skip prices writes")
    p.add_argument("--skip-classification", action="store_true", help="Skip classification writes")
    p.add_argument("--parallel-shards", action="store_true", help="Run shards concurrently per PIT date")
    p.add_argument("--max-workers", type=int, default=4, help="Max concurrent shard workers when --parallel-shards is set")
    p.add_argument(
        "--skip-complete-dates",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip PIT dates already complete for selected write targets (default: true)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    result = run_backfill(
        db_path=args.db_path,
        start_date=str(args.start_date),
        end_date=str(args.end_date),
        shard_count=max(1, int(args.shard_count)),
        max_retries=max(0, int(args.max_retries)),
        sleep_seconds=max(0.0, float(args.sleep_seconds)),
        rics_csv=(str(args.rics).strip() if args.rics else None),
        frequency=str(args.frequency),
        write_fundamentals=not bool(args.skip_fundamentals),
        write_prices=not bool(args.skip_prices),
        write_classification=not bool(args.skip_classification),
        parallel_shards=bool(args.parallel_shards),
        max_workers=max(1, int(args.max_workers)),
        skip_complete_dates=bool(args.skip_complete_dates),
    )
    print(result)
