#!/usr/bin/env python3
"""Backfill canonical PIT fundamentals/classification to earlier dates via batched LSEG pulls."""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.download_data_lseg import download_from_lseg
from trading_calendar import previous_or_same_xnys_session


def _pit_dates(start_date: str, end_date: str, *, frequency: str) -> list[str]:
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    freq = "QE" if str(frequency).strip().lower() == "quarterly" else "ME"
    anchors = pd.date_range(start=start, end=end, freq=freq)
    out = sorted({previous_or_same_xnys_session(d.date().isoformat()) for d in anchors})
    return out


def run_backfill(
    *,
    db_path: Path,
    start_date: str,
    end_date: str,
    shard_count: int,
    max_retries: int,
    sleep_seconds: float,
    rics_csv: str | None = None,
    frequency: str = "quarterly",
    write_fundamentals: bool = True,
    write_prices: bool = True,
    write_classification: bool = True,
) -> dict[str, int | str]:
    dates = _pit_dates(start_date, end_date, frequency=frequency)
    if not dates:
        return {"status": "no-dates", "start_date": start_date, "end_date": end_date}

    dates_done = 0
    shards_done = 0
    failed = 0

    for d in dates:
        print(f"=== PIT DATE {d} ===", flush=True)
        date_ok = True
        for shard_idx in range(max(1, int(shard_count))):
            ok = False
            for attempt in range(max(1, int(max_retries)) + 1):
                try:
                    out = download_from_lseg(
                        db_path=db_path,
                        as_of_date=d,
                        rics_csv=rics_csv,
                        shard_count=int(shard_count),
                        shard_index=int(shard_idx),
                        skip_common_name_backfill=True,
                        write_fundamentals=bool(write_fundamentals),
                        write_prices=bool(write_prices),
                        write_classification=bool(write_classification),
                    )
                    print(
                        {
                            "date": d,
                            "shard_index": int(shard_idx),
                            "attempt": int(attempt + 1),
                            "status": out.get("status"),
                            "fundamental_rows_inserted": out.get("fundamental_rows_inserted"),
                            "classification_rows_inserted": out.get("classification_rows_inserted"),
                            "price_rows_inserted": out.get("price_rows_inserted"),
                        },
                        flush=True,
                    )
                    shards_done += 1
                    ok = True
                    break
                except Exception as exc:
                    print(
                        {
                            "date": d,
                            "shard_index": int(shard_idx),
                            "attempt": int(attempt + 1),
                            "error": str(exc),
                        },
                        flush=True,
                    )
                    if attempt < int(max_retries):
                        time.sleep(float(sleep_seconds))
            if not ok:
                failed += 1
                date_ok = False
                # Continue with next shard/date to avoid one stuck failure blocking full run.
                continue
        if date_ok:
            dates_done += 1

    return {
        "status": "ok",
        "dates_requested": int(len(dates)),
        "dates_completed": int(dates_done),
        "shards_completed": int(shards_done),
        "shard_failures": int(failed),
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
    p.add_argument("--frequency", choices=["quarterly", "monthly"], default="quarterly", help="PIT schedule frequency")
    p.add_argument("--skip-fundamentals", action="store_true", help="Skip fundamentals writes")
    p.add_argument("--skip-prices", action="store_true", help="Skip prices writes")
    p.add_argument("--skip-classification", action="store_true", help="Skip classification writes")
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
    )
    print(result)
