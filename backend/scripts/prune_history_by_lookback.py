#!/usr/bin/env python3
"""Prune analytics history tables by lookback horizon."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from backend import config
from backend.data.retention import prune_history_by_lookback


def _vacuum(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("VACUUM")
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prune analytics history by lookback years")
    parser.add_argument("--data-db", default=str(config.DATA_DB_PATH), help="Path to data SQLite DB")
    parser.add_argument("--cache-db", default=str(config.SQLITE_PATH), help="Path to cache SQLite DB")
    parser.add_argument("--years", type=int, default=5, help="Lookback years to keep (default: 5)")
    parser.add_argument(
        "--as-of-date",
        default=None,
        help="Reference date (YYYY-MM-DD). Defaults to today UTC.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report deletions without writing")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply deletes (default behavior is dry-run unless --apply is set).",
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Run VACUUM on data/cache DBs after pruning (ignored on --dry-run)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    data_db = Path(str(args.data_db)).expanduser().resolve()
    cache_db = Path(str(args.cache_db)).expanduser().resolve()

    dry_run = bool(args.dry_run or not args.apply)
    result = prune_history_by_lookback(
        data_db=data_db,
        cache_db=cache_db,
        keep_years=max(1, int(args.years)),
        as_of_date=args.as_of_date,
        dry_run=dry_run,
    )

    if bool(args.vacuum) and not dry_run:
        _vacuum(data_db)
        _vacuum(cache_db)
        result["vacuum"] = {"data_db": True, "cache_db": True}
    else:
        result["vacuum"] = {"data_db": False, "cache_db": False}

    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
