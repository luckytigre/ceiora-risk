#!/usr/bin/env python3
"""Compact SQLite databases (VACUUM + ANALYZE) and report size deltas."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def _bytes(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def compact(db_path: Path, *, analyze: bool = True) -> dict[str, int | str]:
    before = _bytes(db_path)
    conn = sqlite3.connect(str(db_path), timeout=300)
    try:
        conn.execute("PRAGMA busy_timeout=300000")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("VACUUM")
        if analyze:
            conn.execute("ANALYZE")
        conn.commit()
    finally:
        conn.close()
    after = _bytes(db_path)
    return {
        "db": str(db_path),
        "before_bytes": int(before),
        "after_bytes": int(after),
        "bytes_reclaimed": int(max(0, before - after)),
    }


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "db_paths",
        nargs="+",
        type=Path,
        help="One or more SQLite DB paths",
    )
    p.add_argument(
        "--skip-analyze",
        action="store_true",
        help="Skip ANALYZE after VACUUM",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    for p in args.db_paths:
        out = compact(p.expanduser().resolve(), analyze=not bool(args.skip_analyze))
        print(out)
