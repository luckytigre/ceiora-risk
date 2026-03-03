"""Build and persist cUSE4 ESTU membership for a trading date."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cuse4.estu import build_and_persist_estu_membership


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Persist cUSE4 ESTU membership audit rows.")
    parser.add_argument("--db-path", default="backend/data.db", help="Path to SQLite data DB")
    parser.add_argument("--as-of-date", default=None, help="Optional as-of date (YYYY-MM-DD)")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    out = build_and_persist_estu_membership(
        db_path=Path(args.db_path).expanduser(),
        as_of_date=args.as_of_date,
    )
    print(out)
