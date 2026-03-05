"""Ensure canonical cUSE4 source tables exist and report row counts."""

from __future__ import annotations

import argparse
from pathlib import Path


from backend.universe.bootstrap import bootstrap_cuse4_source_tables


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ensure canonical cUSE4 source tables exist and report row counts."
    )
    parser.add_argument("--db-path", default="backend/data.db", help="Path to SQLite data DB")
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append/upsert without deleting existing cUSE4 rows first.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    out = bootstrap_cuse4_source_tables(
        db_path=Path(args.db_path).expanduser(),
        replace_all=not bool(args.append),
    )
    print(out)
