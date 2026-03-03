"""Bootstrap cUSE4 source-of-truth tables from current legacy source tables."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cuse4.bootstrap import bootstrap_cuse4_source_tables


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build/refresh security_master, fundamentals_history, and trbc_industry_country_history."
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
