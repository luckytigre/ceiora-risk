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
        "--seed-path",
        default="data/reference/security_master_seed.csv",
        help="Path to committed security_master seed CSV",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    out = bootstrap_cuse4_source_tables(
        db_path=Path(args.db_path).expanduser(),
        seed_path=Path(args.seed_path).expanduser(),
    )
    print(out)
