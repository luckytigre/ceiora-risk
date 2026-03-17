"""CLI to build barra_raw_cross_section_history from source tables."""

from __future__ import annotations

import argparse
from pathlib import Path


from backend.risk_model.raw_cross_section_history import rebuild_raw_cross_section_history


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build in-project Barra raw cross-section history table.")
    p.add_argument("--db-path", default="backend/data.db", help="Path to SQLite data database.")
    p.add_argument("--start-date", default=None, help="Optional start date (YYYY-MM-DD).")
    p.add_argument("--end-date", default=None, help="Optional end date (YYYY-MM-DD).")
    p.add_argument(
        "--frequency",
        default="weekly",
        choices=["daily", "weekly", "latest"],
        help="Cross-section date selection mode.",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    out = rebuild_raw_cross_section_history(
        Path(args.db_path),
        start_date=args.start_date,
        end_date=args.end_date,
        frequency=args.frequency,
    )
    print(out)
