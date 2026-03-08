"""Export the canonical security_master seed artifact for git versioning."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path


DEFAULT_OUTPUT = Path("data/reference/security_master_seed.csv")


def export_seed(*, data_db: Path, output_path: Path) -> int:
    conn = sqlite3.connect(str(data_db))
    try:
        rows = conn.execute(
            """
            SELECT
                ric,
                ticker,
                sid,
                permid,
                isin,
                instrument_type,
                asset_category_description,
                exchange_name,
                classification_ok,
                is_equity_eligible,
                source,
                job_run_id,
                updated_at
            FROM security_master
            ORDER BY ric
            """
        ).fetchall()
    finally:
        conn.close()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "ric",
            "ticker",
            "sid",
            "permid",
            "isin",
            "instrument_type",
            "asset_category_description",
            "exchange_name",
            "classification_ok",
            "is_equity_eligible",
            "source",
            "job_run_id",
            "updated_at",
        ])
        writer.writerows(rows)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export security_master seed artifact.")
    parser.add_argument("--data-db", default="backend/runtime/data.db", help="Path to SQLite data DB")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="CSV output path")
    args = parser.parse_args()

    exported = export_seed(
        data_db=Path(args.data_db),
        output_path=Path(args.output),
    )
    print(f"exported_rows={exported} output={Path(args.output)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
