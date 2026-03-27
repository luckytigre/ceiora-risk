"""Export the canonical registry-first security_registry seed artifact for git versioning."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

from backend.universe.schema import SECURITY_POLICY_CURRENT_TABLE, SECURITY_REGISTRY_TABLE


DEFAULT_OUTPUT = Path("data/reference/security_registry_seed.csv")


def export_seed(*, data_db: Path, output_path: Path) -> int:
    conn = sqlite3.connect(str(data_db))
    try:
        rows = conn.execute(
            f"""
            SELECT
                r.ric,
                r.ticker,
                r.isin,
                r.exchange_name,
                r.tracking_status,
                COALESCE(p.price_ingest_enabled, 1) AS price_ingest_enabled,
                COALESCE(p.pit_fundamentals_enabled, 0) AS pit_fundamentals_enabled,
                COALESCE(p.pit_classification_enabled, 0) AS pit_classification_enabled,
                COALESCE(p.allow_cuse_native_core, 0) AS allow_cuse_native_core,
                COALESCE(p.allow_cuse_fundamental_projection, 0) AS allow_cuse_fundamental_projection,
                COALESCE(p.allow_cuse_returns_projection, 0) AS allow_cuse_returns_projection,
                COALESCE(p.allow_cpar_core_target, 0) AS allow_cpar_core_target,
                COALESCE(p.allow_cpar_extended_target, 0) AS allow_cpar_extended_target
            FROM {SECURITY_REGISTRY_TABLE} r
            LEFT JOIN {SECURITY_POLICY_CURRENT_TABLE} p
              ON p.ric = r.ric
            ORDER BY r.ric
            """
        ).fetchall()
    finally:
        conn.close()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "ric",
                "ticker",
                "isin",
                "exchange_name",
                "tracking_status",
                "price_ingest_enabled",
                "pit_fundamentals_enabled",
                "pit_classification_enabled",
                "allow_cuse_native_core",
                "allow_cuse_fundamental_projection",
                "allow_cuse_returns_projection",
                "allow_cpar_core_target",
                "allow_cpar_extended_target",
            ]
        )
        writer.writerows(rows)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export the canonical registry-first security_registry seed artifact.")
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
