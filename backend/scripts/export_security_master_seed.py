"""Export the legacy security_master compatibility seed artifact for git versioning."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

from backend.universe.registry_sync import legacy_coverage_role_from_policy_flags
from backend.universe.schema import (
    SECURITY_MASTER_COMPAT_CURRENT_TABLE,
    SECURITY_POLICY_CURRENT_TABLE,
    SECURITY_REGISTRY_TABLE,
)


DEFAULT_OUTPUT = Path("data/reference/security_master_seed.csv")


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def export_seed(*, data_db: Path, output_path: Path) -> int:
    conn = sqlite3.connect(str(data_db))
    try:
        registry_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (SECURITY_REGISTRY_TABLE,),
        ).fetchone()
        registry_has_rows = False
        if registry_exists:
            row = conn.execute(f"SELECT COUNT(*) FROM {SECURITY_REGISTRY_TABLE}").fetchone()
            registry_has_rows = bool(row and int(row[0] or 0) > 0)
        if registry_has_rows:
            registry_rows = conn.execute(
                f"""
                SELECT
                    ric,
                    ticker,
                    isin,
                    exchange_name
                FROM {SECURITY_REGISTRY_TABLE}
                ORDER BY ric
                """
            ).fetchall()
            policy_by_ric: dict[str, tuple[object, ...]] = {}
            if _table_exists(conn, SECURITY_POLICY_CURRENT_TABLE):
                policy_by_ric = {
                    str(row[0]): (row[1], row[2], row[3])
                    for row in conn.execute(
                        f"""
                        SELECT
                            ric,
                            allow_cuse_returns_projection,
                            pit_fundamentals_enabled,
                            pit_classification_enabled
                        FROM {SECURITY_POLICY_CURRENT_TABLE}
                        """
                    ).fetchall()
                    if row and row[0]
                }
            compat_role_by_ric: dict[str, str] = {}
            if _table_exists(conn, SECURITY_MASTER_COMPAT_CURRENT_TABLE):
                compat_role_by_ric = {
                    str(row[0]): str(row[1] or "").strip() or "native_equity"
                    for row in conn.execute(
                        f"""
                        SELECT ric, coverage_role
                        FROM {SECURITY_MASTER_COMPAT_CURRENT_TABLE}
                        """
                    ).fetchall()
                    if row and row[0]
                }
            rows = [
                (
                    str(ric),
                    ticker,
                    isin,
                    exchange_name,
                    compat_role_by_ric.get(str(ric))
                    or legacy_coverage_role_from_policy_flags(
                        allow_cuse_returns_projection=(policy_by_ric.get(str(ric)) or (0, 0, 0))[0],
                        pit_fundamentals_enabled=(policy_by_ric.get(str(ric)) or (0, 0, 0))[1],
                        pit_classification_enabled=(policy_by_ric.get(str(ric)) or (0, 0, 0))[2],
                    ),
                )
                for ric, ticker, isin, exchange_name in registry_rows
            ]
        else:
            rows = conn.execute(
                """
                SELECT
                    ric,
                    ticker,
                    isin,
                    exchange_name,
                    COALESCE(coverage_role, 'native_equity') AS coverage_role
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
            "isin",
            "exchange_name",
            "coverage_role",
        ])
        writer.writerows(rows)
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export legacy security_master compatibility seed artifact.")
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
