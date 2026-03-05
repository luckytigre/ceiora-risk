"""One-shot hardening pass for source tables in data.db."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


from backend.universe.schema import (
    FUNDAMENTALS_HISTORY_TABLE,
    PRICES_TABLE,
    TRBC_HISTORY_TABLE,
    ensure_cuse4_schema,
)
from backend.data.trbc_schema import ensure_trbc_naming


def _dup_count(conn: sqlite3.Connection, table: str, key_cols: list[str]) -> int:
    select_cols = ", ".join(key_cols)
    group_cols = ", ".join(key_cols)
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT {select_cols}, COUNT(*) AS c
            FROM {table}
            GROUP BY {group_cols}
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    return int(row[0] or 0) if row else 0


def harden(db_path: Path) -> dict[str, object]:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        pre_f = _dup_count(conn, FUNDAMENTALS_HISTORY_TABLE, ["ric", "as_of_date", "stat_date"])
        pre_p = _dup_count(conn, PRICES_TABLE, ["ric", "date"])
        pre_c = _dup_count(conn, TRBC_HISTORY_TABLE, ["ric", "as_of_date"])
        ensure_trbc_naming(conn)
        schema_res = ensure_cuse4_schema(conn)
        conn.commit()
        post_f = _dup_count(conn, FUNDAMENTALS_HISTORY_TABLE, ["ric", "as_of_date", "stat_date"])
        post_p = _dup_count(conn, PRICES_TABLE, ["ric", "date"])
        post_c = _dup_count(conn, TRBC_HISTORY_TABLE, ["ric", "as_of_date"])
    finally:
        conn.close()
    return {
        "db_path": str(db_path),
        "fundamentals_pre_dup_groups": pre_f,
        "fundamentals_post_dup_groups": post_f,
        "prices_pre_dup_groups": pre_p,
        "prices_post_dup_groups": post_p,
        "classification_pre_dup_groups": pre_c,
        "classification_post_dup_groups": post_c,
        "canonical_schema_tables": schema_res,
    }


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Harden canonical source tables and enforce unique canonical keys."
    )
    p.add_argument("--db-path", default="backend/data.db", help="Path to data SQLite DB")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    print(harden(Path(args.db_path).expanduser()))
