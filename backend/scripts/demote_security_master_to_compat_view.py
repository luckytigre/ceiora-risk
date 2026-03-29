#!/usr/bin/env python3
"""Demote local SQLite security_master into a read-only compatibility view."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

SECURITY_MASTER = "security_master"
SECURITY_MASTER_LEGACY = "security_master_legacy"
SECURITY_MASTER_COMPAT_CURRENT = "security_master_compat_current"
SECURITY_MASTER_VIEW_SQL = f"""
CREATE VIEW {SECURITY_MASTER} AS
SELECT
    ric,
    ticker,
    isin,
    exchange_name,
    classification_ok,
    is_equity_eligible,
    coverage_role,
    source,
    job_run_id,
    updated_at
FROM {SECURITY_MASTER_COMPAT_CURRENT}
"""


def _relation_type(conn: sqlite3.Connection, name: str) -> str | None:
    row = conn.execute(
        """
        SELECT type
        FROM sqlite_master
        WHERE type IN ('table', 'view') AND name = ?
        LIMIT 1
        """,
        (name,),
    ).fetchone()
    return str(row[0]) if row and row[0] is not None else None


def _row_count(conn: sqlite3.Connection, relation: str) -> int:
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()
    except sqlite3.OperationalError:
        return 0
    return int(row[0] or 0) if row else 0


def demote_security_master(
    *,
    db_path: Path,
    apply: bool = False,
) -> dict[str, Any]:
    db = Path(db_path).expanduser().resolve()
    if not db.exists():
        raise FileNotFoundError(f"database file not found: {db}")

    conn = sqlite3.connect(str(db), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")
    try:
        compat_kind_before = _relation_type(conn, SECURITY_MASTER_COMPAT_CURRENT)
        master_kind_before = _relation_type(conn, SECURITY_MASTER)
        legacy_kind_before = _relation_type(conn, SECURITY_MASTER_LEGACY)
        if compat_kind_before != "table":
            raise RuntimeError(
                "security_master demotion requires security_master_compat_current to exist as a table"
            )

        out: dict[str, Any] = {
            "status": "dry_run",
            "db_path": str(db),
            "apply": bool(apply),
            "security_master_kind_before": master_kind_before,
            "security_master_legacy_kind_before": legacy_kind_before,
            "security_master_compat_current_kind": compat_kind_before,
            "security_master_row_count_before": _row_count(conn, SECURITY_MASTER)
            if master_kind_before
            else 0,
            "security_master_legacy_row_count_before": _row_count(conn, SECURITY_MASTER_LEGACY)
            if legacy_kind_before
            else 0,
            "security_master_compat_current_row_count": _row_count(conn, SECURITY_MASTER_COMPAT_CURRENT),
        }

        if master_kind_before == "view":
            out["status"] = "already_demoted"
            out["security_master_kind_after"] = master_kind_before
            out["security_master_legacy_kind_after"] = legacy_kind_before
            out["security_master_row_count_after"] = _row_count(conn, SECURITY_MASTER)
            return out

        if master_kind_before != "table":
            raise RuntimeError("security_master demotion requires a physical security_master table")
        if legacy_kind_before is not None:
            raise RuntimeError("security_master demotion refused: security_master_legacy already exists")

        if not apply:
            out["planned_actions"] = [
                "ALTER TABLE security_master RENAME TO security_master_legacy",
                SECURITY_MASTER_VIEW_SQL.strip(),
            ]
            out["security_master_kind_after"] = "view"
            out["security_master_legacy_kind_after"] = "table"
            out["security_master_row_count_after"] = out["security_master_compat_current_row_count"]
            return out

        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(f"ALTER TABLE {SECURITY_MASTER} RENAME TO {SECURITY_MASTER_LEGACY}")
            conn.execute(SECURITY_MASTER_VIEW_SQL)
        except Exception:
            conn.rollback()
            raise
        conn.commit()

        out["status"] = "ok"
        out["security_master_kind_after"] = _relation_type(conn, SECURITY_MASTER)
        out["security_master_legacy_kind_after"] = _relation_type(conn, SECURITY_MASTER_LEGACY)
        out["security_master_row_count_after"] = _row_count(conn, SECURITY_MASTER)
        out["security_master_legacy_row_count_after"] = _row_count(conn, SECURITY_MASTER_LEGACY)
        return out
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=Path("backend/runtime/data.db"))
    parser.add_argument("--apply", action="store_true", help="Apply the demotion instead of dry-run only")
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    out = demote_security_master(db_path=args.db_path, apply=bool(args.apply))
    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(f"status={out['status']} db={out['db_path']}")
        print(f"security_master: {out.get('security_master_kind_before')} -> {out.get('security_master_kind_after')}")
        print(
            "security_master_legacy: "
            f"{out.get('security_master_legacy_kind_before')} -> {out.get('security_master_legacy_kind_after')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
