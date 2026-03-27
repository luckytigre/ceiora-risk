#!/usr/bin/env python3
"""Canonicalize legacy source-history alias RICs onto registry RICs."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.universe.normalize import normalize_ric, normalize_ticker, ticker_from_ric
from backend.universe.schema import (
    PRICES_TABLE,
    FUNDAMENTALS_HISTORY_TABLE,
    SECURITY_INGEST_AUDIT_TABLE,
    SECURITY_MASTER_TABLE,
    SECURITY_REGISTRY_TABLE,
    TRBC_HISTORY_TABLE,
    ensure_cuse4_schema,
)


@dataclass(frozen=True)
class RemapTableSpec:
    name: str
    pk_cols: tuple[str, ...]


REMAP_TABLE_SPECS: tuple[RemapTableSpec, ...] = (
    RemapTableSpec(name=PRICES_TABLE, pk_cols=("ric", "date")),
    RemapTableSpec(name=FUNDAMENTALS_HISTORY_TABLE, pk_cols=("ric", "as_of_date", "stat_date")),
    RemapTableSpec(name=TRBC_HISTORY_TABLE, pk_cols=("ric", "as_of_date")),
    RemapTableSpec(name=SECURITY_INGEST_AUDIT_TABLE, pk_cols=("job_run_id", "ric", "artifact_name")),
)

CLEANUP_TABLES_WITH_RIC = tuple(spec.name for spec in REMAP_TABLE_SPECS) + (SECURITY_MASTER_TABLE,)


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def _sqlite_backup(source_path: Path, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        target_path.unlink()
    source = sqlite3.connect(str(source_path))
    try:
        target = sqlite3.connect(str(target_path))
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (str(table),),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(row[1]) for row in rows]


def _registry_rows_by_ticker(conn: sqlite3.Connection) -> dict[str, list[str]]:
    rows = conn.execute(
        f"""
        SELECT UPPER(TRIM(ric)) AS ric, UPPER(TRIM(ticker)) AS ticker
        FROM {SECURITY_REGISTRY_TABLE}
        WHERE ric IS NOT NULL
          AND TRIM(ric) <> ''
          AND ticker IS NOT NULL
          AND TRIM(ticker) <> ''
        """
    ).fetchall()
    out: dict[str, list[str]] = {}
    for ric, ticker in rows:
        clean_ric = normalize_ric(ric)
        clean_ticker = normalize_ticker(ticker)
        if not clean_ric or not clean_ticker:
            continue
        out.setdefault(clean_ticker, []).append(clean_ric)
    return {ticker: sorted(set(rics)) for ticker, rics in out.items()}


def discover_orphan_rics(conn: sqlite3.Connection) -> list[str]:
    if not _table_exists(conn, SECURITY_REGISTRY_TABLE):
        return []
    union_parts: list[str] = []
    for table in CLEANUP_TABLES_WITH_RIC:
        if _table_exists(conn, table) and "ric" in _table_columns(conn, table):
            union_parts.append(
                f"SELECT DISTINCT UPPER(TRIM(ric)) AS ric FROM {table} WHERE ric IS NOT NULL AND TRIM(ric) <> ''"
            )
    if not union_parts:
        return []
    sql = f"""
        WITH used_rics AS (
            {" UNION ".join(union_parts)}
        ),
        registry_rics AS (
            SELECT DISTINCT UPPER(TRIM(ric)) AS ric
            FROM {SECURITY_REGISTRY_TABLE}
            WHERE ric IS NOT NULL AND TRIM(ric) <> ''
        )
        SELECT u.ric
        FROM used_rics u
        LEFT JOIN registry_rics r
          ON r.ric = u.ric
        WHERE r.ric IS NULL
        ORDER BY u.ric
    """
    rows = conn.execute(sql).fetchall()
    return [normalize_ric(row[0]) for row in rows if row and normalize_ric(row[0])]


def build_unique_alias_mapping(
    conn: sqlite3.Connection,
    *,
    orphan_rics: list[str] | None = None,
) -> dict[str, Any]:
    orphan_list = [normalize_ric(ric) for ric in (orphan_rics or discover_orphan_rics(conn)) if normalize_ric(ric)]
    registry_by_ticker = _registry_rows_by_ticker(conn)
    mapping: dict[str, str] = {}
    unresolved_no_candidate: list[str] = []
    unresolved_ambiguous: list[dict[str, Any]] = []
    for orphan_ric in orphan_list:
        ticker = normalize_ticker(ticker_from_ric(orphan_ric))
        candidates = sorted(set(registry_by_ticker.get(ticker or "", [])))
        if len(candidates) == 1:
            mapping[orphan_ric] = candidates[0]
            continue
        if len(candidates) == 0:
            unresolved_no_candidate.append(orphan_ric)
            continue
        unresolved_ambiguous.append(
            {
                "ric": orphan_ric,
                "ticker": ticker,
                "candidate_rics": candidates,
            }
        )
    return {
        "mapping": mapping,
        "unresolved_no_candidate": unresolved_no_candidate,
        "unresolved_ambiguous": unresolved_ambiguous,
    }


def _rebuild_remap_table(
    conn: sqlite3.Connection,
    *,
    spec: RemapTableSpec,
) -> dict[str, int]:
    if not _table_exists(conn, spec.name):
        return {"source_rows": 0, "rebuilt_rows": 0}

    source_table = f"{spec.name}__alias_source"
    conn.execute(f"DROP TABLE IF EXISTS {source_table}")
    conn.execute(f"ALTER TABLE {spec.name} RENAME TO {source_table}")
    ensure_cuse4_schema(conn)

    target_cols = _table_columns(conn, spec.name)
    source_cols = set(_table_columns(conn, source_table))
    insert_cols = [col for col in target_cols if col in source_cols]
    if not insert_cols:
        raise RuntimeError(f"no insertable columns remain when rebuilding {spec.name}")

    select_exprs: list[str] = []
    partition_exprs: list[str] = []
    for col in insert_cols:
        if col == "ric":
            expr = "COALESCE(map.canonical_ric, UPPER(TRIM(src.ric)))"
        else:
            expr = f"src.{col}"
        select_exprs.append(f"{expr} AS {col}")
    for col in spec.pk_cols:
        if col == "ric":
            partition_exprs.append("COALESCE(map.canonical_ric, UPPER(TRIM(src.ric)))")
        else:
            partition_exprs.append(f"src.{col}")

    source_row_count = int(conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0] or 0)
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {spec.name} ({", ".join(insert_cols)})
        SELECT {", ".join(insert_cols)}
        FROM (
            SELECT
                {", ".join(select_exprs)},
                ROW_NUMBER() OVER (
                    PARTITION BY {", ".join(partition_exprs)}
                    ORDER BY COALESCE(NULLIF(TRIM(src.updated_at), ''), '0000-00-00T00:00:00') DESC, src.rowid DESC
                ) AS rn
            FROM {source_table} src
            LEFT JOIN alias_mapping_temp map
              ON map.alias_ric = UPPER(TRIM(src.ric))
            JOIN {SECURITY_REGISTRY_TABLE} reg
              ON reg.ric = COALESCE(map.canonical_ric, UPPER(TRIM(src.ric)))
            WHERE src.ric IS NOT NULL
              AND TRIM(src.ric) <> ''
        ) ranked
        WHERE rn = 1
        """
    )
    rebuilt_row_count = int(conn.execute(f"SELECT COUNT(*) FROM {spec.name}").fetchone()[0] or 0)
    conn.execute(f"DROP TABLE {source_table}")
    return {
        "source_rows": source_row_count,
        "rebuilt_rows": rebuilt_row_count,
    }


def _purge_security_master_orphans(conn: sqlite3.Connection) -> int:
    if not (_table_exists(conn, SECURITY_MASTER_TABLE) and _table_exists(conn, SECURITY_REGISTRY_TABLE)):
        return 0
    before = int(conn.execute(f"SELECT COUNT(*) FROM {SECURITY_MASTER_TABLE}").fetchone()[0] or 0)
    conn.execute(
        f"""
        DELETE FROM {SECURITY_MASTER_TABLE}
        WHERE UPPER(TRIM(ric)) NOT IN (
            SELECT UPPER(TRIM(ric))
            FROM {SECURITY_REGISTRY_TABLE}
            WHERE ric IS NOT NULL AND TRIM(ric) <> ''
        )
        """
    )
    after = int(conn.execute(f"SELECT COUNT(*) FROM {SECURITY_MASTER_TABLE}").fetchone()[0] or 0)
    return before - after


def _orphan_count(conn: sqlite3.Connection, table: str) -> int:
    if not (_table_exists(conn, table) and _table_exists(conn, SECURITY_REGISTRY_TABLE)):
        return 0
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM {table} x
        LEFT JOIN {SECURITY_REGISTRY_TABLE} reg
          ON reg.ric = x.ric
        WHERE reg.ric IS NULL
        """
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _duplicate_group_count(conn: sqlite3.Connection, *, table: str, pk_cols: tuple[str, ...]) -> int:
    if not _table_exists(conn, table):
        return 0
    group_cols = ", ".join(pk_cols)
    row = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT {group_cols}, COUNT(*) AS c
            FROM {table}
            GROUP BY {group_cols}
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    return int(row[0] or 0) if row else 0


def canonicalize_registry_source_history_in_place(conn: sqlite3.Connection) -> dict[str, Any]:
    ensure_cuse4_schema(conn)
    orphan_rics = discover_orphan_rics(conn)
    mapping_result = build_unique_alias_mapping(conn, orphan_rics=orphan_rics)
    mapping = dict(mapping_result["mapping"])
    conn.execute("DROP TABLE IF EXISTS alias_mapping_temp")
    conn.execute(
        """
        CREATE TEMP TABLE alias_mapping_temp (
            alias_ric TEXT PRIMARY KEY,
            canonical_ric TEXT NOT NULL
        )
        """
    )
    if mapping:
        conn.executemany(
            """
            INSERT INTO alias_mapping_temp (alias_ric, canonical_ric)
            VALUES (?, ?)
            """,
            sorted(mapping.items()),
        )

    table_stats: dict[str, Any] = {}
    for spec in REMAP_TABLE_SPECS:
        table_stats[spec.name] = _rebuild_remap_table(conn, spec=spec)
    security_master_rows_deleted = _purge_security_master_orphans(conn)
    conn.execute("DROP TABLE IF EXISTS alias_mapping_temp")

    validation = {
        table: {
            "orphan_rows": _orphan_count(conn, table),
        }
        for table in CLEANUP_TABLES_WITH_RIC
        if _table_exists(conn, table)
    }
    for spec in REMAP_TABLE_SPECS:
        if spec.name in validation:
            validation[spec.name]["duplicate_groups"] = _duplicate_group_count(conn, table=spec.name, pk_cols=spec.pk_cols)
    if _table_exists(conn, SECURITY_MASTER_TABLE):
        validation[SECURITY_MASTER_TABLE]["duplicate_groups"] = _duplicate_group_count(
            conn,
            table=SECURITY_MASTER_TABLE,
            pk_cols=("ric",),
        )

    return {
        "status": "ok",
        "orphan_ric_count_before": len(orphan_rics),
        "mapped_alias_ric_count": len(mapping),
        "mapped_aliases": sorted(mapping.items()),
        "unresolved_no_candidate": list(mapping_result["unresolved_no_candidate"]),
        "unresolved_ambiguous": list(mapping_result["unresolved_ambiguous"]),
        "table_stats": table_stats,
        "security_master_rows_deleted": int(security_master_rows_deleted),
        "validation": validation,
    }


def canonicalize_registry_source_history(
    *,
    source_db: Path,
    working_db: Path | None = None,
    backup_dir: Path | None = None,
    apply_changes: bool = False,
) -> dict[str, Any]:
    source_path = Path(source_db).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"sqlite db not found: {source_path}")

    working_path = (
        Path(working_db).expanduser().resolve()
        if working_db is not None
        else source_path.with_name(f"{source_path.stem}.registry_canonicalize_{_timestamp()}.db")
    )
    backup_path = (
        Path(backup_dir).expanduser().resolve() / f"{source_path.stem}.pre_registry_canonicalize_{_timestamp()}.db"
        if backup_dir is not None
        else source_path.with_name(f"{source_path.stem}.pre_registry_canonicalize_{_timestamp()}.db")
    )

    _sqlite_backup(source_path, working_path)
    working_conn = sqlite3.connect(str(working_path))
    try:
        working_conn.execute("BEGIN IMMEDIATE")
        result = canonicalize_registry_source_history_in_place(working_conn)
        working_conn.commit()
    except Exception:
        working_conn.rollback()
        raise
    finally:
        working_conn.close()

    validation_conn = sqlite3.connect(str(working_path))
    try:
        integrity = validation_conn.execute("PRAGMA quick_check").fetchone()
        integrity_status = str(integrity[0]) if integrity and integrity[0] is not None else "unknown"
        if integrity_status.lower() != "ok":
            raise RuntimeError(f"post-canonicalization quick_check failed: {integrity_status}")
    finally:
        validation_conn.close()

    out = {
        "status": "ok",
        "source_db": str(source_path),
        "working_db": str(working_path),
        "backup_path": None,
        "applied": False,
        "result": result,
        "quick_check": integrity_status,
    }

    if not apply_changes:
        return out

    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, backup_path)
    try:
        os.replace(str(working_path), str(source_path))
    except Exception:
        if backup_path.exists():
            shutil.copy2(backup_path, source_path)
        raise
    out["backup_path"] = str(backup_path)
    out["applied"] = True
    out["working_db"] = str(source_path)
    return out


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("backend/runtime/data.db"),
        help="Source SQLite archive to canonicalize.",
    )
    parser.add_argument(
        "--working-db",
        type=Path,
        default=None,
        help="Optional working-copy path. Defaults to a sibling timestamped file.",
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=None,
        help="Optional directory for the pre-canonicalization backup when --apply is used.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Atomically replace the source DB with the canonicalized working copy after validation.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    out = canonicalize_registry_source_history(
        source_db=Path(args.db_path),
        working_db=(Path(args.working_db) if args.working_db is not None else None),
        backup_dir=(Path(args.backup_dir) if args.backup_dir is not None else None),
        apply_changes=bool(args.apply),
    )
    if args.json:
        print(json.dumps(out, indent=2))
    else:
        result = dict(out.get("result") or {})
        print(f"Canonicalization status: {out.get('status')}")
        print(f"- source_db: {out.get('source_db')}")
        print(f"- working_db: {out.get('working_db')}")
        print(f"- applied: {out.get('applied')}")
        if out.get("backup_path"):
            print(f"- backup_path: {out.get('backup_path')}")
        print(f"- orphan_ric_count_before: {result.get('orphan_ric_count_before')}")
        print(f"- mapped_alias_ric_count: {result.get('mapped_alias_ric_count')}")
        print(f"- unresolved_no_candidate: {len(result.get('unresolved_no_candidate') or [])}")
        print(f"- unresolved_ambiguous: {len(result.get('unresolved_ambiguous') or [])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
