"""Sync durable cPAR tables from local SQLite into Neon."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from backend import config
from backend.data import cpar_schema, cpar_writers


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _pg_columns(pg_conn, table: str) -> list[str]:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        return [str(row[0]) for row in cur.fetchall()]


def _sqlite_count(conn: sqlite3.Connection, table: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(row[0] or 0) if row else 0


def _pg_count(pg_conn, table: str) -> int:
    with pg_conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def _iter_sqlite_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    columns: list[str],
    batch_size: int,
):
    select_sql = f"SELECT {', '.join(columns)} FROM {table}"
    cur = conn.execute(select_sql)
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        yield [tuple(row) for row in rows]


def sync_cpar_tables_from_sqlite(
    *,
    sqlite_path: Path,
    dsn: str | None = None,
    selected_tables: list[str] | None = None,
    batch_size: int = 5_000,
) -> dict[str, Any]:
    dsn_value = str(dsn or config.neon_dsn() or "").strip()
    if not dsn_value:
        raise RuntimeError("NEON_DATABASE_URL is required for cPAR Neon sync.")
    tables = list(selected_tables or cpar_schema.TABLES)
    unknown = sorted(set(tables) - set(cpar_schema.TABLES))
    if unknown:
        raise ValueError(f"unknown cPAR table(s): {', '.join(unknown)}")

    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row
    try:
        cpar_schema.ensure_sqlite_schema(sqlite_conn)
        with psycopg.connect(dsn_value) as pg_conn:
            cpar_writers.ensure_postgres_schema(pg_conn)
            out: dict[str, Any] = {
                "status": "ok",
                "sqlite_path": str(sqlite_path),
                "tables": {},
            }
            for table in tables:
                src_cols = _sqlite_columns(sqlite_conn, table)
                if not src_cols:
                    out["tables"][table] = {"status": "skipped_missing_source"}
                    continue
                tgt_cols = _pg_columns(pg_conn, table)
                if not tgt_cols:
                    raise RuntimeError(f"target cPAR table missing in Neon: {table}")
                missing = [col for col in src_cols if col not in set(tgt_cols)]
                if missing:
                    raise RuntimeError(
                        f"target cPAR table {table} missing source columns: {', '.join(missing)}"
                    )
                cols = [col for col in src_cols if col in set(tgt_cols)]
                source_rows = _sqlite_count(sqlite_conn, table)
                target_rows_before = _pg_count(pg_conn, table)
                loaded_rows = 0
                insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(table),
                    sql.SQL(", ").join(sql.Identifier(col) for col in cols),
                    sql.SQL(", ").join(sql.Placeholder() for _ in cols),
                )
                with pg_conn.cursor() as cur:
                    cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
                    for batch in _iter_sqlite_rows(
                        sqlite_conn,
                        table=table,
                        columns=cols,
                        batch_size=max(1, int(batch_size)),
                    ):
                        cur.executemany(insert_sql, batch)
                        loaded_rows += len(batch)
                target_rows_after = _pg_count(pg_conn, table)
                if loaded_rows != source_rows or target_rows_after != source_rows:
                    raise RuntimeError(
                        f"cPAR sync row mismatch for {table}: "
                        f"source={source_rows} loaded={loaded_rows} target={target_rows_after}"
                    )
                out["tables"][table] = {
                    "status": "ok",
                    "action": "truncate_and_reload",
                    "source_rows": int(source_rows),
                    "rows_loaded": int(loaded_rows),
                    "target_rows_before": int(target_rows_before),
                    "target_rows_after": int(target_rows_after),
                }
            return out
    finally:
        sqlite_conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=None, help="Neon DSN (defaults to NEON_DATABASE_URL)")
    parser.add_argument(
        "--db-path",
        default="backend/runtime/data.db",
        help="Source SQLite DB path",
    )
    parser.add_argument(
        "--tables",
        default=",".join(cpar_schema.TABLES),
        help="Comma-separated cPAR table names",
    )
    parser.add_argument("--batch-size", type=int, default=5_000, help="Insert batch size")
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    args = parser.parse_args()
    selected_tables = [part.strip() for part in str(args.tables or "").split(",") if part.strip()]
    out = sync_cpar_tables_from_sqlite(
        sqlite_path=Path(args.db_path).expanduser().resolve(),
        dsn=args.dsn,
        selected_tables=selected_tables,
        batch_size=int(args.batch_size),
    )
    if bool(args.json):
        print(json.dumps(out, indent=2))
    else:
        print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
