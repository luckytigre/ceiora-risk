"""Backend-selection and query transport helpers for canonical source reads."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Callable

from psycopg.rows import dict_row

from backend import config
from backend.data.neon import connect, resolve_dsn
from backend.data.trbc_schema import ensure_trbc_naming

_CORE_READ_SURFACE = "core_reads"
_CORE_READ_BACKEND_OVERRIDE: ContextVar[str | None] = ContextVar(
    "core_read_backend_override",
    default=None,
)


def use_neon_core_reads() -> bool:
    override = _CORE_READ_BACKEND_OVERRIDE.get()
    if override == "local":
        return False
    if override == "neon":
        return True
    return bool(config.neon_surface_enabled(_CORE_READ_SURFACE))


def core_read_backend_name() -> str:
    return "neon" if use_neon_core_reads() else "local"


@contextmanager
def core_read_backend(backend: str):
    clean = str(backend or "").strip().lower()
    if clean not in {"local", "neon"}:
        raise ValueError("backend must be 'local' or 'neon'")
    token = _CORE_READ_BACKEND_OVERRIDE.set(clean)
    try:
        yield
    finally:
        _CORE_READ_BACKEND_OVERRIDE.reset(token)


def to_pg_sql(query: str) -> str:
    return str(query).replace("?", "%s")


def fetch_rows(
    sql: str,
    params: list[Any] | None = None,
    *,
    data_db: Path,
    neon_enabled: bool,
) -> list[dict[str, Any]]:
    if neon_enabled:
        pg_conn = connect(dsn=resolve_dsn(None), autocommit=True)
        try:
            with pg_conn.cursor(row_factory=dict_row) as cur:
                cur.execute(to_pg_sql(sql), params or [])
                return [dict(row) for row in cur.fetchall()]
        finally:
            pg_conn.close()

    conn = sqlite3.connect(str(data_db))
    conn.row_factory = sqlite3.Row
    try:
        ensure_trbc_naming(conn)
        cur = conn.execute(sql, params or [])
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def table_exists(
    table: str,
    *,
    fetch_rows_fn: Callable[[str, list[Any] | None], list[dict[str, Any]]],
    neon_enabled: bool,
) -> bool:
    if neon_enabled:
        rows = fetch_rows_fn(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name=?
            LIMIT 1
            """,
            [table],
        )
        return bool(rows)

    rows = fetch_rows_fn(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        [table],
    )
    return bool(rows)


def missing_tables(
    *tables: str,
    table_exists_fn: Callable[[str], bool],
) -> list[str]:
    return [t for t in tables if not table_exists_fn(t)]
