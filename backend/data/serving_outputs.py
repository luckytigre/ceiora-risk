"""Durable serving payload snapshots for cloud-safe frontend reads."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from psycopg.rows import dict_row

from backend import config
from backend.data.neon import connect, resolve_dsn

DATA_DB = Path(config.DATA_DB_PATH)
SURFACE_NAME = "serving_outputs"


def _ensure_sqlite_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS serving_payload_current (
            payload_name TEXT PRIMARY KEY,
            snapshot_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            refresh_mode TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_serving_payload_current_updated ON serving_payload_current(updated_at)"
    )


def _ensure_postgres_schema(pg_conn) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS serving_payload_current (
                payload_name TEXT PRIMARY KEY,
                snapshot_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                refresh_mode TEXT NOT NULL,
                payload_json JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            """
        )


def _use_neon_reads() -> bool:
    return bool(config.serving_outputs_primary_reads_enabled() and config.neon_surface_enabled(SURFACE_NAME))


def persist_current_payloads(
    *,
    data_db: Path,
    run_id: str,
    snapshot_id: str,
    refresh_mode: str,
    payloads: dict[str, Any],
) -> dict[str, Any]:
    now_iso = datetime.now(timezone.utc).isoformat()
    rows = [
        (
            str(name),
            str(snapshot_id),
            str(run_id),
            str(refresh_mode),
            json.dumps(value, sort_keys=True, separators=(",", ":")),
            now_iso,
        )
        for name, value in payloads.items()
    ]
    conn = sqlite3.connect(str(data_db), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")
    try:
        _ensure_sqlite_schema(conn)
        conn.executemany(
            """
            INSERT OR REPLACE INTO serving_payload_current (
                payload_name,
                snapshot_id,
                run_id,
                refresh_mode,
                payload_json,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
        result = {
            "status": "ok",
            "snapshot_id": str(snapshot_id),
            "row_count": len(rows),
            "payload_names": sorted(str(k) for k in payloads.keys()),
        }
        if config.neon_surface_enabled(SURFACE_NAME):
            result["neon_write"] = _persist_current_payloads_neon(rows)
        return result
    finally:
        conn.close()


def load_current_payload(payload_name: str) -> dict[str, Any] | list[Any] | None:
    clean = str(payload_name or "").strip()
    if not clean:
        return None
    if _use_neon_reads():
        payload = _load_current_payload_neon(clean)
        if payload is not None or config.cloud_mode():
            return payload
    return _load_current_payload_sqlite(clean)


def _load_current_payload_sqlite(payload_name: str) -> dict[str, Any] | list[Any] | None:
    db = DATA_DB
    if not db.exists():
        return None
    conn = sqlite3.connect(str(db))
    try:
        _ensure_sqlite_schema(conn)
        row = conn.execute(
            """
            SELECT payload_json
            FROM serving_payload_current
            WHERE payload_name = ?
            LIMIT 1
            """,
            (payload_name,),
        ).fetchone()
    finally:
        conn.close()
    if not row or row[0] is None:
        return None
    return json.loads(str(row[0]))


def _load_current_payload_neon(payload_name: str) -> dict[str, Any] | list[Any] | None:
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=True)
    except Exception:
        return None
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT payload_json
                FROM serving_payload_current
                WHERE payload_name = %s
                LIMIT 1
                """,
                (payload_name,),
            )
            row = cur.fetchone()
    except Exception:
        return None
    finally:
        conn.close()
    if not row:
        return None
    raw = row.get("payload_json")
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    return json.loads(str(raw))


def _persist_current_payloads_neon(rows: list[tuple[str, str, str, str, str, str]]) -> dict[str, Any]:
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=False)
    except Exception as exc:
        return {
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    try:
        _ensure_postgres_schema(conn)
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO serving_payload_current (
                    payload_name,
                    snapshot_id,
                    run_id,
                    refresh_mode,
                    payload_json,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s::timestamptz)
                ON CONFLICT (payload_name) DO UPDATE SET
                    snapshot_id = EXCLUDED.snapshot_id,
                    run_id = EXCLUDED.run_id,
                    refresh_mode = EXCLUDED.refresh_mode,
                    payload_json = EXCLUDED.payload_json,
                    updated_at = EXCLUDED.updated_at
                """,
                rows,
            )
        conn.commit()
        return {"status": "ok", "row_count": len(rows)}
    except Exception as exc:
        conn.rollback()
        return {
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    finally:
        conn.close()
