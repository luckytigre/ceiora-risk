"""Durable serving payload snapshots for cloud-safe frontend reads."""

from __future__ import annotations

from collections.abc import Callable
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from psycopg.rows import dict_row

from backend import config
from backend.data.neon import connect, resolve_dsn
from backend.data.neon_primary_write import execute_neon_primary_write

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


def load_runtime_payload(
    payload_name: str,
    *,
    fallback_loader: Callable[[str], Any | None] | None = None,
) -> Any | None:
    """Load the runtime truth payload, using cache fallback only when policy allows it."""
    payload = load_current_payload(payload_name)
    if payload is not None:
        return payload
    if fallback_loader is None or not config.serving_outputs_cache_fallback_enabled():
        return None
    return fallback_loader(payload_name)


def persist_current_payloads(
    *,
    data_db: Path,
    run_id: str,
    snapshot_id: str,
    refresh_mode: str,
    payloads: dict[str, Any],
    replace_all: bool = False,
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
    result = {
        "status": "ok",
        "snapshot_id": str(snapshot_id),
        "row_count": len(rows),
        "payload_names": sorted(str(k) for k in payloads.keys()),
        "replace_all": bool(replace_all),
    }
    return execute_neon_primary_write(
        base_result=result,
        neon_enabled=bool(config.neon_surface_enabled(SURFACE_NAME)),
        neon_required=bool(config.serving_payload_neon_write_required()),
        perform_neon_write=lambda: _persist_current_payloads_neon(rows, replace_all=replace_all),
        perform_fallback_write=lambda: _persist_current_payloads_sqlite(
            rows,
            data_db=data_db,
            replace_all=replace_all,
        ),
        failure_label="serving payload persistence",
        fallback_result_key="sqlite_mirror_write",
        fallback_authority="sqlite",
    )


def load_current_payload(payload_name: str) -> dict[str, Any] | list[Any] | None:
    clean = str(payload_name or "").strip()
    if not clean:
        return None
    if _use_neon_reads():
        payload = _load_current_payload_neon(clean)
        if payload is not None or not config.serving_outputs_cache_fallback_enabled():
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


def _persist_current_payloads_neon(
    rows: list[tuple[str, str, str, str, str, str]],
    *,
    replace_all: bool,
) -> dict[str, Any]:
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
            if replace_all:
                if rows:
                    cur.execute(
                        """
                        DELETE FROM serving_payload_current
                        WHERE payload_name <> ALL(%s)
                        """,
                        ([row[0] for row in rows],),
                    )
                else:
                    cur.execute("DELETE FROM serving_payload_current")
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
        verification = _verify_current_payloads_neon(
            conn,
            rows=rows,
            replace_all=replace_all,
        )
        if str(verification.get("status") or "") != "ok":
            conn.rollback()
            return {
                "status": "error",
                "row_count": len(rows),
                "replace_all": bool(replace_all),
                "verification": verification,
            }
        conn.commit()
        return {
            "status": "ok",
            "row_count": len(rows),
            "replace_all": bool(replace_all),
            "verification": verification,
        }
    except Exception as exc:
        conn.rollback()
        return {
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    finally:
        conn.close()


def _persist_current_payloads_sqlite(
    rows: list[tuple[str, str, str, str, str, str]],
    *,
    data_db: Path,
    replace_all: bool,
) -> dict[str, Any]:
    conn = sqlite3.connect(str(data_db), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")
    try:
        _ensure_sqlite_schema(conn)
        if replace_all:
            if rows:
                placeholders = ",".join("?" for _ in rows)
                conn.execute(
                    f"DELETE FROM serving_payload_current WHERE payload_name NOT IN ({placeholders})",
                    [row[0] for row in rows],
                )
            else:
                conn.execute("DELETE FROM serving_payload_current")
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
        return {
            "status": "ok",
            "row_count": len(rows),
            "replace_all": bool(replace_all),
        }
    except Exception as exc:
        conn.rollback()
        return {
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    finally:
        conn.close()


def _verify_current_payloads_neon(
    pg_conn,
    *,
    rows: list[tuple[str, str, str, str, str, str]],
    replace_all: bool,
) -> dict[str, Any]:
    expected_by_name = {
        str(row[0]): {
            "snapshot_id": str(row[1]),
            "run_id": str(row[2]),
            "refresh_mode": str(row[3]),
        }
        for row in rows
    }
    payload_names = sorted(expected_by_name.keys())
    out: dict[str, Any] = {
        "status": "ok",
        "expected_row_count": len(expected_by_name),
        "replace_all": bool(replace_all),
        "verified_row_count": 0,
        "verified_payload_names": [],
        "issues": [],
    }

    with pg_conn.cursor() as cur:
        if replace_all:
            cur.execute(
                """
                SELECT payload_name, snapshot_id, run_id, refresh_mode
                FROM serving_payload_current
                ORDER BY payload_name
                """
            )
        elif payload_names:
            cur.execute(
                """
                SELECT payload_name, snapshot_id, run_id, refresh_mode
                FROM serving_payload_current
                WHERE payload_name = ANY(%s)
                ORDER BY payload_name
                """,
                (payload_names,),
            )
        else:
            cur.execute(
                """
                SELECT payload_name, snapshot_id, run_id, refresh_mode
                FROM serving_payload_current
                WHERE FALSE
                """
            )
        fetched = cur.fetchall()

    observed_by_name = {
        str(row[0]): {
            "snapshot_id": str(row[1]),
            "run_id": str(row[2]),
            "refresh_mode": str(row[3]),
        }
        for row in fetched
    }
    observed_names = sorted(observed_by_name.keys())
    out["verified_row_count"] = len(observed_names)
    out["verified_payload_names"] = observed_names

    if replace_all:
        unexpected = sorted(set(observed_names) - set(payload_names))
        missing = sorted(set(payload_names) - set(observed_names))
        if unexpected:
            out["issues"].extend(f"unexpected_payload:{name}" for name in unexpected)
        if missing:
            out["issues"].extend(f"missing_payload:{name}" for name in missing)
        if len(observed_names) != len(payload_names):
            out["issues"].append(
                f"row_count_mismatch:{len(observed_names)}!={len(payload_names)}"
            )
    else:
        missing = sorted(set(payload_names) - set(observed_names))
        if missing:
            out["issues"].extend(f"missing_payload:{name}" for name in missing)

    for payload_name in payload_names:
        expected = expected_by_name.get(payload_name) or {}
        observed = observed_by_name.get(payload_name)
        if observed is None:
            continue
        for field in ("snapshot_id", "run_id", "refresh_mode"):
            if str(observed.get(field) or "") != str(expected.get(field) or ""):
                out["issues"].append(
                    f"metadata_mismatch:{payload_name}:{field}:{observed.get(field)}!={expected.get(field)}"
                )

    if out["issues"]:
        out["status"] = "error"
    return out
