"""Lower runtime-state authority helpers behind the runtime-state facade."""

from __future__ import annotations

from collections.abc import Callable
import json
from datetime import datetime, timezone
from typing import Any

from psycopg.rows import dict_row


def read_neon_runtime_state(
    state_key: str,
    *,
    connect_fn: Callable[..., Any],
    resolve_dsn_fn: Callable[[Any], Any],
) -> dict[str, Any]:
    try:
        conn = connect_fn(dsn=resolve_dsn_fn(None), autocommit=True)
    except Exception as exc:
        return {
            "status": "error",
            "source": "neon",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "value": None,
        }
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT value_json
                FROM runtime_state_current
                WHERE state_key = %s
                LIMIT 1
                """,
                (state_key,),
            )
            row = cur.fetchone()
    except Exception as exc:
        return {
            "status": "error",
            "source": "neon",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "value": None,
        }
    finally:
        conn.close()
    if not row:
        return {"status": "missing", "source": "neon", "value": None}
    raw = row.get("value_json")
    if raw is None:
        return {"status": "missing", "source": "neon", "value": None}
    if isinstance(raw, (dict, list)):
        return {"status": "ok", "source": "neon", "value": raw}
    return {"status": "ok", "source": "neon", "value": json.loads(str(raw))}


def write_neon_runtime_state(
    state_key: str,
    value: Any,
    *,
    connect_fn: Callable[..., Any],
    resolve_dsn_fn: Callable[[Any], Any],
    ensure_postgres_schema: Callable[[Any], None],
) -> dict[str, Any]:
    payload_json = json.dumps(value, default=str, sort_keys=True, separators=(",", ":"))
    updated_at = datetime.now(timezone.utc).isoformat()
    try:
        conn = connect_fn(dsn=resolve_dsn_fn(None), autocommit=False)
    except Exception as exc:
        return {
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    try:
        ensure_postgres_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO runtime_state_current (state_key, value_json, updated_at)
                VALUES (%s, %s::jsonb, %s::timestamptz)
                ON CONFLICT (state_key) DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    updated_at = EXCLUDED.updated_at
                """,
                (state_key, payload_json, updated_at),
            )
        conn.commit()
        return {"status": "ok", "updated_at": updated_at}
    except Exception as exc:
        conn.rollback()
        return {
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    finally:
        conn.close()


def write_fallback_runtime_state(
    state_key: str,
    value: Any,
    *,
    fallback_writer,
) -> dict[str, Any]:
    try:
        fallback_writer(state_key, value)
        return {"status": "ok"}
    except Exception as exc:
        return {
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
