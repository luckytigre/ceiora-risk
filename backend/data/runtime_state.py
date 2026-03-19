"""Neon-first runtime-state persistence for operator and recovery surfaces."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from psycopg.rows import dict_row

from backend import config
from backend.data.neon import connect, resolve_dsn
from backend.data.neon_primary_write import execute_neon_primary_write

SURFACE_NAME = "runtime_state"
_ACTIVE_SNAPSHOT_KEY = "__cache_snapshot_active"
ALLOWED_RUNTIME_STATE_KEYS = frozenset(
    {
        "risk_engine_meta",
        "neon_sync_health",
        "refresh_status",
        "holdings_sync_state",
        _ACTIVE_SNAPSHOT_KEY,
    }
)
logger = logging.getLogger(__name__)


def _use_neon_reads() -> bool:
    return bool(
        config.runtime_state_primary_reads_enabled()
        and config.neon_surface_enabled(SURFACE_NAME)
    )


def _validate_state_key(state_key: str) -> str:
    clean = str(state_key or "").strip()
    if not clean:
        raise ValueError("state_key is required")
    if clean not in ALLOWED_RUNTIME_STATE_KEYS:
        raise ValueError(f"unsupported runtime_state key: {clean}")
    return clean


def _ensure_postgres_schema(pg_conn) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_state_current (
                state_key TEXT PRIMARY KEY,
                value_json JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            """
        )


def _read_neon_runtime_state(state_key: str) -> dict[str, Any]:
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=True)
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


def _write_neon_runtime_state(state_key: str, value: Any) -> dict[str, Any]:
    payload_json = json.dumps(value, default=str, sort_keys=True, separators=(",", ":"))
    updated_at = datetime.now(timezone.utc).isoformat()
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


def load_runtime_state(
    state_key: str,
    *,
    fallback_loader=None,
) -> Any | None:
    return (read_runtime_state(state_key, fallback_loader=fallback_loader) or {}).get("value")


def read_runtime_state(
    state_key: str,
    *,
    fallback_loader=None,
) -> dict[str, Any]:
    clean = _validate_state_key(state_key)
    if _use_neon_reads():
        neon_result = _read_neon_runtime_state(clean)
        if str(neon_result.get("status") or "") == "ok":
            return neon_result
        if not config.runtime_state_cache_fallback_enabled():
            return neon_result
    if fallback_loader is None:
        return {"status": "missing", "source": "none", "value": None}
    try:
        payload = fallback_loader(clean)
    except Exception as exc:
        return {
            "status": "error",
            "source": "sqlite",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "value": None,
        }
    if payload is None:
        return {"status": "missing", "source": "sqlite", "value": None}
    return {"status": "ok", "source": "sqlite", "value": payload}


def persist_runtime_state(
    state_key: str,
    value: Any,
    *,
    fallback_writer=None,
) -> dict[str, Any]:
    clean = _validate_state_key(state_key)
    result = execute_neon_primary_write(
        base_result={
            "status": "ok",
            "state_key": clean,
        },
        neon_enabled=bool(config.neon_surface_enabled(SURFACE_NAME)),
        neon_required=bool(config.runtime_state_neon_write_required()),
        perform_neon_write=lambda: _write_neon_runtime_state(clean, value),
        perform_fallback_write=(
            (lambda: _write_fallback_runtime_state(clean, value, fallback_writer=fallback_writer))
            if fallback_writer is not None
            else None
        ),
        failure_label="runtime-state persistence",
        fallback_result_key="fallback_write",
        fallback_authority="sqlite",
    )
    neon_status = str((result.get("neon_write") or {}).get("status") or "")
    fallback_status = str((result.get("fallback_write") or {}).get("status") or "")
    if neon_status not in {"", "ok", "skipped"}:
        logger.warning("Runtime-state Neon write issue: key=%s status=%s detail=%s", clean, neon_status, result.get("neon_write"))
    if fallback_status not in {"", "ok", "skipped"}:
        logger.warning("Runtime-state fallback write issue: key=%s status=%s detail=%s", clean, fallback_status, result.get("fallback_write"))
    return result


def _write_fallback_runtime_state(
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


def publish_active_snapshot(
    snapshot_id: str,
    *,
    fallback_publisher=None,
) -> dict[str, Any]:
    clean = str(snapshot_id or "").strip()
    if not clean:
        raise ValueError("snapshot_id is required")
    payload = {
        "snapshot_id": clean,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }

    def _fallback_writer(_state_key: str, _value: Any) -> None:
        if fallback_publisher is None:
            return None
        fallback_publisher(clean)
        return None

    return persist_runtime_state(
        _ACTIVE_SNAPSHOT_KEY,
        payload,
        fallback_writer=_fallback_writer if fallback_publisher is not None else None,
    )
