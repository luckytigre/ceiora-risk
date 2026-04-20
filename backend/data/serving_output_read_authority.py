"""Lower serving-payload read authority helpers behind the serving facade."""

from __future__ import annotations

from collections.abc import Callable, Iterable
import logging
import sqlite3
from typing import Any

from psycopg.rows import dict_row

logger = logging.getLogger(__name__)


def _sanitize_error_message(message: str) -> str:
    clean = str(message or "")
    if "postgres://" in clean or "postgresql://" in clean:
        return "<sanitized postgres error>"
    return clean


def _error_result(*, source: str, exc: Exception) -> dict[str, Any]:
    return {
        "status": "error",
        "source": str(source),
        "value": None,
        "error": {
            "type": type(exc).__name__,
            "message": _sanitize_error_message(str(exc)),
        },
    }


def load_current_payload_states_sqlite(
    payload_names: Iterable[str],
    *,
    data_db,
    normalize_payload_names: Callable[[Iterable[str]], list[str]],
    ensure_sqlite_schema: Callable[[Any], None],
    decode_payload_json: Callable[[Any], Any],
) -> dict[str, dict[str, Any]]:
    clean_names = normalize_payload_names(payload_names)
    if not clean_names:
        return {}
    if not data_db.exists():
        return {
            name: {"status": "missing", "source": "sqlite", "value": None}
            for name in clean_names
        }
    try:
        conn = sqlite3.connect(str(data_db))
        try:
            ensure_sqlite_schema(conn)
            placeholders = ",".join("?" for _ in clean_names)
            rows = conn.execute(
                """
                SELECT payload_name, payload_json
                FROM serving_payload_current
                WHERE payload_name IN ("""
                + placeholders
                + ")",
                clean_names,
            ).fetchall()
        finally:
            conn.close()
    except Exception as exc:
        logger.error(
            "SQLite payload read failed during serving payload read: %s: %s",
            type(exc).__name__,
            _sanitize_error_message(str(exc)),
        )
        return {
            name: _error_result(source="sqlite", exc=exc)
            for name in clean_names
        }
    out = {
        name: {"status": "missing", "source": "sqlite", "value": None}
        for name in clean_names
    }
    for payload_name, raw_payload in rows:
        out[str(payload_name)] = {
            "status": "ok",
            "source": "sqlite",
            "value": decode_payload_json(raw_payload),
        }
    return out


def load_current_payloads_sqlite(
    payload_names: Iterable[str],
    *,
    data_db,
    normalize_payload_names: Callable[[Iterable[str]], list[str]],
    ensure_sqlite_schema: Callable[[Any], None],
    decode_payload_json: Callable[[Any], Any],
) -> dict[str, Any | None]:
    states = load_current_payload_states_sqlite(
        payload_names,
        data_db=data_db,
        normalize_payload_names=normalize_payload_names,
        ensure_sqlite_schema=ensure_sqlite_schema,
        decode_payload_json=decode_payload_json,
    )
    return {
        name: state.get("value")
        for name, state in states.items()
    }


def load_current_payload_states_neon(
    payload_names: Iterable[str],
    *,
    normalize_payload_names: Callable[[Iterable[str]], list[str]],
    connect_fn: Callable[..., Any],
    resolve_dsn_fn: Callable[[Any], Any],
    decode_payload_json: Callable[[Any], Any],
) -> dict[str, dict[str, Any]]:
    clean_names = normalize_payload_names(payload_names)
    if not clean_names:
        return {}
    try:
        conn = connect_fn(
            dsn=resolve_dsn_fn(None),
            autocommit=True,
            connect_timeout=5,
            options={"options": "-c statement_timeout=8000"},
        )
    except Exception as exc:
        logger.error(
            "Neon connection failed during serving payload read: %s: %s",
            type(exc).__name__,
            _sanitize_error_message(str(exc)),
        )
        return {
            name: _error_result(source="neon", exc=exc)
            for name in clean_names
        }
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT payload_name, payload_json
                FROM serving_payload_current
                WHERE payload_name = ANY(%s)
                """,
                (clean_names,),
            )
            rows = cur.fetchall()
    except Exception as exc:
        logger.error(
            "Neon query failed during serving payload read: %s: %s",
            type(exc).__name__,
            _sanitize_error_message(str(exc)),
        )
        return {
            name: _error_result(source="neon", exc=exc)
            for name in clean_names
        }
    finally:
        conn.close()
    out = {
        name: {"status": "missing", "source": "neon", "value": None}
        for name in clean_names
    }
    for row in rows:
        payload_name = str(row.get("payload_name") or "").strip()
        if not payload_name:
            continue
        out[payload_name] = {
            "status": "ok",
            "source": "neon",
            "value": decode_payload_json(row.get("payload_json")),
        }
    return out


def load_current_payloads_neon(
    payload_names: Iterable[str],
    *,
    normalize_payload_names: Callable[[Iterable[str]], list[str]],
    connect_fn: Callable[..., Any],
    resolve_dsn_fn: Callable[[Any], Any],
    decode_payload_json: Callable[[Any], Any],
) -> dict[str, Any | None]:
    states = load_current_payload_states_neon(
        payload_names,
        normalize_payload_names=normalize_payload_names,
        connect_fn=connect_fn,
        resolve_dsn_fn=resolve_dsn_fn,
        decode_payload_json=decode_payload_json,
    )
    return {
        name: state.get("value")
        for name, state in states.items()
    }
