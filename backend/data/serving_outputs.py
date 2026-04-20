"""Durable serving payload snapshots for cloud-safe frontend reads."""

from __future__ import annotations

from collections.abc import Callable, Iterable
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend import config
from backend.data import serving_output_manifest
from backend.data import serving_output_read_authority
from backend.data import serving_output_write_authority
from backend.data.neon import connect, resolve_dsn
from backend.data.neon_primary_write import execute_neon_primary_write

DATA_DB = Path(config.DATA_DB_PATH)
SURFACE_NAME = "serving_outputs"
CANONICAL_SERVING_PAYLOAD_NAMES: tuple[str, ...] = (
    "eligibility",
    "exposures",
    "health_diagnostics",
    "model_sanity",
    "portfolio",
    "refresh_meta",
    "risk",
    "risk_engine_cov",
    "risk_engine_specific_risk",
    "universe_factors",
    "universe_loadings",
)
_CANONICAL_SERVING_PAYLOAD_NAME_SET = frozenset(CANONICAL_SERVING_PAYLOAD_NAMES)
_NEON_WRITE_MODES = frozenset({"bulk", "row_by_row"})


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


def canonical_serving_payload_names() -> tuple[str, ...]:
    return CANONICAL_SERVING_PAYLOAD_NAMES


def load_runtime_payload(
    payload_name: str,
    *,
    fallback_loader: Callable[[str], Any | None] | None = None,
) -> Any | None:
    """Load the runtime truth payload, using cache fallback only when policy allows it."""
    clean = str(payload_name or "").strip()
    if not clean:
        return None
    return load_runtime_payloads((clean,), fallback_loader=fallback_loader).get(clean)


def load_runtime_payload_field(
    payload_name: str,
    field_name: str,
    *,
    fallback_loader: Callable[[str], Any | None] | None = None,
) -> Any | None:
    """Load a single top-level field from a runtime payload when available."""
    clean = str(payload_name or "").strip()
    field = str(field_name or "").strip()
    if not clean or not field:
        return None
    value = (
        _load_current_payload_field_neon(clean, field)
        if _use_neon_reads()
        else _load_current_payload_field_sqlite(clean, field)
    )
    if value is not None:
        return value
    if fallback_loader is None or not config.serving_outputs_cache_fallback_enabled():
        return None
    fallback = fallback_loader(clean)
    if isinstance(fallback, dict):
        return fallback.get(field)
    return None


def load_runtime_payload_state(
    payload_name: str,
    *,
    fallback_loader: Callable[[str], Any | None] | None = None,
) -> dict[str, Any]:
    clean = str(payload_name or "").strip()
    if not clean:
        return {"status": "missing", "source": "none", "value": None}
    state = load_current_payload_states((clean,)).get(clean) or {
        "status": "missing",
        "source": "none",
        "value": None,
    }
    if str(state.get("status") or "") == "ok":
        return state
    if fallback_loader is None or not config.serving_outputs_cache_fallback_enabled():
        return state
    try:
        payload = fallback_loader(clean)
    except Exception as exc:
        return {
            "status": "error",
            "source": "cache",
            "value": None,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
            },
        }
    if payload is None:
        return {"status": "missing", "source": "cache", "value": None}
    return {"status": "ok", "source": "cache", "value": payload}


def load_runtime_payloads(
    payload_names: Iterable[str],
    *,
    fallback_loader: Callable[[str], Any | None] | None = None,
) -> dict[str, Any | None]:
    """Load multiple runtime truth payloads with durable reads first and per-key fallback second."""
    clean_names = _normalize_payload_names(payload_names)
    if not clean_names:
        return {}
    payloads = {
        name: (state.get("value") if str(state.get("status") or "") == "ok" else None)
        for name, state in load_current_payload_states(clean_names).items()
    }
    if fallback_loader is None or not config.serving_outputs_cache_fallback_enabled():
        return payloads
    for payload_name in clean_names:
        if payloads.get(payload_name) is None:
            payloads[payload_name] = fallback_loader(payload_name)
    return payloads


def persist_current_payloads(
    *,
    data_db: Path,
    run_id: str,
    snapshot_id: str,
    refresh_mode: str,
    payloads: dict[str, Any],
    replace_all: bool = False,
    neon_write_mode: str = "bulk",
) -> dict[str, Any]:
    payload_names = _normalize_payload_names(payloads.keys())
    if replace_all:
        _validate_replace_all_payload_names(payload_names)
    write_mode = _normalize_neon_write_mode(neon_write_mode)
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
    payload_bytes = {
        str(row[0]): len(str(row[4]).encode("utf-8"))
        for row in rows
    }
    result = {
        "status": "ok",
        "snapshot_id": str(snapshot_id),
        "row_count": len(rows),
        "payload_names": payload_names,
        "replace_all": bool(replace_all),
        "write_mode": write_mode,
        "payload_bytes_total": int(sum(payload_bytes.values())),
        "payload_bytes_max": int(max(payload_bytes.values(), default=0)),
    }
    return execute_neon_primary_write(
        base_result=result,
        neon_enabled=bool(config.neon_surface_enabled(SURFACE_NAME)),
        neon_required=bool(config.serving_payload_neon_write_required()),
        perform_neon_write=lambda: _persist_current_payloads_neon(
            rows,
            replace_all=replace_all,
            write_mode=write_mode,
        ),
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
    return load_current_payloads((clean,)).get(clean)


def load_current_payload_states(payload_names: Iterable[str]) -> dict[str, dict[str, Any]]:
    clean_names = _normalize_payload_names(payload_names)
    if not clean_names:
        return {}
    if _use_neon_reads():
        return _load_current_payload_states_neon(clean_names)
    return _load_current_payload_states_sqlite(clean_names)


def load_current_payloads(payload_names: Iterable[str]) -> dict[str, Any | None]:
    """Load multiple durable serving payloads in one read path where possible."""
    states = load_current_payload_states(payload_names)
    return {
        name: (state.get("value") if str(state.get("status") or "") == "ok" else None)
        for name, state in states.items()
    }


def _normalize_payload_names(payload_names: Iterable[str]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for raw in payload_names:
        clean = str(raw or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        names.append(clean)
    return names


def _normalize_neon_write_mode(write_mode: str) -> str:
    clean = str(write_mode or "bulk").strip().lower() or "bulk"
    if clean not in _NEON_WRITE_MODES:
        raise ValueError(
            f"Unsupported Neon serving payload write mode '{write_mode}'. "
            f"Expected one of {sorted(_NEON_WRITE_MODES)}."
        )
    return clean


def _validate_replace_all_payload_names(payload_names: Iterable[str]) -> None:
    clean_names = _normalize_payload_names(payload_names)
    clean_set = frozenset(clean_names)
    if clean_set != _CANONICAL_SERVING_PAYLOAD_NAME_SET:
        missing = sorted(_CANONICAL_SERVING_PAYLOAD_NAME_SET - clean_set)
        unexpected = sorted(clean_set - _CANONICAL_SERVING_PAYLOAD_NAME_SET)
        raise ValueError(
            "replace_all=True requires the canonical serving payload set. "
            f"missing={missing} unexpected={unexpected}"
        )


def _decode_payload_json(raw: Any) -> dict[str, Any] | list[Any] | None:
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    return json.loads(str(raw))


def _normalize_payload_value(raw: Any) -> Any:
    value = _decode_payload_json(raw)
    return _normalize_json_value(value)


def _normalize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _normalize_json_value(inner)
            for key, inner in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, list):
        return [_normalize_json_value(item) for item in value]
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        normalized = float(value)
        return 0.0 if normalized == 0.0 else normalized
    return value


def _normalize_payload_json_text(raw: Any) -> str:
    normalized = _normalize_payload_value(raw)
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def _payload_semantic_hash(raw: Any) -> str:
    normalized = _normalize_payload_value(raw)
    return hashlib.sha256(repr(normalized).encode("utf-8")).hexdigest()


def collect_current_payload_manifest(
    *,
    store: str,
    payload_names: Iterable[str] | None = None,
    data_db: Path | None = None,
) -> dict[str, Any]:
    clean_store = str(store or "").strip().lower()
    clean_names = _normalize_payload_names(payload_names or ())
    if clean_store == "sqlite":
        rows = _load_current_payload_rows_sqlite(clean_names or None, data_db=data_db)
    elif clean_store == "neon":
        rows = _load_current_payload_rows_neon(clean_names or None)
    else:
        raise ValueError("store must be 'sqlite' or 'neon'")
    return _manifest_from_rows(
        rows,
        store=clean_store,
        requested_payload_names=clean_names,
    )


def compare_current_payload_manifests(
    left: dict[str, Any],
    right: dict[str, Any],
) -> dict[str, Any]:
    return serving_output_manifest.compare_current_payload_manifests(left, right)


def _manifest_from_rows(
    rows: list[tuple[Any, ...]],
    *,
    store: str,
    requested_payload_names: list[str],
) -> dict[str, Any]:
    return serving_output_manifest.manifest_from_rows(
        rows,
        store=store,
        requested_payload_names=requested_payload_names,
        payload_semantic_hash=_payload_semantic_hash,
        canonical_serving_payload_name_set=_CANONICAL_SERVING_PAYLOAD_NAME_SET,
    )


def _load_current_payload_rows_sqlite(
    payload_names: Iterable[str] | None,
    *,
    data_db: Path | None = None,
) -> list[tuple[Any, ...]]:
    db = Path(data_db or DATA_DB)
    if not db.exists():
        return []
    clean_names = _normalize_payload_names(payload_names or ())
    conn = sqlite3.connect(str(db))
    try:
        _ensure_sqlite_schema(conn)
        if clean_names:
            placeholders = ",".join("?" for _ in clean_names)
            rows = conn.execute(
                """
                SELECT payload_name, snapshot_id, run_id, refresh_mode, payload_json, updated_at
                FROM serving_payload_current
                WHERE payload_name IN ("""
                + placeholders
                + ") ORDER BY payload_name",
                clean_names,
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT payload_name, snapshot_id, run_id, refresh_mode, payload_json, updated_at
                FROM serving_payload_current
                ORDER BY payload_name
                """
            ).fetchall()
    finally:
        conn.close()
    return list(rows)


def _load_current_payload_rows_neon(
    payload_names: Iterable[str] | None,
) -> list[tuple[Any, ...]]:
    clean_names = _normalize_payload_names(payload_names or ())
    try:
        conn = connect(
            dsn=resolve_dsn(None),
            autocommit=True,
            connect_timeout=5,
            options={"options": "-c statement_timeout=8000"},
        )
    except Exception:
        return []
    try:
        with conn.cursor() as cur:
            if clean_names:
                cur.execute(
                    """
                    SELECT payload_name, snapshot_id, run_id, refresh_mode, payload_json::text, updated_at
                    FROM serving_payload_current
                    WHERE payload_name = ANY(%s)
                    ORDER BY payload_name
                    """,
                    (clean_names,),
                )
            else:
                cur.execute(
                    """
                    SELECT payload_name, snapshot_id, run_id, refresh_mode, payload_json::text, updated_at
                    FROM serving_payload_current
                    ORDER BY payload_name
                    """
                )
            rows = cur.fetchall()
    except Exception:
        return []
    finally:
        conn.close()
    return list(rows)


def _load_current_payload_field_sqlite(
    payload_name: str,
    field_name: str,
    *,
    data_db: Path | None = None,
) -> Any | None:
    db = Path(data_db or DATA_DB)
    if not db.exists():
        return None
    conn = sqlite3.connect(str(db))
    try:
        _ensure_sqlite_schema(conn)
        row = conn.execute(
            """
            SELECT json_extract(payload_json, ?)
            FROM serving_payload_current
            WHERE payload_name = ?
            """,
            (f"$.{field_name}", payload_name),
        ).fetchone()
    except Exception:
        return None
    finally:
        conn.close()
    if not row:
        return None
    return _decode_payload_json(row[0])


def _load_current_payload_field_neon(
    payload_name: str,
    field_name: str,
) -> Any | None:
    try:
        conn = connect(
            dsn=resolve_dsn(None),
            autocommit=True,
            connect_timeout=5,
            options={"options": "-c statement_timeout=8000"},
        )
    except Exception:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT (payload_json -> %s)::text
                FROM serving_payload_current
                WHERE payload_name = %s
                """,
                (field_name, payload_name),
            )
            row = cur.fetchone()
    except Exception:
        return None
    finally:
        conn.close()
    if not row:
        return None
    return _decode_payload_json(row[0])


def _load_current_payloads_sqlite(payload_names: Iterable[str]) -> dict[str, Any | None]:
    return serving_output_read_authority.load_current_payloads_sqlite(
        payload_names,
        data_db=DATA_DB,
        normalize_payload_names=_normalize_payload_names,
        ensure_sqlite_schema=_ensure_sqlite_schema,
        decode_payload_json=_decode_payload_json,
    )


def _load_current_payload_neon(payload_name: str) -> dict[str, Any] | list[Any] | None:
    return _load_current_payloads_neon((payload_name,)).get(payload_name)


def _load_current_payloads_neon(payload_names: Iterable[str]) -> dict[str, Any | None]:
    return serving_output_read_authority.load_current_payloads_neon(
        payload_names,
        normalize_payload_names=_normalize_payload_names,
        connect_fn=connect,
        resolve_dsn_fn=resolve_dsn,
        decode_payload_json=_decode_payload_json,
    )


def _load_current_payload_states_sqlite(payload_names: Iterable[str]) -> dict[str, dict[str, Any]]:
    return serving_output_read_authority.load_current_payload_states_sqlite(
        payload_names,
        data_db=DATA_DB,
        normalize_payload_names=_normalize_payload_names,
        ensure_sqlite_schema=_ensure_sqlite_schema,
        decode_payload_json=_decode_payload_json,
    )


def _load_current_payload_states_neon(payload_names: Iterable[str]) -> dict[str, dict[str, Any]]:
    return serving_output_read_authority.load_current_payload_states_neon(
        payload_names,
        normalize_payload_names=_normalize_payload_names,
        connect_fn=connect,
        resolve_dsn_fn=resolve_dsn,
        decode_payload_json=_decode_payload_json,
    )


def _persist_current_payloads_neon(
    rows: list[tuple[str, str, str, str, str, str]],
    *,
    replace_all: bool,
    write_mode: str,
) -> dict[str, Any]:
    return serving_output_write_authority.persist_current_payloads_neon(
        rows,
        replace_all=replace_all,
        write_mode=write_mode,
        connect_fn=connect,
        resolve_dsn_fn=resolve_dsn,
        ensure_postgres_schema=_ensure_postgres_schema,
        verify_current_payloads_neon=_verify_current_payloads_neon,
    )


def _persist_current_payloads_sqlite(
    rows: list[tuple[str, str, str, str, str, str]],
    *,
    data_db: Path,
    replace_all: bool,
) -> dict[str, Any]:
    return serving_output_write_authority.persist_current_payloads_sqlite(
        rows,
        data_db=data_db,
        replace_all=replace_all,
        ensure_sqlite_schema=_ensure_sqlite_schema,
    )


def _verify_current_payloads_neon(
    pg_conn,
    *,
    rows: list[tuple[str, str, str, str, str, str]],
    replace_all: bool,
) -> dict[str, Any]:
    return serving_output_write_authority.verify_current_payloads_neon(
        pg_conn,
        rows=rows,
        replace_all=replace_all,
        normalize_payload_value=_normalize_payload_value,
        payload_semantic_hash=_payload_semantic_hash,
    )
