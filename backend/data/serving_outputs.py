"""Durable serving payload snapshots for cloud-safe frontend reads."""

from __future__ import annotations

from collections.abc import Callable, Iterable
import hashlib
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend import config
from backend.data import serving_output_read_authority
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


def load_runtime_payloads(
    payload_names: Iterable[str],
    *,
    fallback_loader: Callable[[str], Any | None] | None = None,
) -> dict[str, Any | None]:
    """Load multiple runtime truth payloads with durable reads first and per-key fallback second."""
    clean_names = _normalize_payload_names(payload_names)
    if not clean_names:
        return {}
    payloads = load_current_payloads(clean_names)
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


def load_current_payloads(payload_names: Iterable[str]) -> dict[str, Any | None]:
    """Load multiple durable serving payloads in one read path where possible."""
    clean_names = _normalize_payload_names(payload_names)
    if not clean_names:
        return {}
    if _use_neon_reads():
        payloads = _load_current_payloads_neon(clean_names)
        if config.serving_outputs_cache_fallback_enabled():
            missing = [name for name in clean_names if payloads.get(name) is None]
            if missing:
                sqlite_payloads = _load_current_payloads_sqlite(missing)
                for name in missing:
                    if payloads.get(name) is None:
                        payloads[name] = sqlite_payloads.get(name)
        return payloads
    return _load_current_payloads_sqlite(clean_names)


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
    left_payloads = dict(left.get("payloads") or {})
    right_payloads = dict(right.get("payloads") or {})
    left_names = sorted(left_payloads.keys())
    right_names = sorted(right_payloads.keys())
    common_names = sorted(set(left_names).intersection(right_names))
    issues: list[str] = []

    missing_left = sorted(set(right_names) - set(left_names))
    missing_right = sorted(set(left_names) - set(right_names))
    issues.extend(f"missing_left:{name}" for name in missing_left)
    issues.extend(f"missing_right:{name}" for name in missing_right)

    for field in ("snapshot_id", "run_id", "refresh_mode", "payload_sha256"):
        for payload_name in common_names:
            left_value = str(left_payloads.get(payload_name, {}).get(field) or "")
            right_value = str(right_payloads.get(payload_name, {}).get(field) or "")
            if left_value != right_value:
                issues.append(
                    f"mismatch:{payload_name}:{field}:{left_value}!={right_value}"
                )

    if sorted(left.get("distinct_snapshot_ids") or []) != sorted(right.get("distinct_snapshot_ids") or []):
        issues.append(
            "manifest_mismatch:distinct_snapshot_ids:"
            f"{sorted(left.get('distinct_snapshot_ids') or [])}!={sorted(right.get('distinct_snapshot_ids') or [])}"
        )
    if sorted(left.get("distinct_run_ids") or []) != sorted(right.get("distinct_run_ids") or []):
        issues.append(
            "manifest_mismatch:distinct_run_ids:"
            f"{sorted(left.get('distinct_run_ids') or [])}!={sorted(right.get('distinct_run_ids') or [])}"
        )
    if sorted(left.get("distinct_refresh_modes") or []) != sorted(right.get("distinct_refresh_modes") or []):
        issues.append(
            "manifest_mismatch:distinct_refresh_modes:"
            f"{sorted(left.get('distinct_refresh_modes') or [])}!={sorted(right.get('distinct_refresh_modes') or [])}"
        )

    return {
        "status": "ok" if not issues else "error",
        "left_store": str(left.get("store") or ""),
        "right_store": str(right.get("store") or ""),
        "issues": issues,
        "left_row_count": int(left.get("row_count") or 0),
        "right_row_count": int(right.get("row_count") or 0),
        "shared_payload_count": len(common_names),
    }


def _manifest_from_rows(
    rows: list[tuple[Any, ...]],
    *,
    store: str,
    requested_payload_names: list[str],
) -> dict[str, Any]:
    payloads: dict[str, dict[str, Any]] = {}
    snapshot_ids: set[str] = set()
    run_ids: set[str] = set()
    refresh_modes: set[str] = set()
    observed_names: list[str] = []
    for payload_name, snapshot_id, run_id, refresh_mode, payload_json, updated_at in rows:
        clean_name = str(payload_name or "").strip()
        if not clean_name:
            continue
        snapshot = str(snapshot_id or "").strip()
        run = str(run_id or "").strip()
        mode = str(refresh_mode or "").strip()
        payload_text = str(payload_json or "")
        payloads[clean_name] = {
            "snapshot_id": snapshot,
            "run_id": run,
            "refresh_mode": mode,
            "updated_at": str(updated_at or ""),
            "payload_sha256": _payload_semantic_hash(payload_text),
            "payload_bytes": len(payload_text.encode("utf-8")),
        }
        observed_names.append(clean_name)
        if snapshot:
            snapshot_ids.add(snapshot)
        if run:
            run_ids.add(run)
        if mode:
            refresh_modes.add(mode)

    observed_name_set = set(observed_names)
    missing_requested = sorted(set(requested_payload_names) - observed_name_set)
    missing_canonical = sorted(_CANONICAL_SERVING_PAYLOAD_NAME_SET - observed_name_set)
    return {
        "status": "ok",
        "store": str(store),
        "row_count": len(observed_names),
        "payload_names": sorted(observed_names),
        "requested_payload_names": list(requested_payload_names),
        "missing_requested_payloads": missing_requested,
        "payloads": payloads,
        "distinct_snapshot_ids": sorted(snapshot_ids),
        "distinct_run_ids": sorted(run_ids),
        "distinct_refresh_modes": sorted(refresh_modes),
        "canonical_payload_set_complete": not missing_canonical,
        "missing_canonical_payloads": missing_canonical,
    }


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


def _persist_current_payloads_neon(
    rows: list[tuple[str, str, str, str, str, str]],
    *,
    replace_all: bool,
    write_mode: str,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=False)
    except Exception as exc:
        return {
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
    try:
        _ensure_postgres_schema(conn)
        upsert_sql = """
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
            """
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
            if write_mode == "row_by_row":
                for row in rows:
                    cur.execute(upsert_sql, row)
            else:
                cur.executemany(upsert_sql, rows)
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
                "error": {
                    "type": "RuntimeError",
                    "message": "Neon serving payload verification failed: "
                    + ", ".join(str(issue) for issue in verification.get("issues") or []),
                },
            }
        conn.commit()
        return {
            "status": "ok",
            "row_count": len(rows),
            "replace_all": bool(replace_all),
            "write_mode": write_mode,
            "duration_seconds": round(float(time.perf_counter() - started_at), 3),
            "verification": verification,
        }
    except Exception as exc:
        conn.rollback()
        return {
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "write_mode": write_mode,
            "duration_seconds": round(float(time.perf_counter() - started_at), 3),
        }
    finally:
        conn.close()


def _persist_current_payloads_sqlite(
    rows: list[tuple[str, str, str, str, str, str]],
    *,
    data_db: Path,
    replace_all: bool,
) -> dict[str, Any]:
    started_at = time.perf_counter()
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
            "duration_seconds": round(float(time.perf_counter() - started_at), 3),
        }
    except Exception as exc:
        conn.rollback()
        return {
            "status": "error",
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "duration_seconds": round(float(time.perf_counter() - started_at), 3),
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
            "payload_value": _normalize_payload_value(row[4]),
            "payload_json_sha256": _payload_semantic_hash(row[4]),
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
                SELECT payload_name, snapshot_id, run_id, refresh_mode, payload_json::text
                FROM serving_payload_current
                ORDER BY payload_name
                """
            )
        elif payload_names:
            cur.execute(
                """
                SELECT payload_name, snapshot_id, run_id, refresh_mode, payload_json::text
                FROM serving_payload_current
                WHERE payload_name = ANY(%s)
                ORDER BY payload_name
                """,
                (payload_names,),
            )
        else:
            cur.execute(
                """
                SELECT payload_name, snapshot_id, run_id, refresh_mode, payload_json::text
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
            "payload_value": _normalize_payload_value(row[4]),
            "payload_json_sha256": _payload_semantic_hash(row[4]),
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
        if observed.get("payload_value") != expected.get("payload_value"):
            out["issues"].append(
                "metadata_mismatch:"
                f"{payload_name}:payload_json_sha256:{observed.get('payload_json_sha256')}!={expected.get('payload_json_sha256')}"
            )

    if out["issues"]:
        out["status"] = "error"
    return out
