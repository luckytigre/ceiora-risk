"""Durable model-output persistence facade."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from backend import config
from backend.data import (
    model_output_payloads as payloads,
    model_output_schema as schema,
    model_output_state as state,
    model_output_writers as writers,
)
from backend.data.neon import connect, resolve_dsn


def _neon_model_output_writes_enabled() -> bool:
    return bool(str(config.neon_dsn() or "").strip())


def _neon_model_output_writes_required() -> bool:
    return bool(
        _neon_model_output_writes_enabled()
        and config.neon_primary_model_data_enabled()
    )

def _resolve_data_db(data_db: Path | None = None) -> Path:
    return Path(data_db or config.DATA_DB_PATH)


def _load_latest_sqlite_payload(
    sqlite_loader,
    *,
    data_db: Path | None = None,
) -> dict[str, Any]:
    db_path = _resolve_data_db(data_db)
    if not db_path.exists():
        return {}
    conn = sqlite3.connect(str(db_path))
    try:
        schema.ensure_schema(conn)
        payload = sqlite_loader(conn)
    finally:
        conn.close()
    return payload if isinstance(payload, dict) else {}


def _load_latest_neon_payload(pg_loader) -> dict[str, Any]:
    if not _neon_model_output_writes_enabled():
        return {}
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=True)
    except Exception:
        return {}
    try:
        payload = pg_loader(conn)
    except Exception:
        return {}
    finally:
        conn.close()
    return payload if isinstance(payload, dict) else {}


def _rebuild_authority_uses_neon() -> bool:
    return bool(
        _neon_model_output_writes_enabled()
        and config.neon_authoritative_rebuilds_enabled()
    )


def load_latest_rebuild_authority_risk_engine_state() -> dict[str, Any]:
    """Read model metadata from the current rebuild-authority store."""
    if _rebuild_authority_uses_neon():
        return _load_latest_neon_payload(state.pg_latest_risk_engine_state)
    return _load_latest_sqlite_payload(state.latest_risk_engine_state)


def load_latest_rebuild_authority_covariance_payload() -> dict[str, Any]:
    """Read covariance from the current rebuild-authority store."""
    if _rebuild_authority_uses_neon():
        return _load_latest_neon_payload(state.pg_latest_covariance_payload)
    return _load_latest_sqlite_payload(state.latest_covariance_payload)


def load_latest_rebuild_authority_specific_risk_payload() -> dict[str, Any]:
    """Read specific risk from the current rebuild-authority store."""
    if _rebuild_authority_uses_neon():
        return _load_latest_neon_payload(state.pg_latest_specific_risk_payload)
    return _load_latest_sqlite_payload(state.latest_specific_risk_payload)


def load_latest_local_diagnostic_risk_engine_state() -> dict[str, Any]:
    """Read local SQLite model metadata for diagnostics and archive inspection."""
    return _load_latest_sqlite_payload(state.latest_risk_engine_state)


def load_latest_local_diagnostic_covariance_payload() -> dict[str, Any]:
    """Read local SQLite covariance for diagnostics and archive inspection."""
    return _load_latest_sqlite_payload(state.latest_covariance_payload)


def load_latest_local_diagnostic_specific_risk_payload() -> dict[str, Any]:
    """Read local SQLite specific risk for diagnostics and archive inspection."""
    return _load_latest_sqlite_payload(state.latest_specific_risk_payload)


def load_latest_persisted_risk_engine_state() -> dict[str, Any]:
    """Compatibility wrapper. Prefer explicit rebuild-authority or diagnostics readers."""
    local_state = load_latest_local_diagnostic_risk_engine_state()
    if local_state:
        return local_state
    return _load_latest_neon_payload(state.pg_latest_risk_engine_state)


def load_latest_persisted_covariance_payload() -> dict[str, Any]:
    """Compatibility wrapper. Prefer explicit rebuild-authority or diagnostics readers."""
    local_payload = load_latest_local_diagnostic_covariance_payload()
    if local_payload:
        return local_payload
    return _load_latest_neon_payload(state.pg_latest_covariance_payload)


def load_latest_persisted_specific_risk_payload() -> dict[str, Any]:
    """Compatibility wrapper. Prefer explicit rebuild-authority or diagnostics readers."""
    local_payload = load_latest_local_diagnostic_specific_risk_payload()
    if local_payload:
        return local_payload
    return _load_latest_neon_payload(state.pg_latest_specific_risk_payload)


def persist_model_outputs(
    *,
    data_db: Path,
    cache_db: Path,
    run_id: str,
    refresh_mode: str,
    status: str,
    started_at: str,
    completed_at: str,
    source_dates: dict[str, Any],
    params: dict[str, Any],
    risk_engine_state: dict[str, Any],
    cov: pd.DataFrame,
    specific_risk_by_ticker: dict[str, dict[str, float | int | str]],
    error: dict[str, str] | None = None,
) -> dict[str, Any]:
    now_iso = datetime.now(timezone.utc).isoformat()
    as_of_date = str(
        risk_engine_state.get("factor_returns_latest_date")
        or source_dates.get("exposures_latest_available_asof")
        or source_dates.get("exposures_asof")
        or source_dates.get("fundamentals_asof")
        or completed_at[:10]
    )
    factor_returns_date_from: str | None = None
    factor_returns_mode = "full"

    if _neon_model_output_writes_enabled():
        try:
            pg_conn = connect(dsn=resolve_dsn(None), autocommit=False)
            try:
                writers.ensure_postgres_schema(pg_conn)
                factor_returns_date_from, factor_returns_mode = state.factor_returns_load_start_postgres(
                    pg_conn,
                    as_of_date=as_of_date,
                    risk_engine_state=risk_engine_state,
                )
            finally:
                pg_conn.close()
        except Exception:
            if _neon_model_output_writes_required():
                raise
            conn = sqlite3.connect(str(data_db), timeout=120)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=120000")
            try:
                schema.ensure_schema(conn)
                factor_returns_date_from, factor_returns_mode = state.factor_returns_load_start_sqlite(
                    conn,
                    as_of_date=as_of_date,
                    risk_engine_state=risk_engine_state,
                )
            finally:
                conn.close()
    else:
        conn = sqlite3.connect(str(data_db), timeout=120)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=120000")
        try:
            schema.ensure_schema(conn)
            factor_returns_date_from, factor_returns_mode = state.factor_returns_load_start_sqlite(
                conn,
                as_of_date=as_of_date,
                risk_engine_state=risk_engine_state,
            )
        finally:
            conn.close()

    factor_returns = payloads.load_factor_returns(cache_db, date_from=factor_returns_date_from)
    factor_returns_payload = payloads.factor_returns_payload(
        rows=factor_returns,
        run_id=run_id,
        updated_at=now_iso,
    )
    factor_returns_min_date = str(factor_returns["date"].min()) if not factor_returns.empty else None
    covariance_factors, covariance_payload = payloads.covariance_payload(
        as_of_date=as_of_date,
        cov=cov,
        run_id=run_id,
        updated_at=now_iso,
    )
    specific_risk_payload = payloads.specific_risk_payload(
        as_of_date=as_of_date,
        specific_risk_by_ticker=specific_risk_by_ticker,
        run_id=run_id,
        updated_at=now_iso,
    )
    row_counts = {
        "model_factor_returns_daily": int(len(factor_returns_payload)),
        "model_factor_covariance_daily": int(len(covariance_payload)),
        "model_specific_risk_daily": int(len(specific_risk_payload)),
    }
    quality_errors = payloads.quality_gate_errors(
        status=str(status),
        n_factor=row_counts["model_factor_returns_daily"],
        n_cov=row_counts["model_factor_covariance_daily"],
        n_spec=row_counts["model_specific_risk_daily"],
    )
    effective_status = "failed" if quality_errors else str(status)
    effective_error = dict(error or {})
    if quality_errors:
        effective_error = {
            "type": "quality_gate_failed",
            "message": " | ".join(quality_errors),
        }
    metadata_values = payloads.metadata_values(
        run_id=run_id,
        refresh_mode=refresh_mode,
        status=effective_status,
        started_at=started_at,
        completed_at=completed_at,
        factor_returns_asof=as_of_date,
        source_dates=source_dates,
        params=params,
        risk_engine_state=risk_engine_state,
        row_counts=row_counts,
        error=effective_error,
        updated_at=now_iso,
    )

    neon_write: dict[str, Any] = {"status": "skipped", "reason": "neon_not_configured"}
    sqlite_mirror_write: dict[str, Any] = {"status": "skipped", "reason": "neon_primary"}
    authority_store = "sqlite"

    if _neon_model_output_writes_enabled():
        authority_store = "neon"
        pg_conn = connect(dsn=resolve_dsn(None), autocommit=False)
        try:
            neon_write = writers.write_model_outputs_postgres(
                pg_conn,
                factor_returns_payload=factor_returns_payload,
                factor_returns_min_date=factor_returns_min_date,
                covariance_factors=covariance_factors,
                covariance_payload=covariance_payload,
                specific_risk_payload=specific_risk_payload,
                metadata_values=metadata_values,
                as_of_date=as_of_date,
            )
        except Exception as exc:
            try:
                pg_conn.rollback()
            except Exception:
                pass
            neon_write = {
                "status": "error",
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
        finally:
            pg_conn.close()

        if str(neon_write.get("status") or "") != "ok" and _neon_model_output_writes_required():
            err = neon_write.get("error") if isinstance(neon_write, dict) else None
            raise RuntimeError(
                "Neon model output persistence failed: "
                + str((err or {}).get("type") or "unknown")
                + ": "
                + str((err or {}).get("message") or "unknown")
            )

        try:
            conn = sqlite3.connect(str(data_db), timeout=120)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=120000")
            try:
                sqlite_mirror_write = writers.write_model_outputs_sqlite(
                    conn,
                    factor_returns_payload=factor_returns_payload,
                    factor_returns_min_date=factor_returns_min_date,
                    covariance_factors=covariance_factors,
                    covariance_payload=covariance_payload,
                    specific_risk_payload=specific_risk_payload,
                    metadata_values=metadata_values,
                    as_of_date=as_of_date,
                )
            finally:
                conn.close()
        except Exception as exc:
            sqlite_mirror_write = {
                "status": "error",
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }

        if str(neon_write.get("status") or "") != "ok" and not _neon_model_output_writes_required():
            authority_store = "sqlite" if str(sqlite_mirror_write.get("status") or "") == "ok" else "neon"
    else:
        conn = sqlite3.connect(str(data_db), timeout=120)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=120000")
        try:
            sqlite_mirror_write = writers.write_model_outputs_sqlite(
                conn,
                factor_returns_payload=factor_returns_payload,
                factor_returns_min_date=factor_returns_min_date,
                covariance_factors=covariance_factors,
                covariance_payload=covariance_payload,
                specific_risk_payload=specific_risk_payload,
                metadata_values=metadata_values,
                as_of_date=as_of_date,
            )
        finally:
            conn.close()

    if quality_errors:
        raise RuntimeError(f"model output quality gate failed: {'; '.join(quality_errors)}")
    return {
        "status": "ok",
        "run_id": run_id,
        "authority_store": authority_store,
        "factor_returns_asof": as_of_date,
        "factor_returns_persistence_mode": factor_returns_mode,
        "factor_returns_reload_from": factor_returns_date_from,
        "row_counts": row_counts,
        "neon_write": neon_write,
        "sqlite_mirror_write": sqlite_mirror_write,
    }
