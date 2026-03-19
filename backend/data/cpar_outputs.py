"""Durable cPAR persistence facade."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from psycopg.rows import dict_row

from backend import config
from backend.cpar.factor_registry import ordered_factor_ids
from backend.data import cpar_queries, cpar_schema, cpar_writers, core_read_backend as core_backend
from backend.data.neon import connect, resolve_dsn
from backend.data.neon_primary_write import execute_neon_primary_write

DATA_DB = Path(config.DATA_DB_PATH)


class CparPackageNotReady(RuntimeError):
    """Raised when no successful cPAR package is available in the authority store."""


class CparAuthorityReadError(RuntimeError):
    """Raised when the Neon authority store cannot be read in cloud mode."""


class CparPersistenceNotAllowed(RuntimeError):
    """Raised when cPAR persistence is attempted outside local-ingest."""


def _error_result(exc: Exception) -> dict[str, Any]:
    return {
        "status": "error",
        "error": {
            "type": type(exc).__name__,
            "message": str(exc),
        },
    }


def _resolve_data_db(data_db: Path | None = None) -> Path:
    return Path(data_db or DATA_DB).expanduser().resolve()


def _connect_sqlite(data_db: Path | None = None) -> sqlite3.Connection:
    conn = sqlite3.connect(str(_resolve_data_db(data_db)), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")
    conn.row_factory = sqlite3.Row
    return conn


def _neon_writes_enabled() -> bool:
    return bool(str(config.neon_dsn() or "").strip())


def _neon_writes_required() -> bool:
    return bool(_neon_writes_enabled() and config.neon_primary_model_data_enabled())


def _use_neon_reads() -> bool:
    return bool(config.cloud_mode() or config.neon_primary_model_data_enabled())


def _sqlite_fetch_rows(sql: str, params: list[Any] | None = None, *, data_db: Path | None = None) -> list[dict[str, Any]]:
    db = _resolve_data_db(data_db)
    if not db.exists():
        return []
    conn = _connect_sqlite(db)
    try:
        cpar_schema.ensure_sqlite_schema(conn)
        rows = conn.execute(sql, params or []).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def _neon_fetch_rows(
    sql: str,
    params: list[Any] | None = None,
    *,
    raise_on_error: bool = False,
) -> list[dict[str, Any]]:
    try:
        conn = connect(dsn=resolve_dsn(None), autocommit=True)
    except Exception as exc:
        if raise_on_error:
            raise CparAuthorityReadError(
                f"Neon cPAR read failed during connection setup: {type(exc).__name__}: {exc}"
            ) from exc
        return []
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(core_backend.to_pg_sql(sql), params or [])
            return [dict(row) for row in cur.fetchall()]
    except Exception as exc:
        if raise_on_error:
            raise CparAuthorityReadError(
                f"Neon cPAR read failed during query execution: {type(exc).__name__}: {exc}"
            ) from exc
        return []
    finally:
        conn.close()


def _neon_fetch(sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    return _neon_fetch_rows(sql, params, raise_on_error=config.cloud_mode())


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _expected_covariance_pairs() -> set[tuple[str, str]]:
    factor_ids = ordered_factor_ids()
    return {(left, right) for left in factor_ids for right in factor_ids}


def _require_complete_covariance_rows(
    rows: list[dict[str, Any]],
    *,
    package_run_id: str,
    context_label: str,
) -> None:
    expected = _expected_covariance_pairs()
    observed = {
        (str(row.get("factor_id") or ""), str(row.get("factor_id_2") or ""))
        for row in rows
    }
    missing = sorted(expected - observed)
    if missing:
        sample = ", ".join(f"{left}/{right}" for left, right in missing[:5])
        raise CparPackageNotReady(
            f"{context_label} has incomplete covariance coverage for package_run_id={package_run_id}. "
            f"Missing factor pairs include {sample}."
        )


def _normalize_package_run(package_run: dict[str, Any], *, now_iso: str) -> dict[str, Any]:
    return {
        "package_run_id": str(package_run["package_run_id"]),
        "package_date": str(package_run["package_date"]),
        "profile": str(package_run["profile"]),
        "status": str(package_run["status"]),
        "started_at": str(package_run["started_at"]),
        "completed_at": (str(package_run["completed_at"]) if package_run.get("completed_at") is not None else None),
        "method_version": str(package_run["method_version"]),
        "factor_registry_version": str(package_run["factor_registry_version"]),
        "lookback_weeks": int(package_run["lookback_weeks"]),
        "half_life_weeks": int(package_run["half_life_weeks"]),
        "min_observations": int(package_run["min_observations"]),
        "proxy_price_rule": str(package_run["proxy_price_rule"]),
        "source_prices_asof": (str(package_run["source_prices_asof"]) if package_run.get("source_prices_asof") else None),
        "classification_asof": (str(package_run["classification_asof"]) if package_run.get("classification_asof") else None),
        "universe_count": int(package_run.get("universe_count") or 0),
        "fit_ok_count": int(package_run.get("fit_ok_count") or 0),
        "fit_limited_count": int(package_run.get("fit_limited_count") or 0),
        "fit_insufficient_count": int(package_run.get("fit_insufficient_count") or 0),
        "data_authority": None,
        "error_type": (str(package_run["error_type"]) if package_run.get("error_type") else None),
        "error_message": (str(package_run["error_message"]) if package_run.get("error_message") else None),
        "updated_at": now_iso,
    }


def _package_run_with_authority(package_run: dict[str, Any], *, data_authority: str) -> dict[str, Any]:
    clean = dict(package_run)
    clean["data_authority"] = str(data_authority)
    return clean


def _normalize_child_rows(
    rows: list[dict[str, Any]],
    *,
    package_date: str,
    package_run_id: str,
    now_iso: str,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        clean = dict(row)
        clean["package_date"] = package_date
        clean["package_run_id"] = package_run_id
        clean["updated_at"] = now_iso
        normalized.append(clean)
    return normalized


def _validate_package_completeness(
    *,
    package_run: dict[str, Any],
    proxy_returns: list[dict[str, Any]],
    proxy_transforms: list[dict[str, Any]],
    covariance_rows: list[dict[str, Any]],
    instrument_fits: list[dict[str, Any]],
) -> None:
    if str(package_run.get("status") or "") != "ok":
        return
    required_counts = {
        cpar_schema.TABLE_PROXY_RETURNS: len(proxy_returns),
        cpar_schema.TABLE_PROXY_TRANSFORM: len(proxy_transforms),
        cpar_schema.TABLE_FACTOR_COVARIANCE: len(covariance_rows),
        cpar_schema.TABLE_INSTRUMENT_FITS: len(instrument_fits),
    }
    missing = [table for table, count in required_counts.items() if int(count) <= 0]
    if missing:
        raise ValueError(
            "Successful cPAR packages require non-empty durable child rows for "
            + ", ".join(sorted(missing))
        )


def persist_cpar_package(
    *,
    data_db: Path | None = None,
    package_run: dict[str, Any],
    proxy_returns: list[dict[str, Any]],
    proxy_transforms: list[dict[str, Any]],
    covariance_rows: list[dict[str, Any]],
    instrument_fits: list[dict[str, Any]],
) -> dict[str, Any]:
    if config.cloud_mode():
        raise CparPersistenceNotAllowed("cPAR persistence is not allowed in cloud-serve runtime role.")

    now_iso = _now_iso()
    normalized_package_run_base = _normalize_package_run(package_run, now_iso=now_iso)
    package_date = normalized_package_run_base["package_date"]
    package_run_id = normalized_package_run_base["package_run_id"]
    normalized_proxy_returns = _normalize_child_rows(proxy_returns, package_date=package_date, package_run_id=package_run_id, now_iso=now_iso)
    normalized_proxy_transforms = _normalize_child_rows(proxy_transforms, package_date=package_date, package_run_id=package_run_id, now_iso=now_iso)
    normalized_covariance = _normalize_child_rows(covariance_rows, package_date=package_date, package_run_id=package_run_id, now_iso=now_iso)
    normalized_instrument_fits = _normalize_child_rows(instrument_fits, package_date=package_date, package_run_id=package_run_id, now_iso=now_iso)
    _validate_package_completeness(
        package_run=normalized_package_run_base,
        proxy_returns=normalized_proxy_returns,
        proxy_transforms=normalized_proxy_transforms,
        covariance_rows=normalized_covariance,
        instrument_fits=normalized_instrument_fits,
    )
    row_counts = {
        cpar_schema.TABLE_PACKAGE_RUNS: 1,
        cpar_schema.TABLE_PROXY_RETURNS: len(normalized_proxy_returns),
        cpar_schema.TABLE_PROXY_TRANSFORM: len(normalized_proxy_transforms),
        cpar_schema.TABLE_FACTOR_COVARIANCE: len(normalized_covariance),
        cpar_schema.TABLE_INSTRUMENT_FITS: len(normalized_instrument_fits),
    }
    neon_write_state: dict[str, Any] = {"status": "skipped"}

    def _write_sqlite() -> dict[str, Any]:
        conn = _connect_sqlite(data_db)
        try:
            authority = "neon" if str(neon_write_state.get("status") or "") == "ok" else "sqlite"
            return cpar_writers.write_cpar_outputs_sqlite(
                conn,
                package_run=_package_run_with_authority(normalized_package_run_base, data_authority=authority),
                proxy_returns=normalized_proxy_returns,
                proxy_transforms=normalized_proxy_transforms,
                covariance_rows=normalized_covariance,
                instrument_fits=normalized_instrument_fits,
            )
        finally:
            conn.close()

    def _write_neon() -> dict[str, Any]:
        pg_conn = connect(dsn=resolve_dsn(None), autocommit=False)
        try:
            return cpar_writers.write_cpar_outputs_postgres(
                pg_conn,
                package_run=_package_run_with_authority(normalized_package_run_base, data_authority="neon"),
                proxy_returns=normalized_proxy_returns,
                proxy_transforms=normalized_proxy_transforms,
                covariance_rows=normalized_covariance,
                instrument_fits=normalized_instrument_fits,
            )
        except Exception:
            try:
                pg_conn.rollback()
            except Exception:
                pass
            raise
        finally:
            pg_conn.close()

    def _write_neon_safe() -> dict[str, Any]:
        try:
            neon_write_state.update(_write_neon())
        except Exception as exc:
            neon_write_state.update(_error_result(exc))
        return dict(neon_write_state)

    return execute_neon_primary_write(
        base_result={
            "status": "ok",
            "package_date": package_date,
            "package_run_id": package_run_id,
            "row_counts": row_counts,
        },
        neon_enabled=_neon_writes_enabled(),
        neon_required=_neon_writes_required(),
        perform_neon_write=_write_neon_safe,
        perform_fallback_write=_write_sqlite,
        failure_label="cPAR package persistence",
        fallback_result_key="sqlite_mirror_write",
        fallback_authority="sqlite",
    )


def load_active_package_run(*, data_db: Path | None = None) -> dict[str, Any] | None:
    if _use_neon_reads():
        package = cpar_queries.latest_successful_package(lambda sql, params=None: _neon_fetch(sql, params))
        if package is not None:
            return package
        if config.cloud_mode():
            raise CparPackageNotReady("No successful cPAR package is available in the cloud-serve authority store.")
    return cpar_queries.latest_successful_package(lambda sql, params=None: _sqlite_fetch_rows(sql, params, data_db=data_db))


def require_active_package_run(*, data_db: Path | None = None) -> dict[str, Any]:
    package = load_active_package_run(data_db=data_db)
    if package is None:
        raise CparPackageNotReady("No successful cPAR package is available.")
    return package


def search_active_package_instrument_fits(
    q: str,
    *,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    package = require_active_package_run(data_db=data_db)
    if _use_neon_reads():
        rows = cpar_queries.active_package_search_rows(
            lambda sql, params=None: _neon_fetch(sql, params),
            package_run_id=package["package_run_id"],
            q=q,
        )
        if rows or config.cloud_mode():
            return rows
    return cpar_queries.active_package_search_rows(
        lambda sql, params=None: _sqlite_fetch_rows(sql, params, data_db=data_db),
        package_run_id=package["package_run_id"],
        q=q,
    )


def load_active_package_instrument_fit(
    ticker: str,
    *,
    ric: str | None = None,
    data_db: Path | None = None,
) -> dict[str, Any] | None:
    package = require_active_package_run(data_db=data_db)
    if _use_neon_reads():
        fit = cpar_queries.active_package_instrument_fit(
            lambda sql, params=None: _neon_fetch(sql, params),
            package_run_id=package["package_run_id"],
            ticker=ticker,
            ric=ric,
        )
        if fit is not None or config.cloud_mode():
            return fit
    return cpar_queries.active_package_instrument_fit(
        lambda sql, params=None: _sqlite_fetch_rows(sql, params, data_db=data_db),
        package_run_id=package["package_run_id"],
        ticker=ticker,
        ric=ric,
    )


def load_previous_successful_instrument_fit(
    ric: str,
    *,
    before_package_date: str,
    data_db: Path | None = None,
) -> dict[str, Any] | None:
    if _use_neon_reads():
        fit = cpar_queries.previous_successful_instrument_fit(
            lambda sql, params=None: _neon_fetch(sql, params),
            ric=ric,
            before_package_date=before_package_date,
        )
        if fit is not None or config.cloud_mode():
            return fit
    return cpar_queries.previous_successful_instrument_fit(
        lambda sql, params=None: _sqlite_fetch_rows(sql, params, data_db=data_db),
        ric=ric,
        before_package_date=before_package_date,
    )


def load_package_covariance_rows(
    package_run_id: str,
    *,
    data_db: Path | None = None,
    require_complete: bool = False,
    context_label: str | None = None,
) -> list[dict[str, Any]]:
    if _use_neon_reads():
        rows = cpar_queries.package_covariance_rows(
            lambda sql, params=None: _neon_fetch(sql, params),
            package_run_id=package_run_id,
        )
        if rows or config.cloud_mode():
            if require_complete:
                _require_complete_covariance_rows(
                    rows,
                    package_run_id=str(package_run_id),
                    context_label=context_label or "Requested cPAR package",
                )
            return rows
    rows = cpar_queries.package_covariance_rows(
        lambda sql, params=None: _sqlite_fetch_rows(sql, params, data_db=data_db),
        package_run_id=package_run_id,
    )
    if require_complete:
        _require_complete_covariance_rows(
            rows,
            package_run_id=str(package_run_id),
            context_label=context_label or "Requested cPAR package",
        )
    return rows


def load_active_package_covariance_rows(*, data_db: Path | None = None) -> list[dict[str, Any]]:
    package = require_active_package_run(data_db=data_db)
    return load_package_covariance_rows(
        package["package_run_id"],
        data_db=data_db,
        require_complete=True,
        context_label="Active cPAR package",
    )
