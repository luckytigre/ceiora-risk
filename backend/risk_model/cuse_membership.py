"""Derive durable cUSE membership truth from current refresh artifacts."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from backend.risk_model.eligibility import (
    build_eligibility_context,
    structural_eligibility_for_date,
)
from backend.universe.runtime_rows import load_security_runtime_map_by_date
from backend.universe.schema import ESTU_MEMBERSHIP_TABLE

_STAGE_NAMES = (
    "source_readiness",
    "structural_eligible",
    "core_country_eligible",
    "regression_candidate",
    "regression_member",
    "estu_candidate",
    "estu_member",
    "fundamental_projection_candidate",
    "returns_projection_candidate",
    "projection_basis_available",
    "served_output_available",
)


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return _text(value).lower() in {"1", "true", "t", "yes", "y", "ok", "passed"}


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (table,),
    ).fetchone()
    return row is not None


def _infer_country_from_ric(ric: str) -> str:
    suffix = _upper(ric).split(".")[-1] if "." in _upper(ric) else ""
    if suffix in {"N", "OQ", "O", "A", "K", "P", "PK", "Q"}:
        return "US"
    return ""


def _load_estu_rows(
    conn: sqlite3.Connection,
    *,
    rics: list[str],
    as_of_dates: list[str],
) -> dict[tuple[str, str], dict[str, Any]]:
    if not rics or not as_of_dates or not _table_exists(conn, ESTU_MEMBERSHIP_TABLE):
        return {}
    ric_placeholders = ",".join("?" for _ in rics)
    date_placeholders = ",".join("?" for _ in as_of_dates)
    rows = conn.execute(
        f"""
        SELECT
            date,
            UPPER(TRIM(ric)) AS ric,
            COALESCE(estu_flag, 0) AS estu_flag,
            COALESCE(drop_reason, '') AS drop_reason,
            COALESCE(drop_reason_detail, '') AS drop_reason_detail
        FROM {ESTU_MEMBERSHIP_TABLE}
        WHERE UPPER(TRIM(ric)) IN ({ric_placeholders})
          AND date IN ({date_placeholders})
        """,
        [*rics, *as_of_dates],
    ).fetchall()
    return {
        (_text(as_of_date), _upper(ric)): {
            "estu_flag": int(estu_flag or 0),
            "drop_reason": _text(drop_reason),
            "drop_reason_detail": _text(drop_reason_detail),
        }
        for as_of_date, ric, estu_flag, drop_reason, drop_reason_detail in rows
        if _text(as_of_date) and _text(ric)
    }


def _load_structural_eligibility_rows_by_date(
    *,
    data_db: Path,
    as_of_dates: list[str],
) -> dict[str, dict[str, dict[str, Any]]]:
    clean_dates = sorted({value for value in as_of_dates if _text(value)})
    if not clean_dates:
        return {}
    context = build_eligibility_context(data_db, dates=clean_dates, force_local=True)
    rows_by_date: dict[str, dict[str, dict[str, Any]]] = {}
    for as_of_date in clean_dates:
        _, frame = structural_eligibility_for_date(context, as_of_date)
        if frame.empty:
            rows_by_date[as_of_date] = {}
            continue
        rows_by_date[as_of_date] = {
            _upper(ric): dict(values)
            for ric, values in frame.to_dict(orient="index").items()
            if _upper(ric)
        }
    return rows_by_date


def _derive_realized_role(*, model_status: str, exposure_origin: str) -> str:
    if model_status == "core_estimated":
        return "core_estimated"
    if model_status == "projected_only":
        if exposure_origin == "projected":
            return "projected_returns"
        return "projected_fundamental"
    return "ineligible"


def _derive_policy_path(
    *,
    realized_role: str,
    structural_eligible: bool,
    core_country_eligible: bool,
    returns_projection_candidate: bool,
    source_row: dict[str, Any],
) -> str:
    if bool(int(source_row.get("allow_cuse_returns_projection") or 0) == 1) or returns_projection_candidate:
        return "returns_projection_candidate"
    if bool(int(source_row.get("allow_cuse_fundamental_projection") or 0) == 1):
        return "fundamental_projection_candidate"
    if bool(int(source_row.get("allow_cuse_native_core") or 0) == 1):
        return "native_core_candidate"
    if structural_eligible and not core_country_eligible:
        return "fundamental_projection_candidate"
    if realized_role == "core_estimated" or core_country_eligible:
        return "native_core_candidate"
    return "untracked"


def _compat_model_status(realized_role: str) -> str:
    if realized_role == "core_estimated":
        return "core_estimated"
    if realized_role in {"projected_fundamental", "projected_returns"}:
        return "projected_only"
    return "ineligible"


def _compat_exposure_origin(realized_role: str, existing_origin: str) -> str:
    if realized_role == "projected_returns":
        return "projected"
    if realized_role == "projected_fundamental":
        return "native"
    if existing_origin:
        return existing_origin
    return "native"


def _derive_quality_label(*, realized_role: str, served_exposure_available: bool, output_status: str) -> str:
    if not served_exposure_available:
        if output_status == "projection_unavailable":
            return "projection_unavailable"
        return "blocked"
    if realized_role == "core_estimated":
        return "native_core"
    if realized_role == "projected_fundamental":
        return "fundamental_projection"
    if realized_role == "projected_returns":
        return "returns_projection"
    return "served_compat"


def _stage_state(*, passed: bool | None, applicable: bool = True) -> str:
    if not applicable:
        return "skipped"
    return "passed" if bool(passed) else "failed"


def _stage_reason(default_reason: str, explicit_reason: str) -> str | None:
    return explicit_reason or default_reason or None


def _append_stage(
    payload: list[tuple[Any, ...]],
    *,
    as_of_date: str,
    ric: str,
    stage_name: str,
    stage_state: str,
    reason_code: str | None,
    detail: dict[str, Any],
    run_id: str,
    updated_at: str,
) -> None:
    payload.append(
        (
            as_of_date,
            ric,
            stage_name,
            stage_state,
            reason_code,
            json.dumps(detail, sort_keys=True),
            run_id,
            updated_at,
        )
    )


def build_cuse_membership_payloads(
    *,
    data_db: Path,
    universe_payload: dict[str, Any] | None,
    risk_engine_state: dict[str, Any] | None,
    run_id: str,
    updated_at: str,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    universe = dict(universe_payload or {})
    by_ticker = dict(universe.get("by_ticker") or {})
    if not by_ticker:
        return [], []

    universe_rows: list[dict[str, Any]] = []
    rics: list[str] = []
    as_of_dates: list[str] = []
    for ticker_key, raw_row in by_ticker.items():
        row = dict(raw_row or {})
        ticker = _upper(row.get("ticker") or ticker_key)
        ric = _upper(row.get("ric"))
        if not ticker:
            continue
        as_of_date = _text(row.get("as_of_date") or universe.get("as_of_date"))
        row["ticker"] = ticker
        row["ric"] = ric
        row["as_of_date"] = as_of_date
        universe_rows.append(row)
        if ric:
            rics.append(ric)
        if as_of_date:
            as_of_dates.append(as_of_date)

    if not universe_rows:
        return [], []

    unique_rics = sorted({ric for ric in rics if ric})
    unique_dates = sorted({value for value in as_of_dates if value})
    max_as_of_date = max(unique_dates) if unique_dates else _text((risk_engine_state or {}).get("core_state_through_date"))
    runtime_dates = sorted({*unique_dates, *([max_as_of_date] if max_as_of_date else [])})

    conn = sqlite3.connect(str(data_db))
    try:
        source_rows_by_date = load_security_runtime_map_by_date(
            conn,
            rics=unique_rics,
            as_of_dates=runtime_dates,
            include_disabled=True,
            allow_empty_registry_fallback=False,
        )
        estu_rows = _load_estu_rows(conn, rics=unique_rics, as_of_dates=unique_dates)
    finally:
        conn.close()
    eligibility_rows_by_date = _load_structural_eligibility_rows_by_date(
        data_db=data_db,
        as_of_dates=runtime_dates,
    )

    membership_payload: list[tuple[Any, ...]] = []
    stage_payload: list[tuple[Any, ...]] = []
    core_state_through_date = _text((risk_engine_state or {}).get("core_state_through_date"))

    for row in universe_rows:
        ticker = _upper(row.get("ticker"))
        ric = _upper(row.get("ric"))
        if not ticker:
            continue
        as_of_date = _text(row.get("as_of_date") or core_state_through_date)
        source_row = ((source_rows_by_date.get(as_of_date) or {}).get(ric, {}) if as_of_date else {})
        reason_code = _text(row.get("model_status_reason") or row.get("eligibility_reason"))
        exposure_origin = _text(row.get("exposure_origin"))
        model_status = _text(row.get("model_status")) or "ineligible"
        exposures = dict(row.get("exposures") or {})
        served_exposure_available = bool(exposures)
        allow_native_core = bool(int(source_row.get("allow_cuse_native_core") or 0) == 1)
        allow_fundamental_projection = bool(int(source_row.get("allow_cuse_fundamental_projection") or 0) == 1)
        allow_returns_projection = bool(int(source_row.get("allow_cuse_returns_projection") or 0) == 1)
        eligibility_row = ((eligibility_rows_by_date.get(as_of_date) or {}).get(ric, {}) if as_of_date and ric else {})
        structural_reason = _text(eligibility_row.get("exclusion_reason"))
        structural_eligible = bool(_bool(eligibility_row.get("is_structural_eligible")))
        hq_country_code = (
            _upper(eligibility_row.get("hq_country_code"))
            or _upper(source_row.get("issuer_country_code"))
            or _infer_country_from_ric(ric)
        )
        core_country_eligible = bool(structural_eligible and hq_country_code == "US")
        regression_candidate = core_country_eligible
        realized_role = _derive_realized_role(model_status=model_status, exposure_origin=exposure_origin)
        regression_member = realized_role == "core_estimated"
        estu_candidate = core_country_eligible
        estu_row = estu_rows.get((as_of_date, ric), {}) if ric and as_of_date else {}
        estu_member = bool(estu_candidate and int(estu_row.get("estu_flag") or 0) == 1)
        effective_reason_code = reason_code or structural_reason or _text(estu_row.get("drop_reason"))
        fundamental_projection_candidate = bool(allow_fundamental_projection or (structural_eligible and not core_country_eligible))
        returns_projection_candidate = bool(
            allow_returns_projection
            or
            exposure_origin == "projected"
            or _text(row.get("projection_method"))
            or reason_code == "projection_unavailable"
        )
        projection_basis_available = bool(
            returns_projection_candidate
            and (
                served_exposure_available
                or _text(row.get("projection_method"))
            )
        )
        output_status = (
            "served"
            if served_exposure_available
            else ("projection_unavailable" if reason_code == "projection_unavailable" else "unavailable")
        )
        projection_candidate_status = (
            "candidate"
            if (fundamental_projection_candidate or returns_projection_candidate)
            else "not_applicable"
        )
        projection_output_status = (
            "available"
            if realized_role in {"projected_fundamental", "projected_returns"} and served_exposure_available
            else (
                "unavailable"
                if (fundamental_projection_candidate or returns_projection_candidate)
                else "not_applicable"
            )
        )
        projection_method = _text(row.get("projection_method"))
        if not projection_method and realized_role == "projected_fundamental":
            projection_method = "native_characteristic_projection"
        projection_basis_status = (
            "available"
            if projection_basis_available
            else ("unavailable" if returns_projection_candidate else "not_applicable")
        )
        projection_source_package_date = (
            _text(row.get("projection_asof"))
            or (core_state_through_date if returns_projection_candidate else None)
            or None
        )
        source_snapshot_status = (
            "observed_snapshot"
            if _text(source_row.get("observation_as_of_date"))
            else ("served_snapshot" if as_of_date else "missing_snapshot")
        )
        policy_path = _derive_policy_path(
            realized_role=realized_role,
            structural_eligible=structural_eligible,
            core_country_eligible=core_country_eligible,
            returns_projection_candidate=returns_projection_candidate,
            source_row=source_row,
        )
        quality_label = _derive_quality_label(
            realized_role=realized_role,
            served_exposure_available=served_exposure_available,
            output_status=output_status,
        )

        membership_payload.append(
            (
                as_of_date,
                ric or None,
                ticker,
                policy_path,
                realized_role,
                output_status,
                projection_candidate_status,
                projection_output_status,
                effective_reason_code or None,
                quality_label,
                source_snapshot_status,
                projection_method or None,
                projection_basis_status,
                projection_source_package_date,
                int(served_exposure_available),
                run_id,
                updated_at,
            )
        )

        stage_reason = effective_reason_code
        stage_detail_base = {
            "ticker": ticker,
            "model_status": model_status,
            "realized_role": realized_role,
            "compat_model_status": _compat_model_status(realized_role),
            "compat_exposure_origin": _compat_exposure_origin(realized_role, exposure_origin),
        }
        _append_stage(
            stage_payload,
            as_of_date=as_of_date,
            ric=ric or ticker,
            stage_name="source_readiness",
            stage_state=_stage_state(passed=bool(as_of_date or row.get("price") is not None)),
            reason_code=None,
            detail={
                **stage_detail_base,
                "source_snapshot_status": source_snapshot_status,
                "price": row.get("price"),
            },
            run_id=run_id,
            updated_at=updated_at,
        )
        _append_stage(
            stage_payload,
            as_of_date=as_of_date,
            ric=ric or ticker,
            stage_name="structural_eligible",
            stage_state=_stage_state(passed=structural_eligible),
            reason_code=_stage_reason("not_structural_eligible", stage_reason if not structural_eligible else ""),
            detail={
                **stage_detail_base,
                "instrument_kind": _text(source_row.get("instrument_kind")),
                "eligibility_exclusion_reason": structural_reason,
                "classification_ready": int(source_row.get("classification_ready") or 0),
                "is_single_name_equity": int(source_row.get("is_single_name_equity") or 0),
                "allow_cuse_native_core": int(source_row.get("allow_cuse_native_core") or 0),
                "allow_cuse_fundamental_projection": int(source_row.get("allow_cuse_fundamental_projection") or 0),
            },
            run_id=run_id,
            updated_at=updated_at,
        )
        _append_stage(
            stage_payload,
            as_of_date=as_of_date,
            ric=ric or ticker,
            stage_name="core_country_eligible",
            stage_state=_stage_state(passed=core_country_eligible, applicable=structural_eligible),
            reason_code=_stage_reason("non_us_or_missing_country", stage_reason if structural_eligible and not core_country_eligible else ""),
            detail={
                **stage_detail_base,
                "hq_country_code": hq_country_code,
            },
            run_id=run_id,
            updated_at=updated_at,
        )
        _append_stage(
            stage_payload,
            as_of_date=as_of_date,
            ric=ric or ticker,
            stage_name="regression_candidate",
            stage_state=_stage_state(passed=regression_candidate, applicable=structural_eligible),
            reason_code=_stage_reason("not_core_country_eligible", stage_reason if structural_eligible and not regression_candidate else ""),
            detail=stage_detail_base,
            run_id=run_id,
            updated_at=updated_at,
        )
        _append_stage(
            stage_payload,
            as_of_date=as_of_date,
            ric=ric or ticker,
            stage_name="regression_member",
            stage_state=_stage_state(passed=regression_member, applicable=regression_candidate),
            reason_code=_stage_reason("not_regression_member", stage_reason if regression_candidate and not regression_member else ""),
            detail=stage_detail_base,
            run_id=run_id,
            updated_at=updated_at,
        )
        _append_stage(
            stage_payload,
            as_of_date=as_of_date,
            ric=ric or ticker,
            stage_name="estu_candidate",
            stage_state=_stage_state(passed=estu_candidate, applicable=regression_candidate),
            reason_code=_stage_reason("not_estu_candidate", stage_reason if regression_candidate and not estu_candidate else ""),
            detail=stage_detail_base,
            run_id=run_id,
            updated_at=updated_at,
        )
        _append_stage(
            stage_payload,
            as_of_date=as_of_date,
            ric=ric or ticker,
            stage_name="estu_member",
            stage_state=_stage_state(passed=estu_member, applicable=estu_candidate),
            reason_code=_stage_reason(_text(estu_row.get("drop_reason")) or "not_estu_member", stage_reason if estu_candidate and not estu_member else ""),
            detail={
                **stage_detail_base,
                "estu_flag": int(estu_row.get("estu_flag") or 0),
                "estu_drop_reason_detail": _text(estu_row.get("drop_reason_detail")),
            },
            run_id=run_id,
            updated_at=updated_at,
        )
        _append_stage(
            stage_payload,
            as_of_date=as_of_date,
            ric=ric or ticker,
            stage_name="fundamental_projection_candidate",
            stage_state=_stage_state(passed=fundamental_projection_candidate, applicable=structural_eligible),
            reason_code=_stage_reason("not_fundamental_projection_candidate", stage_reason if structural_eligible and not fundamental_projection_candidate else ""),
            detail=stage_detail_base,
            run_id=run_id,
            updated_at=updated_at,
        )
        _append_stage(
            stage_payload,
            as_of_date=as_of_date,
            ric=ric or ticker,
            stage_name="returns_projection_candidate",
            stage_state=_stage_state(passed=returns_projection_candidate, applicable=not structural_eligible or returns_projection_candidate),
            reason_code=_stage_reason("not_returns_projection_candidate", stage_reason if not returns_projection_candidate else ""),
            detail={
                **stage_detail_base,
                "legacy_coverage_role": _text(source_row.get("legacy_coverage_role")),
                "allow_cuse_returns_projection": int(source_row.get("allow_cuse_returns_projection") or 0),
            },
            run_id=run_id,
            updated_at=updated_at,
        )
        _append_stage(
            stage_payload,
            as_of_date=as_of_date,
            ric=ric or ticker,
            stage_name="projection_basis_available",
            stage_state=_stage_state(passed=projection_basis_available, applicable=returns_projection_candidate),
            reason_code=_stage_reason("projection_basis_unavailable", stage_reason if returns_projection_candidate and not projection_basis_available else ""),
            detail={
                **stage_detail_base,
                "projection_method": projection_method or None,
                "projection_source_package_date": projection_source_package_date,
            },
            run_id=run_id,
            updated_at=updated_at,
        )
        _append_stage(
            stage_payload,
            as_of_date=as_of_date,
            ric=ric or ticker,
            stage_name="served_output_available",
            stage_state=_stage_state(passed=served_exposure_available),
            reason_code=_stage_reason("served_output_unavailable", stage_reason if not served_exposure_available else ""),
            detail={
                **stage_detail_base,
                "output_status": output_status,
                "served_exposure_available": served_exposure_available,
            },
            run_id=run_id,
            updated_at=updated_at,
        )

    return membership_payload, stage_payload


def membership_row_to_overlay(row: dict[str, Any]) -> dict[str, Any]:
    realized_role = _text(row.get("realized_role"))
    reason_code = _text(row.get("reason_code"))
    compat_model_status = _compat_model_status(realized_role)
    return {
        "model_status": compat_model_status,
        "model_status_reason": reason_code,
        "eligibility_reason": reason_code,
        "exposure_origin": _compat_exposure_origin(realized_role, _text(row.get("exposure_origin"))),
        "cuse_realized_role": realized_role,
        "cuse_output_status": _text(row.get("output_status")),
        "cuse_reason_code": reason_code,
        "quality_label": _text(row.get("quality_label")),
        "projection_basis_status": _text(row.get("projection_basis_status")),
        "projection_candidate_status": _text(row.get("projection_candidate_status")),
        "projection_output_status": _text(row.get("projection_output_status")),
        "served_exposure_available": bool(int(row.get("served_exposure_available") or 0)),
    }
