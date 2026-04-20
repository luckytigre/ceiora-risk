"""Explicit cUSE4 owner for first-render and lazy risk-page payloads."""

from __future__ import annotations

from typing import Any

from backend.analytics.trbc_economic_sector_short import abbreviate_trbc_economic_sector_short
from backend.services import cuse4_dashboard_payload_service, cuse4_portfolio_whatif


EXPOSURE_MODES = ("raw", "sensitivity", "risk_contribution")
SUMMARY_EXPOSURE_MODE = "raw"


def _normalize_account_id(raw: str | None) -> str | None:
    clean = str(raw or "").strip().lower()
    return clean or None


def _normalize_trbc_sector_fields(payload: dict[str, Any]) -> dict[str, Any]:
    trbc_economic_sector_short = str(
        payload.get("trbc_economic_sector_short")
        or payload.get("trbc_sector")
        or payload.get("sector")
        or ""
    )
    model_status_reason = str(
        payload.get("model_status_reason")
        or payload.get("eligibility_reason")
        or ""
    )
    return {
        **payload,
        "model_status_reason": model_status_reason,
        "eligibility_reason": model_status_reason,
        "trbc_economic_sector_short": trbc_economic_sector_short,
        "trbc_economic_sector_short_abbr": str(
            payload.get("trbc_economic_sector_short_abbr")
            or payload.get("trbc_sector_abbr")
            or abbreviate_trbc_economic_sector_short(trbc_economic_sector_short)
        ),
    }


def _scoped_portfolio_payload(
    *,
    scoped_preview: dict[str, Any],
    account_id: str | None,
) -> dict[str, Any]:
    current = dict(scoped_preview.get("current") or {})
    positions = [
        _normalize_trbc_sector_fields(dict(row))
        for row in list(current.get("positions") or [])
        if isinstance(row, dict)
    ]
    response: dict[str, Any] = {
        "positions": positions,
        "total_value": float(current.get("total_value") or 0.0),
        "position_count": int(current.get("position_count") or len(positions)),
        "_cached": False,
        "_account_scoped": True,
        "account_id": account_id,
        "source_dates": scoped_preview.get("source_dates") or {},
    }
    serving_snapshot = scoped_preview.get("serving_snapshot")
    if isinstance(serving_snapshot, dict):
        for key in ("run_id", "snapshot_id", "refresh_started_at"):
            value = serving_snapshot.get(key)
            if value is not None:
                response[key] = value
    return response


def _scoped_exposures_payload(
    *,
    scoped_preview: dict[str, Any],
    account_id: str | None,
    mode: str,
) -> dict[str, Any]:
    return cuse4_dashboard_payload_service.load_account_scoped_exposures_response(
        mode=mode,
        scoped_preview=scoped_preview,
        account_id=account_id or "",
    )


def _risk_summary_payload(risk_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "risk_shares": risk_payload.get("risk_shares") or {},
        "vol_scaled_shares": risk_payload.get("vol_scaled_shares") or {},
        "factor_details": list(risk_payload.get("factor_details") or []),
        "factor_catalog": list(risk_payload.get("factor_catalog") or []),
        "source_dates": risk_payload.get("source_dates") or {},
        "risk_engine": risk_payload.get("risk_engine") or {},
        "model_sanity": risk_payload.get("model_sanity") or {},
        "run_id": risk_payload.get("run_id"),
        "snapshot_id": risk_payload.get("snapshot_id"),
        "refresh_started_at": risk_payload.get("refresh_started_at"),
        "_cached": bool(risk_payload.get("_cached")),
        "_account_scoped": bool(risk_payload.get("_account_scoped")),
        "account_id": risk_payload.get("account_id"),
    }


def _covariance_payload(covariance_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "cov_matrix": covariance_payload.get("cov_matrix") or {},
        "run_id": covariance_payload.get("run_id"),
        "snapshot_id": covariance_payload.get("snapshot_id"),
        "refresh_started_at": covariance_payload.get("refresh_started_at"),
        "_cached": bool(covariance_payload.get("_cached")),
        "_account_scoped": bool(covariance_payload.get("_account_scoped")),
        "account_id": covariance_payload.get("account_id"),
    }


def load_cuse_risk_page_payload(
    *,
    account_id: str | None = None,
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    normalized_account_id = _normalize_account_id(account_id)
    if normalized_account_id is not None:
        scoped_preview = cuse4_portfolio_whatif.preview_portfolio_whatif(
            scenario_rows=[],
            account_id=normalized_account_id,
            allowed_account_ids=allowed_account_ids,
            requested_exposure_modes=(SUMMARY_EXPOSURE_MODE,),
        )
        risk_payload = cuse4_dashboard_payload_service.load_account_scoped_risk_response(
            scoped_preview=scoped_preview,
            account_id=normalized_account_id,
        )
        raw_exposure_payload = _scoped_exposures_payload(
            scoped_preview=scoped_preview,
            account_id=normalized_account_id,
            mode=SUMMARY_EXPOSURE_MODE,
        )
        return {
            "portfolio": _scoped_portfolio_payload(
                scoped_preview=scoped_preview,
                account_id=normalized_account_id,
            ),
            "risk": _risk_summary_payload(risk_payload),
            "exposures": {
                SUMMARY_EXPOSURE_MODE: raw_exposure_payload,
            },
            "_cached": False,
            "_account_scoped": True,
            "account_id": normalized_account_id,
            "truth_surface": scoped_preview.get("truth_surface"),
        }

    return {
        "portfolio": cuse4_dashboard_payload_service.load_portfolio_response(
            position_normalizer=_normalize_trbc_sector_fields,
        ),
        "risk": _risk_summary_payload(cuse4_dashboard_payload_service.load_risk_summary_response()),
        "exposures": {
            SUMMARY_EXPOSURE_MODE: cuse4_dashboard_payload_service.load_exposures_response(mode=SUMMARY_EXPOSURE_MODE)
        },
        "_cached": True,
        "_account_scoped": False,
        "account_id": None,
        "truth_surface": "published_cuse_serving_snapshot",
    }


def load_cuse_risk_page_exposure_mode_payload(
    *,
    mode: str,
    account_id: str | None = None,
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    if mode not in EXPOSURE_MODES:
        raise ValueError(f"Unsupported cUSE risk-page exposure mode: {mode}")
    normalized_account_id = _normalize_account_id(account_id)
    if normalized_account_id is not None:
        scoped_preview = cuse4_portfolio_whatif.preview_portfolio_whatif(
            scenario_rows=[],
            account_id=normalized_account_id,
            allowed_account_ids=allowed_account_ids,
            requested_exposure_modes=(mode,),
        )
        return _scoped_exposures_payload(
            scoped_preview=scoped_preview,
            account_id=normalized_account_id,
            mode=mode,
        )
    return cuse4_dashboard_payload_service.load_exposures_response(mode=mode)


def load_cuse_risk_page_covariance_payload() -> dict[str, Any]:
    return _covariance_payload(cuse4_dashboard_payload_service.load_risk_covariance_response())
