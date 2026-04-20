"""Concrete cUSE4 owner for dashboard payload assembly."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from backend.data import serving_outputs
from backend.data.serving_outputs import load_runtime_payload
from backend.data.sqlite import cache_get


PayloadLoader = Callable[[str], Any]
RuntimePayloadLoader = Callable[[str], Any]
RoutePayloadLoader = Callable[[str], Any]


@dataclass(frozen=True)
class DashboardPayloadNotReady(RuntimeError):
    cache_key: str
    message: str
    refresh_profile: str | None = None


@dataclass(frozen=True)
class DashboardPayloadReaders:
    payload_loader: Callable[..., Any]
    fallback_loader: Callable[..., Any]


def get_dashboard_payload_readers() -> DashboardPayloadReaders:
    return DashboardPayloadReaders(
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
    )


def _load_payload(
    payload_name: str,
    *,
    payload_loader: Callable[..., Any] | None,
    fallback_loader,
) -> Any:
    if payload_loader is None:
        payload_loader = load_runtime_payload
    if fallback_loader is None:
        fallback_loader = cache_get
    return payload_loader(payload_name, fallback_loader=fallback_loader)


def _load_payloads(
    payload_names: tuple[str, ...],
    *,
    payload_loader: Callable[..., Any] | None,
    fallback_loader,
) -> dict[str, Any]:
    if fallback_loader is None:
        fallback_loader = cache_get
    if payload_loader is None:
        if load_runtime_payload is serving_outputs.load_runtime_payload:
            return serving_outputs.load_runtime_payloads(
                payload_names,
                fallback_loader=fallback_loader,
            )
        payload_loader = load_runtime_payload
    return {
        payload_name: payload_loader(payload_name, fallback_loader=fallback_loader)
        for payload_name in payload_names
    }


def _normalize_exposure_factor_rows(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        clean = dict(row)
        factor_token = str(
            clean.get("factor_id")
            or clean.get("factor")
            or clean.get("factor_name")
            or ""
        ).strip()
        if factor_token:
            clean["factor_id"] = factor_token
            clean.setdefault("factor_name", factor_token)
        factor_coverage_asof = str(
            clean.get("factor_coverage_asof")
            or clean.get("coverage_date")
            or ""
        ).strip() or None
        clean["factor_coverage_asof"] = factor_coverage_asof
        clean["coverage_date"] = factor_coverage_asof
        if not isinstance(clean.get("drilldown"), list):
            clean["drilldown"] = []
        normalized.append(clean)
    return normalized


def _normalize_systematic_shares(shares: Any) -> Any:
    if not isinstance(shares, dict):
        return shares
    clean = dict(shares)
    if "market" not in clean and "country" in clean:
        clean["market"] = clean.get("country")
    clean.pop("country", None)
    return clean


def _normalize_risk_factor_details(rows: Any) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        clean = dict(row)
        factor_token = str(
            clean.get("factor_id")
            or clean.get("factor")
            or clean.get("factor_name")
            or ""
        ).strip()
        if factor_token:
            clean["factor_id"] = factor_token
            clean.setdefault("factor_name", factor_token)
        if str(clean.get("category") or "").strip().lower() == "country":
            clean["category"] = "market"
        normalized.append(clean)
    return normalized


def _normalize_risk_engine_state(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    clean = dict(payload)
    core_state_through_date = str(
        clean.get("core_state_through_date")
        or clean.get("factor_returns_latest_date")
        or ""
    ).strip() or None
    core_rebuild_date = str(
        clean.get("core_rebuild_date")
        or clean.get("last_recompute_date")
        or ""
    ).strip() or None
    clean["core_state_through_date"] = core_state_through_date
    clean["factor_returns_latest_date"] = core_state_through_date
    clean["core_rebuild_date"] = core_rebuild_date
    clean["last_recompute_date"] = core_rebuild_date
    return clean


def _normalize_model_sanity(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"status": "no-data", "warnings": [], "checks": {}}
    clean = dict(payload)
    served_loadings_asof = str(
        clean.get("served_loadings_asof")
        or clean.get("coverage_date")
        or ""
    ).strip() or None
    latest_loadings_available_asof = str(
        clean.get("latest_loadings_available_asof")
        or clean.get("latest_available_date")
        or ""
    ).strip() or None
    clean["served_loadings_asof"] = served_loadings_asof
    clean["coverage_date"] = served_loadings_asof
    clean["latest_loadings_available_asof"] = latest_loadings_available_asof
    clean["latest_available_date"] = latest_loadings_available_asof
    return clean


def _risk_payload_complete(data: Any) -> bool:
    if not isinstance(data, dict):
        return False
    cov = data.get("cov_matrix") if isinstance(data, dict) else {}
    factors = cov.get("factors") if isinstance(cov, dict) else []
    correlation = cov.get("correlation") if isinstance(cov, dict) else []
    matrix = cov.get("matrix") if isinstance(cov, dict) else []
    cov_rows = correlation if isinstance(correlation, list) and correlation else matrix
    risk_engine = data.get("risk_engine") if isinstance(data, dict) else {}
    specific_count = int((risk_engine or {}).get("specific_risk_ticker_count") or 0)
    return bool(
        isinstance(factors, list)
        and factors
        and isinstance(cov_rows, list)
        and cov_rows
        and specific_count > 0
    )


def load_exposures_response(
    *,
    mode: str,
    payload_loader: Callable[..., Any] | None = None,
    fallback_loader=None,
) -> dict[str, Any]:
    readers = get_dashboard_payload_readers()
    data = _load_payload(
        "exposures",
        payload_loader=payload_loader or readers.payload_loader,
        fallback_loader=fallback_loader or readers.fallback_loader,
    )
    if data is None:
        raise DashboardPayloadNotReady(
            cache_key="exposures",
            message="Exposure cache is not ready yet. Run refresh and try again.",
        )
    response = {
        "mode": str(mode),
        "factors": _normalize_exposure_factor_rows(data.get(mode, [])),
        "_cached": True,
    }
    for key in ("run_id", "snapshot_id", "refresh_started_at", "source_dates"):
        value = data.get(key)
        if value is not None:
            response[key] = value
    return response


def load_account_scoped_exposures_response(
    *,
    mode: str,
    scoped_preview: dict[str, Any],
    account_id: str,
) -> dict[str, Any]:
    current = dict(scoped_preview.get("current") or {})
    response = {
        "mode": str(mode),
        "factors": _normalize_exposure_factor_rows((current.get("exposure_modes") or {}).get(mode, [])),
        "_cached": False,
        "_account_scoped": True,
        "account_id": str(account_id or "").strip().lower() or None,
    }
    source_dates = scoped_preview.get("source_dates")
    if source_dates is not None:
        response["source_dates"] = source_dates
    serving_snapshot = scoped_preview.get("serving_snapshot")
    if isinstance(serving_snapshot, dict):
        for key in ("run_id", "snapshot_id", "refresh_started_at"):
            value = serving_snapshot.get(key)
            if value is not None:
                response[key] = value
    return response


def load_risk_summary_response(
    *,
    payload_loader: Callable[..., Any] | None = None,
    fallback_loader=None,
) -> dict[str, Any]:
    readers = get_dashboard_payload_readers()
    payloads = _load_payloads(
        ("risk", "model_sanity"),
        payload_loader=payload_loader or readers.payload_loader,
        fallback_loader=fallback_loader or readers.fallback_loader,
    )
    data = payloads.get("risk")
    if data is None:
        raise DashboardPayloadNotReady(
            cache_key="risk",
            message="Risk cache is not ready yet. Run refresh and try again.",
        )
    sanity = payloads.get("model_sanity")
    if sanity is None:
        sanity = {"status": "no-data", "warnings": [], "checks": {}}

    return {
        "risk_shares": _normalize_systematic_shares(data.get("risk_shares")),
        "vol_scaled_shares": _normalize_systematic_shares(data.get("vol_scaled_shares")),
        "factor_details": _normalize_risk_factor_details(data.get("factor_details")),
        "factor_catalog": list(data.get("factor_catalog") or []),
        "source_dates": data.get("source_dates") or {},
        "risk_engine": _normalize_risk_engine_state(data.get("risk_engine")),
        "model_sanity": _normalize_model_sanity(sanity),
        "run_id": data.get("run_id"),
        "snapshot_id": data.get("snapshot_id"),
        "refresh_started_at": data.get("refresh_started_at"),
        "_cached": True,
    }


def load_risk_response(
    *,
    payload_loader: Callable[..., Any] | None = None,
    fallback_loader=None,
) -> dict[str, Any]:
    readers = get_dashboard_payload_readers()
    payloads = _load_payloads(
        ("risk", "model_sanity"),
        payload_loader=payload_loader or readers.payload_loader,
        fallback_loader=fallback_loader or readers.fallback_loader,
    )
    data = payloads.get("risk")
    if data is None:
        raise DashboardPayloadNotReady(
            cache_key="risk",
            message="Risk cache is not ready yet. Run refresh and try again.",
        )
    if not _risk_payload_complete(data):
        raise DashboardPayloadNotReady(
            cache_key="risk",
            message="Risk cache exists but is incomplete. Run a core refresh and try again.",
            refresh_profile="cold-core",
        )
    sanity = payloads.get("model_sanity")
    if sanity is None:
        sanity = {"status": "no-data", "warnings": [], "checks": {}}

    response = dict(data)
    response["risk_shares"] = _normalize_systematic_shares(data.get("risk_shares"))
    response["vol_scaled_shares"] = _normalize_systematic_shares(data.get("vol_scaled_shares"))
    response["component_shares"] = _normalize_systematic_shares(data.get("component_shares"))
    response["factor_details"] = _normalize_risk_factor_details(data.get("factor_details"))
    response["risk_engine"] = _normalize_risk_engine_state(data.get("risk_engine"))
    response["model_sanity"] = _normalize_model_sanity(sanity)
    response["_cached"] = True
    return response


def load_risk_covariance_response(
    *,
    payload_loader: Callable[..., Any] | None = None,
    fallback_loader=None,
) -> dict[str, Any]:
    readers = get_dashboard_payload_readers()
    data = _load_payload(
        "risk",
        payload_loader=payload_loader or readers.payload_loader,
        fallback_loader=fallback_loader or readers.fallback_loader,
    )
    if data is None:
        raise DashboardPayloadNotReady(
            cache_key="risk",
            message="Risk cache is not ready yet. Run refresh and try again.",
        )
    cov = dict(data.get("cov_matrix") or {})
    factors = cov.get("factors") if isinstance(cov, dict) else []
    correlation = cov.get("correlation") if isinstance(cov, dict) else []
    matrix = cov.get("matrix") if isinstance(cov, dict) else []
    cov_rows = correlation if isinstance(correlation, list) and correlation else matrix
    if not isinstance(factors, list) or not factors or not isinstance(cov_rows, list) or not cov_rows:
        raise DashboardPayloadNotReady(
            cache_key="risk",
            message="Risk covariance is not ready yet. Run a core refresh and try again.",
            refresh_profile="cold-core",
        )
    return {
        "cov_matrix": {
            "factors": list(factors),
            "correlation": correlation if isinstance(correlation, list) else [],
            "matrix": matrix if isinstance(matrix, list) else [],
        },
        "run_id": data.get("run_id"),
        "snapshot_id": data.get("snapshot_id"),
        "refresh_started_at": data.get("refresh_started_at"),
        "_cached": True,
    }


def load_account_scoped_risk_response(
    *,
    scoped_preview: dict[str, Any],
    account_id: str,
) -> dict[str, Any]:
    current = dict(scoped_preview.get("current") or {})
    response = dict(current)
    response["risk_shares"] = _normalize_systematic_shares(current.get("risk_shares"))
    response["vol_scaled_shares"] = _normalize_systematic_shares(current.get("vol_scaled_shares")) or {}
    response["component_shares"] = _normalize_systematic_shares(current.get("component_shares"))
    response["factor_details"] = _normalize_risk_factor_details(current.get("factor_details"))
    response["_cached"] = False
    response["_account_scoped"] = True
    response["account_id"] = str(account_id or "").strip().lower() or None
    response["model_sanity"] = {"status": "scoped-preview", "warnings": [], "checks": {}}
    source_dates = scoped_preview.get("source_dates")
    if source_dates is not None:
        response["source_dates"] = source_dates
    serving_snapshot = scoped_preview.get("serving_snapshot")
    if isinstance(serving_snapshot, dict):
        for key in ("run_id", "snapshot_id", "refresh_started_at"):
            value = serving_snapshot.get(key)
            if value is not None:
                response[key] = value
    return response


def load_portfolio_response(
    *,
    position_normalizer=None,
    payload_loader: Callable[..., Any] | None = None,
    fallback_loader=None,
) -> dict[str, Any]:
    readers = get_dashboard_payload_readers()
    data = _load_payload(
        "portfolio",
        payload_loader=payload_loader or readers.payload_loader,
        fallback_loader=fallback_loader or readers.fallback_loader,
    )
    if data is None:
        raise DashboardPayloadNotReady(
            cache_key="portfolio",
            message="Portfolio cache is not ready yet. Run refresh and try again.",
        )
    response = dict(data)
    positions = data.get("positions")
    if position_normalizer is not None and isinstance(positions, list):
        response["positions"] = [position_normalizer(row) for row in positions]
    response["_cached"] = True
    return response


__all__ = [
    "DashboardPayloadNotReady",
    "DashboardPayloadReaders",
    "cache_get",
    "get_dashboard_payload_readers",
    "load_exposures_response",
    "load_portfolio_response",
    "load_risk_covariance_response",
    "load_risk_response",
    "load_risk_summary_response",
    "load_runtime_payload",
]
