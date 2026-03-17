"""Canonical serving-payload assembly for dashboard routes."""

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
    payload_loader=None,
    fallback_loader=None,
) -> dict[str, Any]:
    data = _load_payload(
        "exposures",
        payload_loader=payload_loader,
        fallback_loader=fallback_loader,
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


def load_risk_response(
    *,
    payload_loader=None,
    fallback_loader=None,
) -> dict[str, Any]:
    payloads = _load_payloads(
        ("risk", "model_sanity"),
        payload_loader=payload_loader,
        fallback_loader=fallback_loader,
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
    return {
        **dict(data),
        "risk_shares": _normalize_systematic_shares(data.get("risk_shares")),
        "component_shares": _normalize_systematic_shares(data.get("component_shares")),
        "factor_details": _normalize_risk_factor_details(data.get("factor_details")),
        "risk_engine": _normalize_risk_engine_state(data.get("risk_engine")),
        "model_sanity": _normalize_model_sanity(sanity),
        "_cached": True,
    }


def load_portfolio_response(
    *,
    payload_loader=None,
    fallback_loader=None,
    position_normalizer: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    data = _load_payload(
        "portfolio",
        payload_loader=payload_loader,
        fallback_loader=fallback_loader,
    )
    if data is None:
        raise DashboardPayloadNotReady(
            cache_key="portfolio",
            message="Portfolio cache is empty. Run refresh to build positions.",
        )
    positions = [
        position_normalizer(dict(raw))
        for raw in data.get("positions", [])
        if isinstance(raw, dict)
    ]
    return {**dict(data), "positions": positions, "_cached": True}
