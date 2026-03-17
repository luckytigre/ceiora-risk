"""Health-diagnostics reuse and carry-forward helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from backend.analytics.contracts import PositionPayload, RiskEngineStatePayload, SourceDatesPayload
from backend.analytics.refresh_metadata import finite_float

HEALTH_DIAGNOSTICS_CACHE_VERSION = "2026_03_15_v1"


def positions_fingerprint(positions: list[PositionPayload]) -> str:
    normalized = []
    for row in positions:
        if not isinstance(row, dict):
            continue
        normalized.append(
            {
                "ticker": str(row.get("ticker") or "").upper(),
                "weight": round(finite_float(row.get("weight"), 0.0), 10),
                "market_value": round(finite_float(row.get("market_value"), 0.0), 4),
                "quantity": round(finite_float(row.get("quantity"), 0.0), 6),
            }
        )
    normalized.sort(key=lambda item: item["ticker"])
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def health_reuse_signature(
    *,
    source_dates: SourceDatesPayload,
    risk_engine_state: RiskEngineStatePayload,
    positions: list[PositionPayload],
    total_value: float,
) -> dict[str, Any]:
    return {
        "cache_version": HEALTH_DIAGNOSTICS_CACHE_VERSION,
        "source_dates": {
            "fundamentals_asof": source_dates.get("fundamentals_asof"),
            "classification_asof": source_dates.get("classification_asof"),
            "prices_asof": source_dates.get("prices_asof"),
            "exposures_latest_available_asof": (
                source_dates.get("exposures_latest_available_asof")
                or source_dates.get("exposures_asof")
            ),
            "exposures_served_asof": source_dates.get("exposures_served_asof"),
        },
        "risk_engine": {
            "method_version": str(risk_engine_state.get("method_version") or ""),
            "last_recompute_date": str(risk_engine_state.get("last_recompute_date") or ""),
            "factor_returns_latest_date": str(risk_engine_state.get("factor_returns_latest_date") or ""),
            "lookback_days": int(risk_engine_state.get("lookback_days") or 0),
            "specific_risk_ticker_count": int(risk_engine_state.get("specific_risk_ticker_count") or 0),
        },
        "positions_fingerprint": positions_fingerprint(positions),
        "total_value": round(finite_float(total_value, 0.0), 2),
    }


def can_reuse_cached_health_payload(
    cached_payload: Any,
    *,
    signature: dict[str, Any],
) -> bool:
    if not isinstance(cached_payload, dict):
        return False
    cached_signature = cached_payload.get("_reuse_signature")
    if not isinstance(cached_signature, dict):
        return False
    return cached_signature == signature


def carry_forward_health_payload(
    cached_payload: dict[str, Any] | None,
    *,
    run_id: str,
    snapshot_id: str,
    refresh_started_at: str,
    source_dates: SourceDatesPayload,
    risk_engine_state: RiskEngineStatePayload,
) -> tuple[dict[str, Any], str]:
    if isinstance(cached_payload, dict):
        out = dict(cached_payload)
        out["diagnostics_refresh_state"] = "carried_forward"
        out["diagnostics_generated_from_run_id"] = str(
            out.get("diagnostics_generated_from_run_id")
            or out.get("run_id")
            or ""
        )
        out["diagnostics_generated_from_snapshot_id"] = str(
            out.get("diagnostics_generated_from_snapshot_id")
            or out.get("snapshot_id")
            or ""
        )
        out["run_id"] = str(run_id)
        out["snapshot_id"] = str(snapshot_id)
        out["refresh_started_at"] = str(refresh_started_at)
        out["source_dates"] = dict(source_dates or {})
        out["risk_engine"] = dict(risk_engine_state or {})
        return out, "carried_forward"

    return (
        {
            "status": "deferred",
            "message": "Deep health diagnostics were not recomputed on this refresh.",
            "diagnostics_refresh_state": "deferred",
            "diagnostics_generated_from_run_id": None,
            "diagnostics_generated_from_snapshot_id": None,
            "run_id": str(run_id),
            "snapshot_id": str(snapshot_id),
            "refresh_started_at": str(refresh_started_at),
            "source_dates": dict(source_dates or {}),
            "risk_engine": dict(risk_engine_state or {}),
            "cache_version": HEALTH_DIAGNOSTICS_CACHE_VERSION,
        },
        "deferred",
    )
