"""Refresh-context policy helpers for the analytics pipeline."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from backend import config
from backend.analytics.refresh_policy import (
    latest_factor_return_date as _latest_factor_return_date_impl,
    risk_recompute_due as _risk_recompute_due_impl,
)
from backend.analytics.contracts import RiskEngineMetaPayload
from backend.data import model_outputs, runtime_state
from backend.analytics.refresh_metadata import derive_estimation_exposure_anchor_date

RISK_ENGINE_METHOD_VERSION = "v8_use4_us_core_market_one_stage_projected_non_us_2026_03_15"
_UNIVERSE_REUSE_RISK_KEYS = (
    "status",
    "method_version",
    "last_recompute_date",
    "factor_returns_latest_date",
    "cross_section_min_age_days",
    "lookback_days",
    "specific_risk_ticker_count",
)


def risk_engine_reuse_signature(payload: dict[str, Any] | None) -> dict[str, Any]:
    meta = dict(payload or {})
    return {key: meta.get(key) for key in _UNIVERSE_REUSE_RISK_KEYS}


def risk_engine_meta_score(payload: dict[str, Any] | None) -> tuple[int, str, str, int]:
    meta = dict(payload or {})
    return (
        1 if str(meta.get("method_version") or "") == str(RISK_ENGINE_METHOD_VERSION) else 0,
        str(meta.get("factor_returns_latest_date") or ""),
        str(meta.get("last_recompute_date") or ""),
        int(meta.get("specific_risk_ticker_count") or 0),
    )


def derive_estimation_exposure_anchor_date_from_meta(meta: dict[str, Any] | None) -> str | None:
    payload = dict(meta or {})
    return derive_estimation_exposure_anchor_date(
        factor_returns_latest_date=payload.get("factor_returns_latest_date"),
        cross_section_min_age_days=payload.get("cross_section_min_age_days"),
        existing_anchor_date=payload.get("estimation_exposure_anchor_date"),
    )


def resolve_effective_risk_engine_meta(
    *,
    fallback_loader,
) -> tuple[RiskEngineMetaPayload, str]:
    runtime_meta = runtime_state.load_runtime_state(
        "risk_engine_meta",
        fallback_loader=fallback_loader,
    ) or {}
    persisted_meta = model_outputs.load_latest_rebuild_authority_risk_engine_state() or {}
    runtime_score = risk_engine_meta_score(runtime_meta)
    persisted_score = risk_engine_meta_score(persisted_meta)
    if persisted_score > runtime_score:
        if persisted_meta.get("estimation_exposure_anchor_date") is None:
            persisted_meta = dict(persisted_meta)
            persisted_meta["estimation_exposure_anchor_date"] = derive_estimation_exposure_anchor_date_from_meta(persisted_meta)
        return persisted_meta, "model_run_metadata"
    if runtime_meta and persisted_meta:
        same_core_state = (
            str(runtime_meta.get("method_version") or "") == str(persisted_meta.get("method_version") or "")
            and str(runtime_meta.get("factor_returns_latest_date") or "") == str(persisted_meta.get("factor_returns_latest_date") or "")
            and str(runtime_meta.get("last_recompute_date") or "") == str(persisted_meta.get("last_recompute_date") or "")
        )
        runtime_anchor = runtime_meta.get("estimation_exposure_anchor_date")
        persisted_anchor = persisted_meta.get("estimation_exposure_anchor_date")
        if same_core_state and (
            (runtime_meta.get("latest_r2") is None and persisted_meta.get("latest_r2") is not None)
            or (runtime_anchor is None and persisted_anchor is not None)
        ):
            enriched_meta = dict(runtime_meta)
            enriched_meta["latest_r2"] = persisted_meta.get("latest_r2")
            enriched_meta["estimation_exposure_anchor_date"] = persisted_anchor
            return enriched_meta, "runtime_state_enriched_from_model_run_metadata"
    if runtime_meta.get("estimation_exposure_anchor_date") is None:
        runtime_meta = dict(runtime_meta)
        runtime_meta["estimation_exposure_anchor_date"] = derive_estimation_exposure_anchor_date_from_meta(runtime_meta)
    return runtime_meta, "runtime_state"


def source_dates_reuse_signature(payload: dict[str, Any] | None) -> dict[str, Any]:
    source_dates = dict(payload or {})
    return {
        "fundamentals_asof": source_dates.get("fundamentals_asof"),
        "classification_asof": source_dates.get("classification_asof"),
        "prices_asof": source_dates.get("prices_asof"),
        "exposures_latest_available_asof": (
            source_dates.get("exposures_latest_available_asof")
            or source_dates.get("exposures_asof")
        ),
    }


def risk_recompute_due(meta: dict[str, Any], *, today_utc: date) -> tuple[bool, str]:
    return _risk_recompute_due_impl(
        meta,
        today_utc=today_utc,
        method_version=RISK_ENGINE_METHOD_VERSION,
        interval_days=config.RISK_RECOMPUTE_INTERVAL_DAYS,
    )


def latest_factor_return_date(cache_db: Path) -> str | None:
    return _latest_factor_return_date_impl(cache_db)
