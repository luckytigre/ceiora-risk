"""Durable payload helpers for publish and refresh flows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from backend.data import serving_outputs, sqlite

_PUBLISH_ONLY_PAYLOAD_NAMES = (
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
_PUBLISH_METADATA_PAYLOAD_NAMES = {
    "exposures",
    "health_diagnostics",
    "model_sanity",
    "portfolio",
    "refresh_meta",
    "risk",
    "universe_factors",
    "universe_loadings",
}


def load_publishable_payloads(*, cache_db: Path | None = None) -> tuple[dict[str, Any], list[str]]:
    payloads: dict[str, Any] = {}
    missing: list[str] = []
    for payload_name in _PUBLISH_ONLY_PAYLOAD_NAMES:
        payload = serving_outputs.load_runtime_payload(
            payload_name,
            fallback_loader=lambda key: sqlite.cache_get(key, db_path=cache_db),
        )
        if payload is None:
            missing.append(payload_name)
            continue
        payloads[payload_name] = payload
    return payloads, missing


def restamp_publishable_payloads(
    payloads: dict[str, Any],
    *,
    run_id: str,
    snapshot_id: str,
    refresh_started_at: str,
) -> dict[str, Any]:
    restamped: dict[str, Any] = {}
    for payload_name, payload in payloads.items():
        if payload_name not in _PUBLISH_METADATA_PAYLOAD_NAMES or not isinstance(payload, dict):
            restamped[payload_name] = payload
            continue
        stamped = dict(payload)
        stamped["run_id"] = str(run_id)
        stamped["snapshot_id"] = str(snapshot_id)
        stamped["refresh_started_at"] = str(refresh_started_at)
        restamped[payload_name] = stamped
    return restamped
