"""Durable payload helpers for publish and refresh flows."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from backend import config
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


def persist_publish_only_payloads(
    *,
    data_db,
    cache_db: Path | None = None,
    run_id: str,
    refresh_mode: str,
    refresh_scope_key: str | None,
    refresh_started_at: str,
) -> dict[str, Any]:
    payloads, missing_payloads = load_publishable_payloads(cache_db=cache_db)
    if missing_payloads:
        raise RuntimeError(
            "publish-only requested but cached serving payloads are incomplete: "
            + ", ".join(sorted(missing_payloads))
        )
    snapshot_id = run_id
    payloads = restamp_publishable_payloads(
        payloads,
        run_id=run_id,
        snapshot_id=snapshot_id,
        refresh_started_at=refresh_started_at,
    )
    refresh_meta = dict(payloads.get("refresh_meta") or {})
    risk_payload = dict(payloads.get("risk") or {})
    portfolio_payload = dict(payloads.get("portfolio") or {})
    health_payload = dict(payloads.get("health_diagnostics") or {})
    serving_outputs_write = serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id=run_id,
        snapshot_id=snapshot_id,
        refresh_mode=refresh_mode,
        payloads=payloads,
        replace_all=True,
    )
    neon_write = serving_outputs_write.get("neon_write") if isinstance(serving_outputs_write, dict) else None
    if (
        config.serving_payload_neon_write_required()
        and isinstance(neon_write, dict)
        and str(neon_write.get("status") or "") != "ok"
    ):
        raise RuntimeError(f"Serving payload Neon write failed: {neon_write}")
    model_outputs_write = {
        "status": "skipped",
        "reason": "publish_only",
        "run_id": run_id,
    }
    sqlite.cache_set("model_outputs_write", model_outputs_write, db_path=cache_db)
    sqlite.cache_set("serving_outputs_write", serving_outputs_write, db_path=cache_db)
    return {
        "status": "ok",
        "run_id": run_id,
        "snapshot_id": snapshot_id,
        "positions": int((portfolio_payload.get("position_count") or 0)),
        "total_value": round(float(portfolio_payload.get("total_value") or 0.0), 2),
        "mode": refresh_mode,
        "refresh_scope": refresh_scope_key,
        "cross_section_snapshot": dict(refresh_meta.get("cross_section_snapshot") or {"status": "reused"}),
        "risk_engine": dict(risk_payload.get("risk_engine") or refresh_meta.get("risk_engine") or {}),
        "model_sanity": dict(payloads.get("model_sanity") or {"status": "unknown"}),
        "cuse4_foundation": dict(refresh_meta.get("cuse4_foundation") or {"status": "reused"}),
        "health_refreshed": False,
        "health_refresh_state": str(
            health_payload.get("diagnostics_refresh_state")
            or refresh_meta.get("health_refresh_state")
            or "carried_forward"
        ),
        "universe_loadings_reused": True,
        "universe_loadings_reuse_reason": "publish_only_cached_payloads",
        "model_outputs_write": model_outputs_write,
        "serving_outputs_write": serving_outputs_write,
    }
