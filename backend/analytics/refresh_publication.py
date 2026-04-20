"""Serving publication sequencing helpers for refresh flows."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend import config
from backend.analytics import health_payloads, publish_payloads, refresh_metadata, refresh_persistence, reuse_policy
from backend.data import serving_outputs, sqlite

logger = logging.getLogger(__name__)


def _require_neon_write_ok(write_result: dict[str, Any] | Any, *, error_prefix: str) -> None:
    neon_write = write_result.get("neon_write") if isinstance(write_result, dict) else None
    if (
        config.serving_payload_neon_write_required()
        and isinstance(neon_write, dict)
        and str(neon_write.get("status") or "") != "ok"
    ):
        raise RuntimeError(f"{error_prefix}: {neon_write}")


def run_publish_only_refresh(
    *,
    data_db: Path,
    cache_db: Path,
    run_id: str,
    refresh_mode: str,
    refresh_scope: str | None,
    refresh_started_at: str,
    substage_timings: dict[str, dict[str, Any]],
    progress_callback: Callable[[dict[str, Any]], None] | None,
    emit_progress: Callable[..., None],
    load_projection_ok_tickers_for_payloads: Callable[..., set[str]],
    validate_projection_only_payloads: Callable[..., None],
) -> dict[str, Any]:
    emit_progress(
        progress_callback,
        message="Republishing cached serving payloads",
        refresh_substage="publish_only",
    )
    payloads, missing_payloads = publish_payloads.load_publishable_payloads(cache_db=cache_db)
    if missing_payloads:
        raise RuntimeError(
            "publish-only requested but cached serving payloads are incomplete: "
            + ", ".join(sorted(missing_payloads))
        )

    snapshot_id = run_id
    payloads = publish_payloads.restamp_publishable_payloads(
        payloads,
        run_id=run_id,
        snapshot_id=snapshot_id,
        refresh_started_at=refresh_started_at,
    )
    validate_projection_only_payloads(
        projection_ok_tickers=load_projection_ok_tickers_for_payloads(
            data_db=data_db,
            payloads=payloads,
        ),
        payloads=payloads,
    )
    universe_ok, universe_reason = reuse_policy.universe_loadings_payload_integrity(
        payloads.get("universe_loadings")
    )
    if not universe_ok:
        raise RuntimeError(
            "Publish-only universe payload integrity failed: "
            f"{universe_reason}"
        )
    live_universe_payload = serving_outputs.load_current_payload("universe_loadings")
    regression_ok, regression_reason = reuse_policy.universe_loadings_live_regression_guard(
        payloads.get("universe_loadings"),
        current_live_payload=live_universe_payload,
    )
    if not regression_ok:
        raise RuntimeError(
            "Publish-only universe payload regressed versus the current live modeled snapshot: "
            f"{regression_reason}"
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
    _require_neon_write_ok(
        serving_outputs_write,
        error_prefix="Serving payload Neon write failed",
    )

    model_outputs_write = {
        "status": "skipped",
        "reason": "publish_only",
        "run_id": run_id,
    }
    sqlite.cache_set("model_outputs_write", model_outputs_write, db_path=cache_db)
    sqlite.cache_set("serving_outputs_write", serving_outputs_write, db_path=cache_db)

    publish_completed_at = datetime.now(timezone.utc).isoformat()
    publish_milestone = {
        "published_at": publish_completed_at,
        "snapshot_id": snapshot_id,
        "run_id": run_id,
        "payload_count": len(payloads),
        "payload_names": sorted(str(key) for key in payloads.keys()),
    }
    substage_timings["persist_outputs"] = {"duration_seconds": 0.0, "mode": "publish_only"}
    substage_timings["serving_publish_complete"] = {
        "duration_seconds": 0.0,
        **publish_milestone,
    }
    emit_progress(
        progress_callback,
        message="Serving payload publish complete",
        refresh_substage="serving_publish_complete",
        substage_status="completed",
        publish_complete=True,
        published_at=publish_completed_at,
        published_snapshot_id=snapshot_id,
        published_run_id=run_id,
        published_payload_count=len(payloads),
        published_payload_names=sorted(str(key) for key in payloads.keys()),
    )

    return {
        "status": "ok",
        "run_id": run_id,
        "snapshot_id": snapshot_id,
        "positions": int((portfolio_payload.get("position_count") or 0)),
        "total_value": round(refresh_metadata.finite_float(portfolio_payload.get("total_value"), 0.0), 2),
        "mode": refresh_mode,
        "refresh_scope": refresh_scope,
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
        "publish_milestone": publish_milestone,
        "substage_timings": substage_timings,
        "universe_loadings_reused": True,
        "universe_loadings_reuse_reason": "publish_only_cached_payloads",
        "model_outputs_write": model_outputs_write,
        "serving_outputs_write": serving_outputs_write,
    }


def persist_refresh_publication(
    *,
    data_db: Path,
    cache_db: Path,
    run_id: str,
    snapshot_id: str,
    refresh_mode: str,
    refresh_started_at: str,
    refresh_pipeline_t0: float,
    substage_timings: dict[str, dict[str, Any]],
    progress_callback: Callable[[dict[str, Any]], None] | None,
    emit_progress: Callable[..., None],
    record_substage_timing: Callable[..., dict[str, Any]],
    recomputed_this_refresh: bool,
    params: dict[str, Any],
    source_dates: dict[str, Any],
    risk_engine_state: dict[str, Any],
    cov: Any,
    specific_risk_by_security: dict[str, Any],
    persisted_payloads: dict[str, Any],
) -> dict[str, Any]:
    emit_progress(
        progress_callback,
        message="Persisting model and serving outputs",
        refresh_substage="persist_outputs",
    )
    persist_outputs_t0 = time.perf_counter()
    model_outputs_write, serving_outputs_write = refresh_persistence.persist_refresh_outputs(
        data_db=data_db,
        cache_db=cache_db,
        run_id=run_id,
        snapshot_id=snapshot_id,
        refresh_mode=refresh_mode,
        refresh_started_at=refresh_started_at,
        recomputed_this_refresh=bool(recomputed_this_refresh),
        params=params,
        source_dates=source_dates,
        risk_engine_state=risk_engine_state,
        cov=cov,
        specific_risk_by_security=specific_risk_by_security,
        persisted_payloads=persisted_payloads,
    )
    persist_outputs_timing = record_substage_timing(
        "persist_outputs",
        persist_outputs_t0,
        payload_count=int(len(persisted_payloads)),
    )
    publish_completed_at = datetime.now(timezone.utc).isoformat()
    publish_milestone = {
        "published_at": publish_completed_at,
        "snapshot_id": snapshot_id,
        "run_id": run_id,
        "payload_count": int(len(persisted_payloads)),
        "payload_names": sorted(str(key) for key in persisted_payloads.keys()),
        "persist_duration_seconds": float(persist_outputs_timing["duration_seconds"]),
        "elapsed_since_refresh_start_seconds": round(float(time.perf_counter() - refresh_pipeline_t0), 3),
    }
    substage_timings["serving_publish_complete"] = {
        "duration_seconds": 0.0,
        "published_at": publish_completed_at,
        "snapshot_id": snapshot_id,
        "run_id": run_id,
        "payload_count": int(len(persisted_payloads)),
    }
    emit_progress(
        progress_callback,
        message="Serving payload publish complete",
        refresh_substage="serving_publish_complete",
        substage_status="completed",
        publish_complete=True,
        published_at=publish_completed_at,
        published_snapshot_id=snapshot_id,
        published_run_id=run_id,
        published_payload_count=int(len(persisted_payloads)),
        published_payload_names=sorted(str(key) for key in persisted_payloads.keys()),
    )
    return {
        "model_outputs_write": model_outputs_write,
        "serving_outputs_write": serving_outputs_write,
        "publish_milestone": publish_milestone,
    }


def refresh_post_publish_health_diagnostics(
    *,
    data_db: Path,
    cache_db: Path,
    run_id: str,
    snapshot_id: str,
    refresh_mode: str,
    source_dates: dict[str, Any],
    risk_engine_state: dict[str, Any],
    positions: list[dict[str, Any]],
    total_value: float,
    persisted_payloads: dict[str, Any],
    serving_outputs_write: dict[str, Any] | Any,
    progress_callback: Callable[[dict[str, Any]], None] | None,
    emit_progress: Callable[..., None],
    record_substage_timing: Callable[..., dict[str, Any]],
    compute_health_diagnostics_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    emit_progress(
        progress_callback,
        message="Computing deep health diagnostics",
        refresh_substage="health_diagnostics",
    )
    health_t0 = time.perf_counter()
    payload_source_dates = dict(persisted_payloads.get("refresh_meta", {}).get("source_dates") or source_dates)
    health_payload = compute_health_diagnostics_fn(
        data_db,
        cache_db,
        risk_payload=dict(persisted_payloads.get("risk") or {}),
        portfolio_payload=dict(persisted_payloads.get("portfolio") or {}),
        universe_payload=dict(persisted_payloads.get("universe_loadings") or {}),
        covariance_payload=dict(persisted_payloads.get("risk_engine_cov") or {}),
        source_dates=payload_source_dates,
        run_id=run_id,
        snapshot_id=snapshot_id,
        progress_callback=progress_callback,
    )
    logger.info(
        "Deep health diagnostics completed in %.2fs for run_id=%s snapshot_id=%s",
        time.perf_counter() - health_t0,
        run_id,
        snapshot_id,
    )
    diagnostics_timings = (
        dict(health_payload.get("timings_seconds") or {})
        if isinstance(health_payload, dict)
        else {}
    )
    record_substage_timing(
        "health_diagnostics",
        health_t0,
        diagnostics_timings=diagnostics_timings or None,
    )

    if isinstance(health_payload, dict):
        health_payload = dict(health_payload)
        health_payload["run_id"] = str(run_id)
        health_payload["snapshot_id"] = str(snapshot_id)
        health_payload["_reuse_signature"] = health_payloads.health_reuse_signature(
            source_dates=payload_source_dates,
            risk_engine_state=risk_engine_state,
            positions=positions,
            total_value=total_value,
        )
        health_payload["cache_version"] = health_payloads.HEALTH_DIAGNOSTICS_CACHE_VERSION
        health_payload["diagnostics_refresh_state"] = "recomputed"
        health_payload["diagnostics_generated_from_run_id"] = str(run_id)
        health_payload["diagnostics_generated_from_snapshot_id"] = str(snapshot_id)

    refresh_meta_payload = dict(persisted_payloads.get("refresh_meta") or {})
    refresh_meta_payload["health_refreshed"] = True
    refresh_meta_payload["health_refresh_state"] = "recomputed"

    updated_payloads = dict(persisted_payloads)
    updated_payloads["health_diagnostics"] = health_payload
    updated_payloads["refresh_meta"] = refresh_meta_payload

    emit_progress(
        progress_callback,
        message="Persisting refreshed health diagnostics",
        refresh_substage="health_diagnostics_persist",
    )
    health_persist_t0 = time.perf_counter()
    sqlite.cache_set(
        "health_diagnostics",
        health_payload,
        snapshot_id=snapshot_id,
        db_path=cache_db,
    )
    sqlite.cache_set(
        "refresh_meta",
        refresh_meta_payload,
        snapshot_id=snapshot_id,
        db_path=cache_db,
    )
    health_patch_write = serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id=run_id,
        snapshot_id=snapshot_id,
        refresh_mode=refresh_mode,
        payloads={
            "health_diagnostics": health_payload,
            "refresh_meta": refresh_meta_payload,
        },
        replace_all=False,
    )
    _require_neon_write_ok(
        health_patch_write,
        error_prefix="Serving payload Neon write failed during health refresh patch",
    )
    if isinstance(serving_outputs_write, dict):
        serving_outputs_write = dict(serving_outputs_write)
        serving_outputs_write["health_patch_write"] = health_patch_write
    record_substage_timing("health_diagnostics_persist", health_persist_t0)

    return {
        "persisted_payloads": updated_payloads,
        "serving_outputs_write": serving_outputs_write,
        "health_refreshed": True,
        "health_refresh_state": "recomputed",
    }
