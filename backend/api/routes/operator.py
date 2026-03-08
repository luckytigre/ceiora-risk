"""GET /api/operator/status — operator-facing run-lane summary and recency."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from backend.data import job_runs, postgres, sqlite
from backend import config
from backend.orchestration.run_model_pipeline import (
    DATA_DB,
    _risk_recompute_due,
    profile_catalog,
)
from backend.services.holdings_runtime_state import get_holdings_sync_state
from backend.trading_calendar import previous_or_same_xnys_session

router = APIRouter()


def _today_session_date():
    return datetime.fromisoformat(
        previous_or_same_xnys_session(datetime.now(timezone.utc).date().isoformat())
    ).date()


@router.get("/operator/status")
def get_operator_status():
    catalog = profile_catalog()
    profiles = [str(item.get("profile") or "") for item in catalog]
    latest_runs = job_runs.latest_run_summary_by_profile(db_path=DATA_DB, profiles=profiles)
    recent_runs = job_runs.recent_run_summaries_by_profile(db_path=DATA_DB, profiles=profiles, limit_per_profile=8)
    try:
        source_dates = postgres.load_source_dates()
    except Exception:
        source_dates = {
            "prices_asof": None,
            "fundamentals_asof": None,
            "classification_asof": None,
            "exposures_asof": None,
        }
    risk_engine_meta = sqlite.cache_get("risk_engine_meta") or {}
    refresh_status = sqlite.cache_get("refresh_status") or {}
    neon_sync_health = sqlite.cache_get("neon_sync_health") or {}
    active_snapshot = sqlite.cache_get("__cache_snapshot_active")
    holdings_sync = get_holdings_sync_state()
    core_due, core_due_reason = _risk_recompute_due(risk_engine_meta, today_utc=_today_session_date())
    runtime_warnings: list[str] = []
    if str(config.DATA_BACKEND).strip().lower() != "neon":
        runtime_warnings.append("DATA_BACKEND override is not Neon; this is non-standard for the current operating model.")
    if not bool(config.NEON_AUTO_SYNC_ENABLED):
        runtime_warnings.append("Neon auto-sync is disabled; parity artifacts and mirror health will only update on manual Neon sync.")
    if not bool(config.NEON_AUTO_PARITY_ENABLED):
        runtime_warnings.append("Neon auto-parity is disabled; post-run parity evidence will be incomplete.")
    if not bool(config.NEON_AUTO_PRUNE_ENABLED):
        runtime_warnings.append("Neon auto-prune is disabled; retained history may exceed the cloud retention window.")

    lanes = []
    for item in catalog:
        profile = str(item.get("profile") or "")
        lanes.append(
            {
                **item,
                "recent_runs": recent_runs.get(profile, []),
                "latest_run": latest_runs.get(
                    profile,
                    {
                        "run_id": None,
                        "profile": profile,
                        "status": "missing",
                        "started_at": None,
                        "finished_at": None,
                        "updated_at": None,
                        "stage_count": 0,
                        "completed_stage_count": 0,
                        "failed_stage_count": 0,
                        "running_stage_count": 0,
                        "stages": [],
                    },
                ),
            }
        )

    return {
        "status": "ok",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lanes": lanes,
        "source_dates": source_dates,
        "risk_engine": risk_engine_meta,
        "core_due": {
            "due": bool(core_due),
            "reason": str(core_due_reason),
        },
        "refresh": refresh_status,
        "holdings_sync": holdings_sync,
        "neon_sync_health": neon_sync_health,
        "active_snapshot": active_snapshot,
        "latest_parity_artifact": neon_sync_health.get("artifact_path") if isinstance(neon_sync_health, dict) else None,
        "runtime": {
            "data_backend": str(config.DATA_BACKEND),
            "neon_database_configured": bool(str(config.NEON_DATABASE_URL).strip()),
            "neon_auto_sync_enabled": bool(config.NEON_AUTO_SYNC_ENABLED),
            "neon_auto_parity_enabled": bool(config.NEON_AUTO_PARITY_ENABLED),
            "neon_auto_prune_enabled": bool(config.NEON_AUTO_PRUNE_ENABLED),
            "neon_read_surfaces": sorted(config.NEON_READ_SURFACES),
            "warnings": runtime_warnings,
        },
    }
