"""GET /api/operator/status — operator-facing run-lane summary and recency."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from backend.data import job_runs, postgres, sqlite
from backend.orchestration.run_model_pipeline import (
    DATA_DB,
    _risk_recompute_due,
    profile_catalog,
)
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
    core_due, core_due_reason = _risk_recompute_due(risk_engine_meta, today_utc=_today_session_date())

    lanes = []
    for item in catalog:
        profile = str(item.get("profile") or "")
        lanes.append(
            {
                **item,
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
        "neon_sync_health": neon_sync_health,
        "active_snapshot": active_snapshot,
        "latest_parity_artifact": neon_sync_health.get("artifact_path") if isinstance(neon_sync_health, dict) else None,
    }
