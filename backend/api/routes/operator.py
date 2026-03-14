"""GET /api/operator/status — operator-facing run-lane summary and recency."""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Header

from backend import config
from backend.api.auth import require_role
from backend.data import core_reads, job_runs, sqlite
from backend.orchestration.run_model_pipeline import (
    DATA_DB,
    _risk_recompute_due,
    profile_catalog,
)
from backend.services.refresh_manager import _runtime_allowed_profiles
from backend.services.holdings_runtime_state import get_holdings_sync_state
from backend.trading_calendar import previous_or_same_xnys_session

router = APIRouter()


def _today_session_date():
    return datetime.fromisoformat(
        previous_or_same_xnys_session(datetime.now(timezone.utc).date().isoformat())
    ).date()


def _enrich_run_deltas(run_summaries: list[dict]) -> list[dict]:
    out: list[dict] = []
    normalized_durations: list[float | None] = []
    for summary in run_summaries:
        duration = summary.get("duration_seconds")
        try:
            normalized_durations.append(float(duration) if duration is not None else None)
        except (TypeError, ValueError):
            normalized_durations.append(None)
    for idx, summary in enumerate(run_summaries):
        item = dict(summary)
        duration_val = normalized_durations[idx]
        if duration_val is not None:
            item["duration_seconds"] = round(duration_val, 3)
        previous_duration = normalized_durations[idx + 1] if idx + 1 < len(normalized_durations) else None
        if duration_val is not None and previous_duration is not None:
            delta = round(duration_val - float(previous_duration), 3)
            item["duration_delta_seconds"] = delta
            if previous_duration > 0:
                item["duration_delta_pct"] = round((delta / float(previous_duration)) * 100.0, 2)
        else:
            item["duration_delta_seconds"] = None
            item["duration_delta_pct"] = None
        out.append(item)
    return out


@router.get("/operator/status")
def get_operator_status(
    x_operator_token: str | None = Header(default=None, alias="X-Operator-Token"),
    x_refresh_token: str | None = Header(default=None, alias="X-Refresh-Token"),
    authorization: str | None = Header(default=None),
):
    if config.cloud_mode():
        require_role(
            "operator",
            x_operator_token=x_operator_token,
            x_refresh_token=x_refresh_token,
            authorization=authorization,
        )
    catalog = profile_catalog()
    profiles = [str(item.get("profile") or "") for item in catalog]
    latest_runs = job_runs.latest_run_summary_by_profile(db_path=DATA_DB, profiles=profiles)
    recent_runs = job_runs.recent_run_summaries_by_profile(db_path=DATA_DB, profiles=profiles, limit_per_profile=8)
    try:
        source_dates = core_reads.load_source_dates()
    except Exception:
        source_dates = {
            "prices_asof": None,
            "fundamentals_asof": None,
            "classification_asof": None,
            "exposures_asof": None,
        }
    risk_engine_meta = sqlite.cache_get_live_first("risk_engine_meta") or {}
    refresh_status = sqlite.cache_get("refresh_status") or {}
    neon_sync_health = sqlite.cache_get("neon_sync_health") or {}
    active_snapshot = sqlite.cache_get("__cache_snapshot_active")
    holdings_sync = get_holdings_sync_state()
    core_due, core_due_reason = _risk_recompute_due(risk_engine_meta, today_utc=_today_session_date())
    runtime_warnings: list[str] = []
    if str(config.DATA_BACKEND).strip().lower() != "neon":
        runtime_warnings.append("DATA_BACKEND override is not Neon; this is non-standard for the current operating model.")
    if not bool(config.neon_auto_sync_enabled_effective()):
        runtime_warnings.append("Neon auto-sync is disabled; parity artifacts and mirror health will only update on manual Neon sync.")
    if not bool(config.neon_auto_parity_enabled_effective()):
        runtime_warnings.append("Neon auto-parity is disabled; post-run parity evidence will be incomplete.")
    if not bool(config.neon_auto_prune_enabled_effective()):
        runtime_warnings.append("Neon auto-prune is disabled; retained history may exceed the cloud retention window.")
    allowed_profiles = sorted(_runtime_allowed_profiles())
    local_only_profiles = sorted(set(profiles) - set(allowed_profiles))

    lanes = []
    for item in catalog:
        profile = str(item.get("profile") or "")
        profile_recent_runs = _enrich_run_deltas(recent_runs.get(profile, []))
        latest_run = dict(
            latest_runs.get(
                profile,
                {
                    "run_id": None,
                    "profile": profile,
                    "status": "missing",
                    "started_at": None,
                    "finished_at": None,
                    "updated_at": None,
                    "duration_seconds": None,
                    "stage_count": 0,
                    "completed_stage_count": 0,
                    "failed_stage_count": 0,
                    "running_stage_count": 0,
                    "current_stage": None,
                    "stage_duration_seconds_total": 0.0,
                    "slowest_stage": None,
                    "stages": [],
                },
            )
        )
        if profile_recent_runs and str(profile_recent_runs[0].get("run_id") or "") == str(latest_run.get("run_id") or ""):
            latest_run["duration_delta_seconds"] = profile_recent_runs[0].get("duration_delta_seconds")
            latest_run["duration_delta_pct"] = profile_recent_runs[0].get("duration_delta_pct")
        else:
            latest_run["duration_delta_seconds"] = None
            latest_run["duration_delta_pct"] = None
        lanes.append(
            {
                **item,
                "recent_runs": profile_recent_runs,
                "latest_run": latest_run,
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
            "app_runtime_role": str(config.APP_RUNTIME_ROLE),
            "allowed_profiles": allowed_profiles,
            "local_only_profiles": local_only_profiles,
            "canonical_serving_profile": "serve-refresh",
            "dashboard_truth_surface": "durable_serving_payloads",
            "dashboard_truth_plain_english": (
                "Dashboard pages should read durable serving payloads plus live holdings/runtime state, "
                "not rebuild directly from raw source tables in the browser."
            ),
            "diagnostics_scope": "local_sqlite_and_cache",
            "diagnostics_scope_plain_english": (
                "Detailed diagnostics are local-instance diagnostics. Operator status, refresh state, holdings dirty state, "
                "and Neon mirror/parity health are the live operator truth surfaces."
            ),
            "data_backend": str(config.DATA_BACKEND),
            "neon_database_configured": bool(str(config.NEON_DATABASE_URL).strip()),
            "neon_auto_sync_enabled": bool(config.NEON_AUTO_SYNC_ENABLED),
            "neon_auto_sync_enabled_effective": bool(config.neon_auto_sync_enabled_effective()),
            "neon_auto_parity_enabled": bool(config.NEON_AUTO_PARITY_ENABLED),
            "neon_auto_parity_enabled_effective": bool(config.neon_auto_parity_enabled_effective()),
            "neon_auto_prune_enabled": bool(config.NEON_AUTO_PRUNE_ENABLED),
            "neon_auto_prune_enabled_effective": bool(config.neon_auto_prune_enabled_effective()),
            "neon_read_surfaces": sorted(config.NEON_READ_SURFACES),
            "serving_outputs_primary_reads": bool(config.SERVING_OUTPUTS_PRIMARY_READS),
            "serving_outputs_primary_reads_effective": bool(config.serving_outputs_primary_reads_enabled()),
            "warnings": runtime_warnings,
        },
    }
