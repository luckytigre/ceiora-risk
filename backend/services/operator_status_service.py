"""Compatibility module for cUSE4 operator-status payload assembly.

Prefer importing `backend.services.cuse4_operator_status_service` from the
default cUSE4 operator route. This module remains as the compatibility home for
older callers and tests that still bind directly to it.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from backend import config
from backend.analytics.pipeline import RISK_ENGINE_METHOD_VERSION
from backend.analytics.refresh_context import derive_estimation_exposure_anchor_date_from_meta
from backend.analytics.refresh_policy import risk_recompute_due as _risk_recompute_due_impl
from backend.data import core_reads, job_runs, runtime_state, sqlite
from backend.orchestration.profiles import profile_catalog
from backend.services.refresh_profile_policy import runtime_allowed_profiles
from backend.services.holdings_runtime_state import get_holdings_sync_state
from backend.services.refresh_status_service import load_persisted_refresh_status
from backend.trading_calendar import previous_or_same_xnys_session


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today_session_date():
    return datetime.fromisoformat(
        previous_or_same_xnys_session(datetime.now(timezone.utc).date().isoformat())
    ).date()


def _risk_recompute_due(meta: dict, *, today_utc):
    return _risk_recompute_due_impl(
        meta,
        today_utc=today_utc,
        method_version=RISK_ENGINE_METHOD_VERSION,
        interval_days=config.RISK_RECOMPUTE_INTERVAL_DAYS,
    )


def _refresh_manager_owned_lane(*, profile: str, run_id: str) -> bool:
    # Only the API-managed serve-refresh lane is safe to reconcile from
    # process-local refresh manager state. Deeper profiles may also run via CLI.
    return str(profile or "") == "serve-refresh" and str(run_id or "").startswith("api_")


def _reconcile_latest_run_with_refresh_status(
    *,
    profile: str,
    latest_run: dict[str, object],
    refresh_status: dict[str, object],
) -> dict[str, object]:
    if str(latest_run.get("status") or "") != "running":
        return latest_run
    latest_run_id = str(latest_run.get("run_id") or "")
    if not _refresh_manager_owned_lane(profile=profile, run_id=latest_run_id):
        return latest_run
    refresh_profile = str(refresh_status.get("profile") or "")
    refresh_run_id = str(refresh_status.get("pipeline_run_id") or "")
    if (
        str(refresh_status.get("status") or "") == "running"
        and refresh_profile == str(profile)
        and refresh_run_id
        and refresh_run_id == latest_run_id
    ):
        return latest_run

    reconciled_status = "unknown"
    same_refresh_run = (
        refresh_profile == str(profile)
        and refresh_run_id
        and refresh_run_id == latest_run_id
    )
    finished_at = latest_run.get("finished_at")
    updated_at = latest_run.get("updated_at")
    if same_refresh_run:
        candidate = str(refresh_status.get("status") or "unknown").strip().lower() or "unknown"
        reconciled_status = ("unknown" if candidate == "running" else candidate)
        finished_at = refresh_status.get("finished_at") or latest_run.get("finished_at")
        updated_at = refresh_status.get("finished_at") or latest_run.get("updated_at")
    else:
        finished_at = _now_iso()
        updated_at = finished_at
    stages = []
    for raw_stage in list(latest_run.get("stages") or []):
        if not isinstance(raw_stage, dict):
            continue
        clean_stage = dict(raw_stage)
        if str(clean_stage.get("status") or "") == "running":
            clean_stage["status"] = reconciled_status
            clean_stage["completed_at"] = finished_at
            clean_stage["error_type"] = "refresh_state_reconciled"
            clean_stage["error_message"] = "Reconciled against refresh status because no live worker is running."
        stages.append(clean_stage)

    return {
        **latest_run,
        "status": reconciled_status,
        "finished_at": finished_at,
        "updated_at": updated_at,
        "running_stage_count": 0,
        "current_stage": None,
        "stages": stages,
        "reconciled_from_refresh_status": True,
        "reconciled_reason": "no_live_refresh_worker_for_running_lane",
    }


def _status_timestamp(payload: dict[str, object] | None) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in ("finished_at", "updated_at", "started_at", "requested_at"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def _latest_terminal_run(latest_runs: dict[str, dict[str, object]]) -> dict[str, object] | None:
    candidates: list[dict[str, object]] = []
    for run in latest_runs.values():
        if not isinstance(run, dict):
            continue
        status = str(run.get("status") or "").strip().lower()
        if status in {"ok", "failed", "skipped", "unknown"}:
            candidates.append(run)
    if not candidates:
        return None
    return max(candidates, key=lambda run: _status_timestamp(run))


def _promote_latest_run_into_refresh_status(
    *,
    refresh_status: dict[str, object],
    latest_runs: dict[str, dict[str, object]],
) -> dict[str, object]:
    current = dict(refresh_status or {})
    if str(current.get("status") or "").strip().lower() == "running":
        return current
    latest_run = _latest_terminal_run(latest_runs)
    if latest_run is None:
        return current
    latest_ts = _status_timestamp(latest_run)
    refresh_ts = _status_timestamp(current)
    if refresh_ts and latest_ts and latest_ts <= refresh_ts:
        return current
    profile = str(latest_run.get("profile") or "").strip() or str(current.get("profile") or "")
    run_id = str(latest_run.get("run_id") or "").strip() or str(current.get("pipeline_run_id") or "")
    promoted = {
        **current,
        "status": str(latest_run.get("status") or current.get("status") or "unknown"),
        "profile": profile,
        "requested_profile": profile,
        "pipeline_run_id": run_id,
        "started_at": latest_run.get("started_at"),
        "finished_at": latest_run.get("finished_at") or latest_run.get("updated_at"),
        "result": {
            "status": str(latest_run.get("status") or "unknown"),
            "run_id": run_id,
            "profile": profile,
        },
        "error": None if str(latest_run.get("status") or "").strip().lower() == "ok" else current.get("error"),
        "promoted_from_job_runs": True,
        "promoted_reason": "latest_terminal_run_newer_than_refresh_cache",
    }
    return promoted


def _load_authoritative_operator_source_dates() -> dict[str, object]:
    return core_reads.load_source_dates()


def _load_local_archive_source_dates() -> dict[str, object] | None:
    if not config.runtime_role_allows_ingest():
        return None
    with core_reads.core_read_backend("local"):
        return core_reads.load_source_dates()


def _newer_local_archive_fields(
    authoritative_source_dates: dict[str, object] | None,
    local_archive_source_dates: dict[str, object] | None,
) -> list[str]:
    authoritative = authoritative_source_dates or {}
    local_archive = local_archive_source_dates or {}
    newer: list[str] = []
    for field in (
        "prices_asof",
        "fundamentals_asof",
        "classification_asof",
        "exposures_latest_available_asof",
    ):
        auth_value = str(authoritative.get(field) or "").strip()
        if field == "exposures_latest_available_asof":
            auth_value = auth_value or str(authoritative.get("exposures_asof") or "").strip()
            local_value = str(
                local_archive.get("exposures_latest_available_asof")
                or local_archive.get("exposures_asof")
                or ""
            ).strip()
        else:
            local_value = str(local_archive.get(field) or "").strip()
        if local_value and local_value > auth_value:
            newer.append(field)
    return newer


def _decorate_risk_engine_meta(meta: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(meta or {})
    factor_returns_latest_date = str(out.get("factor_returns_latest_date") or "").strip() or None
    last_recompute_date = str(out.get("last_recompute_date") or "").strip() or None
    if factor_returns_latest_date is not None:
        out.setdefault("core_state_through_date", factor_returns_latest_date)
    if last_recompute_date is not None:
        out.setdefault("core_rebuild_date", last_recompute_date)
    out.setdefault(
        "estimation_exposure_anchor_date",
        derive_estimation_exposure_anchor_date_from_meta(out),
    )
    return out


def get_refresh_status() -> dict[str, Any]:
    return load_persisted_refresh_status(fallback_loader=sqlite.cache_get)


def build_operator_status_payload() -> dict[str, Any]:
    catalog = profile_catalog()
    profiles = [str(item.get("profile") or "") for item in catalog]
    latest_runs = job_runs.latest_run_summary_by_profile(
        db_path=job_runs.default_db_path(),
        profiles=profiles,
    )
    try:
        source_dates = _load_authoritative_operator_source_dates()
    except Exception:
        source_dates = {
            "prices_asof": None,
            "fundamentals_asof": None,
            "classification_asof": None,
            "exposures_latest_available_asof": None,
            "exposures_asof": None,
        }
    try:
        local_archive_source_dates = _load_local_archive_source_dates()
    except Exception:
        local_archive_source_dates = None
    risk_engine_meta_state = runtime_state.read_runtime_state(
        "risk_engine_meta",
        fallback_loader=sqlite.cache_get_live_first,
    )
    risk_engine_meta = _decorate_risk_engine_meta(risk_engine_meta_state.get("value") or {})
    refresh_status = get_refresh_status()
    if isinstance(refresh_status, dict):
        refresh_status = _promote_latest_run_into_refresh_status(
            refresh_status=refresh_status,
            latest_runs=latest_runs,
        )
    neon_sync_health_state = runtime_state.read_runtime_state(
        "neon_sync_health",
        fallback_loader=sqlite.cache_get,
    )
    neon_sync_health = neon_sync_health_state.get("value") or {}
    active_snapshot_state = runtime_state.read_runtime_state(
        "__cache_snapshot_active",
        fallback_loader=sqlite.cache_get,
    )
    active_snapshot = active_snapshot_state.get("value")
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
    runtime_truth_states = {
        "risk_engine_meta": risk_engine_meta_state,
        "neon_sync_health": neon_sync_health_state,
        "active_snapshot": active_snapshot_state,
    }
    for key, state in runtime_truth_states.items():
        status = str((state or {}).get("status") or "")
        source = str((state or {}).get("source") or "")
        if status != "ok":
            runtime_warnings.append(f"Runtime-state truth is not healthy for {key}: status={status or 'unknown'} source={source or 'unknown'}.")
        elif source == "sqlite" and config.runtime_state_primary_reads_enabled():
            runtime_warnings.append(f"Runtime-state truth for {key} fell back to local SQLite instead of Neon.")
    newer_local_archive_fields = _newer_local_archive_fields(source_dates, local_archive_source_dates)
    if newer_local_archive_fields:
        runtime_warnings.append(
            "Local LSEG/archive data is newer than Neon for "
            + ", ".join(sorted(newer_local_archive_fields))
            + "; run a source-syncing lane before trusting Neon-authoritative rebuilds."
        )
    allowed_profiles = sorted(runtime_allowed_profiles())
    local_only_profiles = sorted(set(profiles) - set(allowed_profiles))
    source_authority = core_reads.core_read_backend_name()
    rebuild_authority = "neon" if config.neon_authoritative_rebuilds_enabled() else "local_sqlite"

    lanes = []
    for item in catalog:
        profile = str(item.get("profile") or "")
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
        latest_run = _reconcile_latest_run_with_refresh_status(
            profile=profile,
            latest_run=latest_run,
            refresh_status=refresh_status if isinstance(refresh_status, dict) else {},
        )
        lanes.append(
            {
                **item,
                "latest_run": latest_run,
            }
        )

    return {
        "status": "ok",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "lanes": lanes,
        "source_dates": source_dates,
        "local_archive_source_dates": local_archive_source_dates,
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
            "storage_contract_plain_english": (
                "Local SQLite is the private LSEG landing zone and deep archive. Neon is the trimmed operating database "
                "the app is meant to serve and, when enabled, rebuild from."
            ),
            "source_authority": source_authority,
            "source_authority_plain_english": (
                "Operator source recency tracks the authoritative operating database."
                if source_authority == "neon"
                else "Operator source recency is still reading the local SQLite authority during migration."
            ),
            "local_archive_enabled": bool(config.runtime_role_allows_ingest()),
            "local_archive_plain_english": (
                "The local SQLite archive is the only place LSEG can land directly. It remains available for ingest, deep history, "
                "and intentional Neon retention expansion."
            ),
            "rebuild_authority": rebuild_authority,
            "rebuild_authority_plain_english": (
                "Core rebuild lanes are configured to read from Neon after source sync."
                if rebuild_authority == "neon"
                else "Core rebuild lanes are pinned to local SQLite because Neon-authoritative rebuilds are disabled or Neon is not the active backend."
            ),
            "diagnostics_scope": "local_sqlite_and_cache",
            "diagnostics_scope_plain_english": (
                "Detailed diagnostics are local-instance diagnostics. Operator status, refresh state, holdings dirty state, "
                "and Neon mirror/parity health are the live operator truth surfaces."
            ),
            "runtime_state_status": {
                key: {
                    "status": str((state or {}).get("status") or "unknown"),
                    "source": str((state or {}).get("source") or "unknown"),
                    "error": (state or {}).get("error"),
                }
                for key, state in runtime_truth_states.items()
            },
            "data_backend": str(config.DATA_BACKEND),
            "neon_database_configured": bool(str(config.NEON_DATABASE_URL).strip()),
            "neon_auto_sync_enabled": bool(config.NEON_AUTO_SYNC_ENABLED),
            "neon_auto_sync_enabled_effective": bool(config.neon_auto_sync_enabled_effective()),
            "neon_auto_parity_enabled": bool(config.NEON_AUTO_PARITY_ENABLED),
            "neon_auto_parity_enabled_effective": bool(config.neon_auto_parity_enabled_effective()),
            "neon_auto_prune_enabled": bool(config.NEON_AUTO_PRUNE_ENABLED),
            "neon_auto_prune_enabled_effective": bool(config.neon_auto_prune_enabled_effective()),
            "neon_authoritative_rebuilds": bool(config.neon_authoritative_rebuilds_enabled()),
            "neon_read_surfaces": sorted(config.NEON_READ_SURFACES),
            "serving_outputs_primary_reads": bool(config.SERVING_OUTPUTS_PRIMARY_READS),
            "serving_outputs_primary_reads_effective": bool(config.serving_outputs_primary_reads_enabled()),
            "warnings": runtime_warnings,
        },
    }
