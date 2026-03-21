"""Read-only refresh-status persistence helpers."""

from __future__ import annotations

from typing import Any

from backend.data import runtime_state

_STATUS_CACHE_KEY = "refresh_status"


def default_refresh_status_state() -> dict[str, Any]:
    return {
        "status": "idle",
        "job_id": None,
        "pipeline_run_id": None,
        "profile": None,
        "requested_profile": None,
        "mode": None,
        "as_of_date": None,
        "resume_run_id": None,
        "from_stage": None,
        "to_stage": None,
        "refresh_scope": None,
        "force_core": False,
        "force_risk_recompute": False,
        "current_stage": None,
        "current_stage_substage": None,
        "current_stage_substage_status": None,
        "current_stage_diagnostics_section": None,
        "stage_index": None,
        "stage_count": None,
        "stage_started_at": None,
        "current_stage_message": None,
        "current_stage_progress_pct": None,
        "current_stage_items_processed": None,
        "current_stage_items_total": None,
        "current_stage_unit": None,
        "current_stage_heartbeat_at": None,
        "serving_publish_completed_at": None,
        "serving_publish_snapshot_id": None,
        "serving_publish_run_id": None,
        "serving_publish_payload_count": None,
        "requested_at": None,
        "started_at": None,
        "finished_at": None,
        "result": None,
        "error": None,
    }


def load_persisted_refresh_status(*, fallback_loader=None) -> dict[str, Any]:
    cached = runtime_state.load_runtime_state(
        _STATUS_CACHE_KEY,
        fallback_loader=fallback_loader,
    )
    state = default_refresh_status_state()
    if isinstance(cached, dict):
        state.update(cached)
    return state


def read_persisted_refresh_status(*, fallback_loader=None) -> dict[str, Any]:
    raw = runtime_state.read_runtime_state(
        _STATUS_CACHE_KEY,
        fallback_loader=fallback_loader,
    )
    state = default_refresh_status_state()
    if isinstance(raw.get("value"), dict):
        state.update(raw["value"])
    return {
        **raw,
        "value": state,
    }


def persist_refresh_status(state: dict[str, Any], *, fallback_writer=None) -> dict[str, Any]:
    return runtime_state.persist_runtime_state(
        _STATUS_CACHE_KEY,
        state,
        fallback_writer=fallback_writer,
    )
