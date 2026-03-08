"""Single-process refresh manager with background execution and status tracking."""

from __future__ import annotations

import logging
import threading
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any

from backend.data.cache import cache_get, cache_set
from backend.orchestration.run_model_pipeline import (
    PROFILE_CONFIG,
    STAGES,
    resolve_profile_name,
    run_model_pipeline,
)
from backend.services.holdings_runtime_state import mark_refresh_started
from backend.services.holdings_runtime_state import mark_refresh_finished

logger = logging.getLogger(__name__)

_STATUS_CACHE_KEY = "refresh_status"
_RUN_LOCK = threading.Lock()
_STATE_LOCK = threading.Lock()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _default_state() -> dict[str, Any]:
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
        "force_core": False,
        "force_risk_recompute": False,
        "requested_at": None,
        "started_at": None,
        "finished_at": None,
        "result": None,
        "error": None,
    }


def _load_initial_state() -> dict[str, Any]:
    cached = cache_get(_STATUS_CACHE_KEY)
    if isinstance(cached, dict):
        base = _default_state()
        base.update(cached)
        if base.get("status") == "running":
            # After process restart, a previously running task cannot be resumed.
            base["status"] = "unknown"
            base["error"] = {
                "type": "process_restart",
                "message": "Refresh state was running before process restart.",
            }
            base["finished_at"] = _now_iso()
        return base
    return _default_state()


_STATE = _load_initial_state()


def _snapshot() -> dict[str, Any]:
    with _STATE_LOCK:
        return dict(_STATE)


def _set_state(**updates: Any) -> dict[str, Any]:
    with _STATE_LOCK:
        _STATE.update(updates)
        snap = dict(_STATE)
    cache_set(_STATUS_CACHE_KEY, snap)
    return snap


def get_refresh_status() -> dict[str, Any]:
    """Return current or most recent refresh status."""
    return _snapshot()


def _resolve_profile(profile: str | None, mode: str | None) -> str:
    prof = str(profile or "").strip().lower()
    if prof:
        prof = resolve_profile_name(prof)
        if prof not in PROFILE_CONFIG:
            raise ValueError(
                f"Invalid profile '{profile}'. Valid profiles: {', '.join(sorted(PROFILE_CONFIG.keys()))}"
            )
        return prof
    clean_mode = str(mode or "full").strip().lower()
    if clean_mode == "light":
        return "serve-refresh"
    if clean_mode == "cold":
        return "cold-core"
    if clean_mode == "full":
        return "source-daily-plus-core-if-due"
    raise ValueError("Invalid mode. Expected 'full', 'light', or 'cold' when profile is omitted.")


def _normalize_stage(name: str | None) -> str | None:
    if name is None:
        return None
    clean = str(name).strip().lower()
    if not clean:
        return None
    if clean not in STAGES:
        raise ValueError(f"Invalid stage '{name}'. Valid stages: {', '.join(STAGES)}")
    return clean


def _run_in_background(
    *,
    job_id: str,
    profile: str,
    mode: str,
    as_of_date: str | None,
    resume_run_id: str | None,
    from_stage: str | None,
    to_stage: str | None,
    force_core: bool,
) -> None:
    try:
        result = run_model_pipeline(
            profile=profile,
            as_of_date=as_of_date,
            run_id=(None if resume_run_id else f"api_{job_id}"),
            resume_run_id=resume_run_id,
            from_stage=from_stage,
            to_stage=to_stage,
            force_core=bool(force_core),
        )
        terminal = "ok" if str(result.get("status") or "") == "ok" else "failed"
        _set_state(
            status=terminal,
            pipeline_run_id=result.get("run_id"),
            finished_at=_now_iso(),
            result=result,
            error=None if terminal == "ok" else {
                "type": "pipeline_failed",
                "message": "Orchestrated pipeline returned failed status.",
            },
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Background refresh failed")
        try:
            mark_refresh_finished(
                profile=profile,
                run_id=(str(resume_run_id).strip() if resume_run_id else f"api_{job_id}"),
                status="failed",
                message=str(exc),
                clear_pending=False,
            )
        except Exception:
            logger.exception("Failed to mark holdings refresh failure state")
        _set_state(
            status="failed",
            finished_at=_now_iso(),
            error={
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(limit=12),
            },
        )
    finally:
        _RUN_LOCK.release()
        logger.info("Background refresh %s finished", job_id)


def start_refresh(
    *,
    mode: str,
    force_risk_recompute: bool,
    profile: str | None = None,
    as_of_date: str | None = None,
    resume_run_id: str | None = None,
    from_stage: str | None = None,
    to_stage: str | None = None,
    force_core: bool = False,
) -> tuple[bool, dict[str, Any]]:
    """Start refresh in a background thread. Returns (started, status)."""
    resolved_profile = _resolve_profile(profile, mode)
    stage_from = _normalize_stage(from_stage)
    stage_to = _normalize_stage(to_stage)
    force_core_effective = bool(force_core or force_risk_recompute)

    if not _RUN_LOCK.acquire(blocking=False):
        return False, _snapshot()

    job_id = uuid.uuid4().hex[:12]
    now = _now_iso()
    running_state = _set_state(
        status="running",
        job_id=job_id,
        pipeline_run_id=(str(resume_run_id).strip() if resume_run_id else None),
        profile=resolved_profile,
        requested_profile=(str(profile).strip().lower() if profile else None),
        mode=mode,
        as_of_date=(str(as_of_date).strip() if as_of_date else None),
        resume_run_id=(str(resume_run_id).strip() if resume_run_id else None),
        from_stage=stage_from,
        to_stage=stage_to,
        force_core=bool(force_core_effective),
        force_risk_recompute=bool(force_risk_recompute),
        requested_at=now,
        started_at=now,
        finished_at=None,
        result=None,
        error=None,
    )
    try:
        mark_refresh_started(profile=resolved_profile, run_id=job_id)
    except Exception:
        logger.exception("Failed to mark holdings refresh start state")

    worker = threading.Thread(
        target=_run_in_background,
        kwargs={
            "job_id": job_id,
            "profile": resolved_profile,
            "mode": str(mode),
            "as_of_date": (str(as_of_date).strip() if as_of_date else None),
            "resume_run_id": (str(resume_run_id).strip() if resume_run_id else None),
            "from_stage": stage_from,
            "to_stage": stage_to,
            "force_core": bool(force_core_effective),
        },
        name=f"refresh-{job_id}",
        daemon=True,
    )
    try:
        worker.start()
    except Exception as exc:  # noqa: BLE001
        _RUN_LOCK.release()
        try:
            mark_refresh_finished(
                profile=resolved_profile,
                run_id=job_id,
                status="failed",
                message=str(exc),
                clear_pending=False,
            )
        except Exception:
            logger.exception("Failed to mark holdings refresh worker-start failure state")
        failed_state = _set_state(
            status="failed",
            finished_at=_now_iso(),
            error={"type": type(exc).__name__, "message": str(exc)},
        )
        return False, failed_state
    return True, running_state
