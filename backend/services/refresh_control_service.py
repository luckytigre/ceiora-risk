"""Application-facing refresh control surface for routes and control clients."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from backend import config
from backend.ops import cloud_run_jobs
from backend.services.holdings_runtime_state import mark_refresh_finished
from backend.services.holdings_runtime_state import mark_refresh_started
from backend.services.refresh_request_policy import resolve_refresh_request
from backend.services.refresh_status_service import load_persisted_refresh_status
from backend.services.refresh_status_service import persist_refresh_status
from backend.services.refresh_status_service import try_claim_refresh_status

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _persist_state(**updates: Any) -> dict[str, Any]:
    state = load_persisted_refresh_status()
    state.update(updates)
    persist_refresh_status(state)
    return state


def _reconcile_cloud_run_refresh_state(current: dict[str, Any] | None = None) -> dict[str, Any]:
    state = dict(current or load_persisted_refresh_status())
    if str(state.get("status") or "").strip() != "running":
        return state
    if str(state.get("dispatch_backend") or "").strip() != "cloud_run_job":
        return state
    execution_name = str(state.get("dispatch_id") or state.get("job_id") or "").strip()
    if not execution_name:
        return state
    try:
        execution = cloud_run_jobs.describe_execution(execution_name)
        terminal = cloud_run_jobs.execution_terminal_summary(execution)
    except FileNotFoundError:
        return state
    except Exception:  # noqa: BLE001
        logger.exception("Failed to inspect Cloud Run execution %s for refresh reconciliation", execution_name)
        return state
    if not bool(terminal.get("terminal")):
        return state

    latest = load_persisted_refresh_status()
    if str(latest.get("status") or "").strip() != "running":
        return latest
    latest_execution = str(latest.get("dispatch_id") or latest.get("job_id") or "").strip()
    if latest_execution != execution_name:
        return latest

    status = "ok" if str(terminal.get("status") or "").strip() == "ok" else "failed"
    message = str(terminal.get("message") or "").strip() or None
    try:
        mark_refresh_finished(
            profile=str(latest.get("profile") or "").strip() or None,
            run_id=str(latest.get("pipeline_run_id") or "").strip() or None,
            status=status,
            message=message,
            clear_pending=(status == "ok"),
        )
    except Exception:
        logger.exception("Failed to reconcile holdings refresh terminal state from Cloud Run execution")

    latest.update(
        status=status,
        finished_at=str(terminal.get("finished_at") or _now_iso()).strip(),
        current_stage=None,
        current_stage_substage=None,
        current_stage_substage_status=None,
        current_stage_diagnostics_section=None,
        stage_started_at=None,
        current_stage_message=None,
        current_stage_progress_pct=None,
        current_stage_items_processed=None,
        current_stage_items_total=None,
        current_stage_unit=None,
        current_stage_heartbeat_at=None,
        result=(
            {
                "status": "ok",
                "dispatch_backend": "cloud_run_job",
                "execution_name": execution_name,
                "reconciled": True,
            }
            if status == "ok"
            else None
        ),
        error=(
            None
            if status == "ok"
            else {
                "type": "cloud_run_job_failed",
                "message": message or "Cloud Run Job execution terminated without updating refresh status.",
            }
        ),
        dispatch_backend="cloud_run_job",
        dispatch_id=execution_name,
    )
    persist_refresh_status(latest)
    return latest


def _load_reconciled_refresh_status() -> dict[str, Any]:
    return _reconcile_cloud_run_refresh_state(load_persisted_refresh_status())


def _cloud_dispatch_unconfigured_state() -> dict[str, Any]:
    state = dict(load_persisted_refresh_status())
    state.setdefault("status", "unavailable")
    state["dispatch_backend"] = "cloud_run_job"
    state["error"] = {
        "type": "cloud_run_job_unconfigured",
        "message": (
            "Cloud serve-refresh dispatch is unavailable because the Cloud Run Job "
            "environment contract is incomplete."
        ),
    }
    return state


def start_refresh(
    *,
    force_risk_recompute: bool,
    profile: str | None = None,
    refresh_scope: str | None = None,
    as_of_date: str | None = None,
    resume_run_id: str | None = None,
    from_stage: str | None = None,
    to_stage: str | None = None,
    force_core: bool = False,
) -> tuple[bool, dict[str, Any]]:
    if config.cloud_mode() and not config.serve_refresh_cloud_job_configured():
        return False, _cloud_dispatch_unconfigured_state()

    if not config.serve_refresh_cloud_job_configured():
        from backend.services.refresh_manager import start_refresh as _start_refresh

        return _start_refresh(
            force_risk_recompute=force_risk_recompute,
            profile=profile,
            refresh_scope=refresh_scope,
            as_of_date=as_of_date,
            resume_run_id=resume_run_id,
            from_stage=from_stage,
            to_stage=to_stage,
            force_core=force_core,
        )

    request = resolve_refresh_request(
        profile=profile,
        from_stage=from_stage,
        to_stage=to_stage,
        force_core=force_core,
        force_risk_recompute=force_risk_recompute,
    )
    current = _load_reconciled_refresh_status()
    if str(current.get("status") or "").strip() == "running":
        return False, current
    job_id = uuid.uuid4().hex[:12]
    pipeline_run_id = (str(resume_run_id).strip() if resume_run_id else f"crj_{job_id}")
    now = _now_iso()
    claimed, running_state = try_claim_refresh_status(
        {
            "status": "running",
            "job_id": job_id,
            "pipeline_run_id": pipeline_run_id,
            "profile": request["profile"],
            "requested_profile": (str(profile).strip().lower() if profile else None),
            "mode": request["mode"],
            "refresh_scope": (str(refresh_scope).strip().lower() if refresh_scope else None),
            "as_of_date": (str(as_of_date).strip() if as_of_date else None),
            "resume_run_id": (str(resume_run_id).strip() if resume_run_id else None),
            "from_stage": request["from_stage"],
            "to_stage": request["to_stage"],
            "force_core": bool(request["force_core"]),
            "force_risk_recompute": bool(force_risk_recompute),
            "current_stage": "dispatch",
            "current_stage_message": "Dispatching serve-refresh to Cloud Run Job.",
            "current_stage_heartbeat_at": now,
            "requested_at": now,
            "started_at": now,
            "finished_at": None,
            "result": None,
            "error": None,
            "dispatch_backend": "cloud_run_job",
            "dispatch_id": None,
        }
    )
    if not claimed:
        return False, running_state

    try:
        mark_refresh_started(profile=request["profile"], run_id=pipeline_run_id)
    except Exception:
        pass
    try:
        dispatch = cloud_run_jobs.dispatch_serve_refresh(
            pipeline_run_id=pipeline_run_id,
            profile=request["profile"],
            as_of_date=(str(as_of_date).strip() if as_of_date else None),
            from_stage=request["from_stage"],
            to_stage=request["to_stage"],
            force_core=bool(request["force_core"]),
            refresh_scope=(str(refresh_scope).strip().lower() if refresh_scope else None),
        )
    except Exception as exc:  # noqa: BLE001
        try:
            mark_refresh_finished(
                profile=request["profile"],
                run_id=pipeline_run_id,
                status="failed",
                message=str(exc),
                clear_pending=False,
            )
        except Exception:
            pass
        failed_state = _persist_state(
            status="failed",
            finished_at=_now_iso(),
            current_stage=None,
            current_stage_message=None,
            error={"type": type(exc).__name__, "message": str(exc)},
            dispatch_backend="cloud_run_job",
            dispatch_id=None,
        )
        return False, failed_state

    dispatched_state = _persist_state(
        dispatch_backend="cloud_run_job",
        dispatch_id=str(dispatch.get("execution_name") or "").strip() or None,
        current_stage="dispatch",
        current_stage_message="Refresh dispatched to Cloud Run Job.",
        current_stage_heartbeat_at=_now_iso(),
    )
    return True, dispatched_state


def get_refresh_status() -> dict[str, Any]:
    if config.cloud_mode() and not config.serve_refresh_cloud_job_configured():
        return _cloud_dispatch_unconfigured_state()

    if config.serve_refresh_cloud_job_configured():
        return _load_reconciled_refresh_status()

    from backend.services.refresh_manager import get_refresh_status as _get_refresh_status

    return _get_refresh_status()
