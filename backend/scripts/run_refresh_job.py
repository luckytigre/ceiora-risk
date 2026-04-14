"""Cloud Run Job entrypoint for serve-refresh execution."""

from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend import config
from backend.orchestration.refresh_execution import run_refresh_execution
from backend.services.holdings_runtime_state import mark_refresh_started
from backend.services.refresh_request_policy import resolve_refresh_request
from backend.services.refresh_status_service import default_refresh_status_state
from backend.services.refresh_status_service import load_persisted_refresh_status
from backend.services.refresh_status_service import persist_refresh_status


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _env_bool(name: str) -> bool:
    return str(os.getenv(name, "")).strip().lower() in {"1", "true", "yes", "y", "on"}


def _snapshot_state() -> dict[str, Any]:
    return load_persisted_refresh_status()


def _set_state(**updates: Any) -> dict[str, Any]:
    state = default_refresh_status_state()
    state.update(load_persisted_refresh_status())
    state.update(updates)
    persist_refresh_status(state)
    return state


def _required_runtime_asset_paths() -> list[Path]:
    base = Path(__file__).resolve().parents[2] / "docs" / "reference" / "migrations" / "neon"
    return [
        base / "NEON_CANONICAL_SCHEMA.sql",
        base / "NEON_CPAR_SCHEMA.sql",
        base / "NEON_HOLDINGS_SCHEMA.sql",
        base / "NEON_REGISTRY_FIRST_CLEANUP.sql",
    ]


def _validate_runtime_assets() -> None:
    missing = [str(p) for p in _required_runtime_asset_paths() if not p.is_file()]
    if missing:
        raise FileNotFoundError(
            "Missing required runtime schema assets in container: " + ", ".join(sorted(missing))
        )


def _validate_cold_core_request(request: dict[str, Any]) -> None:
    profile = str(request.get("profile") or "").strip().lower()
    if profile != "cold-core":
        return
    from_stage = str(request.get("from_stage") or "").strip()
    to_stage = str(request.get("to_stage") or "").strip()
    if from_stage or to_stage:
        raise RuntimeError(
            "cold-core does not support partial stage windows in Cloud Run Jobs; use core-weekly or another profile."
        )


def main() -> int:
    profile_env = str(os.getenv("REFRESH_PROFILE", "")).strip()
    if config.cloud_job_mode() and not profile_env:
        raise RuntimeError("cloud-job refresh execution requires REFRESH_PROFILE to be set explicitly.")

    request = resolve_refresh_request(
        profile=profile_env or None,
        from_stage=os.getenv("REFRESH_FROM_STAGE"),
        to_stage=os.getenv("REFRESH_TO_STAGE"),
        force_core=_env_bool("REFRESH_FORCE_CORE"),
        force_risk_recompute=False,
    )
    _validate_cold_core_request(request)
    pipeline_run_id = str(os.getenv("REFRESH_PIPELINE_RUN_ID", "")).strip() or f"job_{uuid.uuid4().hex[:12]}"
    now = _now_iso()
    _set_state(
        status="running",
        job_id=str(os.getenv("CLOUD_RUN_EXECUTION", "")).strip() or pipeline_run_id,
        pipeline_run_id=pipeline_run_id,
        profile=request["profile"],
        requested_profile=request["profile"],
        mode=request["mode"],
        refresh_scope=(str(os.getenv("REFRESH_SCOPE", "")).strip().lower() or None),
        as_of_date=(str(os.getenv("REFRESH_AS_OF_DATE", "")).strip() or None),
        resume_run_id=None,
        from_stage=request["from_stage"],
        to_stage=request["to_stage"],
        force_core=bool(request["force_core"]),
        force_risk_recompute=False,
        current_stage="dispatch",
        current_stage_message="Cloud Run Job execution started.",
        current_stage_heartbeat_at=now,
        requested_at=now,
        started_at=now,
        finished_at=None,
        result=None,
        error=None,
        dispatch_backend="cloud_run_job",
        dispatch_id=str(os.getenv("CLOUD_RUN_EXECUTION", "")).strip() or None,
    )
    if config.cloud_job_mode():
        _validate_runtime_assets()
    try:
        mark_refresh_started(profile=request["profile"], run_id=pipeline_run_id)
    except Exception:
        pass
    outcome = run_refresh_execution(
        job_id=str(os.getenv("CLOUD_RUN_EXECUTION", "")).strip() or pipeline_run_id,
        pipeline_run_id=pipeline_run_id,
        profile=request["profile"],
        mode=request["mode"],
        as_of_date=(str(os.getenv("REFRESH_AS_OF_DATE", "")).strip() or None),
        resume_run_id=None,
        from_stage=request["from_stage"],
        to_stage=request["to_stage"],
        force_core=bool(request["force_core"]),
        refresh_scope=(str(os.getenv("REFRESH_SCOPE", "")).strip().lower() or None),
        snapshot_state=_snapshot_state,
        set_state=_set_state,
    )
    return 0 if outcome.get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
