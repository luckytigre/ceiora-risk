#!/usr/bin/env python3
"""Dedicated cPAR package-build orchestration."""

from __future__ import annotations

import argparse
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from backend import config
from backend.orchestration import cpar_profiles, cpar_stages

DATA_DB = Path(config.DATA_DB_PATH)
logger = logging.getLogger(__name__)

StageCallback = Callable[[dict[str, Any]], None]


def _default_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"cpar_{timestamp}_{uuid.uuid4().hex[:8]}"


def _resolve_data_db(data_db: Path | None = None) -> Path:
    return Path(data_db or DATA_DB).expanduser().resolve()


def _stage_payload(
    *,
    stage: str,
    stage_order: int,
    stage_index: int,
    stage_count: int,
    started_at: str,
    event: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "stage": stage,
        "stage_order": int(stage_order),
        "stage_index": int(stage_index),
        "stage_count": int(stage_count),
        "started_at": started_at,
    }
    if event:
        payload.update(event)
    return payload


def _run_row(
    *,
    run_id: str,
    profile: str,
    stage_name: str,
    stage_order: int,
    status: str,
    started_at: str,
    completed_at: str | None,
    details: dict[str, Any],
    error: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "run_id": str(run_id),
        "profile": str(profile),
        "stage_name": str(stage_name),
        "stage_order": int(stage_order),
        "status": str(status),
        "started_at": str(started_at),
        "completed_at": completed_at,
        "details": dict(details),
        "error_type": (error or {}).get("type"),
        "error_message": (error or {}).get("message"),
        "updated_at": str(completed_at or started_at),
    }


def run_cpar_pipeline(
    *,
    profile: str,
    as_of_date: str | None = None,
    run_id: str | None = None,
    from_stage: str | None = None,
    to_stage: str | None = None,
    data_db: Path | None = None,
    stage_callback: StageCallback | None = None,
) -> dict[str, Any]:
    db_path = _resolve_data_db(data_db)
    profile_key, cfg, selected_stages = cpar_profiles.planned_stages_for_profile(
        profile=profile,
        from_stage=from_stage,
        to_stage=to_stage,
    )
    effective_run_id = str(run_id).strip() if run_id and str(run_id).strip() else _default_run_id()
    requested_as_of_date = str(as_of_date).strip() if as_of_date and str(as_of_date).strip() else None

    if config.cloud_mode():
        return {
            "status": "failed",
            "run_id": effective_run_id,
            "profile": profile_key,
            "profile_label": str(cfg.get("label") or profile_key),
            "requested_as_of_date": requested_as_of_date,
            "package_date": None,
            "selected_stages": selected_stages,
            "reason": "runtime_role_disallows_cpar_build",
            "stage_results": [],
            "run_rows": [],
        }

    package_date = cpar_profiles.resolve_package_date(profile=profile_key, as_of_date=requested_as_of_date)

    pipeline_started_at = datetime.now(timezone.utc).isoformat()
    context: dict[str, Any] = {
        "run_started_at": pipeline_started_at,
        "package_date": package_date,
        "profile": profile_key,
        "run_id": effective_run_id,
    }
    stage_results: list[dict[str, Any]] = []
    run_rows: list[dict[str, Any]] = []
    overall_status = "ok"
    total_stages = len(selected_stages)

    for stage_index, stage_name in enumerate(selected_stages, start=1):
        stage_order = cpar_profiles.STAGES.index(stage_name) + 1
        stage_started_at = datetime.now(timezone.utc).isoformat()

        def emit_stage_event(event: dict[str, Any] | None = None) -> None:
            payload = _stage_payload(
                stage=stage_name,
                stage_order=stage_order,
                stage_index=stage_index,
                stage_count=total_stages,
                started_at=stage_started_at,
                event=event,
            )
            if stage_callback is not None:
                stage_callback(payload)

        emit_stage_event({"message": f"Starting {stage_name.replace('_', ' ')}", "progress_kind": "stage"})
        stage_started = time.perf_counter()
        try:
            out = cpar_stages.run_stage(
                stage=stage_name,
                profile=profile_key,
                package_date=package_date,
                run_id=effective_run_id,
                data_db=db_path,
                context=context,
                progress_callback=emit_stage_event,
            )
            duration_seconds = round(float(time.perf_counter() - stage_started), 3)
            details = dict(out)
            details.update(
                {
                    "duration_seconds": duration_seconds,
                    "stage_order": int(stage_order),
                    "stage_index": int(stage_index),
                    "stage_count": int(total_stages),
                    "package_date": package_date,
                    "requested_as_of_date": requested_as_of_date,
                }
            )
            final_status = "skipped" if str(out.get("status") or "") == "skipped" else "completed"
            completed_at = datetime.now(timezone.utc).isoformat()
            run_rows.append(
                _run_row(
                    run_id=effective_run_id,
                    profile=profile_key,
                    stage_name=stage_name,
                    stage_order=stage_order,
                    status=final_status,
                    started_at=stage_started_at,
                    completed_at=completed_at,
                    details=details,
                )
            )
            stage_results.append({"stage": stage_name, "status": final_status, "details": details})
        except Exception as exc:  # noqa: BLE001
            overall_status = "failed"
            error = {"type": type(exc).__name__, "message": str(exc)}
            details = {
                "duration_seconds": round(float(time.perf_counter() - stage_started), 3),
                "stage_order": int(stage_order),
                "stage_index": int(stage_index),
                "stage_count": int(total_stages),
                "package_date": package_date,
                "requested_as_of_date": requested_as_of_date,
            }
            completed_at = datetime.now(timezone.utc).isoformat()
            run_rows.append(
                _run_row(
                    run_id=effective_run_id,
                    profile=profile_key,
                    stage_name=stage_name,
                    stage_order=stage_order,
                    status="failed",
                    started_at=stage_started_at,
                    completed_at=completed_at,
                    details=details,
                    error=error,
                )
            )
            stage_results.append({"stage": stage_name, "status": "failed", "details": details, "error": error})
            break

    return {
        "status": overall_status,
        "run_id": effective_run_id,
        "profile": profile_key,
        "profile_label": str(cfg.get("label") or profile_key),
        "requested_as_of_date": requested_as_of_date,
        "package_date": package_date,
        "selected_stages": selected_stages,
        "stage_results": stage_results,
        "run_rows": run_rows,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        required=True,
        choices=sorted(cpar_profiles.PROFILE_CONFIG.keys()),
        help="cPAR execution profile.",
    )
    parser.add_argument(
        "--as-of-date",
        default=None,
        help="Optional ISO date. For cpar-package-date this must be the explicit package date.",
    )
    parser.add_argument("--run-id", default=None, help="Optional explicit run id.")
    parser.add_argument("--from-stage", default=None, choices=cpar_profiles.STAGES, help="Start stage.")
    parser.add_argument("--to-stage", default=None, choices=cpar_profiles.STAGES, help="End stage.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Console log verbosity.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    result = run_cpar_pipeline(
        profile=args.profile,
        as_of_date=args.as_of_date,
        run_id=args.run_id,
        from_stage=args.from_stage,
        to_stage=args.to_stage,
    )
    print(result)
    return 0 if str(result.get("status") or "") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
