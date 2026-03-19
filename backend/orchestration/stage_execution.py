"""Execution loop for orchestrator-selected stages."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


logger = logging.getLogger(__name__)


def _workspace_path_from_payload(
    workspace_payload: dict[str, Any],
    *,
    field: str,
    expect_directory: bool,
) -> Path:
    raw = str(workspace_payload.get(field) or "").strip()
    if not raw:
        raise RuntimeError(f"neon_readiness returned ok without workspace.{field}")
    path = Path(raw).expanduser().resolve()
    if expect_directory:
        if not path.exists() or not path.is_dir():
            raise RuntimeError(f"neon_readiness returned invalid workspace.{field}: {path}")
    else:
        if not path.exists() or not path.is_file():
            raise RuntimeError(f"neon_readiness returned invalid workspace.{field}: {path}")
    return path


def _resolve_workspace_paths(
    out: dict[str, Any],
    *,
    neon_authority_module,
):
    workspace_payload = dict(out.get("workspace") or {})
    if not workspace_payload:
        raise RuntimeError("neon_readiness returned ok without workspace payload")
    workspace_root = _workspace_path_from_payload(
        workspace_payload,
        field="root_dir",
        expect_directory=True,
    )
    workspace_data_db = _workspace_path_from_payload(
        workspace_payload,
        field="data_db",
        expect_directory=False,
    )
    workspace_cache_db = _workspace_path_from_payload(
        workspace_payload,
        field="cache_db",
        expect_directory=False,
    )
    return neon_authority_module.WorkspacePaths(
        root_dir=workspace_root,
        data_db=workspace_data_db,
        cache_db=workspace_cache_db,
    )


def run_selected_stages(
    *,
    selected: list[str],
    stages: list[str],
    db_path: Path,
    cache_db: Path,
    profile_key: str,
    effective_run_id: str,
    as_of: str,
    should_run_core: bool,
    serving_mode: str,
    force_core: bool,
    core_reason: str,
    raw_history_policy: str,
    reset_core_cache: bool,
    enable_ingest: bool,
    prefer_local_source_archive: bool,
    refresh_scope: str | None,
    rebuild_backend: str,
    app_data_dir: str,
    completed_stages: set[str],
    stage_callback: Callable[[dict[str, Any]], None] | None,
    run_stage_fn: Callable[..., dict[str, Any]],
    job_runs_module,
    neon_authority_module,
) -> dict[str, Any]:
    stage_results: list[dict[str, Any]] = []
    overall_status = "ok"
    workspace_paths = None
    neon_mirror_sqlite_path = db_path
    neon_mirror_cache_path = cache_db
    total_stages = len(selected)

    for idx, stage in enumerate(selected, start=1):
        stage_t0 = time.perf_counter()
        logger.info("Starting stage %s/%s: %s", idx, total_stages, stage)
        stage_order = stages.index(stage) + 1
        if stage in completed_stages:
            elapsed = time.perf_counter() - stage_t0
            logger.info(
                "Skipping stage %s/%s: %s (already completed) in %.1fs",
                idx,
                total_stages,
                stage,
                elapsed,
            )
            stage_results.append(
                {
                    "stage": stage,
                    "status": "skipped",
                    "reason": "already_completed_in_resume_run",
                }
            )
            continue

        stage_started_at = datetime.now(timezone.utc).isoformat()
        stage_base_details = {
            "stage_order": int(stage_order),
            "stage_index": int(idx),
            "stage_count": int(total_stages),
            "started_at": stage_started_at,
            "message": f"Starting {stage.replace('_', ' ')}",
            "progress_kind": "stage",
        }
        job_runs_module.begin_stage(
            db_path=db_path,
            run_id=effective_run_id,
            profile=profile_key,
            stage_name=stage,
            stage_order=stage_order,
            details=stage_base_details,
        )

        def _emit_stage_event(event: dict[str, Any] | None = None) -> None:
            payload = {
                "stage": stage,
                "stage_order": int(stage_order),
                "stage_index": int(idx),
                "stage_count": int(total_stages),
                "started_at": stage_started_at,
            }
            if event:
                payload.update(event)
            details_update = {k: v for k, v in payload.items() if k != "stage"}
            try:
                job_runs_module.heartbeat_stage(
                    db_path=db_path,
                    run_id=effective_run_id,
                    stage_name=stage,
                    details=details_update,
                )
            except Exception:  # noqa: BLE001
                logger.exception("Failed to persist stage heartbeat: %s", stage)
            if stage_callback is not None:
                try:
                    stage_callback(payload)
                except Exception:  # noqa: BLE001
                    logger.exception("Stage callback failed during stage heartbeat: %s", stage)

        _emit_stage_event({"message": f"Starting {stage.replace('_', ' ')}", "progress_kind": "stage"})
        try:
            stage_data_db = db_path
            stage_cache_db = cache_db
            stage_workspace_root: Path | None = None
            neon_compute_stages = {"raw_history", "feature_build", "estu_audit", "factor_returns", "risk_model"}
            if rebuild_backend == "neon" and stage in neon_compute_stages:
                if workspace_paths is None:
                    raise RuntimeError(
                        "Neon-authoritative rebuild requires neon_readiness before core stages. "
                        "Run the default lane or include neon_readiness in the explicit stage window."
                    )
                stage_data_db = workspace_paths.data_db
                stage_cache_db = workspace_paths.cache_db
            elif stage == "serving_refresh" and workspace_paths is not None:
                stage_data_db = workspace_paths.data_db
                stage_cache_db = workspace_paths.cache_db
            elif stage == "neon_readiness":
                stage_workspace_root = Path(app_data_dir) / "neon_rebuild_workspace" / effective_run_id

            out = run_stage_fn(
                profile=profile_key,
                stage=stage,
                as_of_date=as_of,
                should_run_core=bool(should_run_core),
                serving_mode=serving_mode,
                data_db=stage_data_db,
                cache_db=stage_cache_db,
                refresh_scope=refresh_scope,
                force_core=bool(force_core),
                core_reason=str(core_reason),
                raw_history_policy=raw_history_policy,
                reset_core_cache=reset_core_cache,
                enable_ingest=bool(enable_ingest),
                prefer_local_source_archive=prefer_local_source_archive,
                workspace_root=stage_workspace_root,
                progress_callback=_emit_stage_event,
            )
            if stage == "neon_readiness" and str(out.get("status") or "") == "ok":
                workspace_paths = _resolve_workspace_paths(
                    out,
                    neon_authority_module=neon_authority_module,
                )
                neon_mirror_sqlite_path = workspace_paths.data_db
                neon_mirror_cache_path = workspace_paths.cache_db
            stage_status = "skipped" if str(out.get("status")) == "skipped" else "completed"
            elapsed = time.perf_counter() - stage_t0
            stage_details = dict(out)
            stage_details["duration_seconds"] = round(float(elapsed), 3)
            stage_details["stage_order"] = int(stage_order)
            stage_details["stage_index"] = int(idx)
            stage_details["stage_count"] = int(total_stages)
            stage_details["started_at"] = stage_started_at
            job_runs_module.finish_stage(
                db_path=db_path,
                run_id=effective_run_id,
                stage_name=stage,
                status=stage_status,
                details=stage_details,
            )
            logger.info(
                "Finished stage %s/%s: %s (%s) in %.1fs",
                idx,
                total_stages,
                stage,
                stage_status,
                elapsed,
            )
            stage_results.append(
                {
                    "stage": stage,
                    "status": stage_status,
                    "details": stage_details,
                }
            )
        except Exception as exc:  # noqa: BLE001
            overall_status = "failed"
            err = {"type": type(exc).__name__, "message": str(exc)}
            elapsed = time.perf_counter() - stage_t0
            failed_details = {
                "duration_seconds": round(float(elapsed), 3),
                "stage_order": int(stage_order),
                "stage_index": int(idx),
                "stage_count": int(total_stages),
                "started_at": stage_started_at,
            }
            logger.exception(
                "Stage failed %s/%s: %s after %.1fs",
                idx,
                total_stages,
                stage,
                elapsed,
            )
            job_runs_module.finish_stage(
                db_path=db_path,
                run_id=effective_run_id,
                stage_name=stage,
                status="failed",
                details=failed_details,
                error=err,
            )
            stage_results.append(
                {
                    "stage": stage,
                    "status": "failed",
                    "details": failed_details,
                    "error": err,
                }
            )
            break

    return {
        "overall_status": overall_status,
        "stage_results": stage_results,
        "workspace_paths": workspace_paths,
        "neon_mirror_sqlite_path": neon_mirror_sqlite_path,
        "neon_mirror_cache_path": neon_mirror_cache_path,
    }
