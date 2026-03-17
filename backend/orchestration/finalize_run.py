"""Post-stage finalization for orchestrator runs."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable


logger = logging.getLogger(__name__)


def finalize_pipeline_run(
    *,
    overall_status: str,
    stage_results: list[dict[str, Any]],
    profile_key: str,
    as_of: str,
    effective_run_id: str,
    workspace_paths,
    data_db: Path,
    cache_db: Path,
    neon_mirror_sqlite_path: Path,
    neon_mirror_cache_path: Path,
    neon_sync_enabled: bool,
    neon_parity_enabled: bool,
    neon_prune_enabled: bool,
    broad_neon_mirror_enabled: bool,
    neon_mirror_required: bool,
    serving_payload_neon_failure: dict[str, str] | None,
    run_neon_mirror_cycle_fn: Callable[..., dict[str, Any]],
    sync_workspace_derivatives_to_local_mirror_fn: Callable[..., dict[str, Any]],
    write_neon_mirror_artifact_fn: Callable[..., str],
    publish_neon_sync_health_fn: Callable[..., None],
    publish_neon_serving_write_health_fn: Callable[..., None],
    mark_refresh_finished_fn: Callable[..., Any],
    serving_payload_neon_write_required_fn: Callable[[], bool],
    config_module,
) -> dict[str, Any]:
    local_mirror_sync: dict[str, Any] = {"status": "skipped", "reason": "no_workspace"}
    neon_mirror: dict[str, Any] = {
        "status": "skipped",
        "reason": "NEON_AUTO_SYNC_ENABLED=false",
    }

    if overall_status == "ok" and neon_sync_enabled and broad_neon_mirror_enabled:
        try:
            logger.info(
                "Running Neon mirror cycle: mode=%s parity=%s prune=%s source_years=%s analytics_years=%s",
                config_module.NEON_AUTO_SYNC_MODE,
                neon_parity_enabled,
                neon_prune_enabled,
                int(config_module.NEON_SOURCE_RETENTION_YEARS),
                int(config_module.NEON_ANALYTICS_RETENTION_YEARS),
            )
            neon_mirror = run_neon_mirror_cycle_fn(
                sqlite_path=neon_mirror_sqlite_path,
                cache_path=neon_mirror_cache_path,
                dsn=(str(config_module.NEON_DATABASE_URL).strip() or None),
                mode=str(config_module.NEON_AUTO_SYNC_MODE or "incremental"),
                tables=(list(config_module.NEON_AUTO_SYNC_TABLES) or None),
                parity_enabled=neon_parity_enabled,
                prune_enabled=neon_prune_enabled,
                source_years=int(config_module.NEON_SOURCE_RETENTION_YEARS),
                analytics_years=int(config_module.NEON_ANALYTICS_RETENTION_YEARS),
            )
            mirror_status = str(neon_mirror.get("status") or "")
            if mirror_status not in {"ok"}:
                logger.warning("Neon mirror cycle reported non-ok status: %s", mirror_status)
                if neon_mirror_required:
                    overall_status = "failed"
        except Exception as exc:  # noqa: BLE001
            logger.exception("Neon mirror cycle failed")
            neon_mirror = {
                "status": "failed",
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
            if neon_mirror_required:
                overall_status = "failed"
    elif overall_status != "ok":
        neon_mirror = {"status": "skipped", "reason": "pipeline_failed"}
    elif neon_sync_enabled and not broad_neon_mirror_enabled:
        neon_mirror = {
            "status": "skipped",
            "reason": "profile_skips_broad_neon_mirror",
            "profile": profile_key,
        }
    elif broad_neon_mirror_enabled and not neon_sync_enabled:
        neon_mirror = {
            "status": ("failed" if neon_mirror_required else "skipped"),
            "reason": "broad_neon_mirror_not_enabled",
            "runtime_role": str(config_module.APP_RUNTIME_ROLE),
            "auto_sync_enabled": bool(config_module.NEON_AUTO_SYNC_ENABLED),
            "profile": profile_key,
        }
        if neon_mirror_required:
            overall_status = "failed"

    if overall_status == "ok" and workspace_paths is not None:
        try:
            local_mirror_sync = sync_workspace_derivatives_to_local_mirror_fn(
                workspace_data_db=workspace_paths.data_db,
                workspace_cache_db=workspace_paths.cache_db,
                local_data_db=data_db,
                local_cache_db=cache_db,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to sync Neon-authoritative workspace back to local mirror")
            local_mirror_sync = {
                "status": "error",
                "error": {"type": type(exc).__name__, "message": str(exc)},
            }
            overall_status = "failed"

    neon_artifact_path: str | None = None
    should_publish_neon_mirror_status = bool(
        str(neon_mirror.get("status") or "").strip().lower() not in {"skipped"}
        and (bool(config_module.NEON_AUTO_SYNC_ENABLED) or neon_mirror_required)
    )
    if should_publish_neon_mirror_status:
        try:
            neon_artifact_path = write_neon_mirror_artifact_fn(
                run_id=effective_run_id,
                profile=profile_key,
                as_of_date=as_of,
                overall_status=overall_status,
                neon_mirror=neon_mirror,
            )
            neon_mirror["artifact_path"] = neon_artifact_path
        except Exception:  # noqa: BLE001
            logger.exception("Failed to persist Neon mirror artifact")

        try:
            publish_neon_sync_health_fn(
                run_id=effective_run_id,
                profile=profile_key,
                as_of_date=as_of,
                neon_mirror=neon_mirror,
                artifact_path=neon_artifact_path,
            )
        except Exception:  # noqa: BLE001
            logger.exception("Failed to publish Neon sync health status")
    elif serving_payload_neon_failure is not None and serving_payload_neon_write_required_fn():
        try:
            publish_neon_serving_write_health_fn(
                run_id=effective_run_id,
                profile=profile_key,
                as_of_date=as_of,
                error=serving_payload_neon_failure,
            )
        except Exception:  # noqa: BLE001
            logger.exception("Failed to publish Neon serving payload health status")

    try:
        serving_completed = any(
            str(item.get("stage") or "") == "serving_refresh"
            and str(item.get("status") or "") == "completed"
            for item in stage_results
        )
        mark_refresh_finished_fn(
            profile=profile_key,
            run_id=effective_run_id,
            status=("ok" if overall_status == "ok" else "failed"),
            message="Serving outputs refreshed" if serving_completed and overall_status == "ok" else "Refresh finished",
            clear_pending=bool(serving_completed and overall_status == "ok"),
        )
    except Exception:  # noqa: BLE001
        logger.exception("Failed to update holdings refresh state")

    return {
        "overall_status": overall_status,
        "neon_mirror": neon_mirror,
        "local_mirror_sync": local_mirror_sync,
    }
