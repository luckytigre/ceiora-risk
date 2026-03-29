"""Stage dispatch for the model pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from backend.orchestration import stage_core, stage_serving, stage_source

_SOURCE_STAGES = {"ingest", "source_sync", "neon_readiness"}
_CORE_STAGES = {"raw_history", "feature_build", "estu_audit", "factor_returns", "risk_model"}
_SERVING_STAGES = {"serving_refresh"}


def run_stage(
    *,
    profile: str,
    run_id: str,
    stage: str,
    as_of_date: str,
    should_run_core: bool,
    serving_mode: str,
    force_core: bool,
    core_reason: str,
    data_db: Path,
    cache_db: Path,
    raw_history_policy: str = "none",
    reset_core_cache: bool = False,
    enable_ingest: bool = False,
    prefer_local_source_archive: bool = False,
    refresh_scope: str | None = None,
    workspace_root: Path | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    config_module,
    core_reads_module,
    sqlite_module,
    persist_model_outputs_fn: Callable[..., dict[str, Any]],
    bootstrap_cuse4_source_tables_fn: Callable[..., Any],
    download_from_lseg_fn: Callable[..., Any],
    repair_price_gap_fn: Callable[..., dict[str, Any]],
    repair_pit_gap_fn: Callable[..., dict[str, Any]],
    profile_source_sync_required_fn: Callable[..., bool],
    profile_neon_readiness_required_fn: Callable[..., bool],
    run_neon_source_sync_cycle_fn: Callable[..., dict[str, Any]],
    neon_authority_module,
    rebuild_raw_cross_section_history_fn: Callable[..., Any],
    rebuild_cross_section_snapshot_fn: Callable[..., Any],
    build_and_persist_estu_membership_fn: Callable[..., Any],
    reset_core_caches_fn: Callable[..., dict[str, int]],
    compute_daily_factor_returns_fn: Callable[..., Any],
    build_factor_covariance_from_cache_fn: Callable[..., Any],
    build_specific_risk_from_cache_fn: Callable[..., Any],
    latest_factor_return_date_fn: Callable[..., str | None],
    serialize_covariance_fn: Callable[..., dict[str, Any]],
    serving_refresh_skip_risk_engine_fn: Callable[..., tuple[bool, str]],
    run_refresh_fn: Callable[..., dict[str, Any]],
    previous_or_same_xnys_session_fn: Callable[[str], str],
    risk_engine_method_version: str,
    canonical_data_db: Path,
    canonical_cache_db: Path,
) -> dict[str, Any]:
    if stage in _SOURCE_STAGES:
        return stage_source.run_source_stage(
            profile=profile,
            stage=stage,
            as_of_date=as_of_date,
            should_run_core=should_run_core,
            core_reason=core_reason,
            data_db=data_db,
            cache_db=cache_db,
            enable_ingest=enable_ingest,
            workspace_root=workspace_root,
            progress_callback=progress_callback,
            config_module=config_module,
            core_reads_module=core_reads_module,
            bootstrap_cuse4_source_tables_fn=bootstrap_cuse4_source_tables_fn,
            download_from_lseg_fn=download_from_lseg_fn,
            repair_price_gap_fn=repair_price_gap_fn,
            repair_pit_gap_fn=repair_pit_gap_fn,
            profile_source_sync_required_fn=profile_source_sync_required_fn,
            profile_neon_readiness_required_fn=profile_neon_readiness_required_fn,
            run_neon_source_sync_cycle_fn=run_neon_source_sync_cycle_fn,
            neon_authority_module=neon_authority_module,
        )

    if stage in _CORE_STAGES:
        return stage_core.run_core_stage(
            profile=profile,
            run_id=run_id,
            stage=stage,
            as_of_date=as_of_date,
            should_run_core=should_run_core,
            force_core=force_core,
            core_reason=core_reason,
            data_db=data_db,
            cache_db=cache_db,
            raw_history_policy=raw_history_policy,
            reset_core_cache=reset_core_cache,
            progress_callback=progress_callback,
            config_module=config_module,
            core_reads_module=core_reads_module,
            sqlite_module=sqlite_module,
            persist_model_outputs_fn=persist_model_outputs_fn,
            rebuild_raw_cross_section_history_fn=rebuild_raw_cross_section_history_fn,
            rebuild_cross_section_snapshot_fn=rebuild_cross_section_snapshot_fn,
            build_and_persist_estu_membership_fn=build_and_persist_estu_membership_fn,
            reset_core_caches_fn=reset_core_caches_fn,
            compute_daily_factor_returns_fn=compute_daily_factor_returns_fn,
            build_factor_covariance_from_cache_fn=build_factor_covariance_from_cache_fn,
            build_specific_risk_from_cache_fn=build_specific_risk_from_cache_fn,
            latest_factor_return_date_fn=latest_factor_return_date_fn,
            serialize_covariance_fn=serialize_covariance_fn,
            previous_or_same_xnys_session_fn=previous_or_same_xnys_session_fn,
            risk_engine_method_version=risk_engine_method_version,
        )

    if stage in _SERVING_STAGES:
        return stage_serving.run_serving_stage(
            stage=stage,
            should_run_core=should_run_core,
            serving_mode=serving_mode,
            data_db=data_db,
            cache_db=cache_db,
            prefer_local_source_archive=prefer_local_source_archive,
            refresh_scope=refresh_scope,
            progress_callback=progress_callback,
            core_reads_module=core_reads_module,
            serving_refresh_skip_risk_engine_fn=serving_refresh_skip_risk_engine_fn,
            run_refresh_fn=run_refresh_fn,
            previous_or_same_xnys_session_fn=previous_or_same_xnys_session_fn,
            canonical_data_db=canonical_data_db,
            canonical_cache_db=canonical_cache_db,
        )

    raise ValueError(f"Unknown stage: {stage}")
