#!/usr/bin/env python3
"""Profile-driven model pipeline orchestrator with stage checkpoints."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from backend import config
from backend.analytics.pipeline import RISK_ENGINE_METHOD_VERSION, run_refresh
from backend.data import core_reads, job_runs, model_outputs, rebuild_cross_section_snapshot, sqlite
from backend.orchestration import finalize_run, post_run_publish, runtime_support, stage_execution, stage_planning, stage_runner
from backend.orchestration.profiles import (
    PROFILE_CONFIG,
    STAGES,
    _apply_force_core_stage_selection,
    planned_stages_for_profile,
    profile_catalog,
    profile_neon_readiness_required,
    profile_rebuild_backend,
    profile_requires_neon_sync_before_core,
    profile_source_sync_required,
    resolve_profile_name,
)
from backend.risk_model import (
    build_factor_covariance_from_cache,
    build_specific_risk_from_cache,
    compute_daily_factor_returns,
    rebuild_raw_cross_section_history,
)
from backend.services.neon_mirror import run_neon_mirror_cycle
from backend.services import neon_authority
from backend.services.holdings_runtime_state import mark_refresh_finished
from backend.universe import bootstrap_cuse4_source_tables, build_and_persist_estu_membership
from backend.trading_calendar import is_xnys_session, previous_or_same_xnys_session


DATA_DB = Path(config.DATA_DB_PATH)
CACHE_DB = Path(config.SQLITE_PATH)
logger = logging.getLogger(__name__)


def _download_from_lseg_impl(**kwargs):
    from backend.scripts.download_data_lseg import download_from_lseg

    return download_from_lseg(**kwargs)


def _backfill_prices_impl(**kwargs):
    from backend.scripts.backfill_prices_range_lseg import backfill_prices

    return backfill_prices(**kwargs)


def _backfill_pit_history_impl(**kwargs):
    from backend.scripts.backfill_pit_history_lseg import run_backfill

    return run_backfill(**kwargs)


# Keep patch points stable for existing tests/callers while avoiding eager LSEG imports.
def download_from_lseg(**kwargs):
    return _download_from_lseg_impl(**kwargs)


def backfill_prices(**kwargs):
    return _backfill_prices_impl(**kwargs)


def backfill_pit_history(**kwargs):
    return _backfill_pit_history_impl(**kwargs)


_download_from_lseg = download_from_lseg
_backfill_prices = backfill_prices
_backfill_pit_history = backfill_pit_history


def _neon_primary_backend_selected() -> bool:
    return str(config.DATA_BACKEND or "").strip().lower() == "neon"


def _neon_mirror_health_required() -> bool:
    return bool(config.NEON_AUTO_SYNC_REQUIRED or _neon_primary_backend_selected())


def _serving_payload_neon_write_required() -> bool:
    serving_primary_reads = bool(
        config.SERVING_OUTPUTS_PRIMARY_READS
        or config.cloud_mode()
        or _neon_primary_backend_selected()
    )
    return bool(serving_primary_reads and config.neon_surface_enabled("serving_outputs"))


def _latest_price_date(db_path: Path) -> str | None:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute("SELECT MAX(date) FROM security_prices_eod WHERE date IS NOT NULL").fetchone()
    except sqlite3.OperationalError:
        row = None
    finally:
        conn.close()
    if not row or not row[0]:
        return None
    return str(row[0])


def _latest_pit_date(db_path: Path, table: str) -> str | None:
    conn = sqlite3.connect(str(db_path))
    try:
        row = conn.execute(f"SELECT MAX(as_of_date) FROM {table} WHERE as_of_date IS NOT NULL").fetchone()
    except sqlite3.OperationalError:
        row = None
    finally:
        conn.close()
    if not row or not row[0]:
        return None
    return str(row[0])


def _period_key(date_str: str, *, frequency: str) -> tuple[int, int]:
    parsed = datetime.fromisoformat(str(date_str)).date()
    if frequency == "quarterly":
        return parsed.year, ((parsed.month - 1) // 3) + 1
    return parsed.year, parsed.month


def _period_start(date_str: str, *, frequency: str) -> date:
    parsed = datetime.fromisoformat(str(date_str)).date()
    if frequency == "quarterly":
        quarter_start_month = (((parsed.month - 1) // 3) * 3) + 1
        return date(parsed.year, quarter_start_month, 1)
    return date(parsed.year, parsed.month, 1)


def _next_period_start(date_str: str, *, frequency: str) -> date:
    start = _period_start(date_str, frequency=frequency)
    if frequency == "quarterly":
        year = start.year + (1 if start.month >= 10 else 0)
        month = 1 if start.month >= 10 else start.month + 3
        return date(year, month, 1)
    year = start.year + (1 if start.month == 12 else 0)
    month = 1 if start.month == 12 else start.month + 1
    return date(year, month, 1)


def _latest_closed_period_anchor(as_of_date: str, *, frequency: str) -> str:
    period_start = _period_start(str(as_of_date), frequency=frequency)
    previous_day = (period_start - timedelta(days=1)).isoformat()
    return previous_or_same_xnys_session(previous_day)


def _purge_open_period_pit_rows(
    *,
    data_db: Path,
    as_of_date: str,
    frequency: str,
) -> dict[str, Any]:
    latest_closed_anchor = _latest_closed_period_anchor(str(as_of_date), frequency=frequency)
    conn = sqlite3.connect(str(data_db))
    try:
        deleted: dict[str, int] = {}
        for table in ("security_fundamentals_pit", "security_classification_pit"):
            try:
                cur = conn.execute(f"DELETE FROM {table} WHERE as_of_date > ?", (latest_closed_anchor,))
                deleted[table] = int(cur.rowcount or 0)
            except sqlite3.OperationalError as exc:
                if "no such table" not in str(exc).lower():
                    raise
                deleted[table] = 0
        conn.commit()
    finally:
        conn.close()
    total_deleted = int(sum(deleted.values()))
    return {
        "status": "ok" if total_deleted > 0 else "skipped",
        "reason": None if total_deleted > 0 else "no_open_period_rows",
        "latest_closed_anchor": latest_closed_anchor,
        "deleted_rows": deleted,
    }


def _next_xnys_session_after(date_str: str) -> str:
    current = datetime.fromisoformat(str(date_str)).date()
    probe = current + timedelta(days=1)
    for _ in range(10):
        candidate = previous_or_same_xnys_session(probe.isoformat())
        if candidate > str(date_str):
            return candidate
        probe += timedelta(days=1)
    raise RuntimeError(f"Unable to resolve next XNYS session after {date_str}")


def _repair_price_gap(
    *,
    data_db: Path,
    as_of_date: str,
    latest_price_date_before_ingest: str | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    latest_price_date = latest_price_date_before_ingest or _latest_price_date(data_db)
    if not latest_price_date:
        return {"status": "skipped", "reason": "no_existing_prices"}
    if latest_price_date >= str(as_of_date):
        return {
            "status": "skipped",
            "reason": "latest_price_date_current",
            "latest_price_date": latest_price_date,
            "target_as_of_date": str(as_of_date),
        }
    start_date = _next_xnys_session_after(latest_price_date)
    if start_date > str(as_of_date):
        return {
            "status": "skipped",
            "reason": "no_missing_xnys_sessions",
            "latest_price_date": latest_price_date,
            "target_as_of_date": str(as_of_date),
        }
    if progress_callback is not None:
        progress_callback(
            {
                "message": f"Backfilling missing price sessions {start_date} -> {as_of_date}",
                "progress_kind": "io",
            }
        )
    out = backfill_prices(
        db_path=data_db,
        start_date=start_date,
        end_date=str(as_of_date),
        ticker_batch_size=180,
        days_per_window=30,
        max_retries=1,
        sleep_seconds=2.0,
    )
    out["latest_price_date_before_backfill"] = latest_price_date
    out["target_as_of_date"] = str(as_of_date)
    return out


def _repair_pit_gap(
    *,
    data_db: Path,
    as_of_date: str,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    frequency = str(config.SOURCE_DAILY_PIT_FREQUENCY or "monthly").strip().lower()
    open_period_cleanup = _purge_open_period_pit_rows(
        data_db=data_db,
        as_of_date=str(as_of_date),
        frequency=frequency,
    )
    latest_closed_anchor = str(open_period_cleanup.get("latest_closed_anchor") or "")
    latest_fund_date = _latest_pit_date(data_db, "security_fundamentals_pit")
    latest_class_date = _latest_pit_date(data_db, "security_classification_pit")

    historical_candidates: list[date] = []
    for latest in (latest_fund_date, latest_class_date):
        if latest and latest < latest_closed_anchor:
            historical_candidates.append(_next_period_start(latest, frequency=frequency))

    historical_repair: dict[str, Any] = {
        "status": "skipped",
        "reason": "no_missing_closed_periods",
        "frequency": frequency,
    }
    if historical_candidates:
        start_date = min(historical_candidates)
        end_date = datetime.fromisoformat(latest_closed_anchor).date()
        if start_date <= end_date:
            if progress_callback is not None:
                progress_callback(
                    {
                        "message": f"Backfilling missing {frequency} PIT anchors {start_date.isoformat()} -> {end_date.isoformat()}",
                        "progress_kind": "io",
                    }
                )
            historical_repair = backfill_pit_history(
                db_path=data_db,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                shard_count=1,
                max_retries=1,
                sleep_seconds=2.0,
                frequency=frequency,
                write_fundamentals=True,
                write_prices=False,
                write_classification=True,
                skip_complete_dates=True,
            )

    current_period_repair: dict[str, Any] = {
        "status": "skipped",
        "reason": "closed_period_only_policy",
        "frequency": frequency,
    }
    if historical_repair.get("status") == "failed":
        status = "failed"
    elif historical_repair.get("status") == "ok" or open_period_cleanup.get("status") == "ok":
        status = "ok"
    else:
        status = "skipped"
    return {
        "status": status,
        "frequency": frequency,
        "latest_closed_anchor": latest_closed_anchor,
        "latest_fundamentals_as_of_before_repair": latest_fund_date,
        "latest_classification_as_of_before_repair": latest_class_date,
        "target_as_of_date": str(as_of_date),
        "open_period_cleanup": open_period_cleanup,
        "historical_repair": historical_repair,
        "current_period_repair": current_period_repair,
    }


def _run_stage(
    *,
    run_id: str = "stage_test_run",
    profile: str,
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
) -> dict[str, Any]:
    return stage_runner.run_stage(
        profile=profile,
        run_id=run_id,
        stage=stage,
        as_of_date=as_of_date,
        should_run_core=should_run_core,
        serving_mode=serving_mode,
        force_core=force_core,
        core_reason=core_reason,
        data_db=data_db,
        cache_db=cache_db,
        raw_history_policy=raw_history_policy,
        reset_core_cache=reset_core_cache,
        enable_ingest=enable_ingest,
        prefer_local_source_archive=prefer_local_source_archive,
        refresh_scope=refresh_scope,
        workspace_root=workspace_root,
        progress_callback=progress_callback,
        config_module=config,
        core_reads_module=core_reads,
        sqlite_module=sqlite,
        persist_model_outputs_fn=model_outputs.persist_model_outputs,
        bootstrap_cuse4_source_tables_fn=bootstrap_cuse4_source_tables,
        download_from_lseg_fn=download_from_lseg,
        repair_price_gap_fn=_repair_price_gap,
        repair_pit_gap_fn=_repair_pit_gap,
        profile_source_sync_required_fn=profile_source_sync_required,
        profile_neon_readiness_required_fn=profile_neon_readiness_required,
        run_neon_mirror_cycle_fn=run_neon_mirror_cycle,
        neon_authority_module=neon_authority,
        rebuild_raw_cross_section_history_fn=rebuild_raw_cross_section_history,
        rebuild_cross_section_snapshot_fn=rebuild_cross_section_snapshot,
        build_and_persist_estu_membership_fn=build_and_persist_estu_membership,
        reset_core_caches_fn=runtime_support.reset_core_caches,
        compute_daily_factor_returns_fn=compute_daily_factor_returns,
        build_factor_covariance_from_cache_fn=build_factor_covariance_from_cache,
        build_specific_risk_from_cache_fn=build_specific_risk_from_cache,
        latest_factor_return_date_fn=runtime_support.latest_factor_return_date,
        serialize_covariance_fn=runtime_support.serialize_covariance,
        serving_refresh_skip_risk_engine_fn=runtime_support.serving_refresh_skip_risk_engine,
        run_refresh_fn=run_refresh,
        previous_or_same_xnys_session_fn=previous_or_same_xnys_session,
        risk_engine_method_version=RISK_ENGINE_METHOD_VERSION,
        canonical_data_db=DATA_DB,
        canonical_cache_db=CACHE_DB,
    )


def run_model_pipeline(
    *,
    profile: str,
    as_of_date: str | None = None,
    run_id: str | None = None,
    resume_run_id: str | None = None,
    from_stage: str | None = None,
    to_stage: str | None = None,
    force_core: bool = False,
    refresh_scope: str | None = None,
    stage_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    profile_key, cfg, selected = planned_stages_for_profile(
        profile=profile,
        from_stage=from_stage,
        to_stage=to_stage,
        force_core=bool(force_core),
    )
    prefer_local_source_archive = runtime_support.profile_prefers_local_source_archive(profile_key)
    effective_run_id = (
        str(resume_run_id).strip()
        if resume_run_id and str(resume_run_id).strip()
        else (str(run_id).strip() if run_id and str(run_id).strip() else f"job_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    )
    job_runs.ensure_schema(DATA_DB)
    job_runs.fail_stale_running_stages(db_path=DATA_DB)
    completed = job_runs.completed_stages(db_path=DATA_DB, run_id=effective_run_id) if resume_run_id else set()

    if stage_planning.selected_stages_include_ingest(selected) and not (as_of_date and str(as_of_date).strip()):
        as_of = stage_planning.current_xnys_session(datetime_cls=datetime)
    elif stage_planning.selected_stages_require_source_as_of(selected):
        as_of = stage_planning.resolved_as_of_date(
            as_of_date,
            prefer_local_source_archive=prefer_local_source_archive,
            current_xnys_session_resolver=lambda: stage_planning.current_xnys_session(datetime_cls=datetime),
        )
    elif as_of_date and str(as_of_date).strip():
        as_of = previous_or_same_xnys_session(str(as_of_date).strip())
    else:
        as_of = stage_planning.current_xnys_session(datetime_cls=datetime)
    today_utc = datetime.fromisoformat(stage_planning.current_xnys_session(datetime_cls=datetime)).date()
    effective_risk_engine_meta, _ = runtime_support.resolve_effective_risk_engine_meta(
        cache_db=CACHE_DB,
        sqlite_module=sqlite,
    )
    due, due_reason = runtime_support.risk_recompute_due(
        effective_risk_engine_meta,
        today_utc=today_utc,
        method_version=RISK_ENGINE_METHOD_VERSION,
        interval_days=config.RISK_RECOMPUTE_INTERVAL_DAYS,
    )
    core_policy = str(cfg["core_policy"])
    raw_history_policy = str(cfg.get("raw_history_policy") or "none")
    reset_core_cache = bool(cfg.get("reset_core_cache"))
    rebuild_backend = profile_rebuild_backend(profile_key, cfg=cfg)
    should_run_core = bool(force_core or core_policy == "always" or (core_policy == "due" and due))
    core_reason = "force_core" if force_core else ("due" if should_run_core else due_reason)
    logger.info(
        "Pipeline core-risk decision: profile=%s policy=%s should_run_core=%s reason=%s due=%s due_reason=%s",
        profile_key,
        core_policy,
        should_run_core,
        core_reason,
        due,
        due_reason,
    )

    stage_run = stage_execution.run_selected_stages(
        selected=selected,
        stages=STAGES,
        db_path=DATA_DB,
        cache_db=CACHE_DB,
        profile_key=profile_key,
        effective_run_id=effective_run_id,
        as_of=as_of,
        should_run_core=bool(should_run_core),
        serving_mode=str(cfg["serving_mode"]),
        force_core=bool(force_core),
        core_reason=str(core_reason),
        raw_history_policy=raw_history_policy,
        reset_core_cache=reset_core_cache,
        enable_ingest=bool(cfg.get("enable_ingest")),
        prefer_local_source_archive=prefer_local_source_archive,
        refresh_scope=(str(refresh_scope).strip().lower() if refresh_scope else None),
        rebuild_backend=rebuild_backend,
        app_data_dir=str(config.APP_DATA_DIR),
        completed_stages=completed,
        stage_callback=stage_callback,
        run_stage_fn=_run_stage,
        job_runs_module=job_runs,
        neon_authority_module=neon_authority,
    )
    stage_results = list(stage_run["stage_results"])
    overall_status = str(stage_run["overall_status"])
    workspace_paths = stage_run["workspace_paths"]
    neon_mirror_sqlite_path = Path(stage_run["neon_mirror_sqlite_path"])
    neon_mirror_cache_path = Path(stage_run["neon_mirror_cache_path"])

    neon_sync_enabled = bool(
        str(config.NEON_DATABASE_URL or "").strip()
        and (
            config.neon_auto_sync_enabled_effective()
            or profile_source_sync_required(profile_key, cfg=cfg)
        )
    )
    neon_parity_enabled = bool(config.neon_auto_parity_enabled_effective())
    neon_prune_enabled = bool(config.neon_auto_prune_enabled_effective())
    broad_neon_mirror_enabled = runtime_support.profile_runs_broad_neon_mirror(profile_key)
    neon_mirror_required = _neon_mirror_health_required()
    serving_payload_neon_failure = post_run_publish.extract_serving_payload_neon_failure(stage_results)

    finalization = finalize_run.finalize_pipeline_run(
        overall_status=overall_status,
        stage_results=stage_results,
        profile_key=profile_key,
        as_of=as_of,
        effective_run_id=effective_run_id,
        workspace_paths=workspace_paths,
        data_db=DATA_DB,
        cache_db=CACHE_DB,
        neon_mirror_sqlite_path=neon_mirror_sqlite_path,
        neon_mirror_cache_path=neon_mirror_cache_path,
        neon_sync_enabled=neon_sync_enabled,
        neon_parity_enabled=neon_parity_enabled,
        neon_prune_enabled=neon_prune_enabled,
        broad_neon_mirror_enabled=broad_neon_mirror_enabled,
        neon_mirror_required=neon_mirror_required,
        serving_payload_neon_failure=serving_payload_neon_failure,
        run_neon_mirror_cycle_fn=run_neon_mirror_cycle,
        sync_workspace_derivatives_to_local_mirror_fn=neon_authority.sync_workspace_derivatives_to_local_mirror,
        prune_rebuild_workspaces_fn=neon_authority.prune_rebuild_workspaces,
        write_neon_mirror_artifact_fn=post_run_publish.write_neon_mirror_artifact,
        publish_neon_sync_health_fn=post_run_publish.publish_neon_sync_health,
        publish_neon_serving_write_health_fn=post_run_publish.publish_neon_serving_write_health,
        mark_refresh_finished_fn=mark_refresh_finished,
        serving_payload_neon_write_required_fn=_serving_payload_neon_write_required,
        config_module=config,
    )
    overall_status = str(finalization["overall_status"])
    neon_mirror = dict(finalization["neon_mirror"])
    local_mirror_sync = dict(finalization["local_mirror_sync"])

    return {
        "status": overall_status,
        "run_id": effective_run_id,
        "profile": profile_key,
        "profile_label": str(cfg.get("label") or profile_key),
        "as_of_date": as_of,
        "core_policy": core_policy,
        "core_due": bool(due),
        "core_reason": core_reason,
        "core_will_run": bool(should_run_core),
        "raw_history_policy": raw_history_policy,
        "reset_core_cache": bool(reset_core_cache),
        "selected_stages": selected,
        "stage_results": stage_results,
        "neon_mirror": neon_mirror,
        "workspace": (
            {
                "root_dir": str(workspace_paths.root_dir),
                "data_db": str(workspace_paths.data_db),
                "cache_db": str(workspace_paths.cache_db),
            }
            if workspace_paths is not None
            else None
        ),
        "local_mirror_sync": local_mirror_sync,
        "workspace_prune": dict(finalization["workspace_prune"]),
        "run_rows": job_runs.run_rows(db_path=DATA_DB, run_id=effective_run_id),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        required=True,
        choices=sorted(PROFILE_CONFIG.keys()),
        help="Execution profile for cadence and core-risk policy.",
    )
    parser.add_argument("--as-of-date", default=None, help="Optional as-of date (YYYY-MM-DD).")
    parser.add_argument("--run-id", default=None, help="Optional explicit run id for a new run.")
    parser.add_argument("--resume-run-id", default=None, help="Resume an existing run id.")
    parser.add_argument("--from-stage", default=None, choices=STAGES, help="Start stage.")
    parser.add_argument("--to-stage", default=None, choices=STAGES, help="End stage.")
    parser.add_argument(
        "--force-core",
        action="store_true",
        help="Force core factor-return/covariance/specific-risk recompute regardless of profile policy.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Console log verbosity.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    print(
        run_model_pipeline(
            profile=args.profile,
            as_of_date=args.as_of_date,
            run_id=args.run_id,
            resume_run_id=args.resume_run_id,
            from_stage=args.from_stage,
            to_stage=args.to_stage,
            force_core=bool(args.force_core),
        )
    )
