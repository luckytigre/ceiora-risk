#!/usr/bin/env python3
"""Profile-driven model pipeline orchestrator with stage checkpoints."""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import numpy as np

from backend import config
from backend.analytics import pipeline as analytics_pipeline
from backend.analytics.refresh_policy import (
    latest_factor_return_date as _latest_factor_return_date_impl,
    risk_recompute_due as _risk_recompute_due_impl,
)
from backend.analytics.pipeline import RISK_ENGINE_METHOD_VERSION, run_refresh
from backend.data import core_reads, job_runs, rebuild_cross_section_snapshot, runtime_state, sqlite
from backend.risk_model import (
    build_factor_covariance_from_cache,
    build_specific_risk_from_cache,
    compute_daily_factor_returns,
    rebuild_raw_cross_section_history,
)
from backend.services.neon_mirror import run_neon_mirror_cycle
from backend.services import neon_authority
from backend.services.holdings_runtime_state import mark_refresh_finished
from backend.scripts.backfill_pit_history_lseg import run_backfill as backfill_pit_history
from backend.scripts.backfill_prices_range_lseg import backfill_prices
from backend.universe import bootstrap_cuse4_source_tables, build_and_persist_estu_membership
from backend.scripts.download_data_lseg import download_from_lseg
from backend.trading_calendar import is_xnys_session, previous_or_same_xnys_session


DATA_DB = Path(config.DATA_DB_PATH)
CACHE_DB = Path(config.SQLITE_PATH)
logger = logging.getLogger(__name__)

STAGES = [
    "ingest",
    "source_sync",
    "neon_readiness",
    "raw_history",
    "feature_build",
    "estu_audit",
    "factor_returns",
    "risk_model",
    "serving_refresh",
]

PROFILE_CONFIG: dict[str, dict[str, Any]] = {
    "publish-only": {
        "label": "Publish Only",
        "description": "Republish already-current cached serving payloads without recomputing analytics.",
        "core_policy": "never",
        "serving_mode": "publish",
        "raw_history_policy": "none",
        "reset_core_cache": False,
        "default_stages": ["serving_refresh"],
        "enable_ingest": False,
        "ingest_policy": "none",
    },
    "serve-refresh": {
        "label": "Serve Refresh",
        "description": "Rebuild frontend-facing caches only.",
        "core_policy": "never",
        "serving_mode": "light",
        "raw_history_policy": "none",
        "reset_core_cache": False,
        "default_stages": ["serving_refresh"],
        "enable_ingest": False,
        "ingest_policy": "none",
    },
    "source-daily": {
        "label": "Source Daily",
        "description": "Pull latest source-of-truth data locally, publish the retained operating window, and rebuild serving caches.",
        "core_policy": "never",
        "serving_mode": "light",
        "raw_history_policy": "none",
        "reset_core_cache": False,
        "default_stages": ["ingest", "serving_refresh"],
        "enable_ingest": True,
        "ingest_policy": "local_lseg",
    },
    "source-daily-plus-core-if-due": {
        "label": "Source Daily + Core If Due",
        "description": "Daily source refresh plus a core recompute only when cadence or policy says it is due.",
        "core_policy": "due",
        "serving_mode": "light",
        "raw_history_policy": "none",
        "reset_core_cache": False,
        "default_stages": ["ingest", "factor_returns", "risk_model", "serving_refresh"],
        "enable_ingest": True,
        "ingest_policy": "local_lseg",
    },
    "core-weekly": {
        "label": "Core Weekly",
        "description": "Recompute factor returns, covariance, and specific risk from the current authoritative rebuild store.",
        "core_policy": "always",
        "serving_mode": "full",
        "raw_history_policy": "none",
        "reset_core_cache": False,
        "default_stages": ["factor_returns", "risk_model", "serving_refresh"],
        "enable_ingest": False,
        "ingest_policy": "none",
    },
    "cold-core": {
        "label": "Cold Core",
        "description": "Structural rebuild of raw history and core model state from the current authoritative rebuild store.",
        "core_policy": "always",
        "serving_mode": "full",
        "raw_history_policy": "full-daily",
        "reset_core_cache": True,
        "default_stages": ["raw_history", "feature_build", "estu_audit", "factor_returns", "risk_model", "serving_refresh"],
        "enable_ingest": False,
        "ingest_policy": "none",
    },
    "universe-add": {
        "label": "Universe Add",
        "description": "Post-universe-onboarding serving refresh after targeted source backfills.",
        "core_policy": "never",
        "serving_mode": "full",
        "raw_history_policy": "none",
        "reset_core_cache": False,
        "default_stages": ["serving_refresh"],
        "enable_ingest": False,
        "ingest_policy": "none",
    },
}
_NEON_BROAD_MIRROR_PROFILES = {
    "source-daily",
    "source-daily-plus-core-if-due",
    "core-weekly",
    "cold-core",
    "universe-add",
}
_CORE_REBUILD_PROFILES = {
    "source-daily-plus-core-if-due",
    "core-weekly",
    "cold-core",
}

def resolve_profile_name(profile: str) -> str:
    clean = str(profile or "").strip().lower()
    if not clean:
        raise ValueError("profile is required")
    return clean


def profile_catalog() -> list[dict[str, Any]]:
    return [
        {
            "profile": profile,
            "label": str(cfg.get("label") or profile),
            "description": str(cfg.get("description") or ""),
            "core_policy": str(cfg.get("core_policy") or ""),
            "serving_mode": str(cfg.get("serving_mode") or ""),
            "raw_history_policy": str(cfg.get("raw_history_policy") or "none"),
            "reset_core_cache": bool(cfg.get("reset_core_cache")),
            "default_stages": list(cfg.get("default_stages") or []),
            "enable_ingest": bool(cfg.get("enable_ingest")),
            "ingest_policy": str(cfg.get("ingest_policy") or "none"),
            "rebuild_backend": profile_rebuild_backend(profile, cfg=cfg),
            "requires_neon_sync_before_core": bool(profile_requires_neon_sync_before_core(profile, cfg=cfg)),
            "source_sync_required": bool(profile_source_sync_required(profile, cfg=cfg)),
            "neon_readiness_required": bool(profile_neon_readiness_required(profile, cfg=cfg)),
        }
        for profile, cfg in PROFILE_CONFIG.items()
    ]


def _profile_runs_broad_neon_mirror(profile: str) -> bool:
    return str(profile or "").strip().lower() in _NEON_BROAD_MIRROR_PROFILES


def profile_source_sync_required(profile: str, *, cfg: dict[str, Any] | None = None) -> bool:
    clean = str(profile or "").strip().lower()
    _ = cfg
    return bool(
        config.neon_primary_model_data_enabled()
        and clean in _NEON_BROAD_MIRROR_PROFILES
        and clean not in {"serve-refresh", "publish-only"}
    )


def profile_rebuild_backend(profile: str, *, cfg: dict[str, Any] | None = None) -> str:
    clean = str(profile or "").strip().lower()
    spec = cfg or PROFILE_CONFIG.get(clean) or {}
    if str(spec.get("core_policy") or "never") == "never":
        return "none"
    return "neon" if config.neon_authoritative_rebuilds_enabled() else "local"


def profile_requires_neon_sync_before_core(profile: str, *, cfg: dict[str, Any] | None = None) -> bool:
    clean = str(profile or "").strip().lower()
    if clean not in _CORE_REBUILD_PROFILES:
        return False
    return profile_rebuild_backend(clean, cfg=cfg) == "neon"


def profile_neon_readiness_required(profile: str, *, cfg: dict[str, Any] | None = None) -> bool:
    clean = str(profile or "").strip().lower()
    if clean not in _CORE_REBUILD_PROFILES:
        return False
    return profile_rebuild_backend(clean, cfg=cfg) == "neon"


def _extract_neon_mirror_error(neon_mirror: dict[str, Any] | None) -> dict[str, str] | None:
    if not isinstance(neon_mirror, dict):
        return None
    candidates = [
        neon_mirror.get("error"),
        (neon_mirror.get("sync") or {}).get("error"),
        (neon_mirror.get("factor_returns_sync") or {}).get("error"),
        (neon_mirror.get("prune") or {}).get("error"),
        (neon_mirror.get("parity") or {}).get("error"),
    ]
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        error_type = str(candidate.get("type") or "").strip()
        error_message = str(candidate.get("message") or "").strip()
        if error_type or error_message:
            return {
                "type": error_type or "RuntimeError",
                "message": error_message or "Neon mirror step failed.",
            }
    return None


def _write_neon_mirror_artifact(
    *,
    run_id: str,
    profile: str,
    as_of_date: str,
    overall_status: str,
    neon_mirror: dict[str, Any],
) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    reports_dir = Path(config.APP_DATA_DIR) / "audit_reports" / "neon_parity"
    reports_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": str(run_id),
        "profile": str(profile),
        "as_of_date": str(as_of_date),
        "overall_status": str(overall_status),
        "neon_mirror": neon_mirror,
    }
    artifact_path = reports_dir / f"neon_mirror_{stamp}_{run_id}.json"
    artifact_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    latest_path = reports_dir / "latest_neon_mirror_report.json"
    latest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return str(artifact_path)


def _publish_neon_sync_health(
    *,
    run_id: str,
    profile: str,
    as_of_date: str,
    neon_mirror: dict[str, Any],
    artifact_path: str | None,
) -> None:
    mirror_status = str(neon_mirror.get("status") or "").strip().lower()
    sync_status = str((neon_mirror.get("sync") or {}).get("status") or "").strip().lower()
    parity = neon_mirror.get("parity") if isinstance(neon_mirror.get("parity"), dict) else {}
    parity_status = str((parity or {}).get("status") or "").strip().lower()
    parity_issues = list((parity or {}).get("issues") or [])
    error_details = _extract_neon_mirror_error(neon_mirror)

    has_error = (
        mirror_status in {"failed", "mismatch"}
        or sync_status in {"failed", "mismatch"}
        or parity_status in {"failed", "mismatch"}
    )
    status = "error" if has_error else "ok"
    message = (
        f"Neon mirror={mirror_status or 'unknown'} sync={sync_status or 'n/a'} "
        f"parity={parity_status or 'n/a'}"
    )
    if not has_error and mirror_status in {"", "unknown", "skipped"}:
        status = "warning"

    payload = {
        "status": status,
        "message": message,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": str(run_id),
        "profile": str(profile),
        "as_of_date": str(as_of_date),
        "mirror_status": mirror_status or None,
        "sync_status": sync_status or None,
        "parity_status": parity_status or None,
        "parity_issue_count": int(len(parity_issues)),
        "parity_issue_examples": [str(x) for x in parity_issues[:10]],
        "error_type": (error_details or {}).get("type"),
        "error_message": (error_details or {}).get("message"),
        "artifact_path": str(artifact_path) if artifact_path else None,
    }
    runtime_state.persist_runtime_state(
        "neon_sync_health",
        payload,
        fallback_writer=lambda key, value: sqlite.cache_set(key, value),
    )

    if status == "error":
        logger.error("Neon sync/parity health ERROR: %s", message)
    elif status == "warning":
        logger.warning("Neon sync/parity health WARNING: %s", message)
    else:
        logger.info("Neon sync/parity health OK: %s", message)


def _extract_serving_payload_neon_failure(stage_results: list[dict[str, Any]]) -> dict[str, str] | None:
    for item in stage_results:
        if str(item.get("stage") or "") != "serving_refresh":
            continue
        if str(item.get("status") or "") != "failed":
            continue
        error = item.get("error") if isinstance(item.get("error"), dict) else {}
        message = str((error or {}).get("message") or "").strip()
        if "Serving payload" not in message or "Neon" not in message:
            continue
        return {
            "type": str((error or {}).get("type") or "RuntimeError"),
            "message": message,
        }
    return None


def _publish_neon_serving_write_health(
    *,
    run_id: str,
    profile: str,
    as_of_date: str,
    error: dict[str, str],
) -> None:
    message = "Required Neon serving-payload publish failed"
    payload = {
        "status": "error",
        "message": message,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": str(run_id),
        "profile": str(profile),
        "as_of_date": str(as_of_date),
        "mirror_status": "skipped",
        "sync_status": "serving_payload_write_failed",
        "parity_status": None,
        "parity_issue_count": 0,
        "parity_issue_examples": [],
        "error_type": str(error.get("type") or "RuntimeError"),
        "error_message": str(error.get("message") or message),
        "artifact_path": None,
        "health_scope": "serving_payload_write",
    }
    runtime_state.persist_runtime_state(
        "neon_sync_health",
        payload,
        fallback_writer=lambda key, value: sqlite.cache_set(key, value),
    )
    logger.error("Neon serving payload health ERROR: %s", payload["error_message"])


def _risk_recompute_due(meta: dict[str, Any], *, today_utc: date) -> tuple[bool, str]:
    return _risk_recompute_due_impl(
        meta,
        today_utc=today_utc,
        method_version=RISK_ENGINE_METHOD_VERSION,
        interval_days=config.RISK_RECOMPUTE_INTERVAL_DAYS,
    )


def _latest_factor_return_date(cache_db: Path) -> str | None:
    return _latest_factor_return_date_impl(cache_db)


def _serialize_covariance(cov) -> dict[str, Any]:
    if cov is None or cov.empty:
        return {"factors": [], "matrix": []}
    factors = [str(c) for c in cov.columns]
    mat = cov.reindex(index=factors, columns=factors).to_numpy(dtype=float)
    return {
        "factors": factors,
        "matrix": [[float(v) for v in row] for row in mat.tolist()],
    }


def _risk_cache_ready() -> bool:
    cov_payload = sqlite.cache_get_live_first("risk_engine_cov")
    specific_payload = sqlite.cache_get_live_first("risk_engine_specific_risk")
    factors = cov_payload.get("factors") if isinstance(cov_payload, dict) else None
    matrix = cov_payload.get("matrix") if isinstance(cov_payload, dict) else None
    return bool(
        isinstance(cov_payload, dict)
        and isinstance(factors, list)
        and isinstance(matrix, list)
        and len(factors) > 0
        and len(matrix) > 0
        and isinstance(specific_payload, dict)
        and len(specific_payload) > 0
    )


def _serving_refresh_skip_risk_engine(*, today_utc: date) -> tuple[bool, str]:
    if not _risk_cache_ready():
        return False, "risk_cache_missing"
    risk_engine_meta = runtime_state.load_runtime_state(
        "risk_engine_meta",
        fallback_loader=sqlite.cache_get_live_first,
    ) or {}
    should_recompute, recompute_reason = _risk_recompute_due(
        risk_engine_meta,
        today_utc=today_utc,
    )
    if should_recompute:
        return False, f"core_due_{recompute_reason}"
    return True, "risk_cache_current"


def _profile_prefers_local_source_archive(profile: str) -> bool:
    if profile_source_sync_required(profile):
        return False
    return bool(
        config.runtime_role_allows_ingest()
        and str(profile or "").strip().lower() not in {"serve-refresh", "publish-only"}
    )


@contextmanager
def _temporary_runtime_paths(*, data_db: Path, cache_db: Path):
    data_db_path = Path(data_db).expanduser().resolve()
    cache_db_path = Path(cache_db).expanduser().resolve()
    old_data_db_path = str(config.DATA_DB_PATH)
    old_cache_db_path = str(config.SQLITE_PATH)
    old_pipeline_data_db = analytics_pipeline.DATA_DB
    old_pipeline_cache_db = analytics_pipeline.CACHE_DB
    config.DATA_DB_PATH = str(data_db_path)
    config.SQLITE_PATH = str(cache_db_path)
    analytics_pipeline.DATA_DB = data_db_path
    analytics_pipeline.CACHE_DB = cache_db_path
    try:
        yield
    finally:
        config.DATA_DB_PATH = old_data_db_path
        config.SQLITE_PATH = old_cache_db_path
        analytics_pipeline.DATA_DB = old_pipeline_data_db
        analytics_pipeline.CACHE_DB = old_pipeline_cache_db


def _resolved_as_of_date(
    user_as_of_date: str | None,
    *,
    prefer_local_source_archive: bool = False,
) -> str:
    if user_as_of_date and str(user_as_of_date).strip():
        return previous_or_same_xnys_session(str(user_as_of_date).strip())
    try:
        if prefer_local_source_archive:
            with core_reads.core_read_backend("local"):
                source_dates = core_reads.load_source_dates()
        else:
            source_dates = core_reads.load_source_dates()
    except Exception:  # noqa: BLE001
        logger.warning("Falling back to today's session because source dates could not be loaded.", exc_info=True)
        source_dates = {}
    return previous_or_same_xnys_session(
        str(
            source_dates.get("fundamentals_asof")
            or source_dates.get("exposures_asof")
            or datetime.now(timezone.utc).date().isoformat()
        )
    )


def _reset_core_caches(cache_db: Path) -> dict[str, int]:
    conn = sqlite3.connect(str(cache_db))
    cleared: dict[str, int] = {}
    try:
        for table in ("daily_factor_returns", "daily_specific_residuals", "daily_universe_eligibility_summary"):
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table,),
            ).fetchone()
            if not exists:
                cleared[table] = 0
                continue
            before = conn.total_changes
            conn.execute(f"DELETE FROM {table}")
            cleared[table] = int(conn.total_changes - before)

        meta_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='daily_factor_returns_meta' LIMIT 1"
        ).fetchone()
        if meta_exists:
            before = conn.total_changes
            conn.execute("DELETE FROM daily_factor_returns_meta")
            cleared["daily_factor_returns_meta"] = int(conn.total_changes - before)
        else:
            cleared["daily_factor_returns_meta"] = 0

        cache_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cache' LIMIT 1"
        ).fetchone()
        if cache_exists:
            before = conn.total_changes
            conn.execute(
                """
                DELETE FROM cache
                WHERE key IN ('risk_engine_cov', 'risk_engine_specific_risk', 'risk_engine_meta')
                """
            )
            cleared["cache_risk_engine_keys"] = int(conn.total_changes - before)
        else:
            cleared["cache_risk_engine_keys"] = 0
        conn.commit()
    finally:
        conn.close()
    return cleared


def _stage_window(from_stage: str | None, to_stage: str | None) -> list[str]:
    start = STAGES.index(from_stage) if from_stage else 0
    end = STAGES.index(to_stage) if to_stage else len(STAGES) - 1
    if start > end:
        raise ValueError("--from-stage must be before or equal to --to-stage")
    return STAGES[start : end + 1]


def _default_stage_selection(cfg: dict[str, Any], from_stage: str | None, to_stage: str | None) -> list[str]:
    if from_stage or to_stage:
        return _stage_window(from_stage, to_stage)
    selected = [str(stage) for stage in (cfg.get("default_stages") or []) if str(stage) in STAGES]
    return selected or list(STAGES)


def _apply_neon_authority_stage_selection(
    *,
    profile: str,
    cfg: dict[str, Any],
    selected: list[str],
    from_stage: str | None,
    to_stage: str | None,
) -> list[str]:
    if from_stage or to_stage:
        return selected
    wanted = set(selected)
    rebuild_stages = {"raw_history", "feature_build", "estu_audit", "factor_returns", "risk_model"}
    if profile_source_sync_required(profile, cfg=cfg) and wanted.intersection(rebuild_stages | {"serving_refresh"}):
        wanted.add("source_sync")
    if profile_neon_readiness_required(profile, cfg=cfg) and wanted.intersection(rebuild_stages):
        wanted.add("neon_readiness")
    return [stage for stage in STAGES if stage in wanted]


def _apply_force_core_stage_selection(
    *,
    selected: list[str],
    force_core: bool,
    from_stage: str | None,
    to_stage: str | None,
) -> list[str]:
    if not force_core:
        return selected
    required = {"factor_returns", "risk_model"}
    if required.issubset(set(selected)):
        return selected
    if from_stage or to_stage:
        raise ValueError(
            "force_core requires a stage window that includes factor_returns and risk_model, "
            "or no explicit --from-stage/--to-stage."
        )
    wanted = set(selected) | required
    return [stage for stage in STAGES if stage in wanted]


def planned_stages_for_profile(
    *,
    profile: str,
    from_stage: str | None = None,
    to_stage: str | None = None,
    force_core: bool = False,
) -> tuple[str, dict[str, Any], list[str]]:
    profile_key = resolve_profile_name(profile)
    if profile_key not in PROFILE_CONFIG:
        raise ValueError(
            f"Unsupported profile '{profile}'. Expected one of: {', '.join(sorted(PROFILE_CONFIG))}"
        )
    cfg = PROFILE_CONFIG[profile_key]
    selected = _default_stage_selection(cfg, from_stage, to_stage)
    selected = _apply_neon_authority_stage_selection(
        profile=profile_key,
        cfg=cfg,
        selected=selected,
        from_stage=from_stage,
        to_stage=to_stage,
    )
    selected = _apply_force_core_stage_selection(
        selected=selected,
        force_core=bool(force_core),
        from_stage=from_stage,
        to_stage=to_stage,
    )
    return profile_key, cfg, selected


def _selected_stages_require_source_as_of(selected: list[str]) -> bool:
    return any(stage in {"ingest", "estu_audit"} for stage in selected)


def _selected_stages_include_ingest(selected: list[str]) -> bool:
    return "ingest" in selected


def _current_xnys_session() -> str:
    ny_now = datetime.now(ZoneInfo("America/New_York"))
    ny_date = ny_now.date()
    if is_xnys_session(ny_date.isoformat()) and ny_now.time() < datetime.strptime("18:00", "%H:%M").time():
        return previous_or_same_xnys_session((ny_date - timedelta(days=1)).isoformat())
    return previous_or_same_xnys_session(ny_date.isoformat())


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
    if stage == "ingest":
        if progress_callback is not None:
            progress_callback({"message": "Bootstrapping source tables", "progress_kind": "stage"})
        bootstrap = bootstrap_cuse4_source_tables(
            db_path=data_db,
        )
        if not config.runtime_role_allows_ingest():
            return {
                "status": "skipped",
                "mode": "bootstrap_only",
                "reason": "runtime_role_disallows_ingest",
                "bootstrap": bootstrap,
                "runtime_role": str(config.APP_RUNTIME_ROLE),
            }
        if not enable_ingest:
            return {
                "status": "ok",
                "mode": "bootstrap_only",
                "reason": "profile_skip_lseg_ingest",
                "bootstrap": bootstrap,
                "runtime_role": str(config.APP_RUNTIME_ROLE),
            }
        if not bool(config.ORCHESTRATOR_ENABLE_INGEST):
            return {
                "status": "ok",
                "mode": "bootstrap_only",
                "reason": "ORCHESTRATOR_ENABLE_INGEST=false",
                "bootstrap": bootstrap,
                "runtime_role": str(config.APP_RUNTIME_ROLE),
            }
        if progress_callback is not None:
            progress_callback({"message": "Pulling latest source data from LSEG", "progress_kind": "io"})
        latest_price_date_before_ingest = _latest_price_date(data_db)
        ingest = download_from_lseg(
            db_path=data_db,
            as_of_date=as_of_date,
            # The orchestrator stage must cover the full universe in one pass.
            # Explicit sharding remains available on the direct LSEG ingest script.
            shard_count=1,
            shard_index=0,
            write_fundamentals=False,
            write_prices=True,
            write_classification=False,
        )
        price_gap_repair = {"status": "skipped", "reason": "ingest_not_ok"}
        pit_gap_repair = {"status": "skipped", "reason": "ingest_not_ok"}
        if str(ingest.get("status") or "").strip().lower() == "ok":
            price_gap_repair = _repair_price_gap(
                data_db=data_db,
                as_of_date=as_of_date,
                latest_price_date_before_ingest=latest_price_date_before_ingest,
                progress_callback=progress_callback,
            )
            pit_gap_repair = _repair_pit_gap(
                data_db=data_db,
                as_of_date=as_of_date,
                progress_callback=progress_callback,
            )
        return {
            "status": str(ingest.get("status") or "ok"),
            "mode": "bootstrap_plus_lseg_ingest",
            "bootstrap": bootstrap,
            "ingest": ingest,
            "price_gap_repair": price_gap_repair,
            "pit_gap_repair": pit_gap_repair,
        }

    if stage == "source_sync":
        if not profile_source_sync_required(profile):
            return {
                "status": "skipped",
                "reason": "profile_skip_source_sync",
            }
        dsn = str(config.NEON_DATABASE_URL or "").strip()
        if not dsn:
            raise RuntimeError("source_sync requires NEON_DATABASE_URL for Neon-authoritative profiles.")
        local_source_dates: dict[str, Any] | None = None
        neon_source_dates: dict[str, Any] | None = None
        try:
            with core_reads.core_read_backend("local"):
                local_source_dates = core_reads.load_source_dates()
            with core_reads.core_read_backend("neon"):
                neon_source_dates = core_reads.load_source_dates()
        except Exception:  # noqa: BLE001
            logger.warning("Could not compare local vs Neon source dates before source_sync.", exc_info=True)
        older_than_neon: list[str] = []
        ignored_newer_than_target: list[str] = []
        pit_latest_closed_anchor = _latest_closed_period_anchor(
            str(as_of_date),
            frequency=str(config.SOURCE_DAILY_PIT_FREQUENCY or "monthly").strip().lower(),
        )
        if isinstance(local_source_dates, dict) and isinstance(neon_source_dates, dict):
            for field in ("prices_asof", "fundamentals_asof", "classification_asof"):
                local_value = str(local_source_dates.get(field) or "").strip()
                neon_value = str(neon_source_dates.get(field) or "").strip()
                allowed_ceiling = str(as_of_date)
                if field in {"fundamentals_asof", "classification_asof"}:
                    allowed_ceiling = pit_latest_closed_anchor
                if local_value and neon_value and neon_value > allowed_ceiling:
                    ignored_newer_than_target.append(field)
                    continue
                if local_value and neon_value and local_value < neon_value:
                    older_than_neon.append(field)
        if older_than_neon:
            raise RuntimeError(
                "source_sync refused to overwrite newer Neon source tables from an older local archive: "
                + ", ".join(sorted(older_than_neon))
            )
        if progress_callback is not None:
            progress_callback({"message": "Syncing retained source/model window into Neon", "progress_kind": "io"})
        out = run_neon_mirror_cycle(
            sqlite_path=data_db,
            cache_path=cache_db,
            dsn=dsn,
            mode=str(config.NEON_AUTO_SYNC_MODE or "incremental"),
            tables=[
                "security_master",
                "security_prices_eod",
                "security_fundamentals_pit",
                "security_classification_pit",
            ],
            parity_enabled=False,
            prune_enabled=False,
            source_years=int(config.NEON_SOURCE_RETENTION_YEARS),
            analytics_years=int(config.NEON_ANALYTICS_RETENTION_YEARS),
        )
        if str(out.get("status") or "") != "ok":
            raise RuntimeError(f"source_sync stage failed: {out}")
        return {
            "status": "ok",
            "local_source_dates": local_source_dates,
            "neon_source_dates_before_sync": neon_source_dates,
            "ignored_newer_than_target": ignored_newer_than_target,
            "source_sync": out,
        }

    if stage == "neon_readiness":
        if not profile_neon_readiness_required(profile):
            return {
                "status": "skipped",
                "reason": "profile_skip_neon_readiness",
            }
        if not should_run_core:
            return {
                "status": "skipped",
                "reason": f"core_policy_skip_{core_reason}",
            }
        root = Path(workspace_root or (Path(config.APP_DATA_DIR) / "neon_rebuild_workspace" / "adhoc"))
        if progress_callback is not None:
            progress_callback({"message": "Preparing Neon-authoritative scratch workspace", "progress_kind": "io"})
        out = neon_authority.prepare_neon_rebuild_workspace(
            profile=profile,
            workspace_root=root,
            dsn=(str(config.NEON_DATABASE_URL).strip() or None),
            analytics_years=int(config.NEON_ANALYTICS_RETENTION_YEARS),
        )
        return {
            "status": "ok",
            **out,
        }

    if stage == "raw_history":
        if str(raw_history_policy or "none") == "none":
            return {
                "status": "skipped",
                "reason": "profile_skip_raw_history_rebuild",
            }
        frequency = "daily" if str(raw_history_policy) == "full-daily" else "weekly"
        out = rebuild_raw_cross_section_history(
            data_db,
            frequency=frequency,
            progress_callback=progress_callback,
        )
        if str(out.get("status") or "") != "ok":
            raise RuntimeError(f"raw_history stage failed: {out}")
        if int(out.get("rows_upserted") or 0) <= 0:
            raise RuntimeError("raw_history stage produced zero rows")
        return {
            "status": "ok",
            "raw_history_policy": str(raw_history_policy),
            "raw_history": out,
        }

    if stage == "feature_build":
        if progress_callback is not None:
            progress_callback({"message": "Rebuilding cross-section snapshot", "progress_kind": "stage"})
        out = rebuild_cross_section_snapshot(
            data_db,
            mode=str(config.CROSS_SECTION_SNAPSHOT_MODE or "current"),
        )
        return {
            "status": "ok",
            "snapshot": out,
        }

    if stage == "estu_audit":
        if progress_callback is not None:
            progress_callback({"message": "Recomputing ESTU membership", "progress_kind": "stage"})
        out = build_and_persist_estu_membership(
            db_path=data_db,
            as_of_date=as_of_date,
        )
        return {
            "status": str(out.get("status") or "ok"),
            "estu": out,
        }

    if stage == "factor_returns":
        if not should_run_core:
            return {
                "status": "skipped",
                "reason": f"core_policy_skip_{core_reason}",
            }
        reset_summary = _reset_core_caches(cache_db) if reset_core_cache else {}
        if progress_callback is not None and reset_core_cache:
            progress_callback(
                {
                    "message": "Resetting factor-return and residual caches before rebuild",
                    "progress_kind": "stage",
                    "cache_rows_cleared": reset_summary,
                }
            )
        df = compute_daily_factor_returns(
            data_db,
            cache_db,
            min_cross_section_age_days=config.CROSS_SECTION_MIN_AGE_DAYS,
            progress_callback=progress_callback,
        )
        if df is None or df.empty:
            raise RuntimeError("factor_returns stage produced zero rows")
        return {
            "status": "ok",
            "factor_return_rows_loaded": int(len(df)),
            "core_cache_reset": bool(reset_core_cache),
            "cache_rows_cleared": reset_summary,
        }

    if stage == "risk_model":
        if not should_run_core:
            return {
                "status": "skipped",
                "reason": f"core_policy_skip_{core_reason}",
            }
        if progress_callback is not None:
            progress_callback({"message": "Building factor covariance matrix", "progress_kind": "stage"})
        cov, latest_r2 = build_factor_covariance_from_cache(
            cache_db,
            lookback_days=config.LOOKBACK_DAYS,
        )
        if progress_callback is not None:
            progress_callback({"message": "Building specific-risk model", "progress_kind": "stage"})
        specific_risk = build_specific_risk_from_cache(
            cache_db,
            lookback_days=config.LOOKBACK_DAYS,
        )
        if cov is None or cov.empty:
            raise RuntimeError("risk_model stage produced empty covariance matrix")
        if not isinstance(specific_risk, dict) or len(specific_risk) == 0:
            raise RuntimeError("risk_model stage produced empty specific-risk map")
        risk_engine_meta = {
            "status": "ok",
            "method_version": RISK_ENGINE_METHOD_VERSION,
            "last_recompute_date": previous_or_same_xnys_session(
                datetime.now(timezone.utc).date().isoformat()
            ),
            "factor_returns_latest_date": _latest_factor_return_date(CACHE_DB),
            "lookback_days": int(config.LOOKBACK_DAYS),
            "cross_section_min_age_days": int(config.CROSS_SECTION_MIN_AGE_DAYS),
            "recompute_interval_days": int(config.RISK_RECOMPUTE_INTERVAL_DAYS),
            "latest_r2": float(latest_r2 if np.isfinite(latest_r2) else 0.0),
            "specific_risk_ticker_count": int(len(specific_risk)),
            "recompute_reason": "force_core" if force_core else core_reason,
        }
        if Path(cache_db).resolve() != CACHE_DB.resolve():
            with _temporary_runtime_paths(data_db=data_db, cache_db=cache_db):
                sqlite.cache_set("risk_engine_cov", _serialize_covariance(cov))
                sqlite.cache_set("risk_engine_specific_risk", specific_risk)
                sqlite.cache_set("risk_engine_meta", risk_engine_meta)
        else:
            sqlite.cache_set("risk_engine_cov", _serialize_covariance(cov))
            sqlite.cache_set("risk_engine_specific_risk", specific_risk)
            sqlite.cache_set("risk_engine_meta", risk_engine_meta)
        if progress_callback is not None:
            progress_callback(
                {
                    "message": "Published risk-engine payloads to cache",
                    "progress_kind": "stage",
                    "factor_count": int(cov.shape[1]) if cov is not None and not cov.empty else 0,
                    "specific_risk_ticker_count": int(len(specific_risk)),
                }
            )
        return {
            "status": "ok",
            "factor_count": int(cov.shape[1]) if cov is not None and not cov.empty else 0,
            "specific_risk_ticker_count": int(len(specific_risk)),
            "risk_engine_meta": risk_engine_meta,
        }

    if stage == "serving_refresh":
        if progress_callback is not None:
            progress_callback({"message": "Publishing serving payloads", "progress_kind": "stage"})
        def _run_refresh_inner() -> dict[str, Any]:
            skip_risk_engine, skip_reason = _serving_refresh_skip_risk_engine(
                today_utc=datetime.fromisoformat(
                    previous_or_same_xnys_session(datetime.now(timezone.utc).date().isoformat())
                ).date()
            )
            if prefer_local_source_archive:
                with core_reads.core_read_backend("local"):
                    out = run_refresh(
                        mode=serving_mode,
                        force_risk_recompute=False,
                        refresh_scope=refresh_scope,
                        skip_snapshot_rebuild=True,
                        skip_cuse4_foundation=True,
                        skip_risk_engine=bool(skip_risk_engine),
                    )
                    out["_skip_risk_engine_reason"] = str(skip_reason)
                    out["_skip_risk_engine"] = bool(skip_risk_engine)
                    return out
            out = run_refresh(
                mode=serving_mode,
                force_risk_recompute=False,
                refresh_scope=refresh_scope,
                skip_snapshot_rebuild=True,
                skip_cuse4_foundation=True,
                skip_risk_engine=bool(skip_risk_engine),
            )
            out["_skip_risk_engine_reason"] = str(skip_reason)
            out["_skip_risk_engine"] = bool(skip_risk_engine)
            return out

        if Path(data_db).resolve() != DATA_DB.resolve() or Path(cache_db).resolve() != CACHE_DB.resolve():
            with _temporary_runtime_paths(data_db=data_db, cache_db=cache_db):
                out = _run_refresh_inner()
        else:
            out = _run_refresh_inner()
        return {
            "status": str(out.get("status") or "ok"),
            "serving_mode": serving_mode,
            "skip_risk_engine": bool(out.get("_skip_risk_engine")),
            "skip_risk_engine_reason": str(out.get("_skip_risk_engine_reason") or ""),
            "refresh": out,
        }

    raise ValueError(f"Unknown stage: {stage}")


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
    prefer_local_source_archive = _profile_prefers_local_source_archive(profile_key)
    effective_run_id = (
        str(resume_run_id).strip()
        if resume_run_id and str(resume_run_id).strip()
        else (str(run_id).strip() if run_id and str(run_id).strip() else f"job_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    )
    job_runs.ensure_schema(DATA_DB)
    job_runs.fail_stale_running_stages(db_path=DATA_DB)
    completed = job_runs.completed_stages(db_path=DATA_DB, run_id=effective_run_id) if resume_run_id else set()

    if _selected_stages_include_ingest(selected) and not (as_of_date and str(as_of_date).strip()):
        as_of = _current_xnys_session()
    elif _selected_stages_require_source_as_of(selected):
        as_of = _resolved_as_of_date(
            as_of_date,
            prefer_local_source_archive=prefer_local_source_archive,
        )
    elif as_of_date and str(as_of_date).strip():
        as_of = previous_or_same_xnys_session(str(as_of_date).strip())
    else:
        as_of = _current_xnys_session()
    today_utc = datetime.fromisoformat(_current_xnys_session()).date()
    due, due_reason = _risk_recompute_due(
        sqlite.cache_get_live_first("risk_engine_meta") or {},
        today_utc=today_utc,
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

    stage_results: list[dict[str, Any]] = []
    overall_status = "ok"
    workspace_paths: neon_authority.WorkspacePaths | None = None
    local_mirror_sync: dict[str, Any] = {"status": "skipped", "reason": "no_workspace"}
    neon_mirror_sqlite_path = DATA_DB
    neon_mirror_cache_path = CACHE_DB
    neon_mirror: dict[str, Any] = {
        "status": "skipped",
        "reason": "NEON_AUTO_SYNC_ENABLED=false",
    }
    total_stages = len(selected)
    for idx, stage in enumerate(selected, start=1):
        stage_t0 = time.perf_counter()
        logger.info("Starting stage %s/%s: %s", idx, total_stages, stage)
        stage_order = STAGES.index(stage) + 1
        if stage in completed:
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
        job_runs.begin_stage(
            db_path=DATA_DB,
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
                job_runs.heartbeat_stage(
                    db_path=DATA_DB,
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
            stage_data_db = DATA_DB
            stage_cache_db = CACHE_DB
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
                stage_workspace_root = Path(config.APP_DATA_DIR) / "neon_rebuild_workspace" / effective_run_id

            out = _run_stage(
                profile=profile_key,
                stage=stage,
                as_of_date=as_of,
                should_run_core=bool(should_run_core),
                serving_mode=str(cfg["serving_mode"]),
                data_db=stage_data_db,
                cache_db=stage_cache_db,
                refresh_scope=(str(refresh_scope).strip().lower() if refresh_scope else None),
                force_core=bool(force_core),
                core_reason=str(core_reason),
                raw_history_policy=raw_history_policy,
                reset_core_cache=reset_core_cache,
                enable_ingest=bool(cfg.get("enable_ingest")),
                prefer_local_source_archive=prefer_local_source_archive,
                workspace_root=stage_workspace_root,
                progress_callback=_emit_stage_event,
            )
            if stage == "neon_readiness" and str(out.get("status") or "") == "ok":
                workspace_payload = dict(out.get("workspace") or {})
                workspace_root = Path(str(workspace_payload.get("root_dir") or "")).expanduser().resolve()
                workspace_data_db = Path(str(workspace_payload.get("data_db") or "")).expanduser().resolve()
                workspace_cache_db = Path(str(workspace_payload.get("cache_db") or "")).expanduser().resolve()
                workspace_paths = neon_authority.WorkspacePaths(
                    root_dir=workspace_root,
                    data_db=workspace_data_db,
                    cache_db=workspace_cache_db,
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
            job_runs.finish_stage(
                db_path=DATA_DB,
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
            job_runs.finish_stage(
                db_path=DATA_DB,
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

    neon_sync_enabled = bool(
        str(config.NEON_DATABASE_URL or "").strip()
        and (
            config.neon_auto_sync_enabled_effective()
            or profile_source_sync_required(profile_key, cfg=cfg)
        )
    )
    neon_parity_enabled = bool(config.neon_auto_parity_enabled_effective())
    neon_prune_enabled = bool(config.neon_auto_prune_enabled_effective())
    broad_neon_mirror_enabled = _profile_runs_broad_neon_mirror(profile_key)
    neon_mirror_required = bool(config.neon_mirror_health_required())
    serving_payload_neon_failure = _extract_serving_payload_neon_failure(stage_results)

    if overall_status == "ok" and neon_sync_enabled and broad_neon_mirror_enabled:
        try:
            logger.info(
                "Running Neon mirror cycle: mode=%s parity=%s prune=%s source_years=%s analytics_years=%s",
                config.NEON_AUTO_SYNC_MODE,
                neon_parity_enabled,
                neon_prune_enabled,
                int(config.NEON_SOURCE_RETENTION_YEARS),
                int(config.NEON_ANALYTICS_RETENTION_YEARS),
            )
            neon_mirror = run_neon_mirror_cycle(
                sqlite_path=neon_mirror_sqlite_path,
                cache_path=neon_mirror_cache_path,
                dsn=(str(config.NEON_DATABASE_URL).strip() or None),
                mode=str(config.NEON_AUTO_SYNC_MODE or "incremental"),
                tables=(list(config.NEON_AUTO_SYNC_TABLES) or None),
                parity_enabled=neon_parity_enabled,
                prune_enabled=neon_prune_enabled,
                source_years=int(config.NEON_SOURCE_RETENTION_YEARS),
                analytics_years=int(config.NEON_ANALYTICS_RETENTION_YEARS),
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
            "runtime_role": str(config.APP_RUNTIME_ROLE),
            "auto_sync_enabled": bool(config.NEON_AUTO_SYNC_ENABLED),
            "profile": profile_key,
        }
        if neon_mirror_required:
            overall_status = "failed"

    if overall_status == "ok" and workspace_paths is not None:
        try:
            local_mirror_sync = neon_authority.sync_workspace_derivatives_to_local_mirror(
                workspace_data_db=workspace_paths.data_db,
                workspace_cache_db=workspace_paths.cache_db,
                local_data_db=DATA_DB,
                local_cache_db=CACHE_DB,
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
        and (bool(config.NEON_AUTO_SYNC_ENABLED) or neon_mirror_required)
    )
    if should_publish_neon_mirror_status:
        try:
            neon_artifact_path = _write_neon_mirror_artifact(
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
            _publish_neon_sync_health(
                run_id=effective_run_id,
                profile=profile_key,
                as_of_date=as_of,
                neon_mirror=neon_mirror,
                artifact_path=neon_artifact_path,
            )
        except Exception:  # noqa: BLE001
            logger.exception("Failed to publish Neon sync health status")
    elif serving_payload_neon_failure is not None and config.serving_payload_neon_write_required():
        try:
            _publish_neon_serving_write_health(
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
        mark_refresh_finished(
            profile=profile_key,
            run_id=effective_run_id,
            status=("ok" if overall_status == "ok" else "failed"),
            message="Serving outputs refreshed" if serving_completed and overall_status == "ok" else "Refresh finished",
            clear_pending=bool(serving_completed and overall_status == "ok"),
        )
    except Exception:  # noqa: BLE001
        logger.exception("Failed to update holdings refresh state")

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
