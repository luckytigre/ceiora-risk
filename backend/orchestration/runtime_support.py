"""Runtime-policy helpers for refresh and rebuild orchestration."""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np

from backend import config
from backend.analytics import pipeline as analytics_pipeline
from backend.analytics.refresh_policy import latest_factor_return_date, risk_recompute_due
from backend.data import core_reads, model_outputs, sqlite
from backend.orchestration.profiles import profile_source_sync_required


def serialize_covariance(cov) -> dict[str, Any]:
    if cov is None or cov.empty:
        return {"factors": [], "matrix": []}
    factors = [str(c) for c in cov.columns]
    mat = cov.reindex(index=factors, columns=factors).to_numpy(dtype=float)
    return {
        "factors": factors,
        "matrix": [[float(v) for v in row] for row in mat.tolist()],
    }


def _risk_artifacts_ready(cov_payload: Any, specific_payload: Any) -> bool:
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


def risk_cache_ready(
    *,
    cache_db: Path | None = None,
    sqlite_module=sqlite,
    persisted_cov_loader=None,
    persisted_specific_loader=None,
) -> bool:
    cov_payload = sqlite_module.cache_get_live_first("risk_engine_cov", db_path=cache_db)
    specific_payload = sqlite_module.cache_get_live_first("risk_engine_specific_risk", db_path=cache_db)
    if _risk_artifacts_ready(cov_payload, specific_payload):
        return True

    cov_loader = persisted_cov_loader or model_outputs.load_latest_rebuild_authority_covariance_payload
    specific_loader = persisted_specific_loader or model_outputs.load_latest_rebuild_authority_specific_risk_payload
    try:
        persisted_cov_payload = cov_loader()
    except Exception:
        persisted_cov_payload = {}
    try:
        persisted_specific_payload = specific_loader()
    except Exception:
        persisted_specific_payload = {}
    return _risk_artifacts_ready(persisted_cov_payload, persisted_specific_payload)


def resolve_effective_risk_engine_meta(
    *,
    cache_db: Path | None = None,
    sqlite_module=sqlite,
    resolve_effective_risk_engine_meta_fn=None,
) -> tuple[dict[str, Any], str]:
    if resolve_effective_risk_engine_meta_fn is None:
        resolve_effective_risk_engine_meta_fn = analytics_pipeline._resolve_effective_risk_engine_meta
    return resolve_effective_risk_engine_meta_fn(
        fallback_loader=lambda key: sqlite_module.cache_get_live_first(key, db_path=cache_db),
    )


def serving_refresh_skip_risk_engine(
    *,
    today_utc: date,
    cache_db: Path | None = None,
    sqlite_module=sqlite,
    resolve_effective_risk_engine_meta_fn=None,
) -> tuple[bool, str]:
    if not risk_cache_ready(cache_db=cache_db, sqlite_module=sqlite_module):
        return False, "risk_cache_missing"
    risk_engine_meta, _ = resolve_effective_risk_engine_meta(
        cache_db=cache_db,
        sqlite_module=sqlite_module,
        resolve_effective_risk_engine_meta_fn=resolve_effective_risk_engine_meta_fn,
    )
    should_recompute, recompute_reason = risk_recompute_due(
        risk_engine_meta,
        today_utc=today_utc,
        method_version=analytics_pipeline.RISK_ENGINE_METHOD_VERSION,
        interval_days=config.RISK_RECOMPUTE_INTERVAL_DAYS,
    )
    if should_recompute:
        return False, f"core_due_{recompute_reason}"
    return True, "risk_cache_current"


def profile_prefers_local_source_archive(profile: str) -> bool:
    if profile_source_sync_required(profile):
        return False
    return bool(
        config.runtime_role_allows_ingest()
        and str(profile or "").strip().lower() != "publish-only"
    )

def reset_core_caches(cache_db: Path) -> dict[str, int]:
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


def profile_runs_broad_neon_mirror(profile: str) -> bool:
    return str(profile or "").strip().lower() in {
        "source-daily",
        "source-daily-plus-core-if-due",
        "core-weekly",
        "cold-core",
        "universe-add",
    }
