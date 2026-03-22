"""Core compute-stage helpers for the model pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np

def run_core_stage(
    *,
    profile: str,
    run_id: str,
    stage: str,
    as_of_date: str,
    should_run_core: bool,
    force_core: bool,
    core_reason: str,
    data_db: Path,
    cache_db: Path,
    raw_history_policy: str = "none",
    reset_core_cache: bool = False,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    config_module,
    core_reads_module,
    sqlite_module,
    persist_model_outputs_fn: Callable[..., dict[str, Any]],
    rebuild_raw_cross_section_history_fn: Callable[..., Any],
    rebuild_cross_section_snapshot_fn: Callable[..., Any],
    build_and_persist_estu_membership_fn: Callable[..., Any],
    reset_core_caches_fn: Callable[..., dict[str, int]],
    compute_daily_factor_returns_fn: Callable[..., Any],
    build_factor_covariance_from_cache_fn: Callable[..., Any],
    build_specific_risk_from_cache_fn: Callable[..., Any],
    latest_factor_return_date_fn: Callable[..., str | None],
    serialize_covariance_fn: Callable[..., dict[str, Any]],
    previous_or_same_xnys_session_fn: Callable[[str], str],
    risk_engine_method_version: str,
) -> dict[str, Any]:
    if stage == "raw_history":
        if str(raw_history_policy or "none") == "none":
            return {
                "status": "skipped",
                "reason": "profile_skip_raw_history_rebuild",
            }
        frequency = "daily" if str(raw_history_policy) == "full-daily" else "weekly"
        out = rebuild_raw_cross_section_history_fn(
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
        out = rebuild_cross_section_snapshot_fn(
            data_db,
            mode=str(config_module.CROSS_SECTION_SNAPSHOT_MODE or "current"),
        )
        return {
            "status": "ok",
            "snapshot": out,
        }

    if stage == "estu_audit":
        if progress_callback is not None:
            progress_callback({"message": "Recomputing ESTU membership", "progress_kind": "stage"})
        out = build_and_persist_estu_membership_fn(
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
        reset_summary = reset_core_caches_fn(cache_db) if reset_core_cache else {}
        if progress_callback is not None and reset_core_cache:
            progress_callback(
                {
                    "message": "Resetting factor-return and residual caches before rebuild",
                    "progress_kind": "stage",
                    "cache_rows_cleared": reset_summary,
                }
            )
        df = compute_daily_factor_returns_fn(
            data_db,
            cache_db,
            min_cross_section_age_days=config_module.CROSS_SECTION_MIN_AGE_DAYS,
            progress_callback=progress_callback,
        )
        if df is None or getattr(df, "empty", False):
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
        cov, latest_r2 = build_factor_covariance_from_cache_fn(
            cache_db,
            lookback_days=config_module.LOOKBACK_DAYS,
        )
        if progress_callback is not None:
            progress_callback({"message": "Building specific-risk model", "progress_kind": "stage"})
        specific_risk = build_specific_risk_from_cache_fn(
            cache_db,
            lookback_days=config_module.LOOKBACK_DAYS,
        )
        if cov is None or cov.empty:
            raise RuntimeError("risk_model stage produced empty covariance matrix")
        if not isinstance(specific_risk, dict) or len(specific_risk) == 0:
            raise RuntimeError("risk_model stage produced empty specific-risk map")
        recompute_date = previous_or_same_xnys_session_fn(
            datetime.now(timezone.utc).date().isoformat()
        )
        latest_factor_return_date = latest_factor_return_date_fn(cache_db)
        estimation_exposure_anchor_date = None
        if latest_factor_return_date:
            lag_days = max(0, int(config_module.CROSS_SECTION_MIN_AGE_DAYS))
            shifted = (
                datetime.fromisoformat(str(latest_factor_return_date)).date() - timedelta(days=lag_days)
            )
            estimation_exposure_anchor_date = previous_or_same_xnys_session_fn(shifted.isoformat())
        risk_engine_meta = {
            "status": "ok",
            "method_version": risk_engine_method_version,
            "last_recompute_date": recompute_date,
            "factor_returns_latest_date": latest_factor_return_date,
            "estimation_exposure_anchor_date": estimation_exposure_anchor_date,
            "lookback_days": int(config_module.LOOKBACK_DAYS),
            "cross_section_min_age_days": int(config_module.CROSS_SECTION_MIN_AGE_DAYS),
            "recompute_interval_days": int(config_module.RISK_RECOMPUTE_INTERVAL_DAYS),
            "latest_r2": float(latest_r2 if np.isfinite(latest_r2) else 0.0),
            "specific_risk_ticker_count": int(len(specific_risk)),
            "recompute_reason": "force_core" if force_core else core_reason,
        }
        sqlite_module.cache_set("risk_engine_cov", serialize_covariance_fn(cov), db_path=cache_db)
        sqlite_module.cache_set("risk_engine_specific_risk", specific_risk, db_path=cache_db)
        sqlite_module.cache_set("risk_engine_meta", risk_engine_meta, db_path=cache_db)
        model_outputs_write = persist_model_outputs_fn(
            data_db=data_db,
            cache_db=cache_db,
            run_id=run_id,
            refresh_mode=profile,
            status="ok",
            started_at=datetime.now(timezone.utc).isoformat(),
            completed_at=datetime.now(timezone.utc).isoformat(),
            source_dates=core_reads_module.load_source_dates(data_db=data_db),
            params={
                "profile": str(profile),
                "force_core": bool(force_core),
                "core_reason": str(core_reason),
                "lookback_days": int(config_module.LOOKBACK_DAYS),
                "cross_section_min_age_days": int(config_module.CROSS_SECTION_MIN_AGE_DAYS),
                "risk_recompute_interval_days": int(config_module.RISK_RECOMPUTE_INTERVAL_DAYS),
            },
            risk_engine_state=risk_engine_meta,
            cov=cov,
            specific_risk_by_ticker=specific_risk,
        )
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
            "model_outputs_write": model_outputs_write,
        }

    raise ValueError(f"Unsupported core stage: {stage}")
