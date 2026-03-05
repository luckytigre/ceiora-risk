#!/usr/bin/env python3
"""Profile-driven model pipeline orchestrator with stage checkpoints."""

from __future__ import annotations

import argparse
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from backend import config
from backend.analytics.pipeline import RISK_ENGINE_METHOD_VERSION, run_refresh
from backend.barra.covariance import build_factor_covariance_from_cache
from backend.barra.daily_factor_returns import compute_daily_factor_returns
from backend.barra.raw_cross_section_history import rebuild_raw_cross_section_history
from backend.barra.specific_risk import build_specific_risk_from_cache
from backend.cuse4.bootstrap import bootstrap_cuse4_source_tables
from backend.cuse4.estu import build_and_persist_estu_membership
from backend.db import job_runs, postgres, sqlite
from backend.db.cross_section_snapshot import rebuild_cross_section_snapshot
from backend.scripts.download_data_lseg import download_from_lseg
from backend.trading_calendar import previous_or_same_xnys_session


DATA_DB = Path(config.DATA_DB_PATH)
CACHE_DB = Path(config.SQLITE_PATH)

STAGES = [
    "ingest",
    "raw_history",
    "feature_build",
    "estu_audit",
    "factor_returns",
    "risk_model",
    "serving_refresh",
]

PROFILE_CONFIG: dict[str, dict[str, Any]] = {
    "daily-fast": {
        "core_policy": "never",
        "serving_mode": "light",
        "raw_history_policy": "none",
        "reset_core_cache": False,
    },
    "daily-with-core-if-due": {
        "core_policy": "due",
        "serving_mode": "light",
        "raw_history_policy": "none",
        "reset_core_cache": False,
    },
    "weekly-core": {
        "core_policy": "always",
        "serving_mode": "full",
        "raw_history_policy": "none",
        "reset_core_cache": False,
    },
    "cold-core": {
        "core_policy": "always",
        "serving_mode": "full",
        "raw_history_policy": "full-daily",
        "reset_core_cache": True,
    },
}


def _parse_iso_date(value: Any) -> date | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except (TypeError, ValueError):
        return None


def _risk_recompute_due(meta: dict[str, Any], *, today_utc: date) -> tuple[bool, str]:
    if not meta:
        return True, "missing_meta"
    if str(meta.get("method_version") or "") != RISK_ENGINE_METHOD_VERSION:
        return True, "method_version_change"
    last_recompute = _parse_iso_date(meta.get("last_recompute_date"))
    if last_recompute is None:
        return True, "missing_last_recompute_date"
    interval = max(1, int(config.RISK_RECOMPUTE_INTERVAL_DAYS))
    if (today_utc - last_recompute).days >= interval:
        return True, f"interval_elapsed_{interval}d"
    return False, "within_interval"


def _serialize_covariance(cov) -> dict[str, Any]:
    if cov is None or cov.empty:
        return {"factors": [], "matrix": []}
    factors = [str(c) for c in cov.columns]
    mat = cov.reindex(index=factors, columns=factors).to_numpy(dtype=float)
    return {
        "factors": factors,
        "matrix": [[float(v) for v in row] for row in mat.tolist()],
    }


def _latest_factor_return_date(cache_db: Path) -> str | None:
    conn = sqlite3.connect(str(cache_db))
    try:
        row = conn.execute("SELECT MAX(date) FROM daily_factor_returns").fetchone()
    except sqlite3.OperationalError:
        row = None
    finally:
        conn.close()
    if not row or not row[0]:
        return None
    return str(row[0])


def _risk_cache_ready() -> bool:
    cov_payload = sqlite.cache_get("risk_engine_cov")
    specific_payload = sqlite.cache_get("risk_engine_specific_risk")
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


def _resolved_as_of_date(user_as_of_date: str | None) -> str:
    if user_as_of_date and str(user_as_of_date).strip():
        return previous_or_same_xnys_session(str(user_as_of_date).strip())
    source_dates = postgres.load_source_dates()
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


def _run_stage(
    *,
    stage: str,
    as_of_date: str,
    should_run_core: bool,
    serving_mode: str,
    force_core: bool,
    core_reason: str,
    raw_history_policy: str = "none",
    reset_core_cache: bool = False,
) -> dict[str, Any]:
    if stage == "ingest":
        bootstrap = bootstrap_cuse4_source_tables(
            db_path=DATA_DB,
            replace_all=False,
        )
        if not bool(config.ORCHESTRATOR_ENABLE_INGEST):
            return {
                "status": "ok",
                "mode": "bootstrap_only",
                "reason": "ORCHESTRATOR_ENABLE_INGEST=false",
                "bootstrap": bootstrap,
            }
        ingest = download_from_lseg(
            db_path=DATA_DB,
            as_of_date=as_of_date,
            shard_count=int(config.ORCHESTRATOR_INGEST_SHARD_COUNT),
            shard_index=0,
            write_fundamentals=True,
            write_prices=True,
            write_classification=True,
        )
        return {
            "status": str(ingest.get("status") or "ok"),
            "mode": "bootstrap_plus_lseg_ingest",
            "bootstrap": bootstrap,
            "ingest": ingest,
        }

    if stage == "raw_history":
        if str(raw_history_policy or "none") == "none":
            return {
                "status": "skipped",
                "reason": "profile_skip_raw_history_rebuild",
            }
        frequency = "daily" if str(raw_history_policy) == "full-daily" else "weekly"
        out = rebuild_raw_cross_section_history(
            DATA_DB,
            frequency=frequency,
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
        out = rebuild_cross_section_snapshot(
            DATA_DB,
            mode=str(config.CROSS_SECTION_SNAPSHOT_MODE or "current"),
        )
        return {
            "status": "ok",
            "snapshot": out,
        }

    if stage == "estu_audit":
        out = build_and_persist_estu_membership(
            db_path=DATA_DB,
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
        reset_summary = _reset_core_caches(CACHE_DB) if reset_core_cache else {}
        df = compute_daily_factor_returns(
            DATA_DB,
            CACHE_DB,
            min_cross_section_age_days=config.CROSS_SECTION_MIN_AGE_DAYS,
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
        cov, latest_r2 = build_factor_covariance_from_cache(
            CACHE_DB,
            lookback_days=config.LOOKBACK_DAYS,
        )
        specific_risk = build_specific_risk_from_cache(
            CACHE_DB,
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
        sqlite.cache_set("risk_engine_cov", _serialize_covariance(cov))
        sqlite.cache_set("risk_engine_specific_risk", specific_risk)
        sqlite.cache_set("risk_engine_meta", risk_engine_meta)
        return {
            "status": "ok",
            "factor_count": int(cov.shape[1]) if cov is not None and not cov.empty else 0,
            "specific_risk_ticker_count": int(len(specific_risk)),
            "risk_engine_meta": risk_engine_meta,
        }

    if stage == "serving_refresh":
        skip_risk_engine = _risk_cache_ready()
        out = run_refresh(
            mode=serving_mode,
            force_risk_recompute=False,
            skip_snapshot_rebuild=True,
            skip_cuse4_foundation=True,
            skip_risk_engine=bool(skip_risk_engine),
        )
        return {
            "status": str(out.get("status") or "ok"),
            "serving_mode": serving_mode,
            "skip_risk_engine": bool(skip_risk_engine),
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
) -> dict[str, Any]:
    profile_key = str(profile).strip().lower()
    if profile_key not in PROFILE_CONFIG:
        raise ValueError(
            f"Unsupported profile '{profile}'. Expected one of: {', '.join(sorted(PROFILE_CONFIG))}"
        )

    cfg = PROFILE_CONFIG[profile_key]
    selected = _stage_window(from_stage, to_stage)
    effective_run_id = (
        str(resume_run_id).strip()
        if resume_run_id and str(resume_run_id).strip()
        else (str(run_id).strip() if run_id and str(run_id).strip() else f"job_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    )
    job_runs.ensure_schema(DATA_DB)
    completed = job_runs.completed_stages(db_path=DATA_DB, run_id=effective_run_id) if resume_run_id else set()

    as_of = _resolved_as_of_date(as_of_date)
    today_utc = datetime.fromisoformat(previous_or_same_xnys_session(datetime.now(timezone.utc).date().isoformat())).date()
    due, due_reason = _risk_recompute_due(sqlite.cache_get("risk_engine_meta") or {}, today_utc=today_utc)
    core_policy = str(cfg["core_policy"])
    raw_history_policy = str(cfg.get("raw_history_policy") or "none")
    reset_core_cache = bool(cfg.get("reset_core_cache"))
    should_run_core = bool(force_core or core_policy == "always" or (core_policy == "due" and due))
    core_reason = "force_core" if force_core else ("due" if should_run_core else due_reason)

    stage_results: list[dict[str, Any]] = []
    overall_status = "ok"
    for stage in selected:
        stage_order = STAGES.index(stage) + 1
        if stage in completed:
            stage_results.append(
                {
                    "stage": stage,
                    "status": "skipped",
                    "reason": "already_completed_in_resume_run",
                }
            )
            continue

        job_runs.begin_stage(
            db_path=DATA_DB,
            run_id=effective_run_id,
            profile=profile_key,
            stage_name=stage,
            stage_order=stage_order,
        )
        try:
            out = _run_stage(
                stage=stage,
                as_of_date=as_of,
                should_run_core=bool(should_run_core),
                serving_mode=str(cfg["serving_mode"]),
                force_core=bool(force_core),
                core_reason=str(core_reason),
                raw_history_policy=raw_history_policy,
                reset_core_cache=reset_core_cache,
            )
            stage_status = "skipped" if str(out.get("status")) == "skipped" else "completed"
            job_runs.finish_stage(
                db_path=DATA_DB,
                run_id=effective_run_id,
                stage_name=stage,
                status=stage_status,
                details=out,
            )
            stage_results.append(
                {
                    "stage": stage,
                    "status": stage_status,
                    "details": out,
                }
            )
        except Exception as exc:  # noqa: BLE001
            overall_status = "failed"
            err = {"type": type(exc).__name__, "message": str(exc)}
            job_runs.finish_stage(
                db_path=DATA_DB,
                run_id=effective_run_id,
                stage_name=stage,
                status="failed",
                details={},
                error=err,
            )
            stage_results.append(
                {
                    "stage": stage,
                    "status": "failed",
                    "error": err,
                }
            )
            break

    return {
        "status": overall_status,
        "run_id": effective_run_id,
        "profile": profile_key,
        "as_of_date": as_of,
        "core_policy": core_policy,
        "core_due": bool(due),
        "core_reason": core_reason,
        "core_will_run": bool(should_run_core),
        "raw_history_policy": raw_history_policy,
        "reset_core_cache": bool(reset_core_cache),
        "selected_stages": selected,
        "stage_results": stage_results,
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
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
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
