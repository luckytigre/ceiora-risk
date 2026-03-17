"""Serving-stage helpers for the model pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


def run_serving_stage(
    *,
    stage: str,
    should_run_core: bool,
    serving_mode: str,
    data_db: Path,
    cache_db: Path,
    prefer_local_source_archive: bool = False,
    refresh_scope: str | None = None,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    core_reads_module,
    serving_refresh_skip_risk_engine_fn: Callable[..., tuple[bool, str]],
    run_refresh_fn: Callable[..., dict[str, Any]],
    previous_or_same_xnys_session_fn: Callable[[str], str],
    canonical_data_db: Path,
    canonical_cache_db: Path,
) -> dict[str, Any]:
    if stage != "serving_refresh":
        raise ValueError(f"Unsupported serving stage: {stage}")

    if progress_callback is not None:
        progress_callback({"message": "Publishing serving payloads", "progress_kind": "stage"})

    force_local_core_reads = bool(
        prefer_local_source_archive
        or should_run_core
        or Path(data_db).resolve() != canonical_data_db.resolve()
        or Path(cache_db).resolve() != canonical_cache_db.resolve()
    )

    def _run_refresh_inner() -> dict[str, Any]:
        today_utc = datetime.fromisoformat(
            previous_or_same_xnys_session_fn(datetime.now(timezone.utc).date().isoformat())
        ).date()
        skip_risk_engine, skip_reason = serving_refresh_skip_risk_engine_fn(
            today_utc=today_utc,
            cache_db=cache_db,
        )
        if force_local_core_reads:
            with core_reads_module.core_read_backend("local"):
                out = run_refresh_fn(
                    data_db=data_db,
                    cache_db=cache_db,
                    mode=serving_mode,
                    force_risk_recompute=False,
                    refresh_scope=refresh_scope,
                    skip_snapshot_rebuild=True,
                    skip_cuse4_foundation=True,
                    skip_risk_engine=bool(skip_risk_engine),
                    refresh_deep_health_diagnostics=bool(should_run_core),
                )
                out["_skip_risk_engine_reason"] = str(skip_reason)
                out["_skip_risk_engine"] = bool(skip_risk_engine)
                return out
        out = run_refresh_fn(
            data_db=data_db,
            cache_db=cache_db,
            mode=serving_mode,
            force_risk_recompute=False,
            refresh_scope=refresh_scope,
            skip_snapshot_rebuild=True,
            skip_cuse4_foundation=True,
            skip_risk_engine=bool(skip_risk_engine),
            refresh_deep_health_diagnostics=bool(should_run_core),
        )
        out["_skip_risk_engine_reason"] = str(skip_reason)
        out["_skip_risk_engine"] = bool(skip_risk_engine)
        return out

    out = _run_refresh_inner()
    return {
        "status": str(out.get("status") or "ok"),
        "serving_mode": serving_mode,
        "skip_risk_engine": bool(out.get("_skip_risk_engine")),
        "skip_risk_engine_reason": str(out.get("_skip_risk_engine_reason") or ""),
        "refresh": out,
    }
