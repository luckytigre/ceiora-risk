"""Serving-stage helpers for the model pipeline."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path
import sys
import time
from typing import Any, Callable


def _memory_high_water_mb() -> float | None:
    try:
        import resource
    except ImportError:
        return None
    usage = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss or 0.0)
    if usage <= 0.0:
        return None
    divisor = 1024.0 * 1024.0 if sys.platform == "darwin" else 1024.0
    return round(usage / divisor, 2)


def run_serving_stage(
    *,
    stage: str,
    should_run_core: bool,
    upstream_core_recomputed: bool = False,
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
        or upstream_core_recomputed
    )
    uses_workspace_paths = bool(
        Path(data_db).resolve() != canonical_data_db.resolve()
        or Path(cache_db).resolve() != canonical_cache_db.resolve()
    )
    neon_core_read_session = getattr(core_reads_module, "neon_core_read_session", None)

    def _neon_core_session():
        if callable(neon_core_read_session):
            return neon_core_read_session()
        return nullcontext()

    def _run_refresh_inner() -> dict[str, Any]:
        today_utc = datetime.fromisoformat(
            previous_or_same_xnys_session_fn(datetime.now(timezone.utc).date().isoformat())
        ).date()
        skip_risk_engine, skip_reason = serving_refresh_skip_risk_engine_fn(
            today_utc=today_utc,
            cache_db=cache_db,
        )
        enforce_stable_core_package = not bool(upstream_core_recomputed)
        if enforce_stable_core_package and not skip_risk_engine:
            raise RuntimeError(
                "serve-refresh requires a current stable core package and will not recompute "
                f"core artifacts on the serving path ({skip_reason}). Run source-daily-plus-core-if-due, "
                "core-weekly, or cold-core instead."
            )
        if force_local_core_reads:
            with core_reads_module.core_read_backend("local"):
                with _neon_core_session():
                    out = run_refresh_fn(
                        data_db=data_db,
                        cache_db=cache_db,
                        mode=serving_mode,
                        force_risk_recompute=False,
                        refresh_scope=refresh_scope,
                        skip_snapshot_rebuild=True,
                        skip_cuse4_foundation=True,
                        skip_risk_engine=bool(skip_risk_engine),
                        enforce_stable_core_package=enforce_stable_core_package,
                        upstream_core_recomputed=bool(upstream_core_recomputed),
                        refresh_projected_loadings=bool(should_run_core),
                        refresh_deep_health_diagnostics=bool(should_run_core),
                        prefer_local_source_archive=bool(prefer_local_source_archive),
                    )
                out["_skip_risk_engine_reason"] = str(skip_reason)
                out["_skip_risk_engine"] = bool(skip_risk_engine)
                return out
        with _neon_core_session():
            out = run_refresh_fn(
                data_db=data_db,
                cache_db=cache_db,
                mode=serving_mode,
                force_risk_recompute=False,
                refresh_scope=refresh_scope,
                skip_snapshot_rebuild=True,
                skip_cuse4_foundation=True,
                skip_risk_engine=bool(skip_risk_engine),
                enforce_stable_core_package=enforce_stable_core_package,
                upstream_core_recomputed=bool(upstream_core_recomputed),
                refresh_projected_loadings=bool(should_run_core),
                refresh_deep_health_diagnostics=bool(should_run_core),
                prefer_local_source_archive=bool(prefer_local_source_archive),
                uses_workspace_paths=bool(uses_workspace_paths),
            )
        out["_skip_risk_engine_reason"] = str(skip_reason)
        out["_skip_risk_engine"] = bool(skip_risk_engine)
        return out

    stage_t0 = time.perf_counter()
    out = _run_refresh_inner()
    total_seconds = time.perf_counter() - stage_t0
    rows_written = int(
        out.get("published_payload_count")
        or out.get("payload_count")
        or 0
    )
    return {
        "status": str(out.get("status") or "ok"),
        "serving_mode": serving_mode,
        "skip_risk_engine": bool(out.get("_skip_risk_engine")),
        "skip_risk_engine_reason": str(out.get("_skip_risk_engine_reason") or ""),
        "refresh": out,
        "metrics": {
            "compute_seconds": round(float(total_seconds), 3),
            "rows_written": int(rows_written),
            "memory_high_water_mb": _memory_high_water_mb(),
        },
    }
