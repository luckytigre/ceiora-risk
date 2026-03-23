"""Analytics pipeline: fetch → compute → cache."""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from backend import config
from backend.analytics import health_payloads
from backend.analytics.health import compute_health_diagnostics
from backend.analytics.contracts import (
    ComponentSharesPayload,
    CovarianceMatrixPayload,
    CovariancePayload,
    ExposureModesPayload,
    FactorCoveragePayload,
    FactorDetailPayload,
    PositionPayload,
    PositionRiskMixPayload,
    RiskEngineMetaPayload,
    RiskSharesPayload,
    SnapshotBuildPayload,
    SourceDatesPayload,
    SpecificRiskPayload,
    UniverseLoadingsPayload,
)
from backend.analytics import publish_payloads, refresh_context, refresh_metadata, refresh_persistence, reuse_policy
from backend.analytics.services.cache_publisher import stage_refresh_cache_snapshot
from backend.analytics.services.risk_views import (
    build_positions_from_universe as _build_positions_from_universe_impl,
    compute_exposures_modes as _compute_exposures_modes_impl,
    compute_position_risk_mix as _compute_position_risk_mix_impl,
    compute_position_total_risk_contributions as _compute_position_total_risk_contributions_impl,
    specific_risk_by_ticker_view as _specific_risk_by_ticker_view_impl,
)
from backend.analytics.services.universe_loadings import (
    build_universe_ticker_loadings as _build_universe_ticker_loadings_impl,
    load_latest_factor_coverage as _load_latest_factor_coverage_impl,
)
from backend.data import core_reads, model_outputs, rebuild_cross_section_snapshot, runtime_state, serving_outputs, sqlite
from backend.risk_model.factor_catalog import (
    build_factor_catalog_for_factors,
    factor_id_to_entry_map,
    factor_name_to_id_map,
)
from backend.risk_model import (
    build_factor_covariance_from_cache,
    build_specific_risk_from_cache,
    compute_daily_factor_returns,
    risk_decomposition,
    vol_scaled_decomposition,
)
from backend.risk_model.projected_loadings import (
    compute_projected_loadings,
    latest_persisted_projection_asof,
    load_persisted_projected_loadings,
)
from backend.universe import bootstrap_cuse4_source_tables, build_and_persist_estu_membership
from backend.universe.security_master_sync import load_projection_only_universe_rows
from backend.trading_calendar import previous_or_same_xnys_session

logger = logging.getLogger(__name__)

DATA_DB = Path(config.DATA_DB_PATH)
CACHE_DB = Path(config.SQLITE_PATH)
RISK_ENGINE_METHOD_VERSION = refresh_context.RISK_ENGINE_METHOD_VERSION


def _resolve_data_db(data_db: Path | None = None) -> Path:
    return Path(data_db or DATA_DB).expanduser().resolve()


def _resolve_cache_db(cache_db: Path | None = None) -> Path:
    return Path(cache_db or CACHE_DB).expanduser().resolve()


def _resolve_effective_risk_engine_meta(
    *,
    fallback_loader,
) -> tuple[RiskEngineMetaPayload, str]:
    return refresh_context.resolve_effective_risk_engine_meta(
        fallback_loader=fallback_loader,
    )


def _can_reuse_cached_universe_loadings(
    cached_payload: Any,
    *,
    source_dates: SourceDatesPayload,
    risk_engine_meta: RiskEngineMetaPayload,
) -> tuple[bool, str]:
    return reuse_policy.can_reuse_cached_universe_loadings(
        cached_payload,
        source_dates=source_dates,
        risk_engine_meta=risk_engine_meta,
    )


def _load_cached_risk_display_payload(*, cache_db: Path | None = None) -> CovarianceMatrixPayload | None:
    return reuse_policy.load_cached_risk_display_payload(cache_db=cache_db)


def _emit_refresh_progress(
    progress_callback: Callable[[dict[str, Any]], None] | None,
    *,
    message: str,
    progress_kind: str = "analytics",
    **extra: Any,
) -> None:
    if progress_callback is None:
        return
    payload = {
        "message": str(message),
        "progress_kind": str(progress_kind),
    }
    payload.update(extra)
    progress_callback(payload)


def _serialize_covariance(cov: pd.DataFrame) -> CovariancePayload:
    if cov is None or cov.empty:
        return {"factors": [], "matrix": []}
    factors = [str(c) for c in cov.columns]
    mat = cov.reindex(index=factors, columns=factors).to_numpy(dtype=float)
    return {
        "factors": factors,
        "matrix": [[refresh_metadata.finite_float(v, 0.0) for v in row] for row in mat.tolist()],
    }


def _deserialize_covariance(payload: Any) -> pd.DataFrame:
    return reuse_policy.deserialize_covariance(payload)


def _covariance_factor_count(cov: pd.DataFrame | None) -> int:
    if cov is None or cov.empty:
        return 0
    return int(len(cov.columns))


def _specific_risk_entry_count(payload: dict[str, Any] | None) -> int:
    return int(len(payload)) if isinstance(payload, dict) else 0


def _persisted_risk_artifacts_are_richer(
    *,
    cached_cov: pd.DataFrame,
    cached_specific: dict[str, SpecificRiskPayload],
    persisted_cov_payload: dict[str, Any],
    persisted_specific_payload: dict[str, Any],
) -> bool:
    persisted_factor_count = int(len(persisted_cov_payload.get("factors") or [])) if isinstance(persisted_cov_payload, dict) else 0
    persisted_specific_count = _specific_risk_entry_count(persisted_specific_payload)
    cached_factor_count = _covariance_factor_count(cached_cov)
    cached_specific_count = _specific_risk_entry_count(cached_specific)
    return bool(
        (persisted_factor_count > 0 and cached_factor_count < persisted_factor_count)
        or (persisted_specific_count > 0 and cached_specific_count < persisted_specific_count)
    )


def _specific_risk_by_ticker_view(
    specific_risk_by_security: dict[str, SpecificRiskPayload] | None,
) -> dict[str, SpecificRiskPayload]:
    """Create a ticker-keyed view from a canonical security-keyed specific-risk map."""
    return _specific_risk_by_ticker_view_impl(specific_risk_by_security)


def _build_universe_ticker_loadings(
    exposures_df: pd.DataFrame,
    fundamentals_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    cov: pd.DataFrame,
    *,
    data_db: Path | None = None,
    specific_risk_by_ticker: dict[str, SpecificRiskPayload] | None = None,
    factor_catalog_by_name: dict[str, object] | None = None,
    projected_loadings: dict | None = None,
    projection_universe_rows: list[dict[str, str]] | None = None,
    projection_core_state_through_date: str | None = None,
) -> UniverseLoadingsPayload:
    """Build full-universe cached loadings/risk context keyed by ticker."""
    return _build_universe_ticker_loadings_impl(
        exposures_df,
        fundamentals_df,
        prices_df,
        cov,
        data_db=_resolve_data_db(data_db),
        specific_risk_by_ticker=specific_risk_by_ticker,
        factor_catalog_by_name=factor_catalog_by_name,
        projected_loadings=projected_loadings,
        projection_universe_rows=projection_universe_rows,
        projection_core_state_through_date=projection_core_state_through_date,
    )


def _build_positions_from_universe(
    universe_by_ticker: dict[str, dict[str, Any]],
) -> tuple[list[PositionPayload], float]:
    """Project held positions from full-universe cached analytics."""
    return _build_positions_from_universe_impl(universe_by_ticker)


def _load_latest_factor_coverage(
    cache_db: Path,
    *,
    data_db: Path | None = None,
) -> tuple[str | None, dict[str, FactorCoveragePayload]]:
    """Load latest per-factor cross-section coverage stats from durable model outputs."""
    return _load_latest_factor_coverage_impl(cache_db, data_db=data_db)


def _compute_exposures_modes(
    positions: list[PositionPayload],
    cov,
    factor_details: list[FactorDetailPayload],
    factor_coverage: dict[str, FactorCoveragePayload] | None = None,
    factor_coverage_asof: str | None = None,
) -> ExposureModesPayload:
    """Compute the 3-mode exposure data for all factors."""
    return _compute_exposures_modes_impl(
        positions,
        cov,
        factor_details,
        factor_coverage=factor_coverage,
        factor_coverage_asof=factor_coverage_asof,
    )


def _compute_position_risk_mix(
    positions: list[PositionPayload],
    cov,
    specific_risk_by_ticker: dict[str, SpecificRiskPayload] | None = None,
) -> dict[str, PositionRiskMixPayload]:
    """Per-position risk split using cUSE factor plus specific variance."""
    return _compute_position_risk_mix_impl(
        positions,
        cov,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )


def _compute_position_total_risk_contributions(
    positions: list[PositionPayload],
    cov,
    specific_risk_by_ticker: dict[str, SpecificRiskPayload] | None = None,
) -> dict[str, float]:
    return _compute_position_total_risk_contributions_impl(
        positions,
        cov,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )


def run_refresh(
    *,
    data_db: Path | None = None,
    cache_db: Path | None = None,
    force_risk_recompute: bool = False,
    mode: str = "full",
    refresh_scope: str | None = None,
    skip_snapshot_rebuild: bool = False,
    skip_cuse4_foundation: bool = False,
    skip_risk_engine: bool = False,
    enforce_stable_core_package: bool = False,
    refresh_projected_loadings: bool = False,
    refresh_deep_health_diagnostics: bool = False,
    prefer_local_source_archive: bool = False,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Pipeline refresh with serving-oriented modes:
    - full: weekly-gated risk engine + all downstream caches
    - light: fast cache refresh path that prefers cache reuse and avoids risk recompute
      unless risk caches are missing, stale, or explicitly forced.
    - publish: republishes already-current cached payloads without recomputing analytics.
    When `enforce_stable_core_package=True`, light-mode callers must reuse an existing
    stable core package and fail closed instead of recomputing factor returns,
    covariance, or specific risk on the serving path.
    """
    logger.info("Starting refresh pipeline...")
    _emit_refresh_progress(progress_callback, message="Loading refresh context", refresh_substage="context")
    effective_data_db = _resolve_data_db(data_db)
    effective_cache_db = _resolve_cache_db(cache_db)
    refresh_mode = str(mode or "full").strip().lower()
    refresh_scope_key = str(refresh_scope or "").strip().lower() or None
    if refresh_mode not in {"full", "light", "publish"}:
        refresh_mode = "full"
    light_mode = refresh_mode == "light"
    publish_only_mode = refresh_mode == "publish"

    refresh_pipeline_t0 = time.perf_counter()
    refresh_started_at = datetime.now(timezone.utc).isoformat()
    run_id = f"model_run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    today_utc = datetime.fromisoformat(
        previous_or_same_xnys_session(datetime.now(timezone.utc).date().isoformat())
    ).date()
    substage_timings: dict[str, dict[str, Any]] = {}

    def _record_substage_timing(name: str, started_at: float, **extra: Any) -> dict[str, Any]:
        record = {
            "duration_seconds": round(float(time.perf_counter() - started_at), 3),
        }
        for key, value in extra.items():
            if value is not None:
                record[str(key)] = value
        substage_timings[str(name)] = record
        return record

    if publish_only_mode:
        _emit_refresh_progress(
            progress_callback,
            message="Republishing cached serving payloads",
            refresh_substage="publish_only",
        )
        payloads, missing_payloads = publish_payloads.load_publishable_payloads(cache_db=effective_cache_db)
        if missing_payloads:
            raise RuntimeError(
                "publish-only requested but cached serving payloads are incomplete: "
                + ", ".join(sorted(missing_payloads))
            )
        snapshot_id = run_id
        payloads = publish_payloads.restamp_publishable_payloads(
            payloads,
            run_id=run_id,
            snapshot_id=snapshot_id,
            refresh_started_at=refresh_started_at,
        )
        refresh_meta = dict(payloads.get("refresh_meta") or {})
        risk_payload = dict(payloads.get("risk") or {})
        portfolio_payload = dict(payloads.get("portfolio") or {})
        health_payload = dict(payloads.get("health_diagnostics") or {})
        serving_outputs_write = serving_outputs.persist_current_payloads(
            data_db=effective_data_db,
            run_id=run_id,
            snapshot_id=snapshot_id,
            refresh_mode=refresh_mode,
            payloads=payloads,
            replace_all=True,
        )
        neon_write = serving_outputs_write.get("neon_write") if isinstance(serving_outputs_write, dict) else None
        if (
            config.serving_payload_neon_write_required()
            and isinstance(neon_write, dict)
            and str(neon_write.get("status") or "") != "ok"
        ):
            raise RuntimeError(f"Serving payload Neon write failed: {neon_write}")
        model_outputs_write = {
            "status": "skipped",
            "reason": "publish_only",
            "run_id": run_id,
        }
        sqlite.cache_set("model_outputs_write", model_outputs_write, db_path=effective_cache_db)
        sqlite.cache_set("serving_outputs_write", serving_outputs_write, db_path=effective_cache_db)
        publish_completed_at = datetime.now(timezone.utc).isoformat()
        publish_milestone = {
            "published_at": publish_completed_at,
            "snapshot_id": snapshot_id,
            "run_id": run_id,
            "payload_count": len(payloads),
            "payload_names": sorted(str(key) for key in payloads.keys()),
        }
        substage_timings["persist_outputs"] = {"duration_seconds": 0.0, "mode": "publish_only"}
        substage_timings["serving_publish_complete"] = {
            "duration_seconds": 0.0,
            **publish_milestone,
        }
        _emit_refresh_progress(
            progress_callback,
            message="Serving payload publish complete",
            refresh_substage="serving_publish_complete",
            substage_status="completed",
            publish_complete=True,
            published_at=publish_completed_at,
            published_snapshot_id=snapshot_id,
            published_run_id=run_id,
            published_payload_count=len(payloads),
            published_payload_names=sorted(str(key) for key in payloads.keys()),
        )
        return {
            "status": "ok",
            "run_id": run_id,
            "snapshot_id": snapshot_id,
            "positions": int((portfolio_payload.get("position_count") or 0)),
            "total_value": round(refresh_metadata.finite_float(portfolio_payload.get("total_value"), 0.0), 2),
            "mode": refresh_mode,
            "refresh_scope": refresh_scope_key,
            "cross_section_snapshot": dict(refresh_meta.get("cross_section_snapshot") or {"status": "reused"}),
            "risk_engine": dict(risk_payload.get("risk_engine") or refresh_meta.get("risk_engine") or {}),
            "model_sanity": dict(payloads.get("model_sanity") or {"status": "unknown"}),
            "cuse4_foundation": dict(refresh_meta.get("cuse4_foundation") or {"status": "reused"}),
            "health_refreshed": False,
            "health_refresh_state": str(
                health_payload.get("diagnostics_refresh_state")
                or refresh_meta.get("health_refresh_state")
                or "carried_forward"
            ),
            "publish_milestone": publish_milestone,
            "substage_timings": substage_timings,
            "universe_loadings_reused": True,
            "universe_loadings_reuse_reason": "publish_only_cached_payloads",
            "model_outputs_write": model_outputs_write,
            "serving_outputs_write": serving_outputs_write,
        }

    if skip_snapshot_rebuild:
        snapshot_build: SnapshotBuildPayload = {
            "status": "skipped",
            "reason": "orchestrator_precomputed",
            "mode": str(config.CROSS_SECTION_SNAPSHOT_MODE or "current"),
        }
    else:
        logger.info("Rebuilding canonical cross-section snapshot...")
        snapshot_build = rebuild_cross_section_snapshot(
            effective_data_db,
            mode=str(config.CROSS_SECTION_SNAPSHOT_MODE or "current"),
        )

    _emit_refresh_progress(progress_callback, message="Loading source dates", refresh_substage="source_dates")
    source_dates_t0 = time.perf_counter()
    source_dates: SourceDatesPayload = core_reads.load_source_dates(data_db=effective_data_db)
    _record_substage_timing("source_dates", source_dates_t0)
    fundamentals_asof = (
        source_dates.get("fundamentals_asof")
        or source_dates.get("exposures_latest_available_asof")
        or source_dates.get("exposures_asof")
    )

    # Optional cUSE4 foundation maintenance (additive, non-breaking).
    cuse4_foundation: dict[str, Any] = {"status": "disabled"}
    if skip_cuse4_foundation:
        cuse4_foundation = {
            "status": "skipped",
            "reason": "orchestrator_precomputed",
        }
    elif bool(config.CUSE4_ENABLE_ESTU_AUDIT):
        cuse4_bootstrap: dict[str, Any] | None = None
        cuse4_estu: dict[str, Any] | None = None
        try:
            if bool(config.CUSE4_AUTO_BOOTSTRAP):
                cuse4_bootstrap = bootstrap_cuse4_source_tables(
                    db_path=effective_data_db,
                )
            estu_asof = (
                source_dates.get("fundamentals_asof")
                or source_dates.get("exposures_latest_available_asof")
                or source_dates.get("exposures_asof")
                or today_utc.isoformat()
            )
            cuse4_estu = build_and_persist_estu_membership(
                db_path=effective_data_db,
                as_of_date=str(estu_asof),
            )
            cuse4_foundation = {
                "status": "ok",
                "bootstrap": cuse4_bootstrap,
                "estu": cuse4_estu,
            }
        except Exception as exc:  # noqa: BLE001
            logger.exception("cUSE4 foundation update failed")
            cuse4_foundation = {
                "status": "error",
                "error": {"type": type(exc).__name__, "message": str(exc)},
                "bootstrap": cuse4_bootstrap,
                "estu": cuse4_estu,
            }

    # 2. Weekly risk-engine recompute gate.
    risk_engine_meta, risk_engine_meta_source = _resolve_effective_risk_engine_meta(
        fallback_loader=lambda key: sqlite.cache_get_live_first(key, db_path=effective_cache_db),
    )
    should_recompute, recompute_reason = refresh_context.risk_recompute_due(
        risk_engine_meta,
        today_utc=today_utc,
    )
    if skip_risk_engine:
        should_recompute = False
        recompute_reason = "orchestrator_precomputed"
    elif light_mode:
        should_recompute = False
        recompute_reason = "light_mode_skip"
    if force_risk_recompute and not skip_risk_engine:
        should_recompute = True
        recompute_reason = "force_risk_recompute"

    cov = _deserialize_covariance(sqlite.cache_get_live_first("risk_engine_cov", db_path=effective_cache_db))
    cached_specific = sqlite.cache_get_live_first("risk_engine_specific_risk", db_path=effective_cache_db)
    specific_risk_by_security: dict[str, SpecificRiskPayload] = (
        cached_specific if isinstance(cached_specific, dict) else {}
    )
    latest_r2 = refresh_metadata.finite_float_or_none(risk_engine_meta.get("latest_r2"))
    cached_factor_count = _covariance_factor_count(cov)
    cached_specific_count = _specific_risk_entry_count(specific_risk_by_security)

    if risk_engine_meta_source == "model_run_metadata":
        persisted_cov_payload = model_outputs.load_latest_rebuild_authority_covariance_payload()
        persisted_specific_payload = model_outputs.load_latest_rebuild_authority_specific_risk_payload()
        if _persisted_risk_artifacts_are_richer(
            cached_cov=cov,
            cached_specific=specific_risk_by_security,
            persisted_cov_payload=persisted_cov_payload,
            persisted_specific_payload=persisted_specific_payload,
        ):
            persisted_cov = _deserialize_covariance(persisted_cov_payload)
            if not persisted_cov.empty:
                cov = persisted_cov
            if isinstance(persisted_specific_payload, dict) and persisted_specific_payload:
                specific_risk_by_security = {
                    str(key): dict(value)
                    for key, value in persisted_specific_payload.items()
                    if isinstance(value, dict)
                }
            logger.warning(
                "Using persisted model-output risk artifacts instead of degraded runtime cache: "
                "cached_factor_count=%s persisted_factor_count=%s cached_specific_count=%s persisted_specific_count=%s",
                cached_factor_count,
                int(len(persisted_cov_payload.get('factors') or [])) if isinstance(persisted_cov_payload, dict) else 0,
                cached_specific_count,
                _specific_risk_entry_count(persisted_specific_payload),
            )

    if skip_risk_engine:
        if cov.empty or not specific_risk_by_security:
            raise RuntimeError(
                "skip_risk_engine requested but risk-engine cache is missing; "
                "run orchestrator core stages first or disable skip_risk_engine."
            )
    else:
        if cov.empty:
            should_recompute = True
            recompute_reason = "missing_covariance_cache"
        if not specific_risk_by_security:
            should_recompute = True
            recompute_reason = "missing_specific_risk_cache"

    if light_mode and enforce_stable_core_package and not skip_risk_engine:
        raise RuntimeError(
            "Light serving refresh is configured to reuse a stable core package and cannot "
            f"advance core artifacts on the serving path ({recompute_reason}). Run a core lane instead."
        )

    recomputed_this_refresh = False
    if should_recompute:
        _emit_refresh_progress(
            progress_callback,
            message="Recomputing factor returns, covariance, and specific risk",
            refresh_substage="risk_engine_recompute",
        )
        logger.info(
            "Recomputing risk engine (%s): daily factor returns -> covariance -> specific risk",
            recompute_reason,
        )
        compute_daily_factor_returns(
            effective_data_db,
            effective_cache_db,
            min_cross_section_age_days=config.CROSS_SECTION_MIN_AGE_DAYS,
        )
        cov, latest_r2_value = build_factor_covariance_from_cache(
            effective_cache_db, lookback_days=config.LOOKBACK_DAYS
        )
        latest_r2 = float(latest_r2_value) if np.isfinite(latest_r2_value) else None
        latest_factor_return_date = refresh_context.latest_factor_return_date(effective_cache_db)
        specific_risk_by_security = build_specific_risk_from_cache(
            effective_cache_db,
            lookback_days=config.LOOKBACK_DAYS,
        )
        risk_engine_meta = {
            "status": "ok",
            "method_version": RISK_ENGINE_METHOD_VERSION,
            "last_recompute_date": today_utc.isoformat(),
            "factor_returns_latest_date": latest_factor_return_date,
            "estimation_exposure_anchor_date": refresh_context.derive_estimation_exposure_anchor_date_from_meta(
                {
                    "factor_returns_latest_date": latest_factor_return_date,
                    "cross_section_min_age_days": int(config.CROSS_SECTION_MIN_AGE_DAYS),
                },
            ),
            "lookback_days": int(config.LOOKBACK_DAYS),
            "cross_section_min_age_days": int(config.CROSS_SECTION_MIN_AGE_DAYS),
            "recompute_interval_days": int(config.RISK_RECOMPUTE_INTERVAL_DAYS),
            "latest_r2": latest_r2,
            "specific_risk_ticker_count": int(len(specific_risk_by_security)),
        }
        recomputed_this_refresh = True
        logger.info(
            "Risk engine recompute complete: factor_count=%s specific_risk_count=%s latest_r2=%.4f",
            int(cov.shape[1]) if cov is not None and not cov.empty else 0,
            int(len(specific_risk_by_security)),
            float(latest_r2 or 0.0),
        )
    else:
        logger.info(
            "Skipping risk-engine recompute (%s). Reusing cached covariance/specific risk.",
            recompute_reason,
        )
        if risk_engine_meta_source == "model_run_metadata":
            logger.info("Using persisted model-run metadata as effective risk-engine state for refresh.")

    active_core_state_through_date = str(
        risk_engine_meta.get("core_state_through_date")
        or risk_engine_meta.get("factor_returns_latest_date")
        or ""
    ).strip() or None
    if active_core_state_through_date and str(risk_engine_meta.get("core_state_through_date") or "").strip() != active_core_state_through_date:
        risk_engine_meta = dict(risk_engine_meta)
        risk_engine_meta["core_state_through_date"] = active_core_state_through_date

    specific_risk_by_ticker = _specific_risk_by_ticker_view(specific_risk_by_security)
    factor_catalog_by_name = build_factor_catalog_for_factors(
        list(cov.columns),
        method_version=RISK_ENGINE_METHOD_VERSION,
    ) if cov is not None and not cov.empty else {}
    factor_name_to_id = factor_name_to_id_map(factor_catalog_by_name)
    factor_catalog_by_id = factor_id_to_entry_map(factor_catalog_by_name)
    if not cov.empty and factor_name_to_id:
        cov = cov.rename(index=factor_name_to_id, columns=factor_name_to_id)

    # 3. Build/cached full-universe loadings first (portfolio is a final projection only).
    universe_loadings_reused = False
    universe_loadings_reuse_reason = "not_attempted"
    cached_universe_loadings = (
        sqlite.cache_get("universe_loadings", db_path=effective_cache_db)
        if light_mode and not recomputed_this_refresh and not prefer_local_source_archive
        else None
    )
    if light_mode and not recomputed_this_refresh and prefer_local_source_archive:
        universe_loadings_reuse_reason = "local_source_archive_requested"
    if cached_universe_loadings is not None:
        universe_loadings_reused, universe_loadings_reuse_reason = _can_reuse_cached_universe_loadings(
            cached_universe_loadings,
            source_dates=source_dates,
            risk_engine_meta=risk_engine_meta,
        )

    if universe_loadings_reused:
        _emit_refresh_progress(
            progress_callback,
            message="Reusing cached universe loadings",
            refresh_substage="universe_loadings",
        )
        universe_loadings_t0 = time.perf_counter()
        universe_loadings = dict(cached_universe_loadings)
        logger.info(
            "Reusing cached universe loadings for light refresh (%s): ticker_count=%s eligible_ticker_count=%s factor_count=%s",
            universe_loadings_reuse_reason,
            int(universe_loadings.get("ticker_count", 0)),
            int(universe_loadings.get("eligible_ticker_count", 0)),
            int(universe_loadings.get("factor_count", 0)),
        )
        _record_substage_timing(
            "universe_loadings",
            universe_loadings_t0,
            ticker_count=int(universe_loadings.get("ticker_count", 0)),
            eligible_ticker_count=int(universe_loadings.get("eligible_ticker_count", 0)),
            factor_count=int(universe_loadings.get("factor_count", 0)),
            reused=True,
            reuse_reason=universe_loadings_reuse_reason,
        )
    else:
        _emit_refresh_progress(
            progress_callback,
            message="Loading universe inputs",
            refresh_substage="universe_inputs",
        )
        universe_inputs_t0 = time.perf_counter()
        logger.info(
            "Fetching full-universe inputs from local database for rebuild (%s)...",
            universe_loadings_reuse_reason,
        )
        prices_universe_df = core_reads.load_latest_prices(data_db=effective_data_db)
        fundamentals_universe_df = core_reads.load_latest_fundamentals(
            data_db=effective_data_db,
            as_of_date=str(fundamentals_asof) if fundamentals_asof else None,
        )
        exposures_universe_df = core_reads.load_raw_cross_section_latest(data_db=effective_data_db)
        logger.info(
            "Loaded source rows: prices=%s fundamentals=%s exposures=%s",
            int(len(prices_universe_df)),
            int(len(fundamentals_universe_df)),
            int(len(exposures_universe_df)),
        )
        projection_universe_rows: list[dict[str, str]] = []
        _record_substage_timing(
            "universe_inputs",
            universe_inputs_t0,
            prices_row_count=int(len(prices_universe_df)),
            fundamentals_row_count=int(len(fundamentals_universe_df)),
            exposures_row_count=int(len(exposures_universe_df)),
        )
        projected_loadings_map: dict | None = None
        try:
            import sqlite3 as _sqlite3
            _proj_conn = _sqlite3.connect(str(effective_data_db))
            try:
                projection_universe_rows = load_projection_only_universe_rows(_proj_conn)
            finally:
                _proj_conn.close()
            persisted_projection_asof = (
                latest_persisted_projection_asof(
                    data_db=effective_data_db,
                    projection_rics=projection_universe_rows,
                )
                if projection_universe_rows and active_core_state_through_date
                else None
            )
            should_refresh_projection_outputs = bool(
                refresh_projected_loadings
                or recomputed_this_refresh
                or (
                    projection_universe_rows
                    and active_core_state_through_date
                    and persisted_projection_asof != active_core_state_through_date
                )
            )
            if projection_universe_rows and should_refresh_projection_outputs and active_core_state_through_date:
                _emit_refresh_progress(
                    progress_callback,
                    message="Refreshing returns-projected instrument loadings",
                    refresh_substage="projected_loadings_refresh",
                )
                projected_refresh_t0 = time.perf_counter()
                compute_projected_loadings(
                    data_db=effective_data_db,
                    projection_rics=projection_universe_rows,
                    core_state_through_date=active_core_state_through_date,
                )
                _record_substage_timing(
                    "projected_loadings_refresh",
                    projected_refresh_t0,
                    projection_ticker_count=len(projection_universe_rows),
                )
            if projection_universe_rows and active_core_state_through_date:
                _emit_refresh_progress(
                    progress_callback,
                    message="Loading persisted returns-projected instrument loadings",
                    refresh_substage="projected_loadings_load",
                )
                projected_load_t0 = time.perf_counter()
                projected_loadings_map = load_persisted_projected_loadings(
                    data_db=effective_data_db,
                    projection_rics=projection_universe_rows,
                    as_of_date=active_core_state_through_date,
                )
                logger.info(
                    "Loaded persisted projected loadings for %d/%d projection-only instruments at core_state_through_date=%s.",
                    sum(1 for p in projected_loadings_map.values() if p.status == "ok"),
                    len(projection_universe_rows),
                    active_core_state_through_date,
                )
                _record_substage_timing(
                    "projected_loadings_load",
                    projected_load_t0,
                    projection_ticker_count=len(projection_universe_rows),
                    projected_ok_count=sum(1 for p in projected_loadings_map.values() if p.status == "ok"),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Projection-only loadings refresh/load failed; serving will mark them unavailable: %s", exc)
            projected_loadings_map = None

        _emit_refresh_progress(
            progress_callback,
            message="Building served universe loadings",
            refresh_substage="universe_loadings",
        )
        universe_loadings_t0 = time.perf_counter()
        logger.info("Building full-universe ticker loadings...")
        universe_loadings = _build_universe_ticker_loadings(
            exposures_universe_df,
            fundamentals_universe_df,
            prices_universe_df,
            cov,
            data_db=effective_data_db,
            specific_risk_by_ticker=specific_risk_by_ticker,
            factor_catalog_by_name=factor_catalog_by_name,
            projected_loadings=projected_loadings_map,
            projection_universe_rows=projection_universe_rows,
            projection_core_state_through_date=active_core_state_through_date,
        )
        universe_loadings_reuse_reason = "rebuilt"
        logger.info(
            "Universe loadings built: ticker_count=%s eligible_ticker_count=%s factor_count=%s",
            int(universe_loadings.get("ticker_count", 0)),
            int(universe_loadings.get("eligible_ticker_count", 0)),
            int(universe_loadings.get("factor_count", 0)),
        )
        _record_substage_timing(
            "universe_loadings",
            universe_loadings_t0,
            ticker_count=int(universe_loadings.get("ticker_count", 0)),
            eligible_ticker_count=int(universe_loadings.get("eligible_ticker_count", 0)),
            factor_count=int(universe_loadings.get("factor_count", 0)),
            reused=False,
        )

    # 4. Project held positions from full-universe cache
    _emit_refresh_progress(progress_callback, message="Projecting held positions", refresh_substage="positions")
    positions_t0 = time.perf_counter()
    logger.info("Projecting held positions from full-universe cache...")
    positions, total_value = _build_positions_from_universe(universe_loadings["by_ticker"])
    _record_substage_timing(
        "positions",
        positions_t0,
        position_count=int(len(positions)),
        total_value=round(float(total_value), 2),
    )

    # 5. Risk decomposition
    _emit_refresh_progress(
        progress_callback,
        message="Computing portfolio risk decomposition",
        refresh_substage="risk_decomposition",
    )
    risk_decomposition_t0 = time.perf_counter()
    logger.info("Computing risk decomposition...")
    raw_risk_shares, raw_component_shares, raw_factor_details = risk_decomposition(
        cov=cov,
        positions=positions,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )
    risk_shares: RiskSharesPayload = {
        "market": float(raw_risk_shares.get("market", 0.0)),
        "industry": float(raw_risk_shares.get("industry", 0.0)),
        "style": float(raw_risk_shares.get("style", 0.0)),
        "idio": float(raw_risk_shares.get("idio", 0.0)),
    }
    raw_vol_scaled_shares = vol_scaled_decomposition(
        cov=cov,
        positions=positions,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )
    vol_scaled_shares: RiskSharesPayload = {
        "market": float(raw_vol_scaled_shares.get("market", 0.0)),
        "industry": float(raw_vol_scaled_shares.get("industry", 0.0)),
        "style": float(raw_vol_scaled_shares.get("style", 0.0)),
        "idio": float(raw_vol_scaled_shares.get("idio", 0.0)),
    }
    component_shares: ComponentSharesPayload = {
        "market": float(raw_component_shares.get("market", 0.0)),
        "industry": float(raw_component_shares.get("industry", 0.0)),
        "style": float(raw_component_shares.get("style", 0.0)),
    }
    factor_details: list[FactorDetailPayload] = [dict(row) for row in raw_factor_details]
    logger.info(
        "Risk decomposition complete: market=%.2f industry=%.2f style=%.2f idio=%.2f factors=%s",
        float(risk_shares["market"]),
        float(risk_shares["industry"]),
        float(risk_shares["style"]),
        float(risk_shares["idio"]),
        int(len(factor_details)),
    )
    _record_substage_timing(
        "risk_decomposition",
        risk_decomposition_t0,
        factor_detail_count=int(len(factor_details)),
    )
    position_risk_mix = _compute_position_risk_mix(
        positions=positions,
        cov=cov,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )
    position_risk_contrib = _compute_position_total_risk_contributions(
        positions=positions,
        cov=cov,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )

    # 6. Compute per-position risk contributions
    for pos in positions:
        ticker = str(pos.get("ticker", "")).upper()
        pos["risk_contrib_pct"] = float(position_risk_contrib.get(ticker, 0.0))
        pos["risk_mix"] = dict(position_risk_mix.get(ticker, {
            "market": 0.0,
            "industry": 0.0,
            "style": 0.0,
            "idio": 0.0,
        }))

    reuse_cached_risk_display = bool(
        light_mode
        and universe_loadings_reused
        and not recomputed_this_refresh
    )
    cached_risk_display = _load_cached_risk_display_payload(cache_db=effective_cache_db) if reuse_cached_risk_display else None

    # 7. Compute exposure modes
    _emit_refresh_progress(
        progress_callback,
        message="Computing factor exposure views",
        refresh_substage="exposure_modes",
    )
    exposure_modes_t0 = time.perf_counter()
    logger.info("Computing exposure modes...")
    factor_coverage_asof, factor_coverage = _load_latest_factor_coverage(
        effective_cache_db,
        data_db=effective_data_db,
    )
    if factor_name_to_id:
        factor_coverage = {
            factor_name_to_id[factor_name]: payload
            for factor_name, payload in factor_coverage.items()
            if factor_name in factor_name_to_id
        }
    exposure_modes: ExposureModesPayload = _compute_exposures_modes(
        positions,
        cov,
        factor_details,
        factor_coverage=factor_coverage,
        factor_coverage_asof=factor_coverage_asof,
    )
    _record_substage_timing(
        "exposure_modes",
        exposure_modes_t0,
        factor_coverage_asof=factor_coverage_asof,
        factor_coverage_count=int(len(factor_coverage or {})),
    )

    # 8. Build covariance matrix for frontend (correlation) — style factors only
    STYLE_FACTOR_NAMES = {
        "Size", "Nonlinear Size", "Liquidity", "Beta",
        "Book-to-Price", "Earnings Yield", "Leverage",
        "Growth", "Profitability", "Investment", "Dividend Yield",
        "Momentum", "Short-Term Reversal", "Residual Volatility",
    }
    cov_matrix: CovarianceMatrixPayload = {}
    if cached_risk_display is not None:
        cov_matrix = cached_risk_display
    elif not cov.empty:
        all_factors = list(cov.columns)
        style_idx = [
            i for i, factor_id in enumerate(all_factors)
            if str(factor_catalog_by_id.get(str(factor_id)).family if factor_catalog_by_id.get(str(factor_id)) else "") == "style"
        ]
        if style_idx:
            style_factors = [all_factors[i] for i in style_idx]
            sub_cov = cov.to_numpy()[np.ix_(style_idx, style_idx)]
            stds = np.sqrt(np.diag(sub_cov))
            stds[stds == 0] = 1.0
            corr = sub_cov / np.outer(stds, stds)
            corr = np.clip(corr, -1.0, 1.0)
            cov_matrix = {
                "factors": style_factors,
                "correlation": [[round(float(v), 4) for v in row] for row in corr],
            }

    # 9. Sanitize non-finite floats (NaN/Inf break JSON serialization)
    def _safe(v):
        if isinstance(v, float) and not np.isfinite(v):
            return 0.0
        return v

    for d in factor_details:
        for k, v in d.items():
            d[k] = _safe(v)
    for k in risk_shares:
        risk_shares[k] = _safe(risk_shares[k])
    for k in vol_scaled_shares:
        vol_scaled_shares[k] = _safe(vol_scaled_shares[k])
    for k in component_shares:
        component_shares[k] = _safe(component_shares[k])

    # 10. Cache everything
    _emit_refresh_progress(progress_callback, message="Staging serving snapshot", refresh_substage="cache_stage")
    cache_stage_t0 = time.perf_counter()
    logger.info("Caching results...")
    recompute_health_post_publish = bool(refresh_deep_health_diagnostics or recomputed_this_refresh)
    staged = stage_refresh_cache_snapshot(
        run_id=run_id,
        refresh_mode=refresh_mode,
        refresh_started_at=refresh_started_at,
        source_dates=source_dates,
        snapshot_build=snapshot_build,
        risk_engine_meta=risk_engine_meta,
        recomputed_this_refresh=bool(recomputed_this_refresh),
        recompute_reason=str(recompute_reason),
        cov_payload=_serialize_covariance(cov),
        specific_risk_by_security=specific_risk_by_security,
        positions=positions,
        total_value=total_value,
        risk_shares=risk_shares,
        vol_scaled_shares=vol_scaled_shares,
        component_shares=component_shares,
        factor_details=factor_details,
        cov_matrix=cov_matrix,
        latest_r2=latest_r2,
        universe_loadings=universe_loadings,
        exposure_modes=exposure_modes,
        factor_catalog=universe_loadings.get("factor_catalog", []),
        cuse4_foundation=cuse4_foundation,
        recompute_health_diagnostics=False,
        reuse_cached_static_payloads=bool(
            light_mode
            and universe_loadings_reused
            and not recomputed_this_refresh
        ),
        data_db=effective_data_db,
        cache_db=effective_cache_db,
    )
    _record_substage_timing(
        "cache_stage",
        cache_stage_t0,
        payload_count=int(len(dict(staged.get("persisted_payloads") or {}))),
    )
    snapshot_id = str(staged.get("snapshot_id") or run_id)
    risk_engine_state = dict(staged.get("risk_engine_state") or {})
    sanity = dict(staged.get("sanity") or {"status": "no-data", "warnings": [], "checks": {}})
    health_refreshed = bool(staged.get("health_refreshed", False))
    health_refresh_state = str(staged.get("health_refresh_state") or ("recomputed" if health_refreshed else "deferred"))
    persisted_payloads = dict(staged.get("persisted_payloads") or {})

    _emit_refresh_progress(
        progress_callback,
        message="Persisting model and serving outputs",
        refresh_substage="persist_outputs",
    )
    persist_outputs_t0 = time.perf_counter()
    model_outputs_write, serving_outputs_write = refresh_persistence.persist_refresh_outputs(
        data_db=effective_data_db,
        cache_db=effective_cache_db,
        run_id=run_id,
        snapshot_id=snapshot_id,
        refresh_mode=refresh_mode,
        refresh_started_at=refresh_started_at,
        recomputed_this_refresh=bool(recomputed_this_refresh),
        params={
            "force_risk_recompute": bool(force_risk_recompute),
            "mode": refresh_mode,
            "lookback_days": int(config.LOOKBACK_DAYS),
            "cross_section_min_age_days": int(config.CROSS_SECTION_MIN_AGE_DAYS),
            "risk_recompute_interval_days": int(config.RISK_RECOMPUTE_INTERVAL_DAYS),
            "cross_section_snapshot_mode": str(config.CROSS_SECTION_SNAPSHOT_MODE or "current"),
        },
        source_dates=source_dates,
        risk_engine_state=risk_engine_state,
        cov=cov,
        specific_risk_by_security=specific_risk_by_security,
        persisted_payloads=persisted_payloads,
    )
    persist_outputs_timing = _record_substage_timing(
        "persist_outputs",
        persist_outputs_t0,
        payload_count=int(len(persisted_payloads)),
    )
    publish_completed_at = datetime.now(timezone.utc).isoformat()
    publish_milestone = {
        "published_at": publish_completed_at,
        "snapshot_id": snapshot_id,
        "run_id": run_id,
        "payload_count": int(len(persisted_payloads)),
        "payload_names": sorted(str(key) for key in persisted_payloads.keys()),
        "persist_duration_seconds": float(persist_outputs_timing["duration_seconds"]),
        "elapsed_since_refresh_start_seconds": round(float(time.perf_counter() - refresh_pipeline_t0), 3),
    }
    substage_timings["serving_publish_complete"] = {
        "duration_seconds": 0.0,
        "published_at": publish_completed_at,
        "snapshot_id": snapshot_id,
        "run_id": run_id,
        "payload_count": int(len(persisted_payloads)),
    }
    _emit_refresh_progress(
        progress_callback,
        message="Serving payload publish complete",
        refresh_substage="serving_publish_complete",
        substage_status="completed",
        publish_complete=True,
        published_at=publish_completed_at,
        published_snapshot_id=snapshot_id,
        published_run_id=run_id,
        published_payload_count=int(len(persisted_payloads)),
        published_payload_names=sorted(str(key) for key in persisted_payloads.keys()),
    )
    if recompute_health_post_publish:
        _emit_refresh_progress(
            progress_callback,
            message="Computing deep health diagnostics",
            refresh_substage="health_diagnostics",
        )
        health_t0 = time.perf_counter()
        health_payload = compute_health_diagnostics(
            effective_data_db,
            effective_cache_db,
            risk_payload=dict(persisted_payloads.get("risk") or {}),
            portfolio_payload=dict(persisted_payloads.get("portfolio") or {}),
            universe_payload=dict(persisted_payloads.get("universe_loadings") or {}),
            covariance_payload=dict(persisted_payloads.get("risk_engine_cov") or {}),
            source_dates=dict(persisted_payloads.get("refresh_meta", {}).get("source_dates") or source_dates),
            run_id=run_id,
            snapshot_id=snapshot_id,
            progress_callback=progress_callback,
        )
        logger.info(
            "Deep health diagnostics completed in %.2fs for run_id=%s snapshot_id=%s",
            time.perf_counter() - health_t0,
            run_id,
            snapshot_id,
        )
        diagnostics_timings = (
            dict(health_payload.get("timings_seconds") or {})
            if isinstance(health_payload, dict)
            else {}
        )
        _record_substage_timing(
            "health_diagnostics",
            health_t0,
            diagnostics_timings=diagnostics_timings or None,
        )
        if isinstance(health_payload, dict):
            health_payload = dict(health_payload)
            health_payload["run_id"] = str(run_id)
            health_payload["snapshot_id"] = str(snapshot_id)
            health_payload["_reuse_signature"] = health_payloads.health_reuse_signature(
                source_dates=dict(persisted_payloads.get("refresh_meta", {}).get("source_dates") or source_dates),
                risk_engine_state=risk_engine_state,
                positions=positions,
                total_value=total_value,
            )
            health_payload["cache_version"] = health_payloads.HEALTH_DIAGNOSTICS_CACHE_VERSION
            health_payload["diagnostics_refresh_state"] = "recomputed"
            health_payload["diagnostics_generated_from_run_id"] = str(run_id)
            health_payload["diagnostics_generated_from_snapshot_id"] = str(snapshot_id)

        refresh_meta_payload = dict(persisted_payloads.get("refresh_meta") or {})
        refresh_meta_payload["health_refreshed"] = True
        refresh_meta_payload["health_refresh_state"] = "recomputed"

        persisted_payloads["health_diagnostics"] = health_payload
        persisted_payloads["refresh_meta"] = refresh_meta_payload
        health_refreshed = True
        health_refresh_state = "recomputed"

        _emit_refresh_progress(
            progress_callback,
            message="Persisting refreshed health diagnostics",
            refresh_substage="health_diagnostics_persist",
        )
        health_persist_t0 = time.perf_counter()
        sqlite.cache_set(
            "health_diagnostics",
            health_payload,
            snapshot_id=snapshot_id,
            db_path=effective_cache_db,
        )
        sqlite.cache_set(
            "refresh_meta",
            refresh_meta_payload,
            snapshot_id=snapshot_id,
            db_path=effective_cache_db,
        )
        health_patch_write = serving_outputs.persist_current_payloads(
            data_db=effective_data_db,
            run_id=run_id,
            snapshot_id=snapshot_id,
            refresh_mode=refresh_mode,
            payloads={
                "health_diagnostics": health_payload,
                "refresh_meta": refresh_meta_payload,
            },
            replace_all=False,
        )
        neon_write = health_patch_write.get("neon_write") if isinstance(health_patch_write, dict) else None
        if (
            config.serving_payload_neon_write_required()
            and isinstance(neon_write, dict)
            and str(neon_write.get("status") or "") != "ok"
        ):
            raise RuntimeError(f"Serving payload Neon write failed during health refresh patch: {neon_write}")
        if isinstance(serving_outputs_write, dict):
            serving_outputs_write = dict(serving_outputs_write)
            serving_outputs_write["health_patch_write"] = health_patch_write
        _record_substage_timing("health_diagnostics_persist", health_persist_t0)

    logger.info("Refresh complete.")
    return {
        "status": "ok",
        "run_id": run_id,
        "snapshot_id": snapshot_id,
        "positions": len(positions),
        "total_value": round(total_value, 2),
        "mode": refresh_mode,
        "refresh_scope": refresh_scope_key,
        "cross_section_snapshot": snapshot_build,
        "risk_engine": risk_engine_state,
        "model_sanity": sanity,
        "cuse4_foundation": cuse4_foundation,
        "health_refreshed": bool(health_refreshed),
        "health_refresh_state": str(health_refresh_state),
        "publish_milestone": publish_milestone,
        "substage_timings": substage_timings,
        "universe_loadings_reused": bool(universe_loadings_reused),
        "universe_loadings_reuse_reason": str(universe_loadings_reuse_reason),
        "model_outputs_write": model_outputs_write,
        "serving_outputs_write": serving_outputs_write,
    }
