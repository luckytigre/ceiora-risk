"""Analytics pipeline: fetch → compute → cache."""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend import config
from backend.analytics.refresh_policy import (
    latest_factor_return_date as _latest_factor_return_date_impl,
    risk_recompute_due as _risk_recompute_due_impl,
)
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
)
from backend.universe import bootstrap_cuse4_source_tables, build_and_persist_estu_membership
from backend.trading_calendar import previous_or_same_xnys_session

logger = logging.getLogger(__name__)

DATA_DB = Path(config.DATA_DB_PATH)
CACHE_DB = Path(config.SQLITE_PATH)
RISK_ENGINE_METHOD_VERSION = "v8_use4_us_core_market_one_stage_projected_non_us_2026_03_15"
_UNIVERSE_REUSE_RISK_KEYS = (
    "status",
    "method_version",
    "last_recompute_date",
    "factor_returns_latest_date",
    "cross_section_min_age_days",
    "lookback_days",
    "specific_risk_ticker_count",
)
_PUBLISH_ONLY_PAYLOAD_NAMES = (
    "eligibility",
    "exposures",
    "health_diagnostics",
    "model_sanity",
    "portfolio",
    "refresh_meta",
    "risk",
    "risk_engine_cov",
    "risk_engine_specific_risk",
    "universe_factors",
    "universe_loadings",
)
_PUBLISH_METADATA_PAYLOAD_NAMES = {
    "exposures",
    "health_diagnostics",
    "model_sanity",
    "portfolio",
    "refresh_meta",
    "risk",
    "universe_factors",
    "universe_loadings",
}


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if np.isfinite(out) else float(default)


def _risk_engine_reuse_signature(payload: dict[str, Any] | None) -> dict[str, Any]:
    meta = dict(payload or {})
    return {key: meta.get(key) for key in _UNIVERSE_REUSE_RISK_KEYS}


def _source_dates_reuse_signature(payload: dict[str, Any] | None) -> dict[str, Any]:
    source_dates = dict(payload or {})
    return {
        "fundamentals_asof": source_dates.get("fundamentals_asof"),
        "classification_asof": source_dates.get("classification_asof"),
        "prices_asof": source_dates.get("prices_asof"),
        "exposures_latest_available_asof": (
            source_dates.get("exposures_latest_available_asof")
            or source_dates.get("exposures_asof")
        ),
    }


def _can_reuse_cached_universe_loadings(
    cached_payload: Any,
    *,
    source_dates: SourceDatesPayload,
    risk_engine_meta: RiskEngineMetaPayload,
) -> tuple[bool, str]:
    if not isinstance(cached_payload, dict):
        return False, "missing_cached_payload"
    by_ticker = cached_payload.get("by_ticker")
    if not isinstance(by_ticker, dict) or not by_ticker:
        return False, "missing_by_ticker"
    cached_source_dates = cached_payload.get("source_dates")
    if not isinstance(cached_source_dates, dict):
        return False, "missing_cached_source_dates"
    if _source_dates_reuse_signature(cached_source_dates) != _source_dates_reuse_signature(source_dates):
        return False, "source_dates_changed"
    cached_risk = cached_payload.get("risk_engine")
    if not isinstance(cached_risk, dict):
        return False, "missing_cached_risk_engine"
    if _risk_engine_reuse_signature(cached_risk) != _risk_engine_reuse_signature(risk_engine_meta):
        return False, "risk_engine_state_changed"
    return True, "source_and_risk_engine_match"


def _load_cached_risk_display_payload() -> CovarianceMatrixPayload | None:
    cached_risk = sqlite.cache_get("risk")
    if not isinstance(cached_risk, dict):
        return None
    cov_matrix = cached_risk.get("cov_matrix")
    if not isinstance(cov_matrix, dict):
        return None
    return dict(cov_matrix)


def _risk_recompute_due(meta: dict[str, Any], *, today_utc: date) -> tuple[bool, str]:
    return _risk_recompute_due_impl(
        meta,
        today_utc=today_utc,
        method_version=RISK_ENGINE_METHOD_VERSION,
        interval_days=config.RISK_RECOMPUTE_INTERVAL_DAYS,
    )


def _latest_factor_return_date(cache_db: Path) -> str | None:
    return _latest_factor_return_date_impl(cache_db)


def _serialize_covariance(cov: pd.DataFrame) -> CovariancePayload:
    if cov is None or cov.empty:
        return {"factors": [], "matrix": []}
    factors = [str(c) for c in cov.columns]
    mat = cov.reindex(index=factors, columns=factors).to_numpy(dtype=float)
    return {
        "factors": factors,
        "matrix": [[_finite_float(v, 0.0) for v in row] for row in mat.tolist()],
    }


def _load_publishable_payloads() -> tuple[dict[str, Any], list[str]]:
    payloads: dict[str, Any] = {}
    missing: list[str] = []
    for payload_name in _PUBLISH_ONLY_PAYLOAD_NAMES:
        payload = serving_outputs.load_runtime_payload(
            payload_name,
            fallback_loader=sqlite.cache_get,
        )
        if payload is None:
            missing.append(payload_name)
            continue
        payloads[payload_name] = payload
    return payloads, missing


def _restamp_publishable_payloads(
    payloads: dict[str, Any],
    *,
    run_id: str,
    snapshot_id: str,
    refresh_started_at: str,
) -> dict[str, Any]:
    restamped: dict[str, Any] = {}
    for payload_name, payload in payloads.items():
        if payload_name not in _PUBLISH_METADATA_PAYLOAD_NAMES or not isinstance(payload, dict):
            restamped[payload_name] = payload
            continue
        stamped = dict(payload)
        stamped["run_id"] = str(run_id)
        stamped["snapshot_id"] = str(snapshot_id)
        stamped["refresh_started_at"] = str(refresh_started_at)
        restamped[payload_name] = stamped
    return restamped


def _deserialize_covariance(payload: Any) -> pd.DataFrame:
    if not isinstance(payload, dict):
        return pd.DataFrame()
    factors = [str(f) for f in (payload.get("factors") or []) if str(f).strip()]
    matrix = payload.get("matrix") or []
    if not factors or not isinstance(matrix, list):
        return pd.DataFrame()
    try:
        arr = np.asarray(matrix, dtype=float)
    except Exception:
        return pd.DataFrame()
    if arr.ndim != 2 or arr.shape[0] != len(factors) or arr.shape[1] != len(factors):
        return pd.DataFrame()
    return pd.DataFrame(arr, index=factors, columns=factors)


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
    specific_risk_by_ticker: dict[str, SpecificRiskPayload] | None = None,
    factor_catalog_by_name: dict[str, object] | None = None,
) -> UniverseLoadingsPayload:
    """Build full-universe cached loadings/risk context keyed by ticker."""
    return _build_universe_ticker_loadings_impl(
        exposures_df,
        fundamentals_df,
        prices_df,
        cov,
        data_db=DATA_DB,
        specific_risk_by_ticker=specific_risk_by_ticker,
        factor_catalog_by_name=factor_catalog_by_name,
    )


def _build_positions_from_universe(
    universe_by_ticker: dict[str, dict[str, Any]],
) -> tuple[list[PositionPayload], float]:
    """Project held positions from full-universe cached analytics."""
    return _build_positions_from_universe_impl(universe_by_ticker)


def _load_latest_factor_coverage(
    cache_db: Path,
) -> tuple[str | None, dict[str, FactorCoveragePayload]]:
    """Load latest per-factor cross-section coverage stats from cache DB."""
    return _load_latest_factor_coverage_impl(cache_db)


def _compute_exposures_modes(
    positions: list[PositionPayload],
    cov,
    factor_details: list[FactorDetailPayload],
    factor_coverage: dict[str, FactorCoveragePayload] | None = None,
    coverage_date: str | None = None,
) -> ExposureModesPayload:
    """Compute the 3-mode exposure data for all factors."""
    return _compute_exposures_modes_impl(
        positions,
        cov,
        factor_details,
        factor_coverage=factor_coverage,
        coverage_date=coverage_date,
    )


def _compute_position_risk_mix(
    positions: list[PositionPayload],
    cov,
    specific_risk_by_ticker: dict[str, SpecificRiskPayload] | None = None,
) -> dict[str, PositionRiskMixPayload]:
    """Per-position risk split using Barra-style factor + specific variance."""
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
    force_risk_recompute: bool = False,
    mode: str = "full",
    refresh_scope: str | None = None,
    skip_snapshot_rebuild: bool = False,
    skip_cuse4_foundation: bool = False,
    skip_risk_engine: bool = False,
) -> dict[str, Any]:
    """Pipeline refresh with serving-oriented modes:
    - full: weekly-gated risk engine + all downstream caches
    - light: fast cache refresh path that prefers cache reuse and avoids risk recompute
      unless risk caches are missing, stale, or explicitly forced.
    - publish: republishes already-current cached payloads without recomputing analytics.
    """
    logger.info("Starting refresh pipeline...")
    refresh_mode = str(mode or "full").strip().lower()
    refresh_scope_key = str(refresh_scope or "").strip().lower() or None
    if refresh_mode not in {"full", "light", "publish"}:
        refresh_mode = "full"
    light_mode = refresh_mode == "light"
    publish_only_mode = refresh_mode == "publish"

    refresh_started_at = datetime.now(timezone.utc).isoformat()
    run_id = f"model_run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    today_utc = datetime.fromisoformat(
        previous_or_same_xnys_session(datetime.now(timezone.utc).date().isoformat())
    ).date()

    if publish_only_mode:
        payloads, missing_payloads = _load_publishable_payloads()
        if missing_payloads:
            raise RuntimeError(
                "publish-only requested but cached serving payloads are incomplete: "
                + ", ".join(sorted(missing_payloads))
            )
        snapshot_id = run_id
        payloads = _restamp_publishable_payloads(
            payloads,
            run_id=run_id,
            snapshot_id=snapshot_id,
            refresh_started_at=refresh_started_at,
        )
        refresh_meta = dict(payloads.get("refresh_meta") or {})
        risk_payload = dict(payloads.get("risk") or {})
        portfolio_payload = dict(payloads.get("portfolio") or {})
        serving_outputs_write = serving_outputs.persist_current_payloads(
            data_db=DATA_DB,
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
        sqlite.cache_set("model_outputs_write", model_outputs_write)
        sqlite.cache_set("serving_outputs_write", serving_outputs_write)
        return {
            "status": "ok",
            "run_id": run_id,
            "snapshot_id": snapshot_id,
            "positions": int((portfolio_payload.get("position_count") or 0)),
            "total_value": round(_finite_float(portfolio_payload.get("total_value"), 0.0), 2),
            "mode": refresh_mode,
            "refresh_scope": refresh_scope_key,
            "cross_section_snapshot": dict(refresh_meta.get("cross_section_snapshot") or {"status": "reused"}),
            "risk_engine": dict(risk_payload.get("risk_engine") or refresh_meta.get("risk_engine") or {}),
            "model_sanity": dict(payloads.get("model_sanity") or {"status": "unknown"}),
            "cuse4_foundation": dict(refresh_meta.get("cuse4_foundation") or {"status": "reused"}),
            "health_refreshed": False,
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
            DATA_DB,
            mode=str(config.CROSS_SECTION_SNAPSHOT_MODE or "current"),
        )

    source_dates: SourceDatesPayload = core_reads.load_source_dates()
    fundamentals_asof = source_dates.get("fundamentals_asof") or source_dates.get("exposures_asof")

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
                    db_path=DATA_DB,
                )
            estu_asof = (
                source_dates.get("fundamentals_asof")
                or source_dates.get("exposures_asof")
                or today_utc.isoformat()
            )
            cuse4_estu = build_and_persist_estu_membership(
                db_path=DATA_DB,
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
    risk_engine_meta: RiskEngineMetaPayload = (
        runtime_state.load_runtime_state(
            "risk_engine_meta",
            fallback_loader=sqlite.cache_get_live_first,
        )
        or {}
    )
    should_recompute, recompute_reason = _risk_recompute_due(risk_engine_meta, today_utc=today_utc)
    if skip_risk_engine:
        should_recompute = False
        recompute_reason = "orchestrator_precomputed"
    elif light_mode:
        should_recompute = False
        recompute_reason = "light_mode_skip"
    if force_risk_recompute and not skip_risk_engine:
        should_recompute = True
        recompute_reason = "force_risk_recompute"

    cov = _deserialize_covariance(sqlite.cache_get_live_first("risk_engine_cov"))
    cached_specific = sqlite.cache_get_live_first("risk_engine_specific_risk")
    specific_risk_by_security: dict[str, SpecificRiskPayload] = (
        cached_specific if isinstance(cached_specific, dict) else {}
    )
    latest_r2 = _finite_float(risk_engine_meta.get("latest_r2"), 0.0)

    if skip_risk_engine:
        if cov.empty or not isinstance(cached_specific, dict):
            raise RuntimeError(
                "skip_risk_engine requested but risk-engine cache is missing; "
                "run orchestrator core stages first or disable skip_risk_engine."
            )
    else:
        if cov.empty:
            should_recompute = True
            recompute_reason = "missing_covariance_cache"
        if not isinstance(cached_specific, dict):
            should_recompute = True
            recompute_reason = "missing_specific_risk_cache"

    recomputed_this_refresh = False
    if should_recompute:
        logger.info(
            "Recomputing risk engine (%s): daily factor returns -> covariance -> specific risk",
            recompute_reason,
        )
        compute_daily_factor_returns(
            DATA_DB,
            CACHE_DB,
            min_cross_section_age_days=config.CROSS_SECTION_MIN_AGE_DAYS,
        )
        cov, latest_r2 = build_factor_covariance_from_cache(
            CACHE_DB, lookback_days=config.LOOKBACK_DAYS
        )
        specific_risk_by_security = build_specific_risk_from_cache(
            CACHE_DB,
            lookback_days=config.LOOKBACK_DAYS,
        )
        risk_engine_meta = {
            "status": "ok",
            "method_version": RISK_ENGINE_METHOD_VERSION,
            "last_recompute_date": today_utc.isoformat(),
            "factor_returns_latest_date": _latest_factor_return_date(CACHE_DB),
            "lookback_days": int(config.LOOKBACK_DAYS),
            "cross_section_min_age_days": int(config.CROSS_SECTION_MIN_AGE_DAYS),
            "recompute_interval_days": int(config.RISK_RECOMPUTE_INTERVAL_DAYS),
            "latest_r2": float(latest_r2),
            "specific_risk_ticker_count": int(len(specific_risk_by_security)),
        }
        recomputed_this_refresh = True
        logger.info(
            "Risk engine recompute complete: factor_count=%s specific_risk_count=%s latest_r2=%.4f",
            int(cov.shape[1]) if cov is not None and not cov.empty else 0,
            int(len(specific_risk_by_security)),
            float(latest_r2),
        )
    else:
        logger.info(
            "Skipping risk-engine recompute (%s). Reusing cached covariance/specific risk.",
            recompute_reason,
        )

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
        sqlite.cache_get("universe_loadings")
        if light_mode and not recomputed_this_refresh
        else None
    )
    if cached_universe_loadings is not None:
        universe_loadings_reused, universe_loadings_reuse_reason = _can_reuse_cached_universe_loadings(
            cached_universe_loadings,
            source_dates=source_dates,
            risk_engine_meta=risk_engine_meta,
        )

    if universe_loadings_reused:
        universe_loadings = dict(cached_universe_loadings)
        logger.info(
            "Reusing cached universe loadings for light refresh (%s): ticker_count=%s eligible_ticker_count=%s factor_count=%s",
            universe_loadings_reuse_reason,
            int(universe_loadings.get("ticker_count", 0)),
            int(universe_loadings.get("eligible_ticker_count", 0)),
            int(universe_loadings.get("factor_count", 0)),
        )
    else:
        logger.info(
            "Fetching full-universe inputs from local database for rebuild (%s)...",
            universe_loadings_reuse_reason,
        )
        prices_universe_df = core_reads.load_latest_prices()
        fundamentals_universe_df = core_reads.load_latest_fundamentals(
            as_of_date=str(fundamentals_asof) if fundamentals_asof else None,
        )
        exposures_universe_df = core_reads.load_raw_cross_section_latest()
        logger.info(
            "Loaded source rows: prices=%s fundamentals=%s exposures=%s",
            int(len(prices_universe_df)),
            int(len(fundamentals_universe_df)),
            int(len(exposures_universe_df)),
        )
        logger.info("Building full-universe ticker loadings...")
        universe_loadings = _build_universe_ticker_loadings(
            exposures_universe_df,
            fundamentals_universe_df,
            prices_universe_df,
            cov,
            specific_risk_by_ticker=specific_risk_by_ticker,
            factor_catalog_by_name=factor_catalog_by_name,
        )
        universe_loadings_reuse_reason = "rebuilt"
        logger.info(
            "Universe loadings built: ticker_count=%s eligible_ticker_count=%s factor_count=%s",
            int(universe_loadings.get("ticker_count", 0)),
            int(universe_loadings.get("eligible_ticker_count", 0)),
            int(universe_loadings.get("factor_count", 0)),
        )

    # 4. Project held positions from full-universe cache
    logger.info("Projecting held positions from full-universe cache...")
    positions, total_value = _build_positions_from_universe(universe_loadings["by_ticker"])

    # 5. Risk decomposition
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
    cached_risk_display = _load_cached_risk_display_payload() if reuse_cached_risk_display else None

    # 7. Compute exposure modes
    logger.info("Computing exposure modes...")
    coverage_date, factor_coverage = _load_latest_factor_coverage(CACHE_DB)
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
        coverage_date=coverage_date,
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
    for k in component_shares:
        component_shares[k] = _safe(component_shares[k])

    # 10. Cache everything
    logger.info("Caching results...")
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
        component_shares=component_shares,
        factor_details=factor_details,
        cov_matrix=cov_matrix,
        latest_r2=latest_r2,
        universe_loadings=universe_loadings,
        exposure_modes=exposure_modes,
        factor_catalog=universe_loadings.get("factor_catalog", []),
        cuse4_foundation=cuse4_foundation,
        light_mode=light_mode,
        reuse_cached_static_payloads=bool(
            light_mode
            and universe_loadings_reused
            and not recomputed_this_refresh
        ),
        data_db=DATA_DB,
        cache_db=CACHE_DB,
    )
    snapshot_id = str(staged.get("snapshot_id") or run_id)
    risk_engine_state = dict(staged.get("risk_engine_state") or {})
    sanity = dict(staged.get("sanity") or {"status": "no-data", "warnings": [], "checks": {}})
    health_refreshed = bool(staged.get("health_refreshed", False))
    persisted_payloads = dict(staged.get("persisted_payloads") or {})

    model_outputs_write: dict[str, Any] = {"status": "skipped"}
    serving_outputs_write: dict[str, Any] = {"status": "skipped"}
    skip_model_outputs_persistence = not recomputed_this_refresh
    try:
        if skip_model_outputs_persistence:
            model_outputs_write = {
                "status": "skipped",
                "reason": "risk_engine_reused",
                "run_id": run_id,
            }
        else:
            model_outputs_write = model_outputs.persist_model_outputs(
                data_db=DATA_DB,
                cache_db=CACHE_DB,
                run_id=run_id,
                refresh_mode=refresh_mode,
                status="ok",
                started_at=refresh_started_at,
                completed_at=datetime.now(timezone.utc).isoformat(),
                source_dates=source_dates,
                params={
                    "force_risk_recompute": bool(force_risk_recompute),
                    "mode": refresh_mode,
                    "lookback_days": int(config.LOOKBACK_DAYS),
                    "cross_section_min_age_days": int(config.CROSS_SECTION_MIN_AGE_DAYS),
                    "risk_recompute_interval_days": int(config.RISK_RECOMPUTE_INTERVAL_DAYS),
                    "cross_section_snapshot_mode": str(config.CROSS_SECTION_SNAPSHOT_MODE or "current"),
                },
                risk_engine_state=risk_engine_state,
                cov=cov,
                specific_risk_by_ticker=specific_risk_by_security,
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to persist relational model outputs")
        model_outputs_write = {
            "status": "error",
            "run_id": run_id,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
        sqlite.cache_set("model_outputs_write", model_outputs_write)
        raise RuntimeError(f"Relational model output persistence failed: {type(exc).__name__}: {exc}") from exc
    sqlite.cache_set("model_outputs_write", model_outputs_write)
    try:
        serving_outputs_write = serving_outputs.persist_current_payloads(
            data_db=DATA_DB,
            run_id=run_id,
            snapshot_id=snapshot_id,
            refresh_mode=refresh_mode,
            payloads=persisted_payloads,
            replace_all=True,
        )
        neon_write = serving_outputs_write.get("neon_write") if isinstance(serving_outputs_write, dict) else None
        if (
            config.serving_payload_neon_write_required()
            and isinstance(neon_write, dict)
            and str(neon_write.get("status") or "") != "ok"
        ):
            raise RuntimeError(f"Serving payload Neon write failed: {neon_write}")
    except Exception as exc:  # noqa: BLE001
        logger.exception("Failed to persist serving payloads")
        serving_outputs_write = {
            "status": "error",
            "run_id": run_id,
            "error": {"type": type(exc).__name__, "message": str(exc)},
        }
        sqlite.cache_set("serving_outputs_write", serving_outputs_write)
        raise RuntimeError(f"Serving payload persistence failed: {type(exc).__name__}: {exc}") from exc
    sqlite.cache_set("serving_outputs_write", serving_outputs_write)
    runtime_state.persist_runtime_state(
        "risk_engine_meta",
        risk_engine_meta,
        fallback_writer=lambda key, value: sqlite.cache_set(key, value),
    )
    runtime_state.publish_active_snapshot(
        snapshot_id,
        fallback_publisher=sqlite.cache_publish_snapshot,
    )

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
        "universe_loadings_reused": bool(universe_loadings_reused),
        "universe_loadings_reuse_reason": str(universe_loadings_reuse_reason),
        "model_outputs_write": model_outputs_write,
        "serving_outputs_write": serving_outputs_write,
    }
