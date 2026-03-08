"""Analytics pipeline: fetch → compute → cache."""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend import config
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
    specific_risk_by_ticker_view as _specific_risk_by_ticker_view_impl,
)
from backend.analytics.services.universe_loadings import (
    build_universe_ticker_loadings as _build_universe_ticker_loadings_impl,
    load_latest_factor_coverage as _load_latest_factor_coverage_impl,
)
from backend.data import model_outputs, postgres, rebuild_cross_section_snapshot, sqlite
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
RISK_ENGINE_METHOD_VERSION = "v4_trbc_l2_country_us_dummy_2026_03_08"


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    return out if np.isfinite(out) else float(default)


def _parse_iso_date(value: Any) -> date | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value)).date()
    except (TypeError, ValueError):
        return None


def _serialize_covariance(cov: pd.DataFrame) -> CovariancePayload:
    if cov is None or cov.empty:
        return {"factors": [], "matrix": []}
    factors = [str(c) for c in cov.columns]
    mat = cov.reindex(index=factors, columns=factors).to_numpy(dtype=float)
    return {
        "factors": factors,
        "matrix": [[_finite_float(v, 0.0) for v in row] for row in mat.tolist()],
    }


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
    specific_risk_by_ticker: dict[str, SpecificRiskPayload] | None = None,
) -> UniverseLoadingsPayload:
    """Build full-universe cached loadings/risk context keyed by ticker."""
    return _build_universe_ticker_loadings_impl(
        exposures_df,
        fundamentals_df,
        prices_df,
        cov,
        data_db=DATA_DB,
        specific_risk_by_ticker=specific_risk_by_ticker,
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


def run_refresh(
    *,
    force_risk_recompute: bool = False,
    mode: str = "full",
    skip_snapshot_rebuild: bool = False,
    skip_cuse4_foundation: bool = False,
    skip_risk_engine: bool = False,
) -> dict[str, Any]:
    """Pipeline refresh with two modes:
    - full: weekly-gated risk engine + all downstream caches (including health diagnostics)
    - light: fast cache refresh path; skips health diagnostics and avoids risk recompute
      unless core risk caches are missing.
    """
    logger.info("Starting refresh pipeline...")
    refresh_mode = str(mode or "full").strip().lower()
    if refresh_mode not in {"full", "light"}:
        refresh_mode = "full"
    light_mode = refresh_mode == "light"

    refresh_started_at = datetime.now(timezone.utc).isoformat()
    run_id = f"model_run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    today_utc = datetime.fromisoformat(
        previous_or_same_xnys_session(datetime.now(timezone.utc).date().isoformat())
    ).date()

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

    # 1. Fetch full-universe data from local data.db
    logger.info("Fetching data from local database...")
    source_dates: SourceDatesPayload = postgres.load_source_dates()
    fundamentals_asof = source_dates.get("exposures_asof")
    prices_universe_df = postgres.load_latest_prices()
    fundamentals_universe_df = postgres.load_latest_fundamentals(
        as_of_date=str(fundamentals_asof) if fundamentals_asof else None,
    )
    exposures_universe_df = postgres.load_raw_cross_section_latest()
    logger.info(
        "Loaded source rows: prices=%s fundamentals=%s exposures=%s",
        int(len(prices_universe_df)),
        int(len(fundamentals_universe_df)),
        int(len(exposures_universe_df)),
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
                    db_path=DATA_DB,
                    replace_all=True,
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
        sqlite.cache_get_live("risk_engine_meta")
        or sqlite.cache_get("risk_engine_meta")
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

    cov = _deserialize_covariance(
        sqlite.cache_get_live("risk_engine_cov")
        or sqlite.cache_get("risk_engine_cov")
    )
    cached_specific = (
        sqlite.cache_get_live("risk_engine_specific_risk")
        or sqlite.cache_get("risk_engine_specific_risk")
    )
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

    # 3. Build/cached full-universe loadings first (portfolio is a final projection only).
    logger.info("Building full-universe ticker loadings...")
    universe_loadings = _build_universe_ticker_loadings(
        exposures_universe_df,
        fundamentals_universe_df,
        prices_universe_df,
        cov,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )
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
        "country": float(raw_risk_shares.get("country", 0.0)),
        "industry": float(raw_risk_shares.get("industry", 0.0)),
        "style": float(raw_risk_shares.get("style", 0.0)),
        "idio": float(raw_risk_shares.get("idio", 0.0)),
    }
    component_shares: ComponentSharesPayload = {
        "country": float(raw_component_shares.get("country", 0.0)),
        "industry": float(raw_component_shares.get("industry", 0.0)),
        "style": float(raw_component_shares.get("style", 0.0)),
    }
    factor_details: list[FactorDetailPayload] = [dict(row) for row in raw_factor_details]
    logger.info(
        "Risk decomposition complete: country=%.2f industry=%.2f style=%.2f idio=%.2f factors=%s",
        float(risk_shares["country"]),
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

    # 6. Compute per-position risk contributions
    for pos in positions:
        exps = pos.get("exposures", {})
        risk_score = sum(
            abs(float(exps.get(d["factor"], 0.0)) * d["sensitivity"])
            for d in factor_details
        )
        pos["risk_contrib_pct"] = round(risk_score * pos["weight"] * 100, 2)
        pos["risk_mix"] = dict(position_risk_mix.get(str(pos.get("ticker", "")).upper(), {
            "country": 0.0,
            "industry": 0.0,
            "style": 0.0,
            "idio": 0.0,
        }))

    # 7. Compute condition number from cov matrix
    condition_number = 0.0
    if not cov.empty:
        try:
            cn = float(np.linalg.cond(cov.to_numpy()))
            condition_number = cn if np.isfinite(cn) else 9999.99
        except Exception:
            pass

    # 8. Compute exposure modes
    logger.info("Computing exposure modes...")
    coverage_date, factor_coverage = _load_latest_factor_coverage(CACHE_DB)
    exposure_modes: ExposureModesPayload = _compute_exposures_modes(
        positions,
        cov,
        factor_details,
        factor_coverage=factor_coverage,
        coverage_date=coverage_date,
    )

    # 9. Build covariance matrix for frontend (correlation) — style factors only
    STYLE_FACTOR_NAMES = {
        "Size", "Nonlinear Size", "Liquidity", "Beta",
        "Book-to-Price", "Earnings Yield", "Value", "Leverage",
        "Growth", "Profitability", "Investment", "Dividend Yield",
        "Momentum", "Short-Term Reversal", "Residual Volatility",
    }
    cov_matrix: CovarianceMatrixPayload = {}
    if not cov.empty:
        all_factors = list(cov.columns)
        style_idx = [i for i, f in enumerate(all_factors) if f in STYLE_FACTOR_NAMES]
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

    # 10. Sanitize non-finite floats (NaN/Inf break JSON serialization)
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

    # 11. Cache everything
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
        condition_number=condition_number,
        universe_loadings=universe_loadings,
        exposure_modes=exposure_modes,
        cuse4_foundation=cuse4_foundation,
        light_mode=light_mode,
        data_db=DATA_DB,
        cache_db=CACHE_DB,
    )
    snapshot_id = str(staged.get("snapshot_id") or run_id)
    risk_engine_state = dict(staged.get("risk_engine_state") or {})
    sanity = dict(staged.get("sanity") or {"status": "no-data", "warnings": [], "checks": {}})
    health_refreshed = bool(staged.get("health_refreshed", False))

    model_outputs_write: dict[str, Any] = {"status": "skipped"}
    try:
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
    sqlite.cache_publish_snapshot(snapshot_id)

    logger.info("Refresh complete.")
    return {
        "status": "ok",
        "run_id": run_id,
        "snapshot_id": snapshot_id,
        "positions": len(positions),
        "total_value": round(total_value, 2),
        "mode": refresh_mode,
        "cross_section_snapshot": snapshot_build,
        "risk_engine": risk_engine_state,
        "model_sanity": sanity,
        "cuse4_foundation": cuse4_foundation,
        "health_refreshed": bool(health_refreshed),
        "model_outputs_write": model_outputs_write,
    }
