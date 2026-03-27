"""Dedicated cPAR package-build stages."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from backend.cpar import backtransform, orthogonalization, regression, returns_panel, status_rules
from backend.cpar.contracts import FactorSpec, OrthogonalizationResult, WeeklyReturnSeries, to_serializable_mapping
from backend.cpar.factor_registry import (
    CPAR1_FACTOR_REGISTRY_VERSION,
    CPAR1_METHOD_VERSION,
    MARKET_FACTOR_ID,
    build_cpar1_factor_registry,
    ordered_factor_ids,
)
from backend.cpar.hedge_engine import covariance_matrix_for_factors
from backend.cpar.weekly_anchors import (
    DEFAULT_HALF_LIFE_WEEKS,
    DEFAULT_LOOKBACK_WEEKS,
    generate_weekly_price_anchors,
)
from backend.data import cpar_outputs, cpar_source_reads, core_read_backend

PROXY_PRICE_RULE = "adj_close_fallback_close"

ProgressCallback = Callable[[dict[str, Any]], None]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _emit_progress(progress_callback: ProgressCallback | None, *, message: str, progress_kind: str) -> None:
    if progress_callback is not None:
        progress_callback({"message": message, "progress_kind": progress_kind})


def _group_rows_by_key(rows: list[dict[str, Any]], *, key: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        raw_key = row.get(key)
        if raw_key is None:
            continue
        grouped[str(raw_key)].append(dict(row))
    return dict(grouped)


def _single_row_by_key(rows: list[dict[str, Any]], *, key: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        raw_key = row.get(key)
        if raw_key is None:
            continue
        grouped[str(raw_key)] = dict(row)
    return grouped


def _max_nonempty(rows: list[dict[str, Any]], *, key: str) -> str | None:
    values = [str(row.get(key)) for row in rows if row.get(key)]
    return max(values) if values else None


def _resolve_factor_proxy_map(
    factor_specs: tuple[FactorSpec, ...],
    proxy_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    rows_by_ticker = _group_rows_by_key(proxy_rows, key="ticker")
    resolved: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    duplicates: list[str] = []
    for spec in factor_specs:
        matches = rows_by_ticker.get(spec.ticker, [])
        if not matches:
            missing.append(spec.ticker)
            continue
        if len(matches) > 1:
            duplicates.append(spec.ticker)
            continue
        resolved[spec.factor_id] = dict(matches[0])
    if missing or duplicates:
        parts: list[str] = []
        if missing:
            parts.append(f"missing={','.join(sorted(missing))}")
        if duplicates:
            parts.append(f"duplicates={','.join(sorted(duplicates))}")
        raise ValueError("Unable to resolve unique cPAR factor proxies: " + " ".join(parts))
    return resolved


def _assert_terminal_anchor_prices_available(
    *,
    package_date: str,
    factor_proxy_by_id: dict[str, dict[str, Any]],
    price_rows_by_ric: dict[str, list[dict[str, Any]]],
) -> None:
    missing: list[str] = []
    for factor_id, proxy_row in factor_proxy_by_id.items():
        ric = str(proxy_row.get("ric") or "")
        selections = returns_panel.select_weekly_prices(
            price_rows_by_ric.get(ric, []),
            price_anchors=(package_date,),
        )
        if not selections or selections[0].price_date is None:
            missing.append(factor_id)
    if missing:
        raise ValueError(
            "Local source archive is not current through the requested cPAR package date. "
            f"Missing anchor-week price for factor proxies: {', '.join(sorted(missing))}"
        )


def _build_factor_return_series(
    *,
    factor_specs: tuple[FactorSpec, ...],
    factor_proxy_by_id: dict[str, dict[str, Any]],
    price_rows_by_ric: dict[str, list[dict[str, Any]]],
    price_anchors: tuple[str, ...],
    package_date: str,
) -> tuple[dict[str, WeeklyReturnSeries], list[dict[str, Any]]]:
    series_by_id: dict[str, WeeklyReturnSeries] = {}
    proxy_return_rows: list[dict[str, Any]] = []
    for spec in factor_specs:
        proxy_row = factor_proxy_by_id[spec.factor_id]
        ric = str(proxy_row["ric"])
        series = returns_panel.build_weekly_return_series(
            price_rows_by_ric.get(ric, []),
            price_anchors=price_anchors,
            package_date=package_date,
            lookback_weeks=DEFAULT_LOOKBACK_WEEKS,
            half_life_weeks=DEFAULT_HALF_LIFE_WEEKS,
        )
        if series.observed_weeks != DEFAULT_LOOKBACK_WEEKS or int(series.longest_gap_weeks) != 0:
            raise ValueError(
                f"Incomplete cPAR factor proxy history for {spec.factor_id}: "
                f"observed_weeks={series.observed_weeks} longest_gap_weeks={series.longest_gap_weeks}"
            )
        series_by_id[spec.factor_id] = series
        for week_end, return_value, weight_value in zip(
            series.return_anchors,
            series.returns,
            series.weights,
            strict=True,
        ):
            if not np.isfinite(float(return_value)):
                raise ValueError(f"Non-finite cPAR factor return for {spec.factor_id} on {week_end}")
            proxy_return_rows.append(
                {
                    "package_date": package_date,
                    "week_end": str(week_end),
                    "factor_id": spec.factor_id,
                    "factor_group": spec.group,
                    "proxy_ric": ric,
                    "proxy_ticker": str(proxy_row.get("ticker") or spec.ticker),
                    "return_value": float(return_value),
                    "weight_value": float(weight_value),
                    "price_field_used": str(series.price_field_used),
                }
            )
    return series_by_id, proxy_return_rows


def _weighted_covariance_rows(
    factor_specs: tuple[FactorSpec, ...],
    series_by_id: dict[str, WeeklyReturnSeries],
    *,
    package_date: str,
) -> list[dict[str, Any]]:
    factor_ids = tuple(spec.factor_id for spec in factor_specs)
    matrix = np.column_stack([series_by_id[factor_id].returns for factor_id in factor_ids])
    weights = regression.normalize_weights(series_by_id[MARKET_FACTOR_ID].weights)
    means = np.sum(matrix * weights[:, None], axis=0)
    centered = matrix - means
    covariance = (centered * weights[:, None]).T @ centered
    diag = np.clip(np.diag(covariance), a_min=0.0, a_max=None)
    vol = np.sqrt(diag)
    rows: list[dict[str, Any]] = []
    for row_idx, left in enumerate(factor_ids):
        for col_idx, right in enumerate(factor_ids):
            denom = float(vol[row_idx] * vol[col_idx])
            corr = 0.0 if denom <= 0.0 else float(covariance[row_idx, col_idx] / denom)
            rows.append(
                {
                    "package_date": package_date,
                    "factor_id": left,
                    "factor_id_2": right,
                    "covariance": float(covariance[row_idx, col_idx]),
                    "correlation": corr,
                }
            )
    return rows


def _weighted_residualized_covariance_lookup(
    factor_specs: tuple[FactorSpec, ...],
    market_series: WeeklyReturnSeries,
    orth_result: OrthogonalizationResult,
) -> dict[tuple[str, str], float]:
    factor_ids = tuple(spec.factor_id for spec in factor_specs)
    series_columns: list[np.ndarray] = []
    for factor_id in factor_ids:
        if factor_id == MARKET_FACTOR_ID:
            series_columns.append(np.asarray(market_series.returns, dtype=float))
            continue
        idx = orth_result.factor_ids.index(factor_id)
        series_columns.append(np.asarray(orth_result.residual_matrix[:, idx], dtype=float))
    matrix = np.column_stack(series_columns)
    weights = regression.normalize_weights(market_series.weights)
    means = np.sum(matrix * weights[:, None], axis=0)
    centered = matrix - means
    covariance = (centered * weights[:, None]).T @ centered
    lookup: dict[tuple[str, str], float] = {}
    for row_idx, left in enumerate(factor_ids):
        for col_idx, right in enumerate(factor_ids):
            lookup[(left, right)] = float(covariance[row_idx, col_idx])
    return lookup


def _covariance_lookup(covariance_rows: list[dict[str, Any]]) -> dict[tuple[str, str], float]:
    lookup: dict[tuple[str, str], float] = {}
    for row in covariance_rows:
        lookup[(str(row["factor_id"]), str(row["factor_id_2"]))] = float(row["covariance"])
    return lookup


def _variance_proxy(loadings: dict[str, float], covariance_lookup: dict[tuple[str, str], float]) -> float:
    factor_ids = tuple(
        factor_id
        for factor_id in ordered_factor_ids(include_market=True)
        if factor_id in loadings
    )
    if not factor_ids:
        return 0.0
    beta = np.asarray([float(loadings[factor_id]) for factor_id in factor_ids], dtype=float)
    covariance = covariance_matrix_for_factors(factor_ids, covariance_lookup)
    return float(beta.T @ covariance @ beta)


def _ordered_loadings(loadings: dict[str, float]) -> dict[str, float]:
    ordered = {
        factor_id: float(loadings.get(factor_id, 0.0))
        for factor_id in ordered_factor_ids(include_market=True)
        if factor_id in loadings
    }
    return to_serializable_mapping(ordered)


def _fit_instrument_row(
    *,
    package_date: str,
    universe_row: dict[str, Any],
    price_rows_by_ric: dict[str, list[dict[str, Any]]],
    price_anchors: tuple[str, ...],
    market_series: WeeklyReturnSeries,
    orth_result: Any,
    residualized_covariance_lookup: dict[tuple[str, str], float],
    classification_by_ric: dict[str, dict[str, Any]],
    common_name_by_ric: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    ric = str(universe_row.get("ric") or "")
    ticker = str(universe_row.get("ticker") or "") or None
    classification = classification_by_ric.get(ric, {})
    common_name = str(common_name_by_ric.get(ric, {}).get("common_name") or "") or None
    display_name = common_name or ticker or ric
    series = returns_panel.build_weekly_return_series(
        price_rows_by_ric.get(ric, []),
        price_anchors=price_anchors,
        package_date=package_date,
        lookback_weeks=DEFAULT_LOOKBACK_WEEKS,
        half_life_weeks=DEFAULT_HALF_LIFE_WEEKS,
    )
    summary = status_rules.summarize_return_series(
        series,
        min_observations=status_rules.DEFAULT_MIN_OBSERVATIONS,
        hq_country_code=classification.get("hq_country_code"),
    )
    fit_row: dict[str, Any] = {
        "package_date": package_date,
        "ric": ric,
        "ticker": ticker,
        "display_name": display_name,
        "fit_status": summary.fit_status,
        "warnings": list(summary.warnings),
        "observed_weeks": summary.observed_weeks,
        "lookback_weeks": summary.lookback_weeks,
        "longest_gap_weeks": summary.longest_gap_weeks,
        "price_field_used": series.price_field_used,
        "hq_country_code": classification.get("hq_country_code"),
        "allow_cpar_core_target": int(universe_row.get("allow_cpar_core_target") or 0),
        "allow_cpar_extended_target": int(universe_row.get("allow_cpar_extended_target") or 0),
        "is_single_name_equity": int(universe_row.get("is_single_name_equity") or 0),
        "market_step_alpha": None,
        "market_step_beta": None,
        "block_alpha": None,
        "spy_trade_beta_raw": None,
        "raw_loadings": {},
        "thresholded_loadings": {},
        "factor_variance_proxy": None,
        "factor_volatility_proxy": None,
        "specific_variance_proxy": None,
        "specific_volatility_proxy": None,
    }
    if summary.fit_status == status_rules.FIT_STATUS_INSUFFICIENT:
        return fit_row

    valid_mask = np.asarray(series.observed_mask, dtype=bool) & np.asarray(market_series.observed_mask, dtype=bool)
    if int(np.count_nonzero(valid_mask)) < status_rules.DEFAULT_MIN_OBSERVATIONS:
        return fit_row

    weights = regression.normalize_weights(market_series.weights[valid_mask])
    fit = regression.fit_market_plus_residualized_block(
        series.returns[valid_mask],
        market_series.returns[valid_mask],
        {
            factor_id: orth_result.residual_matrix[valid_mask, idx]
            for idx, factor_id in enumerate(orth_result.factor_ids)
        },
        weights,
        factor_groups=orth_result.factor_groups,
        sector_lambda=1.0,
        style_lambda=2.0,
    )
    trade_space = backtransform.backtransform_trade_space_from_one_shot(
        fit=fit,
        orthogonalization=orth_result,
    )
    residualized_loadings = _ordered_loadings({MARKET_FACTOR_ID: fit.market_beta, **fit.residualized_betas})
    thresholded = _ordered_loadings(backtransform.threshold_trade_space_loadings(residualized_loadings))
    variance_proxy = _variance_proxy(thresholded, residualized_covariance_lookup)
    specific_variance_proxy = float(regression.weighted_std(fit.residuals, weights) ** 2)
    fit_row.update(
        {
            "market_step_alpha": float(fit.alpha),
            "market_step_beta": float(fit.market_beta),
            "block_alpha": None,
            "spy_trade_beta_raw": float(trade_space.spy_trade_beta),
            "raw_loadings": residualized_loadings,
            "thresholded_loadings": thresholded,
            "factor_variance_proxy": float(variance_proxy),
            "factor_volatility_proxy": float(math.sqrt(max(variance_proxy, 0.0))),
            "specific_variance_proxy": specific_variance_proxy,
            "specific_volatility_proxy": float(math.sqrt(max(specific_variance_proxy, 0.0))),
        }
    )
    return fit_row


def run_source_read_stage(
    *,
    package_date: str,
    data_db: Path,
    progress_callback: ProgressCallback | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    factor_specs = build_cpar1_factor_registry()
    price_anchors = generate_weekly_price_anchors(package_date, lookback_weeks=DEFAULT_LOOKBACK_WEEKS)
    _emit_progress(progress_callback, message="Resolving cPAR factor proxies from local source archive.", progress_kind="io")
    with core_read_backend.core_read_backend("local"):
        proxy_rows = cpar_source_reads.resolve_factor_proxy_rows(
            [spec.ticker for spec in factor_specs],
            data_db=data_db,
        )
        factor_proxy_by_id = _resolve_factor_proxy_map(factor_specs, proxy_rows)
        universe_rows = cpar_source_reads.load_build_universe_rows(data_db=data_db)
        if not universe_rows:
            raise ValueError("No cPAR build-universe rows are available.")
        universe_rics = [str(row["ric"]) for row in universe_rows if row.get("ric")]
        all_rics = sorted({*universe_rics, *(str(row["ric"]) for row in factor_proxy_by_id.values())})
        _emit_progress(progress_callback, message="Loading cPAR package-window prices from local source archive.", progress_kind="io")
        price_rows = cpar_source_reads.load_price_rows_for_rics(
            all_rics,
            date_from=price_anchors[0],
            date_to=price_anchors[-1],
            data_db=data_db,
        )
        classification_rows = cpar_source_reads.load_latest_classification_rows(
            universe_rics,
            as_of_date=package_date,
            data_db=data_db,
        )
        common_name_rows = cpar_source_reads.load_latest_common_name_rows(
            universe_rics,
            as_of_date=package_date,
            data_db=data_db,
        )
    if not price_rows:
        raise ValueError("No cPAR price rows are available for the requested package window.")
    price_rows_by_ric = _group_rows_by_key(price_rows, key="ric")
    _assert_terminal_anchor_prices_available(
        package_date=package_date,
        factor_proxy_by_id=factor_proxy_by_id,
        price_rows_by_ric=price_rows_by_ric,
    )
    return (
        {
            "factor_specs": factor_specs,
            "price_anchors": price_anchors,
            "factor_proxy_by_id": factor_proxy_by_id,
            "universe_rows": [dict(row) for row in universe_rows],
            "price_rows_by_ric": price_rows_by_ric,
            "classification_by_ric": _single_row_by_key(classification_rows, key="ric"),
            "common_name_by_ric": _single_row_by_key(common_name_rows, key="ric"),
            "source_prices_asof": _max_nonempty(price_rows, key="date"),
            "classification_asof": _max_nonempty(classification_rows, key="as_of_date") or package_date,
        },
        {
            "package_date": package_date,
            "factor_count": len(factor_specs),
            "build_universe_count": len(universe_rows),
            "price_row_count": len(price_rows),
            "classification_row_count": len(classification_rows),
            "common_name_row_count": len(common_name_rows),
            "price_window_start": price_anchors[0],
            "price_window_end": price_anchors[-1],
        },
    )


def run_package_build_stage(
    *,
    package_date: str,
    stage_state: dict[str, Any],
    progress_callback: ProgressCallback | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    factor_specs: tuple[FactorSpec, ...] = tuple(stage_state["factor_specs"])
    factor_proxy_by_id: dict[str, dict[str, Any]] = dict(stage_state["factor_proxy_by_id"])
    price_rows_by_ric: dict[str, list[dict[str, Any]]] = dict(stage_state["price_rows_by_ric"])
    price_anchors: tuple[str, ...] = tuple(stage_state["price_anchors"])
    classification_by_ric: dict[str, dict[str, Any]] = dict(stage_state["classification_by_ric"])
    common_name_by_ric: dict[str, dict[str, Any]] = dict(stage_state["common_name_by_ric"])
    universe_rows: list[dict[str, Any]] = list(stage_state["universe_rows"])

    _emit_progress(progress_callback, message="Building cPAR weekly factor return panel.", progress_kind="compute")
    factor_series_by_id, proxy_return_rows = _build_factor_return_series(
        factor_specs=factor_specs,
        factor_proxy_by_id=factor_proxy_by_id,
        price_rows_by_ric=price_rows_by_ric,
        price_anchors=price_anchors,
        package_date=package_date,
    )
    market_series = factor_series_by_id[MARKET_FACTOR_ID]
    non_market_factor_ids = tuple(
        factor_id
        for factor_id in ordered_factor_ids(include_market=False)
        if factor_id in factor_series_by_id
    )
    orth_result = orthogonalization.orthogonalize_proxy_panel(
        market_series.returns,
        {factor_id: factor_series_by_id[factor_id].returns for factor_id in non_market_factor_ids},
        market_series.weights,
    )
    proxy_transform_rows = [
        {
            "package_date": package_date,
            "factor_id": factor_id,
            "factor_group": orth_result.factor_groups[factor_id],
            "proxy_ric": str(factor_proxy_by_id[factor_id]["ric"]),
            "proxy_ticker": str(factor_proxy_by_id[factor_id]["ticker"]),
            "market_alpha": float(orth_result.intercepts[factor_id]),
            "market_beta": float(orth_result.market_betas[factor_id]),
        }
        for factor_id in orth_result.factor_ids
    ]
    covariance_rows = _weighted_covariance_rows(
        factor_specs,
        factor_series_by_id,
        package_date=package_date,
    )
    residualized_covariance_lookup = _weighted_residualized_covariance_lookup(
        factor_specs,
        market_series,
        orth_result,
    )
    _emit_progress(progress_callback, message="Fitting cPAR instrument rows in raw ETF trade space.", progress_kind="compute")
    instrument_fits = [
        _fit_instrument_row(
            package_date=package_date,
            universe_row=row,
            price_rows_by_ric=price_rows_by_ric,
            price_anchors=price_anchors,
            market_series=market_series,
            orth_result=orth_result,
            residualized_covariance_lookup=residualized_covariance_lookup,
            classification_by_ric=classification_by_ric,
            common_name_by_ric=common_name_by_ric,
        )
        for row in universe_rows
    ]
    fit_ok_count = sum(1 for row in instrument_fits if row["fit_status"] == status_rules.FIT_STATUS_OK)
    fit_limited_count = sum(1 for row in instrument_fits if row["fit_status"] == status_rules.FIT_STATUS_LIMITED)
    fit_insufficient_count = sum(
        1 for row in instrument_fits if row["fit_status"] == status_rules.FIT_STATUS_INSUFFICIENT
    )
    return (
        {
            "proxy_return_rows": proxy_return_rows,
            "proxy_transform_rows": proxy_transform_rows,
            "covariance_rows": covariance_rows,
            "instrument_fits": instrument_fits,
        },
        {
            "package_date": package_date,
            "factor_count": len(factor_specs),
            "proxy_return_row_count": len(proxy_return_rows),
            "proxy_transform_row_count": len(proxy_transform_rows),
            "covariance_row_count": len(covariance_rows),
            "instrument_fit_count": len(instrument_fits),
            "fit_ok_count": fit_ok_count,
            "fit_limited_count": fit_limited_count,
            "fit_insufficient_count": fit_insufficient_count,
        },
    )


def run_persist_package_stage(
    *,
    profile: str,
    package_date: str,
    run_id: str,
    pipeline_started_at: str,
    data_db: Path,
    stage_state: dict[str, Any],
    progress_callback: ProgressCallback | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    _emit_progress(progress_callback, message="Persisting durable cPAR package rows.", progress_kind="io")
    instrument_fits = list(stage_state["instrument_fits"])
    package_run = {
        "package_run_id": run_id,
        "package_date": package_date,
        "profile": profile,
        "status": "ok",
        "started_at": pipeline_started_at,
        "completed_at": _now_iso(),
        "method_version": CPAR1_METHOD_VERSION,
        "factor_registry_version": CPAR1_FACTOR_REGISTRY_VERSION,
        "lookback_weeks": DEFAULT_LOOKBACK_WEEKS,
        "half_life_weeks": DEFAULT_HALF_LIFE_WEEKS,
        "min_observations": status_rules.DEFAULT_MIN_OBSERVATIONS,
        "proxy_price_rule": PROXY_PRICE_RULE,
        "source_prices_asof": stage_state.get("source_prices_asof"),
        "classification_asof": stage_state.get("classification_asof"),
        "universe_count": len(instrument_fits),
        "fit_ok_count": sum(1 for row in instrument_fits if row["fit_status"] == status_rules.FIT_STATUS_OK),
        "fit_limited_count": sum(1 for row in instrument_fits if row["fit_status"] == status_rules.FIT_STATUS_LIMITED),
        "fit_insufficient_count": sum(
            1 for row in instrument_fits if row["fit_status"] == status_rules.FIT_STATUS_INSUFFICIENT
        ),
        "error_type": None,
        "error_message": None,
    }
    persist_result = cpar_outputs.persist_cpar_package(
        data_db=data_db,
        package_run=package_run,
        proxy_returns=list(stage_state["proxy_return_rows"]),
        proxy_transforms=list(stage_state["proxy_transform_rows"]),
        covariance_rows=list(stage_state["covariance_rows"]),
        instrument_fits=instrument_fits,
    )
    return (
        {"persist_result": persist_result},
        {
            "package_date": package_date,
            "package_run_id": run_id,
            "authority_store": str(persist_result.get("authority_store") or ""),
            "row_counts": dict(persist_result.get("row_counts") or {}),
            "neon_write": dict(persist_result.get("neon_write") or {}),
            "sqlite_mirror_write": dict(persist_result.get("sqlite_mirror_write") or {}),
        },
    )


def run_stage(
    *,
    stage: str,
    profile: str,
    package_date: str,
    run_id: str,
    data_db: Path,
    context: dict[str, Any],
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    if stage == "source_read":
        state_updates, details = run_source_read_stage(
            package_date=package_date,
            data_db=data_db,
            progress_callback=progress_callback,
        )
    elif stage == "package_build":
        state_updates, details = run_package_build_stage(
            package_date=package_date,
            stage_state=context,
            progress_callback=progress_callback,
        )
    elif stage == "persist_package":
        state_updates, details = run_persist_package_stage(
            profile=profile,
            package_date=package_date,
            run_id=run_id,
            pipeline_started_at=str(context["run_started_at"]),
            data_db=data_db,
            stage_state=context,
            progress_callback=progress_callback,
        )
    else:
        raise ValueError(f"Unsupported cPAR stage '{stage}'")
    context.update(state_updates)
    return {
        "status": "ok",
        **details,
    }
