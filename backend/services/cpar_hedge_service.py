"""Read-only cPAR hedge preview payload service."""

from __future__ import annotations

from typing import Any

from backend.cpar import hedge_engine
from backend.cpar.factor_registry import MARKET_FACTOR_ID, build_cpar1_factor_registry
from backend.data import cpar_outputs, cpar_queries
from backend.services import cpar_meta_service


def _covariance_lookup(rows: list[dict[str, Any]]) -> dict[tuple[str, str], float]:
    return {
        (str(row["factor_id"]), str(row["factor_id_2"])): float(row["covariance"])
        for row in rows
    }


def _factor_index() -> dict[str, dict[str, object]]:
    return {
        spec.factor_id: {
            "label": spec.label,
            "group": spec.group,
            "display_order": int(spec.display_order),
        }
        for spec in build_cpar1_factor_registry()
    }


def _hedge_leg_rows(hedge_legs: tuple[Any, ...], *, factor_index: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for leg in hedge_legs:
        spec = factor_index.get(str(leg.factor_id), {})
        rows.append(
            {
                "factor_id": str(leg.factor_id),
                "label": spec.get("label"),
                "group": spec.get("group"),
                "display_order": spec.get("display_order"),
                "weight": float(leg.weight),
            }
        )
    return rows


def _post_hedge_exposure_rows(
    *,
    pre_loadings: dict[str, float],
    hedge_weights: dict[str, float],
    post_loadings: dict[str, float],
    factor_index: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    factor_ids = sorted(
        {*(str(key) for key in pre_loadings.keys()), *(str(key) for key in hedge_weights.keys()), *(str(key) for key in post_loadings.keys())},
        key=lambda factor_id: (
            factor_id != MARKET_FACTOR_ID,
            -abs(float(pre_loadings.get(factor_id, 0.0))),
            factor_id,
        ),
    )
    rows: list[dict[str, object]] = []
    for factor_id in factor_ids:
        spec = factor_index.get(factor_id, {})
        rows.append(
            {
                "factor_id": factor_id,
                "label": spec.get("label"),
                "group": spec.get("group"),
                "display_order": spec.get("display_order"),
                "pre_beta": float(pre_loadings.get(factor_id, 0.0)),
                "hedge_leg": float(hedge_weights.get(factor_id, 0.0)),
                "post_beta": float(post_loadings.get(factor_id, 0.0)),
            }
        )
    return rows


def load_cpar_hedge_payload(
    ticker: str,
    *,
    mode: str,
    ric: str | None = None,
    data_db=None,
) -> dict[str, object]:
    package = cpar_meta_service.require_active_package(data_db=data_db)
    previous_weights: dict[str, float] | None = None
    try:
        fit = cpar_outputs.load_package_instrument_fit(
            ticker=ticker,
            package_run_id=str(package["package_run_id"]),
            ric=ric,
            data_db=data_db,
        )
        covariance_rows = cpar_outputs.load_package_covariance_rows(
            str(package["package_run_id"]),
            data_db=data_db,
            require_complete=True,
            context_label="Active cPAR package",
        )
    except cpar_outputs.CparPackageNotReady as exc:
        raise cpar_meta_service.CparReadNotReady(str(exc)) from exc
    except cpar_outputs.CparAuthorityReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc
    except cpar_queries.CparAmbiguousInstrumentFit as exc:
        raise cpar_meta_service.CparTickerAmbiguous(str(exc)) from exc
    if fit is None:
        raise cpar_meta_service.CparTickerNotFound(
            f"Ticker {str(ticker).upper().strip()} was not found in the active cPAR package."
        )
    covariance = _covariance_lookup(covariance_rows)
    try:
        previous_fit = cpar_outputs.load_previous_successful_instrument_fit(
            ric=str(fit.get("ric") or ""),
            before_package_date=str(package["package_date"]),
            data_db=data_db,
        )
        if previous_fit is not None:
            previous_covariance_rows = cpar_outputs.load_package_covariance_rows(
                str(previous_fit["package_run_id"]),
                data_db=data_db,
                require_complete=True,
                context_label="Previous cPAR package used for hedge stability diagnostics",
            )
            previous_preview = hedge_engine.build_hedge_preview(
                mode=mode,
                thresholded_loadings=dict(previous_fit.get("thresholded_loadings") or {}),
                covariance=_covariance_lookup(previous_covariance_rows),
                fit_status=str(previous_fit.get("fit_status") or ""),
            )
            previous_weights = dict(previous_preview.hedge_weights)
    except (cpar_outputs.CparPackageNotReady, cpar_outputs.CparAuthorityReadError):
        previous_weights = None
    preview = hedge_engine.build_hedge_preview(
        mode=mode,
        thresholded_loadings=dict(fit.get("thresholded_loadings") or {}),
        covariance=covariance,
        fit_status=str(fit.get("fit_status") or ""),
        previous_hedge_weights=previous_weights,
    )
    factor_index = _factor_index()
    return {
        **cpar_meta_service.package_meta_payload(package),
        "ticker": fit.get("ticker"),
        "ric": fit.get("ric"),
        "display_name": fit.get("display_name"),
        "fit_status": fit.get("fit_status"),
        "warnings": list(fit.get("warnings") or []),
        "mode": str(preview.mode),
        "hedge_status": str(preview.status),
        "hedge_reason": preview.reason,
        "hedge_legs": _hedge_leg_rows(preview.hedge_legs, factor_index=factor_index),
        "post_hedge_exposures": _post_hedge_exposure_rows(
            pre_loadings=dict(fit.get("thresholded_loadings") or {}),
            hedge_weights=dict(preview.hedge_weights),
            post_loadings=dict(preview.post_hedge_loadings),
            factor_index=factor_index,
        ),
        "pre_hedge_factor_variance_proxy": float(preview.pre_hedge_variance_proxy),
        "post_hedge_factor_variance_proxy": float(preview.post_hedge_variance_proxy),
        "gross_hedge_notional": float(preview.gross_hedge_notional),
        "net_hedge_notional": float(preview.net_hedge_notional),
        "non_market_reduction_ratio": preview.non_market_reduction_ratio,
        "stability": {
            "leg_overlap_ratio": preview.stability.leg_overlap_ratio,
            "gross_hedge_notional_change": preview.stability.gross_hedge_notional_change,
            "net_hedge_notional_change": preview.stability.net_hedge_notional_change,
        },
    }
