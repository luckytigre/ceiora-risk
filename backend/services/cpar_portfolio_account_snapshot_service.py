"""Shared account-scoped cPAR hedge snapshot builder."""

from __future__ import annotations

from typing import Any

from backend.cpar import hedge_engine
from backend.cpar.factor_registry import build_cpar1_factor_registry
from backend.services import cpar_meta_service


def _hedge_leg_rows(hedge_legs: tuple[Any, ...]) -> list[dict[str, object]]:
    index = {
        spec.factor_id: {
            "label": spec.label,
            "group": spec.group,
            "display_order": int(spec.display_order),
        }
        for spec in build_cpar1_factor_registry()
    }
    rows: list[dict[str, object]] = []
    for leg in hedge_legs:
        spec = index.get(str(leg.factor_id), {})
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
) -> list[dict[str, object]]:
    index = {
        spec.factor_id: {
            "label": spec.label,
            "group": spec.group,
            "display_order": int(spec.display_order),
        }
        for spec in build_cpar1_factor_registry()
    }
    factor_ids = {
        *(str(key) for key in pre_loadings.keys()),
        *(str(key) for key in hedge_weights.keys()),
        *(str(key) for key in post_loadings.keys()),
    }
    ordered = sorted(
        factor_ids,
        key=lambda factor_id: (
            factor_id != "SPY",
            -abs(float(pre_loadings.get(factor_id, 0.0))),
            factor_id,
        ),
    )
    rows: list[dict[str, object]] = []
    for factor_id in ordered:
        spec = index.get(factor_id, {})
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


def build_cpar_portfolio_hedge_snapshot(
    *,
    package: dict[str, object],
    account: dict[str, object],
    positions: list[dict[str, Any]],
    mode: str,
    helper_api: Any,
    fit_by_ric: dict[str, dict[str, Any]],
    price_by_ric: dict[str, dict[str, Any]],
    classification_by_ric: dict[str, dict[str, Any]],
    covariance_rows: list[dict[str, Any]],
    display_covariance_rows: list[dict[str, Any]] | None = None,
) -> dict[str, object]:
    position_count = int(len(positions))
    resolved_display_covariance_rows = list(display_covariance_rows or covariance_rows)

    base_payload: dict[str, object] = {
        **cpar_meta_service.package_meta_payload(package),
        "account_id": str(account.get("account_id") or ""),
        "account_name": str(account.get("account_name") or account.get("account_id") or ""),
        "mode": str(mode),
        "positions_count": position_count,
        "covered_positions_count": 0,
        "excluded_positions_count": 0,
        "gross_market_value": 0.0,
        "net_market_value": 0.0,
        "covered_gross_market_value": 0.0,
        "coverage_ratio": None,
        "coverage_breakdown": helper_api.coverage_breakdown([]),
        "portfolio_status": "empty",
        "portfolio_reason": "No live holdings positions are loaded for this account.",
        "aggregate_thresholded_loadings": [],
        "aggregate_display_loadings": [],
        "risk_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0},
        "vol_scaled_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 100.0},
        "factor_variance_contributions": [],
        "display_factor_variance_contributions": [],
        "factor_chart": [],
        "display_factor_chart": [],
        "cov_matrix": helper_api.cov_matrix_payload(
            covariance_rows=resolved_display_covariance_rows,
        ),
        "factor_variance_proxy": 0.0,
        "idio_variance_proxy": 0.0,
        "total_variance_proxy": 0.0,
        "hedge_status": None,
        "hedge_reason": None,
        "hedge_legs": [],
        "post_hedge_exposures": [],
        "pre_hedge_factor_variance_proxy": None,
        "post_hedge_factor_variance_proxy": None,
        "gross_hedge_notional": None,
        "net_hedge_notional": None,
        "non_market_reduction_ratio": None,
        "positions": [],
    }
    if not positions:
        return base_payload

    provisional_rows = helper_api.build_position_rows(
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covered_gross_market_value=0.0,
    )
    display_loadings_by_ric = helper_api.display_loadings_by_ric(fit_by_ric)
    aggregate_loadings, covered_gross_market_value, net_market_value = helper_api.aggregate_loadings(
        provisional_rows,
        loadings_by_ric={str(ric): dict((fit or {}).get("thresholded_loadings") or {}) for ric, fit in fit_by_ric.items()},
    )
    aggregate_trade_loadings, _, _ = helper_api.aggregate_loadings(
        provisional_rows,
        loadings_by_ric={
            str(ric): helper_api.cpar_display_loadings.hedge_trade_loadings_from_fit(fit, thresholded=True)
            for ric, fit in fit_by_ric.items()
        },
    )
    aggregate_display_loadings, _, _ = helper_api.aggregate_loadings(
        provisional_rows,
        loadings_by_ric=display_loadings_by_ric,
    )
    position_rows = helper_api.build_position_rows(
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covered_gross_market_value=covered_gross_market_value,
    )
    position_rows = helper_api.attach_thresholded_contributions(
        position_rows,
        fit_by_ric=fit_by_ric,
    )
    position_rows = helper_api.attach_display_contributions(
        position_rows,
        display_loadings_by_ric=display_loadings_by_ric,
    )
    idio_contribution_by_ric, idio_variance_proxy = helper_api.specific_risk_contributions(position_rows)
    factor_variance_rows = helper_api.factor_variance_contribution_rows(
        aggregate_loadings,
        covariance_rows=resolved_display_covariance_rows,
    )
    factor_variance_proxy = helper_api.factor_variance_total(factor_variance_rows)
    total_variance_proxy = float(factor_variance_proxy + idio_variance_proxy)
    factor_variance_rows = helper_api.factor_variance_contribution_rows(
        aggregate_loadings,
        covariance_rows=resolved_display_covariance_rows,
        total_variance=total_variance_proxy,
    )
    display_factor_variance_rows = helper_api.factor_variance_contribution_rows(
        aggregate_display_loadings,
        covariance_rows=resolved_display_covariance_rows,
    )
    display_total_variance_proxy = float(helper_api.factor_variance_total(display_factor_variance_rows) + idio_variance_proxy)
    display_factor_variance_rows = helper_api.factor_variance_contribution_rows(
        aggregate_display_loadings,
        covariance_rows=covariance_rows,
        total_variance=display_total_variance_proxy,
    )
    position_rows = helper_api.attach_risk_mix(
        position_rows,
        aggregate_loadings=aggregate_loadings,
        covariance_rows=resolved_display_covariance_rows,
        contribution_field="thresholded_contributions",
        idio_contribution_by_ric=idio_contribution_by_ric,
        total_variance=total_variance_proxy,
    )

    priced_gross_market_value = sum(
        abs(float(row["market_value"]))
        for row in position_rows
        if row.get("market_value") is not None
    )
    covered_positions_count = sum(1 for row in position_rows if str(row["coverage"]) == "covered")
    excluded_positions_count = len(position_rows) - covered_positions_count
    coverage_ratio = None
    if priced_gross_market_value > 0:
        coverage_ratio = covered_gross_market_value / priced_gross_market_value

    payload = {
        **base_payload,
        "positions_count": int(len(position_rows)),
        "covered_positions_count": int(covered_positions_count),
        "excluded_positions_count": int(excluded_positions_count),
        "gross_market_value": float(priced_gross_market_value),
        "net_market_value": float(net_market_value),
        "covered_gross_market_value": float(covered_gross_market_value),
        "coverage_ratio": coverage_ratio,
        "coverage_breakdown": helper_api.coverage_breakdown(position_rows),
        "positions": position_rows,
    }

    if covered_positions_count <= 0 or covered_gross_market_value <= 0:
        payload["portfolio_status"] = "unavailable"
        payload["portfolio_reason"] = (
            "No holdings rows in this account have both price coverage and a usable persisted cPAR fit in the active package."
        )
        return payload

    preview = hedge_engine.build_hedge_preview(
        mode=mode,
        thresholded_loadings=aggregate_trade_loadings,
        covariance={
            (str(row["factor_id"]), str(row["factor_id_2"])): float(row["covariance"])
            for row in covariance_rows
        },
        fit_status="ok",
        hedge_use_status="usable",
    )
    payload.update(
        {
            "portfolio_status": "partial" if excluded_positions_count > 0 else "ok",
            "portfolio_reason": (
                "Some holdings rows were excluded because they lack price coverage or a usable persisted cPAR fit."
                if excluded_positions_count > 0
                else None
            ),
            **helper_api.factor_analytics_payload(
                aggregate_loadings=aggregate_loadings,
                position_rows=position_rows,
                covariance_rows=resolved_display_covariance_rows,
                contribution_field="thresholded_contributions",
                total_variance=total_variance_proxy,
            ),
            "aggregate_display_loadings": helper_api.factor_rows(aggregate_display_loadings),
            "risk_shares": helper_api.risk_share_payload(
                factor_variance_rows,
                idio_variance_proxy=idio_variance_proxy,
                total_variance_proxy=total_variance_proxy,
            ),
            "vol_scaled_shares": helper_api.vol_scaled_share_payload(
                aggregate_display_loadings,
                covariance_rows=covariance_rows,
                idio_variance_proxy=idio_variance_proxy,
            ),
            "factor_variance_proxy": float(factor_variance_proxy),
            "idio_variance_proxy": float(idio_variance_proxy),
            "total_variance_proxy": float(total_variance_proxy),
            "factor_variance_contributions": factor_variance_rows,
            "display_factor_variance_contributions": display_factor_variance_rows,
            "display_factor_chart": helper_api.factor_chart_rows(
                loadings=aggregate_display_loadings,
                variance_rows=display_factor_variance_rows,
                position_rows=position_rows,
                covariance_rows=resolved_display_covariance_rows,
                contribution_field="display_contributions",
                total_variance=display_total_variance_proxy,
            ),
            "hedge_status": str(preview.status),
            "hedge_reason": preview.reason,
            "hedge_legs": _hedge_leg_rows(preview.hedge_legs),
            "post_hedge_exposures": _post_hedge_exposure_rows(
                pre_loadings=aggregate_trade_loadings,
                hedge_weights=dict(preview.hedge_weights),
                post_loadings=dict(preview.post_hedge_loadings),
            ),
            "pre_hedge_factor_variance_proxy": float(preview.pre_hedge_variance_proxy),
            "post_hedge_factor_variance_proxy": float(preview.post_hedge_variance_proxy),
            "gross_hedge_notional": float(preview.gross_hedge_notional),
            "net_hedge_notional": float(preview.net_hedge_notional),
            "non_market_reduction_ratio": preview.non_market_reduction_ratio,
        }
    )
    return payload
