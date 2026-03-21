"""Shared cPAR account-scoped portfolio snapshot assembly."""

from __future__ import annotations

from typing import Any

from backend.cpar import hedge_engine
from backend.cpar.factor_registry import build_cpar1_factor_registry
from backend.data import cpar_outputs, cpar_source_reads, holdings_reads
from backend.services import cpar_meta_service

_EPSILON = 1e-12


class CparPortfolioAccountNotFound(LookupError):
    """Raised when the requested holdings account does not exist."""


def _normalize_account_id(value: str | None) -> str:
    return str(value or "").strip().lower()


def _factor_rows(loadings: dict[str, float]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for spec in build_cpar1_factor_registry():
        if spec.factor_id not in loadings:
            continue
        rows.append(
            {
                "factor_id": spec.factor_id,
                "label": spec.label,
                "group": spec.group,
                "display_order": int(spec.display_order),
                "beta": float(loadings[spec.factor_id]),
            }
        )
    return rows


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


def _select_price(row: dict[str, Any] | None) -> tuple[float | None, str | None, str | None]:
    if not row:
        return None, None, None
    if row.get("adj_close") is not None:
        return float(row["adj_close"]), "adj_close", str(row.get("date") or "") or None
    if row.get("close") is not None:
        return float(row["close"]), "close", str(row.get("date") or "") or None
    return None, None, str(row.get("date") or "") or None


def _coverage_reason(code: str) -> str | None:
    if code == "missing_price":
        return "No latest price on or before the active cPAR package date."
    if code == "missing_cpar_fit":
        return "No persisted cPAR fit row exists for this RIC in the active package."
    if code == "insufficient_history":
        return "The persisted cPAR fit status is `insufficient_history`, so this position is excluded from hedge aggregation."
    return None


def _zero_coverage_bucket() -> dict[str, object]:
    return {
        "positions_count": 0,
        "gross_market_value": 0.0,
    }


def _coverage_breakdown(rows: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    breakdown = {
        "covered": _zero_coverage_bucket(),
        "missing_price": _zero_coverage_bucket(),
        "missing_cpar_fit": _zero_coverage_bucket(),
        "insufficient_history": _zero_coverage_bucket(),
    }
    for row in rows:
        coverage = str(row.get("coverage") or "")
        if coverage not in breakdown:
            continue
        bucket = breakdown[coverage]
        bucket["positions_count"] = int(bucket["positions_count"]) + 1
        market_value = row.get("market_value")
        if market_value is not None:
            bucket["gross_market_value"] = float(bucket["gross_market_value"]) + abs(float(market_value))
    return breakdown


def _build_position_rows(
    *,
    positions: list[dict[str, Any]],
    fit_by_ric: dict[str, dict[str, Any]],
    price_by_ric: dict[str, dict[str, Any]],
    covered_gross_market_value: float,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for position in positions:
        ric = str(position.get("ric") or "")
        fit = fit_by_ric.get(ric)
        price_row = price_by_ric.get(ric)
        price_value, price_field_used, price_date = _select_price(price_row)
        quantity = float(position.get("quantity") or 0.0)
        market_value = None if price_value is None else quantity * price_value
        fit_status = str(fit.get("fit_status") or "") if fit else None
        coverage = "covered"
        if price_value is None:
            coverage = "missing_price"
        elif fit is None:
            coverage = "missing_cpar_fit"
        elif fit_status == "insufficient_history":
            coverage = "insufficient_history"

        rows.append(
            {
                "account_id": str(position.get("account_id") or ""),
                "ric": ric,
                "ticker": position.get("ticker") or (fit.get("ticker") if fit else None),
                "display_name": fit.get("display_name") if fit else None,
                "quantity": quantity,
                "price": price_value,
                "price_date": price_date,
                "price_field_used": price_field_used,
                "market_value": market_value,
                "portfolio_weight": (
                    None
                    if coverage != "covered" or market_value is None or covered_gross_market_value <= 0
                    else float(market_value / covered_gross_market_value)
                ),
                "fit_status": fit_status,
                "warnings": list(fit.get("warnings") or []) if fit else [],
                "beta_spy_trade": fit.get("spy_trade_beta_raw") if fit else None,
                "coverage": coverage,
                "coverage_reason": _coverage_reason(coverage),
            }
        )
    rows.sort(
        key=lambda row: (
            -abs(float(row.get("market_value") or 0.0)),
            str(row.get("ticker") or row.get("ric") or ""),
            str(row.get("ric") or ""),
        )
    )
    return rows


def _attach_thresholded_contributions(
    rows: list[dict[str, object]],
    *,
    fit_by_ric: dict[str, dict[str, Any]],
) -> list[dict[str, object]]:
    enriched: list[dict[str, object]] = []
    for row in rows:
        coverage = str(row.get("coverage") or "")
        portfolio_weight = row.get("portfolio_weight")
        if coverage != "covered" or portfolio_weight is None:
            enriched.append({**row, "thresholded_contributions": []})
            continue

        fit = fit_by_ric.get(str(row.get("ric") or ""))
        contributions = {
            str(factor_id): float(portfolio_weight) * float(beta)
            for factor_id, beta in dict((fit or {}).get("thresholded_loadings") or {}).items()
            if abs(float(portfolio_weight) * float(beta)) > 1e-12
        }
        enriched.append(
            {
                **row,
                "thresholded_contributions": _factor_rows(contributions),
            }
        )
    return enriched


def _aggregate_thresholded_loadings(
    rows: list[dict[str, object]],
    *,
    fit_by_ric: dict[str, dict[str, Any]],
) -> tuple[dict[str, float], float, float]:
    covered = [
        row for row in rows
        if str(row.get("coverage") or "") == "covered" and row.get("market_value") is not None
    ]
    gross_market_value = sum(abs(float(row["market_value"])) for row in covered)
    net_market_value = sum(float(row["market_value"]) for row in covered)
    if gross_market_value <= 0:
        return {}, 0.0, 0.0
    loadings: dict[str, float] = {}
    for row in covered:
        fit = fit_by_ric[str(row["ric"])]
        weight = float(row["market_value"]) / gross_market_value
        for factor_id, beta in dict(fit.get("thresholded_loadings") or {}).items():
            loadings[str(factor_id)] = float(loadings.get(str(factor_id), 0.0) + weight * float(beta))
    return (
        {factor_id: beta for factor_id, beta in loadings.items() if abs(beta) > 1e-12},
        gross_market_value,
        net_market_value,
    )


def _factor_variance_contribution_rows(
    loadings: dict[str, float],
    *,
    covariance_rows: list[dict[str, Any]],
) -> list[dict[str, object]]:
    if not loadings:
        return []
    ordered_rows = _factor_rows(loadings)
    factor_ids = tuple(str(row["factor_id"]) for row in ordered_rows)
    covariance = {
        (str(row["factor_id"]), str(row["factor_id_2"])): float(row["covariance"])
        for row in covariance_rows
    }
    covariance_matrix = hedge_engine.covariance_matrix_for_factors(factor_ids, covariance)
    beta_vector = [float(loadings.get(factor_id, 0.0)) for factor_id in factor_ids]
    sigma_beta = [
        float(
            sum(float(covariance_matrix[row_idx, col_idx]) * beta_vector[col_idx] for col_idx in range(len(factor_ids)))
        )
        for row_idx in range(len(factor_ids))
    ]
    total_variance = float(sum(beta_vector[idx] * sigma_beta[idx] for idx in range(len(factor_ids))))

    rows: list[dict[str, object]] = []
    for idx, factor_row in enumerate(ordered_rows):
        contribution = float(beta_vector[idx] * sigma_beta[idx])
        rows.append(
            {
                **factor_row,
                "variance_contribution": contribution,
                "variance_share": (None if total_variance <= 0 else float(contribution / total_variance)),
            }
        )
    return rows


def _factor_chart_rows(
    *,
    loadings: dict[str, float],
    variance_rows: list[dict[str, object]],
    position_rows: list[dict[str, object]],
    fit_by_ric: dict[str, dict[str, Any]],
) -> list[dict[str, object]]:
    variance_by_factor = {
        str(row["factor_id"]): dict(row)
        for row in variance_rows
        if str(row.get("factor_id") or "")
    }
    factor_specs = list(build_cpar1_factor_registry())
    factor_ids: set[str] = {str(factor_id) for factor_id in loadings.keys()}
    for row in position_rows:
        if str(row.get("coverage") or "") != "covered":
            continue
        portfolio_weight = row.get("portfolio_weight")
        if portfolio_weight is None:
            continue
        fit = fit_by_ric.get(str(row.get("ric") or ""))
        for factor_id, beta in dict((fit or {}).get("thresholded_loadings") or {}).items():
            contribution_beta = float(portfolio_weight) * float(beta)
            if abs(float(beta)) <= _EPSILON and abs(contribution_beta) <= _EPSILON:
                continue
            factor_ids.add(str(factor_id))
    if not factor_ids:
        return []

    chart_rows: list[dict[str, object]] = []
    for spec in factor_specs:
        factor_id = str(spec.factor_id)
        if factor_id not in factor_ids:
            continue
        positive_contribution_beta = 0.0
        negative_contribution_beta = 0.0
        drilldown_rows: list[dict[str, object]] = []
        for row in position_rows:
            if str(row.get("coverage") or "") != "covered":
                continue
            portfolio_weight = row.get("portfolio_weight")
            if portfolio_weight is None:
                continue
            fit = fit_by_ric.get(str(row.get("ric") or ""))
            thresholded_loadings = dict((fit or {}).get("thresholded_loadings") or {})
            factor_beta = float(thresholded_loadings.get(factor_id, 0.0))
            contribution_beta = float(portfolio_weight) * factor_beta
            if abs(factor_beta) <= _EPSILON and abs(contribution_beta) <= _EPSILON:
                continue
            if contribution_beta > _EPSILON:
                positive_contribution_beta += contribution_beta
            elif contribution_beta < -_EPSILON:
                negative_contribution_beta += contribution_beta
            drilldown_rows.append(
                {
                    "ric": str(row.get("ric") or ""),
                    "ticker": row.get("ticker"),
                    "display_name": row.get("display_name"),
                    "market_value": row.get("market_value"),
                    "portfolio_weight": row.get("portfolio_weight"),
                    "fit_status": row.get("fit_status"),
                    "warnings": list(row.get("warnings") or []),
                    "coverage": row.get("coverage"),
                    "coverage_reason": row.get("coverage_reason"),
                    "factor_beta": factor_beta,
                    "contribution_beta": contribution_beta,
                }
            )
        drilldown_rows.sort(
            key=lambda row: (
                -abs(float(row.get("contribution_beta") or 0.0)),
                str(row.get("ticker") or row.get("ric") or ""),
                str(row.get("ric") or ""),
            )
        )
        variance_row = variance_by_factor.get(factor_id, {})
        chart_rows.append(
            {
                "factor_id": factor_id,
                "label": spec.label,
                "group": spec.group,
                "display_order": int(spec.display_order),
                "beta": float(loadings.get(factor_id, 0.0)),
                "aggregate_beta": float(loadings.get(factor_id, 0.0)),
                "positive_contribution_beta": float(positive_contribution_beta),
                "negative_contribution_beta": float(negative_contribution_beta),
                "variance_contribution": (
                    0.0
                    if variance_row.get("variance_contribution") is None
                    else float(variance_row["variance_contribution"])
                ),
                "variance_share": (
                    0.0
                    if variance_row.get("variance_share") is None
                    else float(variance_row["variance_share"])
                ),
                "drilldown": drilldown_rows,
            }
        )
    return chart_rows


def load_cpar_portfolio_account_context(
    *,
    account_id: str,
    data_db=None,
) -> tuple[dict[str, object], dict[str, object], list[dict[str, Any]]]:
    package = cpar_meta_service.require_active_package(data_db=data_db)

    try:
        accounts = holdings_reads.load_holdings_accounts()
        positions = holdings_reads.load_holdings_positions(account_id=account_id)
    except holdings_reads.HoldingsReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(f"Holdings read failed: {exc}") from exc

    normalized_account_id = _normalize_account_id(account_id)
    account = next(
        (row for row in accounts if _normalize_account_id(str(row.get("account_id") or "")) == normalized_account_id),
        None,
    )
    if account is None:
        raise CparPortfolioAccountNotFound(f"Holdings account {account_id!r} was not found.")

    return package, account, positions


def load_cpar_portfolio_support_rows(
    *,
    rics: list[str],
    package_run_id: str,
    package_date: str,
    data_db=None,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], list[dict[str, Any]]]:
    try:
        fit_rows = cpar_outputs.load_package_instrument_fits_for_rics(
            rics,
            package_run_id=str(package_run_id),
            data_db=data_db,
        )
        covariance_rows = cpar_outputs.load_package_covariance_rows(
            str(package_run_id),
            data_db=data_db,
            require_complete=True,
            context_label="Active cPAR package",
        )
        price_rows = cpar_source_reads.load_latest_price_rows(
            rics,
            as_of_date=str(package_date),
            data_db=data_db,
        )
    except cpar_outputs.CparPackageNotReady as exc:
        raise cpar_meta_service.CparReadNotReady(str(exc)) from exc
    except cpar_outputs.CparAuthorityReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc
    except cpar_source_reads.CparSourceReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(f"Shared-source read failed: {exc}") from exc

    fit_by_ric = {str(row["ric"]): row for row in fit_rows}
    price_by_ric = {str(row["ric"]): row for row in price_rows}
    return fit_by_ric, price_by_ric, covariance_rows


def build_cpar_portfolio_hedge_snapshot(
    *,
    package: dict[str, object],
    account: dict[str, object],
    positions: list[dict[str, Any]],
    mode: str,
    fit_by_ric: dict[str, dict[str, Any]],
    price_by_ric: dict[str, dict[str, Any]],
    covariance_rows: list[dict[str, Any]],
) -> dict[str, object]:
    position_count = int(len(positions))

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
        "coverage_breakdown": _coverage_breakdown([]),
        "portfolio_status": "empty",
        "portfolio_reason": "No live holdings positions are loaded for this account.",
        "aggregate_thresholded_loadings": [],
        "factor_variance_contributions": [],
        "factor_chart": [],
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

    provisional_rows = _build_position_rows(
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        covered_gross_market_value=0.0,
    )
    aggregate_loadings, covered_gross_market_value, net_market_value = _aggregate_thresholded_loadings(
        provisional_rows,
        fit_by_ric=fit_by_ric,
    )
    position_rows = _build_position_rows(
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        covered_gross_market_value=covered_gross_market_value,
    )
    position_rows = _attach_thresholded_contributions(position_rows, fit_by_ric=fit_by_ric)

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
        "coverage_breakdown": _coverage_breakdown(position_rows),
        "positions": position_rows,
    }

    if covered_positions_count <= 0 or covered_gross_market_value <= 0:
        payload["portfolio_status"] = "unavailable"
        payload["portfolio_reason"] = (
            "No holdings rows in this account have both price coverage and a usable persisted cPAR fit in the active package."
        )
        return payload

    factor_variance_rows = _factor_variance_contribution_rows(
        aggregate_loadings,
        covariance_rows=covariance_rows,
    )
    preview = hedge_engine.build_hedge_preview(
        mode=mode,
        thresholded_loadings=aggregate_loadings,
        covariance={
            (str(row["factor_id"]), str(row["factor_id_2"])): float(row["covariance"])
            for row in covariance_rows
        },
        fit_status="ok",
    )
    payload.update(
        {
            "portfolio_status": "partial" if excluded_positions_count > 0 else "ok",
            "portfolio_reason": (
                "Some holdings rows were excluded because they lack price coverage or a usable persisted cPAR fit."
                if excluded_positions_count > 0
                else None
            ),
            "aggregate_thresholded_loadings": _factor_rows(aggregate_loadings),
            "factor_variance_contributions": factor_variance_rows,
            "factor_chart": _factor_chart_rows(
                loadings=aggregate_loadings,
                variance_rows=factor_variance_rows,
                position_rows=position_rows,
                fit_by_ric=fit_by_ric,
            ),
            "hedge_status": str(preview.status),
            "hedge_reason": preview.reason,
            "hedge_legs": _hedge_leg_rows(preview.hedge_legs),
            "post_hedge_exposures": _post_hedge_exposure_rows(
                pre_loadings=aggregate_loadings,
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


def load_cpar_portfolio_hedge_payload(
    *,
    account_id: str,
    mode: str,
    data_db=None,
) -> dict[str, object]:
    package, account, positions = load_cpar_portfolio_account_context(
        account_id=account_id,
        data_db=data_db,
    )
    if not positions:
        return build_cpar_portfolio_hedge_snapshot(
            package=package,
            account=account,
            positions=[],
            mode=mode,
            fit_by_ric={},
            price_by_ric={},
            covariance_rows=[],
        )
    rics = [str(row.get("ric") or "") for row in positions if str(row.get("ric") or "").strip()]
    fit_by_ric, price_by_ric, covariance_rows = load_cpar_portfolio_support_rows(
        rics=rics,
        package_run_id=str(package["package_run_id"]),
        package_date=str(package["package_date"]),
        data_db=data_db,
    )
    return build_cpar_portfolio_hedge_snapshot(
        package=package,
        account=account,
        positions=positions,
        mode=mode,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        covariance_rows=covariance_rows,
    )
