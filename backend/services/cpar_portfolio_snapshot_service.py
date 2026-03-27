"""Shared cPAR account-scoped portfolio snapshot assembly."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import math
from typing import Any

from backend.cpar import hedge_engine
from backend.cpar.factor_registry import build_cpar1_factor_registry
from backend.data import cpar_outputs, cpar_source_reads, holdings_reads
from backend.services import cpar_display_covariance, cpar_display_loadings, cpar_meta_service

_EPSILON = 1e-12


class CparPortfolioAccountNotFound(LookupError):
    """Raised when the requested holdings account does not exist."""


def _normalize_account_id(value: str | None) -> str:
    return str(value or "").strip().lower()


def _factor_rows(loadings: dict[str, float]) -> list[dict[str, object]]:
    return cpar_display_loadings.ordered_factor_rows(loadings)


def _cov_matrix_payload(
    *,
    covariance_rows: list[dict[str, Any]],
) -> dict[str, object]:
    factor_ids = [str(spec.factor_id) for spec in build_cpar1_factor_registry()]
    correlation_lookup = {
        (str(row.get("factor_id") or ""), str(row.get("factor_id_2") or "")): float(row.get("correlation") or 0.0)
        for row in covariance_rows
    }
    correlation = [
        [
            float(correlation_lookup.get((left, right), 1.0 if left == right else 0.0))
            for right in factor_ids
        ]
        for left in factor_ids
    ]
    return {
        "factors": factor_ids,
        "correlation": correlation,
    }


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


def _fit_runtime_fields(fit: dict[str, Any] | None) -> dict[str, Any]:
    if not fit:
        return {
            "target_scope": None,
            "fit_family": None,
            "price_on_package_date_status": None,
            "fit_row_status": None,
            "fit_quality_status": None,
            "portfolio_use_status": None,
            "ticker_detail_use_status": None,
            "hedge_use_status": None,
            "reason_code": None,
            "quality_label": None,
        }
    return {
        "target_scope": fit.get("target_scope"),
        "fit_family": fit.get("fit_family"),
        "price_on_package_date_status": fit.get("price_on_package_date_status"),
        "fit_row_status": fit.get("fit_row_status"),
        "fit_quality_status": fit.get("fit_quality_status"),
        "portfolio_use_status": fit.get("portfolio_use_status"),
        "ticker_detail_use_status": fit.get("ticker_detail_use_status"),
        "hedge_use_status": fit.get("hedge_use_status"),
        "reason_code": fit.get("reason_code"),
        "quality_label": fit.get("quality_label"),
    }


def _compat_coverage_label(
    *,
    fit: dict[str, Any] | None,
    price_value: float | None,
) -> str:
    if fit is not None:
        persisted = str(fit.get("portfolio_use_status") or "").strip()
        if persisted == "covered":
            return "covered"
        if persisted in {"missing_price", "insufficient_history"}:
            return persisted
    if price_value is None:
        return "missing_price"
    if fit is None:
        return "missing_cpar_fit"
    if str(fit.get("fit_status") or "") == "insufficient_history":
        return "insufficient_history"
    return "covered"


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
    classification_by_ric: dict[str, dict[str, Any]],
    covered_gross_market_value: float,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for position in positions:
        ric = str(position.get("ric") or "")
        fit = fit_by_ric.get(ric)
        runtime_fields = _fit_runtime_fields(fit)
        price_row = price_by_ric.get(ric)
        classification_row = classification_by_ric.get(ric)
        price_value, price_field_used, price_date = _select_price(price_row)
        if str(runtime_fields.get("price_on_package_date_status") or "") == "missing":
            price_value = None
            price_field_used = None
            price_date = None
        quantity = float(position.get("quantity") or 0.0)
        market_value = None if price_value is None else quantity * price_value
        fit_status = str(fit.get("fit_status") or "") if fit else None
        coverage = _compat_coverage_label(fit=fit, price_value=price_value)

        rows.append(
            {
                "account_id": str(position.get("account_id") or ""),
                "ric": ric,
                "ticker": position.get("ticker") or (fit.get("ticker") if fit else None),
                "display_name": fit.get("display_name") if fit else None,
                "trbc_industry_group": classification_row.get("trbc_industry_group") if classification_row else None,
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
                "specific_variance_proxy": fit.get("specific_variance_proxy") if fit else None,
                "specific_volatility_proxy": fit.get("specific_volatility_proxy") if fit else None,
                "coverage": coverage,
                "coverage_reason": _coverage_reason(coverage),
                **runtime_fields,
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


def _display_loadings_by_ric(
    fit_by_ric: dict[str, dict[str, Any]],
) -> dict[str, dict[str, float]]:
    return {
        str(ric): cpar_display_loadings.display_loadings_from_fit(fit)
        for ric, fit in fit_by_ric.items()
    }


def _attach_display_contributions(
    rows: list[dict[str, object]],
    *,
    display_loadings_by_ric: dict[str, dict[str, float]],
) -> list[dict[str, object]]:
    enriched: list[dict[str, object]] = []
    for row in rows:
        coverage = str(row.get("coverage") or "")
        portfolio_weight = row.get("portfolio_weight")
        if coverage != "covered" or portfolio_weight is None:
            enriched.append({**row, "display_contributions": []})
            continue

        resolved_loadings = display_loadings_by_ric.get(str(row.get("ric") or ""), {})
        contributions = {
            factor_id: float(portfolio_weight) * float(beta)
            for factor_id, beta in resolved_loadings.items()
            if abs(float(portfolio_weight) * float(beta)) > _EPSILON
        }
        enriched.append(
            {
                **row,
                "display_contributions": _factor_rows(contributions),
            }
        )
    return enriched


def _aggregate_positions_across_accounts(
    positions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, object]]]:
    aggregated_by_ric: dict[str, dict[str, Any]] = {}
    account_index: dict[str, dict[str, object]] = {}

    for position in positions:
        account_id = str(position.get("account_id") or "")
        normalized_account_id = _normalize_account_id(account_id)
        if normalized_account_id and normalized_account_id not in account_index:
            account_index[normalized_account_id] = {
                "account_id": account_id,
                "account_name": account_id,
            }

        ric = str(position.get("ric") or "").strip().upper()
        if not ric:
            continue
        aggregate = aggregated_by_ric.setdefault(
            ric,
            {
                "account_id": "all_accounts",
                "ric": ric,
                "ticker": position.get("ticker"),
                "quantity": 0.0,
                "source": "aggregate",
                "updated_at": position.get("updated_at"),
            },
        )
        aggregate["quantity"] = float(aggregate.get("quantity") or 0.0) + float(position.get("quantity") or 0.0)
        if not aggregate.get("ticker") and position.get("ticker"):
            aggregate["ticker"] = position.get("ticker")
        current_updated = str(aggregate.get("updated_at") or "")
        next_updated = str(position.get("updated_at") or "")
        if next_updated and next_updated > current_updated:
            aggregate["updated_at"] = next_updated

    aggregated_positions = [
        row
        for row in aggregated_by_ric.values()
        if abs(float(row.get("quantity") or 0.0)) > _EPSILON
    ]
    aggregated_positions.sort(
        key=lambda row: (
            str(row.get("ticker") or row.get("ric") or ""),
            str(row.get("ric") or ""),
        )
    )
    accounts = sorted(
        account_index.values(),
        key=lambda row: (
            str(row.get("account_id") or ""),
        ),
    )
    return aggregated_positions, accounts


def _aggregate_loadings(
    rows: list[dict[str, object]],
    *,
    loadings_by_ric: dict[str, dict[str, float]],
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
        instrument_loadings = loadings_by_ric.get(str(row["ric"]), {})
        weight = float(row["market_value"]) / gross_market_value
        for factor_id, beta in instrument_loadings.items():
            loadings[str(factor_id)] = float(loadings.get(str(factor_id), 0.0) + weight * float(beta))
    return (
        {factor_id: beta for factor_id, beta in loadings.items() if abs(beta) > 1e-12},
        gross_market_value,
        net_market_value,
    )


def _factor_analytics_payload(
    *,
    aggregate_loadings: dict[str, float],
    position_rows: list[dict[str, object]],
    covariance_rows: list[dict[str, Any]],
    contribution_field: str,
    total_variance: float | None = None,
) -> dict[str, object]:
    factor_variance_rows = _factor_variance_contribution_rows(
        aggregate_loadings,
        covariance_rows=covariance_rows,
        total_variance=total_variance,
    )
    return {
        "aggregate_thresholded_loadings": _factor_rows(aggregate_loadings),
        "factor_variance_contributions": factor_variance_rows,
        "factor_chart": _factor_chart_rows(
            loadings=aggregate_loadings,
            variance_rows=factor_variance_rows,
            position_rows=position_rows,
            covariance_rows=covariance_rows,
            contribution_field=contribution_field,
            total_variance=total_variance,
        ),
    }


def _factor_variance_contribution_rows(
    loadings: dict[str, float],
    *,
    covariance_rows: list[dict[str, Any]],
    total_variance: float | None = None,
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
    factor_variance_total = float(sum(beta_vector[idx] * sigma_beta[idx] for idx in range(len(factor_ids))))
    variance_denominator = float(total_variance) if total_variance is not None else factor_variance_total

    rows: list[dict[str, object]] = []
    for idx, factor_row in enumerate(ordered_rows):
        contribution = float(beta_vector[idx] * sigma_beta[idx])
        rows.append(
            {
                **factor_row,
                "variance_contribution": contribution,
                "variance_share": (None if variance_denominator <= 0 else float(contribution / variance_denominator)),
            }
        )
    return rows


def _factor_risk_metrics(
    loadings: dict[str, float],
    *,
    covariance_rows: list[dict[str, Any]],
    total_variance: float | None = None,
) -> dict[str, dict[str, float]]:
    if not loadings:
        return {}
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
    factor_variance_total = float(sum(beta_vector[idx] * sigma_beta[idx] for idx in range(len(factor_ids))))
    variance_denominator = float(total_variance) if total_variance is not None else factor_variance_total

    metrics: dict[str, dict[str, float]] = {}
    for idx, factor_id in enumerate(factor_ids):
        variance_contribution = float(beta_vector[idx] * sigma_beta[idx])
        metrics[factor_id] = {
            "factor_volatility": float(math.sqrt(max(float(covariance_matrix[idx, idx]), 0.0))),
            "covariance_adjustment": float(sigma_beta[idx]),
            "variance_contribution": variance_contribution,
            "variance_share": 0.0 if variance_denominator <= 0 else float(variance_contribution / variance_denominator),
            "total_variance": float(variance_denominator),
        }
    return metrics


def _factor_chart_rows(
    *,
    loadings: dict[str, float],
    variance_rows: list[dict[str, object]],
    position_rows: list[dict[str, object]],
    covariance_rows: list[dict[str, Any]],
    contribution_field: str,
    total_variance: float | None = None,
) -> list[dict[str, object]]:
    variance_by_factor = {
        str(row["factor_id"]): dict(row)
        for row in variance_rows
        if str(row.get("factor_id") or "")
    }
    risk_metrics_by_factor = _factor_risk_metrics(loadings, covariance_rows=covariance_rows, total_variance=total_variance)
    factor_specs = list(build_cpar1_factor_registry())
    factor_ids: set[str] = {str(factor_id) for factor_id in loadings.keys()}
    for row in position_rows:
        if str(row.get("coverage") or "") != "covered":
            continue
        for contribution in list(row.get(contribution_field) or []):
            factor_id = str(contribution.get("factor_id") or "")
            contribution_beta = float(contribution.get("beta") or 0.0)
            if not factor_id or abs(contribution_beta) <= _EPSILON:
                continue
            factor_ids.add(factor_id)
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
        factor_risk = risk_metrics_by_factor.get(
            factor_id,
            {
                "factor_volatility": 0.0,
                "covariance_adjustment": 0.0,
                "variance_contribution": 0.0,
                "variance_share": 0.0,
                "total_variance": 0.0,
            },
        )
        factor_volatility = float(factor_risk["factor_volatility"])
        covariance_adjustment = float(factor_risk["covariance_adjustment"])
        total_variance = float(factor_risk["total_variance"])
        for row in position_rows:
            if str(row.get("coverage") or "") != "covered":
                continue
            portfolio_weight = row.get("portfolio_weight")
            if portfolio_weight is None:
                continue
            contributions = {
                str(item.get("factor_id") or ""): float(item.get("beta") or 0.0)
                for item in list(row.get(contribution_field) or [])
                if str(item.get("factor_id") or "")
            }
            contribution_beta = float(contributions.get(factor_id, 0.0))
            factor_beta = (
                0.0
                if abs(float(portfolio_weight)) <= _EPSILON
                else float(contribution_beta / float(portfolio_weight))
            )
            if abs(factor_beta) <= _EPSILON and abs(contribution_beta) <= _EPSILON:
                continue
            if contribution_beta > _EPSILON:
                positive_contribution_beta += contribution_beta
            elif contribution_beta < -_EPSILON:
                negative_contribution_beta += contribution_beta
            vol_scaled_loading = float(factor_beta * factor_volatility)
            vol_scaled_contribution = float(contribution_beta * factor_volatility)
            covariance_adjusted_loading = float(factor_beta * covariance_adjustment)
            risk_contribution_pct = (
                0.0
                if total_variance <= _EPSILON
                else float((contribution_beta * covariance_adjustment) / total_variance * 100.0)
            )
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
                    "vol_scaled_loading": vol_scaled_loading,
                    "vol_scaled_contribution": vol_scaled_contribution,
                    "covariance_adjusted_loading": covariance_adjusted_loading,
                    "risk_contribution_pct": risk_contribution_pct,
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
                "factor_volatility": factor_volatility,
                "covariance_adjustment": covariance_adjustment,
                "sensitivity_beta": float(loadings.get(factor_id, 0.0) * factor_volatility),
                "risk_contribution_pct": float(variance_row.get("variance_share") or 0.0) * 100.0,
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


def _factor_variance_total(rows: list[dict[str, object]]) -> float:
    return float(sum(float(row.get("variance_contribution") or 0.0) for row in rows))


def _specific_risk_contributions(
    rows: list[dict[str, object]],
) -> tuple[dict[str, float], float]:
    contributions: dict[str, float] = {}
    total = 0.0
    for row in rows:
        if str(row.get("coverage") or "") != "covered":
            continue
        portfolio_weight = row.get("portfolio_weight")
        specific_variance = row.get("specific_variance_proxy")
        if portfolio_weight is None or specific_variance is None:
            continue
        contribution = float(portfolio_weight) ** 2 * float(specific_variance)
        if abs(contribution) <= _EPSILON:
            continue
        ric = str(row.get("ric") or "")
        contributions[ric] = contribution
        total += contribution
    return contributions, float(total)


def _require_specific_risk_fit_rows(
    fit_rows: list[dict[str, Any]],
    *,
    package_run_id: str,
) -> None:
    incomplete = sorted(
        str(row.get("ric") or "")
        for row in fit_rows
        if str(row.get("fit_status") or "") != "insufficient_history"
        and (
            row.get("specific_variance_proxy") is None
            or row.get("specific_volatility_proxy") is None
        )
    )
    if incomplete:
        sample = ", ".join(incomplete[:5])
        raise cpar_meta_service.CparReadNotReady(
            "The active cPAR package is missing specific-risk fields required by idiosyncratic-risk-aware "
            f"surfaces (package_run_id={package_run_id}; sample rics: {sample}). "
            "Run a fresh cPAR package build."
        )


def _risk_share_payload(
    factor_variance_rows: list[dict[str, object]],
    *,
    idio_variance_proxy: float,
    total_variance_proxy: float,
) -> dict[str, float]:
    buckets = {
        "market": 0.0,
        "industry": 0.0,
        "style": 0.0,
        "idio": 0.0,
    }
    if total_variance_proxy > _EPSILON:
        buckets["idio"] = float(idio_variance_proxy / total_variance_proxy * 100.0)
    for row in factor_variance_rows:
        share = float(row.get("variance_share") or 0.0) * 100.0
        group = str(row.get("group") or "")
        if group == "market":
            buckets["market"] += share
        elif group == "sector":
            buckets["industry"] += share
        elif group == "style":
            buckets["style"] += share
    return buckets


def _vol_scaled_share_payload(
    loadings: dict[str, float],
    *,
    covariance_rows: list[dict[str, Any]],
    idio_variance_proxy: float,
) -> dict[str, float]:
    buckets = {
        "market": 0.0,
        "industry": 0.0,
        "style": 0.0,
        "idio": float(math.sqrt(max(float(idio_variance_proxy), 0.0))),
    }
    metrics_by_factor = _factor_risk_metrics(loadings, covariance_rows=covariance_rows)
    for row in _factor_rows(loadings):
        factor_id = str(row.get("factor_id") or "")
        if not factor_id:
            continue
        group = str(row.get("group") or "")
        factor_vol = float((metrics_by_factor.get(factor_id) or {}).get("factor_volatility") or 0.0)
        sensitivity = abs(float(row.get("beta") or 0.0) * factor_vol)
        if sensitivity <= _EPSILON:
            continue
        if group == "market":
            buckets["market"] += sensitivity
        elif group == "sector":
            buckets["industry"] += sensitivity
        elif group == "style":
            buckets["style"] += sensitivity
    total = float(sum(buckets.values()))
    if total <= _EPSILON:
        return {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 100.0}
    return {
        key: round(float(value / total * 100.0), 2)
        for key, value in buckets.items()
    }


def _attach_risk_mix(
    rows: list[dict[str, object]],
    *,
    aggregate_loadings: dict[str, float],
    covariance_rows: list[dict[str, Any]],
    contribution_field: str,
    idio_contribution_by_ric: dict[str, float],
    total_variance: float,
) -> list[dict[str, object]]:
    factor_adjustment_by_id = {
        factor_id: float(metrics.get("covariance_adjustment") or 0.0)
        for factor_id, metrics in _factor_risk_metrics(
            aggregate_loadings,
            covariance_rows=covariance_rows,
            total_variance=total_variance,
        ).items()
    }
    enriched: list[dict[str, object]] = []
    for row in rows:
        if str(row.get("coverage") or "") != "covered":
            enriched.append({**row, "risk_mix": None})
            continue
        bucket_totals = {
            "market": 0.0,
            "industry": 0.0,
            "style": 0.0,
            "idio": abs(float(idio_contribution_by_ric.get(str(row.get("ric") or ""), 0.0))),
        }
        for contribution in list(row.get(contribution_field) or []):
            factor_id = str(contribution.get("factor_id") or "")
            group = str(contribution.get("group") or "")
            value = abs(float(contribution.get("beta") or 0.0) * float(factor_adjustment_by_id.get(factor_id, 0.0)))
            if value <= _EPSILON:
                continue
            if group == "market":
                bucket_totals["market"] += value
            elif group == "sector":
                bucket_totals["industry"] += value
            elif group == "style":
                bucket_totals["style"] += value
        bucket_total = sum(bucket_totals.values())
        if bucket_total <= _EPSILON:
            enriched.append({**row, "risk_mix": None})
            continue
        scale = 100.0 / bucket_total
        enriched.append(
            {
                **row,
                "risk_mix": {
                    key: float(value * scale)
                    for key, value in bucket_totals.items()
                },
            }
        )
    return enriched


def load_cpar_portfolio_account_context(
    *,
    account_id: str,
    data_db=None,
) -> tuple[dict[str, object], dict[str, object], list[dict[str, Any]]]:
    package = cpar_meta_service.require_specific_risk_package(data_db=data_db)

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


def load_cpar_portfolio_holdings_context(
    *,
    data_db=None,
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, Any]]]:
    package = cpar_meta_service.require_specific_risk_package(data_db=data_db)

    try:
        accounts = holdings_reads.load_holdings_accounts()
        live_positions = holdings_reads.load_all_holdings_positions()
    except holdings_reads.HoldingsReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(f"Holdings read failed: {exc}") from exc

    return package, accounts, live_positions


def aggregate_cpar_positions_across_accounts(
    positions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, object]]]:
    return _aggregate_positions_across_accounts(positions)


def load_cpar_portfolio_aggregate_context(
    *,
    data_db=None,
) -> tuple[dict[str, object], list[dict[str, object]], list[dict[str, Any]]]:
    package = cpar_meta_service.require_specific_risk_package(data_db=data_db)

    try:
        accounts = holdings_reads.load_contributing_holdings_accounts()
        live_positions = holdings_reads.load_aggregate_holdings_positions()
    except holdings_reads.HoldingsReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(f"Holdings read failed: {exc}") from exc

    return package, accounts, live_positions


def load_cpar_portfolio_support_rows(
    *,
    rics: list[str],
    package_run_id: str,
    package_date: str,
    data_db=None,
) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    list[dict[str, Any]],
]:
    with ThreadPoolExecutor(max_workers=4) as executor:
        fit_future = executor.submit(
            cpar_outputs.load_package_instrument_fits_for_rics,
            rics,
            package_run_id=str(package_run_id),
            data_db=data_db,
        )
        covariance_future = executor.submit(
            cpar_outputs.load_package_covariance_rows,
            str(package_run_id),
            data_db=data_db,
            require_complete=True,
            context_label="Active cPAR package",
        )
        price_future = executor.submit(
            cpar_source_reads.load_latest_price_rows,
            rics,
            as_of_date=str(package_date),
            data_db=data_db,
        )
        classification_future = executor.submit(
            cpar_source_reads.load_latest_classification_rows,
            rics,
            as_of_date=str(package_date),
            data_db=data_db,
        )

        try:
            fit_rows = fit_future.result()
            covariance_rows = covariance_future.result()
            price_rows = price_future.result()
        except cpar_outputs.CparPackageNotReady as exc:
            raise cpar_meta_service.CparReadNotReady(str(exc)) from exc
        except cpar_outputs.CparAuthorityReadError as exc:
            raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc
        except cpar_source_reads.CparSourceReadError as exc:
            raise cpar_meta_service.CparReadUnavailable(f"Shared-source read failed: {exc}") from exc

        try:
            classification_rows = classification_future.result()
        except cpar_source_reads.CparSourceReadError:
            classification_rows = []

    _require_specific_risk_fit_rows(fit_rows, package_run_id=str(package_run_id))
    fit_by_ric = {str(row["ric"]): row for row in fit_rows}
    price_by_ric = {str(row["ric"]): row for row in price_rows}
    classification_by_ric = {str(row["ric"]): row for row in classification_rows}
    return fit_by_ric, price_by_ric, classification_by_ric, covariance_rows


def build_cpar_portfolio_hedge_snapshot(
    *,
    package: dict[str, object],
    account: dict[str, object],
    positions: list[dict[str, Any]],
    mode: str,
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
        "coverage_breakdown": _coverage_breakdown([]),
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
        "cov_matrix": _cov_matrix_payload(covariance_rows=resolved_display_covariance_rows),
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

    provisional_rows = _build_position_rows(
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covered_gross_market_value=0.0,
    )
    display_loadings_by_ric = _display_loadings_by_ric(fit_by_ric)
    aggregate_loadings, covered_gross_market_value, net_market_value = _aggregate_loadings(
        provisional_rows,
        loadings_by_ric={str(ric): dict((fit or {}).get("thresholded_loadings") or {}) for ric, fit in fit_by_ric.items()},
    )
    aggregate_trade_loadings, _, _ = _aggregate_loadings(
        provisional_rows,
        loadings_by_ric={
            str(ric): cpar_display_loadings.hedge_trade_loadings_from_fit(fit, thresholded=True)
            for ric, fit in fit_by_ric.items()
        },
    )
    aggregate_display_loadings, _, _ = _aggregate_loadings(
        provisional_rows,
        loadings_by_ric=display_loadings_by_ric,
    )
    position_rows = _build_position_rows(
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covered_gross_market_value=covered_gross_market_value,
    )
    position_rows = _attach_thresholded_contributions(position_rows, fit_by_ric=fit_by_ric)
    position_rows = _attach_display_contributions(position_rows, display_loadings_by_ric=display_loadings_by_ric)
    idio_contribution_by_ric, idio_variance_proxy = _specific_risk_contributions(position_rows)
    factor_variance_rows = _factor_variance_contribution_rows(
        aggregate_loadings,
        covariance_rows=resolved_display_covariance_rows,
    )
    factor_variance_proxy = _factor_variance_total(factor_variance_rows)
    total_variance_proxy = float(factor_variance_proxy + idio_variance_proxy)
    factor_variance_rows = _factor_variance_contribution_rows(
        aggregate_loadings,
        covariance_rows=resolved_display_covariance_rows,
        total_variance=total_variance_proxy,
    )
    display_factor_variance_rows = _factor_variance_contribution_rows(
        aggregate_display_loadings,
        covariance_rows=resolved_display_covariance_rows,
    )
    display_total_variance_proxy = float(_factor_variance_total(display_factor_variance_rows) + idio_variance_proxy)
    display_factor_variance_rows = _factor_variance_contribution_rows(
        aggregate_display_loadings,
        covariance_rows=covariance_rows,
        total_variance=display_total_variance_proxy,
    )
    position_rows = _attach_risk_mix(
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
        "coverage_breakdown": _coverage_breakdown(position_rows),
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
            **_factor_analytics_payload(
                aggregate_loadings=aggregate_loadings,
                position_rows=position_rows,
                covariance_rows=resolved_display_covariance_rows,
                contribution_field="thresholded_contributions",
                total_variance=total_variance_proxy,
            ),
            "aggregate_display_loadings": _factor_rows(aggregate_display_loadings),
            "risk_shares": _risk_share_payload(
                factor_variance_rows,
                idio_variance_proxy=idio_variance_proxy,
                total_variance_proxy=total_variance_proxy,
            ),
            "vol_scaled_shares": _vol_scaled_share_payload(
                aggregate_display_loadings,
                covariance_rows=covariance_rows,
                idio_variance_proxy=idio_variance_proxy,
            ),
            "factor_variance_proxy": float(factor_variance_proxy),
            "idio_variance_proxy": float(idio_variance_proxy),
            "total_variance_proxy": float(total_variance_proxy),
            "factor_variance_contributions": factor_variance_rows,
            "display_factor_variance_contributions": display_factor_variance_rows,
            "display_factor_chart": _factor_chart_rows(
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


def build_cpar_risk_snapshot(
    *,
    package: dict[str, object],
    accounts: list[dict[str, object]],
    positions: list[dict[str, Any]],
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
        "scope": "all_accounts",
        "accounts_count": int(len(accounts)),
        "portfolio_status": "empty",
        "portfolio_reason": "No live holdings positions are loaded across any account.",
        "positions_count": position_count,
        "covered_positions_count": 0,
        "excluded_positions_count": 0,
        "gross_market_value": 0.0,
        "net_market_value": 0.0,
        "covered_gross_market_value": 0.0,
        "coverage_ratio": None,
        "coverage_breakdown": _coverage_breakdown([]),
        "aggregate_thresholded_loadings": [],
        "aggregate_display_loadings": [],
        "risk_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 0.0},
        "vol_scaled_shares": {"market": 0.0, "industry": 0.0, "style": 0.0, "idio": 100.0},
        "factor_variance_contributions": [],
        "display_factor_variance_contributions": [],
        "factor_chart": [],
        "display_factor_chart": [],
        "cov_matrix": _cov_matrix_payload(covariance_rows=resolved_display_covariance_rows),
        "display_cov_matrix": _cov_matrix_payload(covariance_rows=resolved_display_covariance_rows),
        "factor_variance_proxy": 0.0,
        "idio_variance_proxy": 0.0,
        "total_variance_proxy": 0.0,
        "positions": [],
    }
    if not positions:
        return base_payload

    provisional_rows = _build_position_rows(
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covered_gross_market_value=0.0,
    )
    display_loadings_by_ric = _display_loadings_by_ric(fit_by_ric)
    aggregate_loadings, covered_gross_market_value, net_market_value = _aggregate_loadings(
        provisional_rows,
        loadings_by_ric={str(ric): dict((fit or {}).get("thresholded_loadings") or {}) for ric, fit in fit_by_ric.items()},
    )
    aggregate_display_loadings, _, _ = _aggregate_loadings(
        provisional_rows,
        loadings_by_ric=display_loadings_by_ric,
    )
    position_rows = _build_position_rows(
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covered_gross_market_value=covered_gross_market_value,
    )
    position_rows = _attach_thresholded_contributions(position_rows, fit_by_ric=fit_by_ric)
    position_rows = _attach_display_contributions(position_rows, display_loadings_by_ric=display_loadings_by_ric)
    idio_contribution_by_ric, idio_variance_proxy = _specific_risk_contributions(position_rows)
    factor_variance_rows = _factor_variance_contribution_rows(
        aggregate_loadings,
        covariance_rows=resolved_display_covariance_rows,
    )
    factor_variance_proxy = _factor_variance_total(factor_variance_rows)
    total_variance_proxy = float(factor_variance_proxy + idio_variance_proxy)
    factor_variance_rows = _factor_variance_contribution_rows(
        aggregate_loadings,
        covariance_rows=resolved_display_covariance_rows,
        total_variance=total_variance_proxy,
    )
    display_factor_variance_rows = _factor_variance_contribution_rows(
        aggregate_display_loadings,
        covariance_rows=resolved_display_covariance_rows,
    )
    display_total_variance_proxy = float(_factor_variance_total(display_factor_variance_rows) + idio_variance_proxy)
    display_factor_variance_rows = _factor_variance_contribution_rows(
        aggregate_display_loadings,
        covariance_rows=resolved_display_covariance_rows,
        total_variance=display_total_variance_proxy,
    )
    position_rows = _attach_risk_mix(
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
        "coverage_breakdown": _coverage_breakdown(position_rows),
        "positions": position_rows,
    }

    if covered_positions_count <= 0 or covered_gross_market_value <= 0:
        payload["portfolio_status"] = "unavailable"
        payload["portfolio_reason"] = (
            "No aggregated holdings rows across all accounts have both price coverage and a usable persisted cPAR fit "
            "in the active package."
        )
        return payload

    payload.update(
        {
            "portfolio_status": "partial" if excluded_positions_count > 0 else "ok",
            "portfolio_reason": (
                "Some aggregated holdings rows were excluded because they lack price coverage or a usable persisted "
                "cPAR fit."
                if excluded_positions_count > 0
                else None
            ),
            **_factor_analytics_payload(
                aggregate_loadings=aggregate_loadings,
                position_rows=position_rows,
                covariance_rows=resolved_display_covariance_rows,
                contribution_field="thresholded_contributions",
                total_variance=total_variance_proxy,
            ),
            "aggregate_display_loadings": _factor_rows(aggregate_display_loadings),
            "risk_shares": _risk_share_payload(
                factor_variance_rows,
                idio_variance_proxy=idio_variance_proxy,
                total_variance_proxy=total_variance_proxy,
            ),
            "vol_scaled_shares": _vol_scaled_share_payload(
                aggregate_display_loadings,
                covariance_rows=resolved_display_covariance_rows,
                idio_variance_proxy=idio_variance_proxy,
            ),
            "factor_variance_proxy": float(factor_variance_proxy),
            "idio_variance_proxy": float(idio_variance_proxy),
            "total_variance_proxy": float(total_variance_proxy),
            "factor_variance_contributions": factor_variance_rows,
            "display_factor_variance_contributions": display_factor_variance_rows,
            "display_factor_chart": _factor_chart_rows(
                loadings=aggregate_display_loadings,
                variance_rows=display_factor_variance_rows,
                position_rows=position_rows,
                covariance_rows=resolved_display_covariance_rows,
                contribution_field="display_contributions",
                total_variance=display_total_variance_proxy,
            ),
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
            classification_by_ric={},
            covariance_rows=[],
        )
    rics = [str(row.get("ric") or "") for row in positions if str(row.get("ric") or "").strip()]
    fit_by_ric, price_by_ric, classification_by_ric, covariance_rows = load_cpar_portfolio_support_rows(
        rics=rics,
        package_run_id=str(package["package_run_id"]),
        package_date=str(package["package_date"]),
        data_db=data_db,
    )
    try:
        display_covariance_rows = cpar_display_covariance.load_package_display_covariance_rows(
            package_run_id=str(package["package_run_id"]),
            data_db=data_db,
        )
    except Exception:
        display_covariance_rows = None
    return build_cpar_portfolio_hedge_snapshot(
        package=package,
        account=account,
        positions=positions,
        mode=mode,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covariance_rows=covariance_rows,
        display_covariance_rows=display_covariance_rows,
    )
