"""Read-only account-scoped cPAR portfolio hedge payload service."""

from __future__ import annotations

from typing import Any

from backend.cpar import hedge_engine
from backend.cpar.factor_registry import build_cpar1_factor_registry
from backend.data import cpar_outputs, cpar_source_reads, holdings_reads
from backend.services import cpar_meta_service


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


def load_cpar_portfolio_hedge_payload(
    *,
    account_id: str,
    mode: str,
    data_db=None,
) -> dict[str, object]:
    package = cpar_meta_service.require_active_package(data_db=data_db)

    try:
        accounts = holdings_reads.load_holdings_accounts()
        positions = holdings_reads.load_holdings_positions(account_id=account_id)
    except Exception as exc:  # noqa: BLE001
        raise cpar_meta_service.CparReadUnavailable(f"Holdings read failed: {exc}") from exc

    normalized_account_id = _normalize_account_id(account_id)
    account = next(
        (row for row in accounts if _normalize_account_id(str(row.get("account_id") or "")) == normalized_account_id),
        None,
    )
    if account is None:
        raise CparPortfolioAccountNotFound(f"Holdings account {account_id!r} was not found.")

    base_payload: dict[str, object] = {
        **cpar_meta_service.package_meta_payload(package),
        "account_id": str(account.get("account_id") or account_id),
        "account_name": str(account.get("account_name") or account_id),
        "mode": str(mode),
        "positions_count": int(len(positions)),
        "covered_positions_count": 0,
        "excluded_positions_count": 0,
        "gross_market_value": 0.0,
        "net_market_value": 0.0,
        "covered_gross_market_value": 0.0,
        "coverage_ratio": None,
        "portfolio_status": "empty",
        "portfolio_reason": "No live holdings positions are loaded for this account.",
        "aggregate_thresholded_loadings": [],
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

    rics = [str(row.get("ric") or "") for row in positions if str(row.get("ric") or "").strip()]
    try:
        fit_rows = cpar_outputs.load_package_instrument_fits_for_rics(
            rics,
            package_run_id=str(package["package_run_id"]),
            data_db=data_db,
        )
        covariance_rows = cpar_outputs.load_package_covariance_rows(
            str(package["package_run_id"]),
            data_db=data_db,
            require_complete=True,
            context_label="Active cPAR package",
        )
        price_rows = cpar_source_reads.load_latest_price_rows(
            rics,
            as_of_date=str(package["package_date"]),
            data_db=data_db,
        )
    except cpar_outputs.CparPackageNotReady as exc:
        raise cpar_meta_service.CparReadNotReady(str(exc)) from exc
    except cpar_outputs.CparAuthorityReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise cpar_meta_service.CparReadUnavailable(f"Shared-source read failed: {exc}") from exc

    fit_by_ric = {str(row["ric"]): row for row in fit_rows}
    price_by_ric = {str(row["ric"]): row for row in price_rows}

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
        "positions": position_rows,
    }

    if covered_positions_count <= 0 or covered_gross_market_value <= 0 or not aggregate_loadings:
        payload["portfolio_status"] = "unavailable"
        payload["portfolio_reason"] = (
            "No holdings rows in this account have both price coverage and a usable persisted cPAR fit in the active package."
        )
        return payload

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
