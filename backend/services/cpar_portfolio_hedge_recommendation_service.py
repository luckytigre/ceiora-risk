"""Read-only cPAR portfolio hedge recommendation payload service."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any, Literal

from backend.cpar import hedge_engine
from backend.data import cpar_outputs
from backend.services import (
    cpar_aggregate_risk_service,
    cpar_display_covariance,
    cpar_display_loadings,
    cpar_hedge_trade_sizing,
    cpar_meta_service,
    cpar_portfolio_account_snapshot_service,
    cpar_portfolio_snapshot_service,
)

ScopeLiteral = Literal["all_permitted_accounts", "account"]

CparPortfolioAccountNotFound = cpar_portfolio_snapshot_service.CparPortfolioAccountNotFound
_EPSILON = 1e-12


def _covariance_lookup(covariance_rows: list[dict[str, Any]]) -> dict[tuple[str, str], float]:
    return {
        (str(row.get("factor_id") or ""), str(row.get("factor_id_2") or "")): float(row.get("covariance") or 0.0)
        for row in covariance_rows
    }


def _load_scope_context(
    *,
    scope: ScopeLiteral,
    account_id: str | None,
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
    data_db=None,
) -> tuple[dict[str, object], dict[str, object] | None, list[dict[str, Any]], list[dict[str, object]]]:
    if scope == "account":
        clean_account_id = str(account_id or "").strip()
        if not clean_account_id:
            raise ValueError("account_id is required when scope=account.")
        package, account, positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_account_context(
            account_id=clean_account_id,
            allowed_account_ids=allowed_account_ids,
            data_db=data_db,
        )
        return package, account, positions, [account]
    package, accounts, positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_aggregate_context(
        allowed_account_ids=allowed_account_ids,
        data_db=data_db,
    )
    return package, None, positions, accounts


def _build_current_snapshot(
    *,
    package: dict[str, object],
    scope: ScopeLiteral,
    account: dict[str, object] | None,
    positions: list[dict[str, Any]],
    accounts: list[dict[str, object]],
    fit_by_ric: dict[str, dict[str, Any]],
    price_by_ric: dict[str, dict[str, Any]],
    classification_by_ric: dict[str, dict[str, Any]],
    covariance_rows: list[dict[str, Any]],
    display_covariance_rows: list[dict[str, Any]] | None,
) -> dict[str, object]:
    if scope == "account":
        assert account is not None
        snapshot = cpar_portfolio_snapshot_service.build_cpar_portfolio_hedge_snapshot(
            package=package,
            account=account,
            positions=positions,
            mode="factor_neutral",
            fit_by_ric=fit_by_ric,
            price_by_ric=price_by_ric,
            classification_by_ric=classification_by_ric,
            covariance_rows=covariance_rows,
            display_covariance_rows=display_covariance_rows,
        )
        snapshot["scope"] = "account"
        return snapshot
    snapshot = cpar_aggregate_risk_service.build_cpar_risk_snapshot(
        package=package,
        accounts=accounts,
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covariance_rows=covariance_rows,
        display_covariance_rows=display_covariance_rows,
    )
    snapshot["scope"] = "all_permitted_accounts"
    return snapshot


def _unavailable_recommendation(
    *,
    reason: str,
) -> dict[str, object]:
    return {
        "mode": "factor_neutral",
        "objective": "minimize_residual_trade_space_loading_magnitude",
        "max_hedge_legs": 10,
        "base_notional": 0.0,
        "hedge_status": "hedge_unavailable",
        "hedge_reason": reason,
        "trade_rows": [],
        "post_hedge_exposures": [],
        "pre_hedge_factor_variance_proxy": 0.0,
        "post_hedge_factor_variance_proxy": 0.0,
        "non_market_reduction_ratio": None,
    }


def _display_space_hedge_weights(
    *,
    hedge_weights: dict[str, float],
    factor_proxy_price_context: dict[str, dict[str, object]],
    package_run_id: str,
    data_db=None,
) -> dict[str, float]:
    proxy_rics = sorted(
        {
            str(context.get("proxy_ric") or "").strip().upper()
            for context in factor_proxy_price_context.values()
            if str(context.get("proxy_ric") or "").strip()
        }
    )
    if not proxy_rics:
        return {}
    proxy_fit_rows = cpar_outputs.load_package_instrument_fits_for_rics(
        proxy_rics,
        package_run_id=str(package_run_id),
        data_db=data_db,
    )
    proxy_fit_by_ric = {
        str(row.get("ric") or "").strip().upper(): row
        for row in proxy_fit_rows
        if str(row.get("ric") or "").strip()
    }
    display_weights: dict[str, float] = {}
    for factor_id, trade_weight in hedge_weights.items():
        context = factor_proxy_price_context.get(str(factor_id))
        if context is None:
            continue
        proxy_ric = str(context.get("proxy_ric") or "").strip().upper()
        proxy_fit = proxy_fit_by_ric.get(proxy_ric)
        if proxy_fit is None:
            display_weights[str(factor_id)] = float(display_weights.get(str(factor_id), 0.0) + float(trade_weight))
            continue
        for display_factor_id, beta in cpar_display_loadings.display_loadings_from_fit(proxy_fit).items():
            display_weights[str(display_factor_id)] = float(
                display_weights.get(str(display_factor_id), 0.0) + float(trade_weight) * float(beta)
            )
    return {
        factor_id: float(beta)
        for factor_id, beta in display_weights.items()
        if abs(float(beta)) > _EPSILON
    }


def _display_space_reduction_ratio(
    *,
    pre_loadings: dict[str, float],
    post_loadings: dict[str, float],
) -> float | None:
    pre_non_market = float(
        sum(abs(float(beta)) for factor_id, beta in pre_loadings.items() if str(factor_id) != "SPY")
    )
    post_non_market = float(
        sum(abs(float(beta)) for factor_id, beta in post_loadings.items() if str(factor_id) != "SPY")
    )
    if pre_non_market <= 0.0:
        return 1.0
    return max(0.0, 1.0 - (post_non_market / pre_non_market))


def load_cpar_portfolio_hedge_recommendation_payload(
    *,
    scope: ScopeLiteral,
    account_id: str | None = None,
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
    data_db=None,
) -> dict[str, object]:
    package, account, positions, accounts = _load_scope_context(
        scope=scope,
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
        data_db=data_db,
    )
    rics = [str(row.get("ric") or "") for row in positions if str(row.get("ric") or "").strip()]
    with ThreadPoolExecutor(max_workers=2) as executor:
        display_covariance_future = executor.submit(
            cpar_display_covariance.load_package_display_covariance_rows,
            package_run_id=str(package["package_run_id"]),
            data_db=data_db,
        )
        support_rows_future = executor.submit(
            cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows,
            rics=rics,
            package_run_id=str(package["package_run_id"]),
            package_date=str(package["package_date"]),
            positions=positions,
            data_db=data_db,
        )
        try:
            display_covariance_rows = display_covariance_future.result()
        except cpar_outputs.CparPackageNotReady as exc:
            raise cpar_meta_service.CparReadNotReady(str(exc)) from exc
        except cpar_outputs.CparAuthorityReadError as exc:
            raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc
        fit_by_ric, price_by_ric, classification_by_ric, covariance_rows = support_rows_future.result()

    snapshot = _build_current_snapshot(
        package=package,
        scope=scope,
        account=account,
        positions=positions,
        accounts=accounts,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covariance_rows=covariance_rows,
        display_covariance_rows=display_covariance_rows,
    )
    provisional_rows = cpar_portfolio_snapshot_service._build_position_rows(
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covered_gross_market_value=0.0,
    )
    aggregate_trade_loadings, covered_gross_market_value, _ = cpar_portfolio_snapshot_service._aggregate_loadings(
        provisional_rows,
        loadings_by_ric={
            str(row_ric): cpar_display_loadings.hedge_trade_loadings_from_fit(fit, thresholded=True)
            for row_ric, fit in fit_by_ric.items()
        },
    )
    if str(snapshot.get("portfolio_status") or "") in {"empty", "unavailable"} or covered_gross_market_value <= 0.0:
        return {
            **snapshot,
            "scope": str(scope),
            "account_id": None if account is None else str(account.get("account_id") or ""),
            "account_name": None if account is None else str(account.get("account_name") or account.get("account_id") or ""),
            "hedge_recommendation": _unavailable_recommendation(
                reason="no_covered_positions_in_selected_scope",
            ),
        }
    recommendation_preview = hedge_engine.build_factor_neutral_recommendation(
        aggregate_trade_loadings,
        _covariance_lookup(covariance_rows),
        fit_status="ok",
        hedge_use_status="usable",
        max_hedge_legs=10,
    )
    factor_proxy_price_context = cpar_hedge_trade_sizing.load_factor_proxy_price_context(
        list(dict(recommendation_preview.hedge_weights).keys()),
        package_date=str(package["package_date"]),
        data_db=data_db,
    )
    aggregate_display_loadings = {
        str(row.get("factor_id") or ""): float(row.get("beta") or 0.0)
        for row in list(snapshot.get("aggregate_display_loadings") or [])
        if str(row.get("factor_id") or "")
    }
    display_hedge_weights = _display_space_hedge_weights(
        hedge_weights=dict(recommendation_preview.hedge_weights),
        factor_proxy_price_context=factor_proxy_price_context,
        package_run_id=str(package["package_run_id"]),
        data_db=data_db,
    )
    display_post_loadings = {
        factor_id: float(aggregate_display_loadings.get(factor_id, 0.0)) + float(display_hedge_weights.get(factor_id, 0.0))
        for factor_id in sorted({*aggregate_display_loadings.keys(), *display_hedge_weights.keys()})
    }
    return {
        **snapshot,
        "scope": str(scope),
        "account_id": None if account is None else str(account.get("account_id") or ""),
        "account_name": None if account is None else str(account.get("account_name") or account.get("account_id") or ""),
        "hedge_recommendation": {
            "mode": "factor_neutral",
            "objective": "minimize_residual_trade_space_loading_magnitude",
            "max_hedge_legs": 10,
            "base_notional": float(covered_gross_market_value),
            "hedge_status": str(recommendation_preview.status),
            "hedge_reason": recommendation_preview.reason,
            "trade_rows": cpar_hedge_trade_sizing.sized_trade_rows_from_hedge_weights(
                dict(recommendation_preview.hedge_weights),
                base_notional=float(covered_gross_market_value),
                factor_proxy_price_context=factor_proxy_price_context,
            ),
            "post_hedge_exposures": cpar_portfolio_account_snapshot_service._post_hedge_exposure_rows(
                pre_loadings=aggregate_display_loadings,
                hedge_weights=display_hedge_weights,
                post_loadings=display_post_loadings,
            ),
            "pre_hedge_factor_variance_proxy": float(recommendation_preview.pre_hedge_variance_proxy),
            "post_hedge_factor_variance_proxy": float(recommendation_preview.post_hedge_variance_proxy),
            "non_market_reduction_ratio": _display_space_reduction_ratio(
                pre_loadings=aggregate_display_loadings,
                post_loadings=display_post_loadings,
            ),
        },
    }
