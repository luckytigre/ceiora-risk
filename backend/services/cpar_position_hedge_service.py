"""Read-only cPAR position hedge payload service for risk-row popovers."""

from __future__ import annotations

from typing import Any, Literal

from backend.cpar import hedge_engine
from backend.services import (
    cpar_display_loadings,
    cpar_hedge_trade_sizing,
    cpar_meta_service,
    cpar_portfolio_account_snapshot_service,
    cpar_portfolio_snapshot_service,
)

ScopeLiteral = Literal["all_permitted_accounts", "account"]


class CparPositionHedgeNotFound(LookupError):
    """Raised when the requested scoped holdings row does not exist."""


def _normalize_ric(value: str | None) -> str:
    return str(value or "").strip().upper()


def _resolve_scope_context(
    *,
    scope: ScopeLiteral,
    account_id: str | None,
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
    data_db=None,
) -> tuple[dict[str, object], dict[str, object] | None, list[dict[str, Any]]]:
    if scope == "account":
        clean_account_id = str(account_id or "").strip()
        if not clean_account_id:
            raise ValueError("account_id is required when scope=account.")
        package, account, positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_account_context(
            account_id=clean_account_id,
            allowed_account_ids=allowed_account_ids,
            data_db=data_db,
        )
        return package, account, positions
    package, _accounts, positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_aggregate_context(
        allowed_account_ids=allowed_account_ids,
        data_db=data_db,
    )
    return package, None, positions


def _resolve_position_row(*, positions: list[dict[str, Any]], ric: str) -> dict[str, Any]:
    normalized_ric = _normalize_ric(ric)
    for row in positions:
        if _normalize_ric(row.get("ric")) == normalized_ric:
            return row
    raise CparPositionHedgeNotFound(f"cPAR holdings row {ric!r} was not found in the selected scope.")


def _covariance_lookup(covariance_rows: list[dict[str, Any]]) -> dict[tuple[str, str], float]:
    return {
        (str(row.get("factor_id") or ""), str(row.get("factor_id_2") or "")): float(row.get("covariance") or 0.0)
        for row in covariance_rows
    }


def _package_payload(
    *,
    preview,
    base_notional: float,
    factor_proxy_price_context: dict[str, dict[str, object]],
    aggregate_trade_loadings: dict[str, float],
) -> dict[str, object]:
    return {
        "mode": str(preview.mode),
        "hedge_status": str(preview.status),
        "hedge_reason": preview.reason,
        "trade_rows": cpar_hedge_trade_sizing.sized_trade_rows_from_hedge_weights(
            dict(preview.hedge_weights),
            base_notional=base_notional,
            factor_proxy_price_context=factor_proxy_price_context,
        ),
        "post_hedge_exposures": cpar_portfolio_account_snapshot_service._post_hedge_exposure_rows(
            pre_loadings=aggregate_trade_loadings,
            hedge_weights=dict(preview.hedge_weights),
            post_loadings=dict(preview.post_hedge_loadings),
        ),
        "pre_hedge_factor_variance_proxy": float(preview.pre_hedge_variance_proxy),
        "post_hedge_factor_variance_proxy": float(preview.post_hedge_variance_proxy),
        "non_market_reduction_ratio": preview.non_market_reduction_ratio,
    }


def load_cpar_position_hedge_payload(
    *,
    ric: str,
    scope: ScopeLiteral,
    account_id: str | None = None,
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
    data_db=None,
) -> dict[str, object]:
    package, account, positions = _resolve_scope_context(
        scope=scope,
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
        data_db=data_db,
    )
    scoped_position = _resolve_position_row(positions=positions, ric=ric)
    target_positions = [scoped_position]
    fit_by_ric, price_by_ric, classification_by_ric, covariance_rows = (
        cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows(
            rics=[str(scoped_position.get("ric") or "")],
            package_run_id=str(package["package_run_id"]),
            package_date=str(package["package_date"]),
            positions=target_positions,
            data_db=data_db,
        )
    )
    provisional_rows = cpar_portfolio_snapshot_service._build_position_rows(
        positions=target_positions,
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
    position_rows = cpar_portfolio_snapshot_service._build_position_rows(
        positions=target_positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covered_gross_market_value=covered_gross_market_value,
    )
    if not position_rows:
        raise CparPositionHedgeNotFound(f"cPAR holdings row {ric!r} was not found in the selected scope.")
    position_row = position_rows[0]
    base_notional = abs(float(position_row.get("market_value") or 0.0))
    if str(position_row.get("coverage") or "") != "covered" or base_notional <= 0.0:
        raise CparPositionHedgeNotFound(f"cPAR holdings row {ric!r} is not hedge-eligible in the selected scope.")

    fit = fit_by_ric.get(str(position_row.get("ric") or ""))
    fit_status = str((fit or {}).get("fit_status") or "ok")
    hedge_use_status = (fit or {}).get("hedge_use_status")
    covariance = _covariance_lookup(covariance_rows)
    market_preview = hedge_engine.build_market_neutral_hedge(
        aggregate_trade_loadings,
        covariance,
        fit_status=fit_status,
        hedge_use_status=None if hedge_use_status is None else str(hedge_use_status),
    )
    factor_preview = hedge_engine.build_factor_neutral_hedge(
        aggregate_trade_loadings,
        covariance,
        fit_status=fit_status,
        hedge_use_status=None if hedge_use_status is None else str(hedge_use_status),
    )
    factor_proxy_price_context = cpar_hedge_trade_sizing.load_factor_proxy_price_context(
        sorted({*dict(market_preview.hedge_weights).keys(), *dict(factor_preview.hedge_weights).keys()}),
        package_date=str(package["package_date"]),
        data_db=data_db,
    )
    return {
        **cpar_meta_service.package_meta_payload(package),
        "scope": str(scope),
        "account_id": None if account is None else str(account.get("account_id") or ""),
        "account_name": None if account is None else str(account.get("account_name") or account.get("account_id") or ""),
        "position": {
            **position_row,
            "base_notional": float(base_notional),
            "classification": classification_by_ric.get(str(position_row.get("ric") or "")),
        },
        "packages": {
            "market_neutral": _package_payload(
                preview=market_preview,
                base_notional=base_notional,
                factor_proxy_price_context=factor_proxy_price_context,
                aggregate_trade_loadings=aggregate_trade_loadings,
            ),
            "factor_neutral": _package_payload(
                preview=factor_preview,
                base_notional=base_notional,
                factor_proxy_price_context=factor_proxy_price_context,
                aggregate_trade_loadings=aggregate_trade_loadings,
            ),
        },
    }
