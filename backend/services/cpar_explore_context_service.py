"""Explicit cPAR owner for Explore held-position context payloads."""

from __future__ import annotations

from typing import Any

from backend.services import cpar_meta_service, cpar_portfolio_snapshot_service


def _held_positions_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "ric": str(row.get("ric") or ""),
            "ticker": row.get("ticker"),
            "quantity": float(row.get("quantity") or 0.0),
            "price": (float(row["price"]) if row.get("price") is not None else None),
            "market_value": (float(row["market_value"]) if row.get("market_value") is not None else None),
            "portfolio_weight": (
                float(row["portfolio_weight"])
                if row.get("portfolio_weight") is not None
                else None
            ),
            "long_short": "LONG" if float(row.get("quantity") or 0.0) >= 0 else "SHORT",
            "fit_status": row.get("fit_status"),
            "coverage": row.get("coverage"),
        }
        for row in rows
        if str(row.get("ric") or "").strip()
    ]


def load_cpar_explore_context_payload(
    *,
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
    data_db=None,
) -> dict[str, Any]:
    package, accounts, positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_aggregate_context(
        allowed_account_ids=allowed_account_ids,
        data_db=data_db,
    )
    base_payload: dict[str, Any] = {
        **cpar_meta_service.package_meta_payload(package),
        "scope": "restricted_accounts" if allowed_account_ids is not None else "all_accounts",
        "accounts_count": int(len(accounts)),
        "positions_count": int(len(positions)),
        "covered_positions_count": 0,
        "excluded_positions_count": 0,
        "gross_market_value": 0.0,
        "net_market_value": 0.0,
        "covered_gross_market_value": 0.0,
        "coverage_ratio": None,
        "portfolio_status": "empty",
        "portfolio_reason": "No live holdings positions are loaded across any account.",
        "held_positions": [],
    }
    if not positions:
        return base_payload

    rics = [str(row.get("ric") or "") for row in positions if str(row.get("ric") or "").strip()]
    fit_by_ric, price_by_ric, classification_by_ric = (
        cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows_without_covariance(
            rics=rics,
            package_run_id=str(package["package_run_id"]),
            package_date=str(package["package_date"]),
            positions=positions,
            data_db=data_db,
        )
    )
    provisional_rows = cpar_portfolio_snapshot_service._build_position_rows(
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covered_gross_market_value=0.0,
    )
    _, covered_gross_market_value, net_market_value = cpar_portfolio_snapshot_service._aggregate_loadings(
        provisional_rows,
        loadings_by_ric={},
    )
    position_rows = cpar_portfolio_snapshot_service._build_position_rows(
        positions=positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covered_gross_market_value=covered_gross_market_value,
    )
    covered_positions_count = sum(1 for row in position_rows if str(row.get("coverage") or "") == "covered")
    excluded_positions_count = len(position_rows) - covered_positions_count
    gross_market_value = sum(
        abs(float(row["market_value"]))
        for row in position_rows
        if row.get("market_value") is not None
    )
    coverage_ratio = None
    if gross_market_value > 0:
        coverage_ratio = float(covered_gross_market_value / gross_market_value)

    payload = {
        **base_payload,
        "positions_count": int(len(position_rows)),
        "covered_positions_count": int(covered_positions_count),
        "excluded_positions_count": int(excluded_positions_count),
        "gross_market_value": float(gross_market_value),
        "net_market_value": float(net_market_value),
        "covered_gross_market_value": float(covered_gross_market_value),
        "coverage_ratio": coverage_ratio,
        "held_positions": _held_positions_payload(position_rows),
    }
    if covered_positions_count <= 0 or covered_gross_market_value <= 0:
        payload["portfolio_status"] = "unavailable"
        payload["portfolio_reason"] = (
            "No aggregated holdings rows across all accounts have both price coverage and a usable persisted cPAR fit "
            "in the active package."
        )
        return payload

    payload["portfolio_status"] = "partial" if excluded_positions_count > 0 else "ok"
    payload["portfolio_reason"] = (
        "Some aggregated holdings rows were excluded because they lack price coverage or a usable persisted cPAR fit."
        if excluded_positions_count > 0
        else None
    )
    return payload


__all__ = ["load_cpar_explore_context_payload"]
