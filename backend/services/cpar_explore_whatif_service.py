"""Aggregate cPAR explore what-if preview service."""

from __future__ import annotations

import math
from typing import Any

from backend.cpar.factor_registry import build_cpar1_factor_registry
from backend.services import (
    cpar_aggregate_risk_service,
    cpar_meta_service,
    cpar_portfolio_snapshot_service,
)


MAX_CPAR_EXPLORE_WHATIF_ROWS = 100
_EPSILON = 1e-12


def _normalize_account_id(value: str | None) -> str:
    return str(value or "").strip().lower()


def _normalize_ric(value: str | None) -> str:
    return str(value or "").strip().upper()


def _normalize_ticker(value: str | None) -> str | None:
    clean = str(value or "").strip().upper()
    return clean or None


def _scenario_key(account_id: str | None, ric: str | None) -> str:
    return f"{_normalize_account_id(account_id)}:{_normalize_ric(ric)}"


def _factor_catalog_payload() -> list[dict[str, object]]:
    family_map = {
        "market": "market",
        "sector": "industry",
        "style": "style",
    }
    block_map = {
        "market": "Market",
        "sector": "Industry",
        "style": "Style",
    }
    return [
        {
            "factor_id": str(spec.factor_id),
            "factor_name": spec.label,
            "short_label": spec.label,
            "family": family_map.get(str(spec.group), "style"),
            "block": block_map.get(str(spec.group), "Style"),
            "display_order": int(spec.display_order),
            "active": True,
        }
        for spec in build_cpar1_factor_registry()
    ]


def _normalize_scenario_rows(
    *,
    accounts: list[dict[str, object]],
    live_positions: list[dict[str, Any]],
    scenario_rows: list[dict[str, Any]],
) -> list[dict[str, object]]:
    valid_accounts = {
        _normalize_account_id(str(row.get("account_id") or "")): str(row.get("account_id") or "")
        for row in accounts
        if _normalize_account_id(str(row.get("account_id") or ""))
    }
    live_by_key = {
        _scenario_key(row.get("account_id"), row.get("ric")): dict(row)
        for row in live_positions
        if _normalize_ric(str(row.get("ric") or ""))
    }
    normalized: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in list(scenario_rows or []):
        account_id = _normalize_account_id(raw.get("account_id"))
        ric = _normalize_ric(raw.get("ric"))
        if not account_id:
            raise ValueError("Each cPAR explore scenario row requires account_id.")
        if account_id not in valid_accounts:
            raise ValueError(f"Holdings account {raw.get('account_id')!r} was not found.")
        if not ric:
            raise ValueError("Each cPAR explore scenario row requires ric.")
        key = _scenario_key(account_id, ric)
        if key in seen:
            raise ValueError(f"Duplicate cPAR explore scenario row for {account_id}:{ric}.")
        seen.add(key)
        try:
            quantity = float(raw.get("quantity"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Scenario row for {account_id}:{ric} requires a finite quantity.") from exc
        if not math.isfinite(quantity):
            raise ValueError(f"Scenario row for {account_id}:{ric} requires a finite quantity.")
        if abs(quantity) <= _EPSILON:
            continue

        live_row = live_by_key.get(key)
        live_ticker = _normalize_ticker(live_row.get("ticker")) if live_row else None
        requested_ticker = _normalize_ticker(raw.get("ticker"))
        if live_row and requested_ticker and live_ticker and requested_ticker != live_ticker:
            raise ValueError(
                f"Scenario row for {account_id}:{ric} specified ticker {requested_ticker}, "
                f"but holdings currently map that ric to ticker {live_ticker}."
            )
        if live_row is None and requested_ticker is None:
            raise ValueError(
                f"Scenario row for {account_id}:{ric} is not in holdings and requires ticker."
            )

        normalized.append(
            {
                "account_id": valid_accounts[account_id],
                "ric": ric,
                "ticker": requested_ticker or live_ticker,
                "quantity": float(quantity),
                "source": str(raw.get("source") or "cpar_explore"),
            }
        )
    if not normalized:
        raise ValueError("At least one non-zero cPAR explore scenario row is required.")
    return normalized


def _apply_scenario_rows(
    *,
    live_positions: list[dict[str, Any]],
    scenario_rows: list[dict[str, object]],
) -> list[dict[str, Any]]:
    by_key = {
        _scenario_key(row.get("account_id"), row.get("ric")): dict(row)
        for row in live_positions
        if _normalize_ric(str(row.get("ric") or ""))
    }
    for scenario in scenario_rows:
        key = _scenario_key(scenario.get("account_id"), scenario.get("ric"))
        updated = dict(by_key.get(key) or {})
        hypothetical_quantity = float(updated.get("quantity") or 0.0) + float(scenario["quantity"])
        if abs(hypothetical_quantity) <= _EPSILON:
            by_key.pop(key, None)
            continue
        updated["account_id"] = scenario["account_id"]
        updated["ric"] = scenario["ric"]
        updated["ticker"] = scenario.get("ticker")
        updated["quantity"] = hypothetical_quantity
        updated["source"] = updated.get("source") or scenario.get("source") or "cpar_explore"
        by_key[key] = updated
    return sorted(
        by_key.values(),
        key=lambda row: (
            str(row.get("account_id") or ""),
            str(row.get("ticker") or row.get("ric") or ""),
            str(row.get("ric") or ""),
        ),
    )


def _build_holding_delta_rows(
    *,
    live_positions: list[dict[str, Any]],
    hypothetical_positions: list[dict[str, Any]],
    scenario_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    current_by_key = {
        _scenario_key(row.get("account_id"), row.get("ric")): dict(row)
        for row in live_positions
        if _normalize_ric(str(row.get("ric") or ""))
    }
    hypothetical_by_key = {
        _scenario_key(row.get("account_id"), row.get("ric")): dict(row)
        for row in hypothetical_positions
        if _normalize_ric(str(row.get("ric") or ""))
    }
    rows: list[dict[str, object]] = []
    for scenario in scenario_rows:
        key = _scenario_key(scenario.get("account_id"), scenario.get("ric"))
        current_row = current_by_key.get(key, {})
        hypothetical_row = hypothetical_by_key.get(key, {})
        rows.append(
            {
                "account_id": str(scenario.get("account_id") or ""),
                "ticker": (
                    scenario.get("ticker")
                    or hypothetical_row.get("ticker")
                    or current_row.get("ticker")
                ),
                "ric": str(scenario.get("ric") or ""),
                "current_quantity": float(current_row.get("quantity") or 0.0),
                "hypothetical_quantity": float(hypothetical_row.get("quantity") or 0.0),
                "delta_quantity": float(scenario.get("quantity") or 0.0),
            }
        )
    rows.sort(
        key=lambda row: (
            str(row.get("account_id") or ""),
            str(row.get("ticker") or row.get("ric") or ""),
        )
    )
    return rows


def _risk_shares(snapshot: dict[str, object]) -> dict[str, float]:
    if isinstance(snapshot.get("risk_shares"), dict):
        shares = dict(snapshot.get("risk_shares") or {})
        return {
            key: round(float(shares.get(key, 0.0) or 0.0), 2)
            for key in ("market", "industry", "style", "idio")
        }
    totals = {
        "market": 0.0,
        "industry": 0.0,
        "style": 0.0,
        "idio": 0.0,
    }
    for row in list(snapshot.get("factor_variance_contributions") or []):
        share = float(row.get("variance_share") or 0.0) * 100.0
        group = str(row.get("group") or "")
        if group == "market":
            totals["market"] += share
        elif group == "sector":
            totals["industry"] += share
        elif group == "style":
            totals["style"] += share
    return {key: round(float(value), 2) for key, value in totals.items()}


def _exposure_mode_rows(snapshot: dict[str, object], *, mode: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in list(snapshot.get("factor_chart") or []):
        if mode == "risk_contribution":
            value = float(row.get("risk_contribution_pct") or 0.0)
        elif mode == "sensitivity":
            value = float(row.get("sensitivity_beta") or 0.0)
        else:
            value = float(row.get("aggregate_beta") or row.get("beta") or 0.0)
        drilldown_rows: list[dict[str, object]] = []
        for drilldown in list(row.get("drilldown") or []):
            if mode == "risk_contribution":
                contribution = float(drilldown.get("risk_contribution_pct") or 0.0)
            elif mode == "sensitivity":
                contribution = float(drilldown.get("vol_scaled_contribution") or 0.0)
            else:
                contribution = float(drilldown.get("contribution_beta") or 0.0)
            drilldown_rows.append(
                {
                    "ric": drilldown.get("ric"),
                    "ticker": drilldown.get("ticker"),
                    "display_name": drilldown.get("display_name"),
                    "weight": float(drilldown.get("portfolio_weight") or 0.0),
                    "exposure": float(drilldown.get("factor_beta") or 0.0),
                    "sensitivity": float(drilldown.get("vol_scaled_loading") or 0.0),
                    "contribution": contribution,
                    "fit_status": drilldown.get("fit_status"),
                    "coverage": drilldown.get("coverage"),
                }
            )
        rows.append(
            {
                "factor_id": str(row.get("factor_id") or ""),
                "label": row.get("label"),
                "group": row.get("group"),
                "display_order": int(row.get("display_order") or 0),
                "value": value,
                "factor_volatility": float(row.get("factor_volatility") or 0.0),
                "drilldown": drilldown_rows,
            }
        )
    return rows


def _display_exposure_mode_rows(snapshot: dict[str, object], *, mode: str) -> list[dict[str, object]]:
    display_snapshot = {
        **snapshot,
        "factor_chart": list(snapshot.get("display_factor_chart") or []),
    }
    return _exposure_mode_rows(display_snapshot, mode=mode)


def _factor_delta_rows(
    *,
    current_rows: list[dict[str, object]],
    hypothetical_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    current_by_factor = {
        str(row.get("factor_id") or ""): dict(row)
        for row in current_rows
        if str(row.get("factor_id") or "")
    }
    hypothetical_by_factor = {
        str(row.get("factor_id") or ""): dict(row)
        for row in hypothetical_rows
        if str(row.get("factor_id") or "")
    }
    factor_ids = {
        *current_by_factor.keys(),
        *hypothetical_by_factor.keys(),
    }
    rows: list[dict[str, object]] = []
    for factor_id in factor_ids:
        current_value = float(current_by_factor.get(factor_id, {}).get("value") or 0.0)
        hypothetical_value = float(hypothetical_by_factor.get(factor_id, {}).get("value") or 0.0)
        rows.append(
            {
                "factor_id": factor_id,
                "current": current_value,
                "hypothetical": hypothetical_value,
                "delta": float(hypothetical_value - current_value),
            }
        )
    rows.sort(
        key=lambda row: (
            -abs(float(row.get("delta") or 0.0)),
            str(row.get("factor_id") or ""),
        )
    )
    return rows[:20]


def _preview_side(
    snapshot: dict[str, object],
    *,
    scope: str | None = None,
) -> dict[str, object]:
    return {
        "scope": scope or snapshot.get("scope") or "all_accounts",
        "positions": list(snapshot.get("positions") or []),
        "total_value": float(snapshot.get("gross_market_value") or 0.0),
        "position_count": int(snapshot.get("positions_count") or 0),
        "risk_shares": _risk_shares(snapshot),
        "exposure_modes": {
            "raw": _exposure_mode_rows(snapshot, mode="raw"),
            "sensitivity": _exposure_mode_rows(snapshot, mode="sensitivity"),
            "risk_contribution": _exposure_mode_rows(snapshot, mode="risk_contribution"),
        },
        "display_exposure_modes": {
            "raw": _display_exposure_mode_rows(snapshot, mode="raw"),
            "sensitivity": _display_exposure_mode_rows(snapshot, mode="sensitivity"),
            "risk_contribution": _display_exposure_mode_rows(snapshot, mode="risk_contribution"),
        },
        "factor_catalog": _factor_catalog_payload(),
        "portfolio_status": snapshot.get("portfolio_status"),
        "portfolio_reason": snapshot.get("portfolio_reason"),
    }


def _preview_scope_payload(*, normalized_rows: list[dict[str, object]]) -> dict[str, object]:
    account_ids = list(
        dict.fromkeys(
            str(row.get("account_id") or "")
            for row in normalized_rows
            if str(row.get("account_id") or "").strip()
        )
    )
    return {
        "kind": "staged_accounts",
        "account_ids": account_ids,
        "accounts_count": len(account_ids),
    }


def load_cpar_explore_whatif_payload(
    *,
    scenario_rows: list[dict[str, Any]],
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
    data_db=None,
) -> dict[str, object]:
    package, accounts, live_positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_holdings_context(
        allowed_account_ids=allowed_account_ids,
        data_db=data_db,
    )
    normalized_rows = _normalize_scenario_rows(
        accounts=accounts,
        live_positions=live_positions,
        scenario_rows=scenario_rows,
    )
    preview_account_ids = {
        _normalize_account_id(str(row.get("account_id") or ""))
        for row in normalized_rows
        if _normalize_account_id(str(row.get("account_id") or ""))
    }
    scoped_live_positions = [
        dict(position)
        for position in live_positions
        if _normalize_account_id(str(position.get("account_id") or "")) in preview_account_ids
    ]
    hypothetical_positions = _apply_scenario_rows(
        live_positions=scoped_live_positions,
        scenario_rows=normalized_rows,
    )

    current_aggregated_positions, current_accounts = cpar_portfolio_snapshot_service.aggregate_cpar_positions_across_accounts(
        scoped_live_positions
    )
    hypothetical_aggregated_positions, hypothetical_accounts = cpar_portfolio_snapshot_service.aggregate_cpar_positions_across_accounts(
        hypothetical_positions
    )
    rics = sorted(
        {
            *(_normalize_ric(str(row.get("ric") or "")) for row in current_aggregated_positions),
            *(_normalize_ric(str(row.get("ric") or "")) for row in hypothetical_aggregated_positions),
            *(_normalize_ric(str(row.get("ric") or "")) for row in normalized_rows),
        }
    )
    rics = [ric for ric in rics if ric]

    fit_by_ric, price_by_ric, classification_by_ric, covariance_rows = cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows(
        rics=rics,
        package_run_id=str(package["package_run_id"]),
        package_date=str(package["package_date"]),
        positions=[*current_aggregated_positions, *hypothetical_aggregated_positions, *normalized_rows],
        data_db=data_db,
    )
    for row in normalized_rows:
        key = _scenario_key(row.get("account_id"), row.get("ric"))
        live_exists = any(_scenario_key(pos.get("account_id"), pos.get("ric")) == key for pos in scoped_live_positions)
        if not live_exists and str(row.get("ric") or "") not in fit_by_ric:
            raise ValueError(
                f"RIC {row['ric']} is not present in the active cPAR package. "
                "Stage additions from active-package cPAR search results only."
            )

    current_snapshot = cpar_aggregate_risk_service.build_cpar_risk_snapshot(
        package=package,
        accounts=current_accounts,
        positions=current_aggregated_positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covariance_rows=covariance_rows,
    )
    hypothetical_snapshot = cpar_aggregate_risk_service.build_cpar_risk_snapshot(
        package=package,
        accounts=hypothetical_accounts,
        positions=hypothetical_aggregated_positions,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covariance_rows=covariance_rows,
    )

    preview_scope = _preview_scope_payload(normalized_rows=normalized_rows)
    preview_side_scope = "restricted_accounts" if preview_scope["account_ids"] else None
    current_side = _preview_side(current_snapshot, scope=preview_side_scope)
    hypothetical_side = _preview_side(hypothetical_snapshot, scope=preview_side_scope)

    diff_factor_deltas = {
        mode: _factor_delta_rows(
            current_rows=list(current_side["exposure_modes"][mode]),
            hypothetical_rows=list(hypothetical_side["exposure_modes"][mode]),
        )
        for mode in ("raw", "sensitivity", "risk_contribution")
    }
    diff_display_factor_deltas = {
        mode: _factor_delta_rows(
            current_rows=list(current_side["display_exposure_modes"][mode]),
            hypothetical_rows=list(hypothetical_side["display_exposure_modes"][mode]),
        )
        for mode in ("raw", "sensitivity", "risk_contribution")
    }
    diff_risk_shares = {
        bucket: round(
            float(hypothetical_side["risk_shares"].get(bucket, 0.0))
            - float(current_side["risk_shares"].get(bucket, 0.0)),
            2,
        )
        for bucket in ("market", "industry", "style", "idio")
    }

    return {
        **cpar_meta_service.package_meta_payload(package),
        "scenario_rows": [
            {
                "account_id": str(row.get("account_id") or ""),
                "ticker": row.get("ticker"),
                "ric": str(row.get("ric") or ""),
                "quantity": float(row.get("quantity") or 0.0),
                "source": str(row.get("source") or "cpar_explore"),
            }
            for row in normalized_rows
        ],
        "holding_deltas": _build_holding_delta_rows(
            live_positions=scoped_live_positions,
            hypothetical_positions=hypothetical_positions,
            scenario_rows=normalized_rows,
        ),
        "current": current_side,
        "hypothetical": hypothetical_side,
        "diff": {
            "total_value": round(
                float(hypothetical_side["total_value"]) - float(current_side["total_value"]),
                2,
            ),
            "position_count": int(hypothetical_side["position_count"]) - int(current_side["position_count"]),
            "risk_shares": diff_risk_shares,
            "factor_deltas": diff_factor_deltas,
            "display_factor_deltas": diff_display_factor_deltas,
        },
        "source_dates": {
            "prices_asof": package.get("source_prices_asof"),
            "classification_asof": package.get("classification_asof"),
            "exposures_asof": package.get("package_date"),
            "exposures_served_asof": package.get("package_date"),
        },
        "truth_surface": "aggregate_holdings_projected_through_active_cpar_package",
        "preview_scope": preview_scope,
        "_preview_only": True,
    }
