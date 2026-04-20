"""Read-only account-scoped cPAR what-if preview service."""

from __future__ import annotations

import math
from typing import Any

from backend.services import cpar_meta_service, cpar_portfolio_snapshot_service


MAX_CPAR_WHATIF_ROWS = 50
_EPSILON = 1e-12


def _normalize_ric(value: str | None) -> str:
    return str(value or "").strip().upper()


def _normalize_ticker(value: str | None) -> str | None:
    clean = str(value or "").strip().upper()
    return clean or None


def _normalize_scenario_rows(
    *,
    account_id: str,
    live_positions: list[dict[str, Any]],
    scenario_rows: list[dict[str, Any]],
) -> list[dict[str, object]]:
    live_by_ric = {
        _normalize_ric(str(row.get("ric") or "")): dict(row)
        for row in live_positions
        if _normalize_ric(str(row.get("ric") or ""))
    }
    normalized: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in list(scenario_rows or []):
        ric = _normalize_ric(raw.get("ric"))
        if not ric:
            raise ValueError("Each cPAR what-if scenario row requires ric.")
        if ric in seen:
            raise ValueError(f"Duplicate cPAR what-if scenario row for ric {ric}.")
        seen.add(ric)

        try:
            quantity_delta = float(raw.get("quantity_delta"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Scenario row for ric {ric} requires a finite quantity_delta.") from exc
        if not math.isfinite(quantity_delta):
            raise ValueError(f"Scenario row for ric {ric} requires a finite quantity_delta.")
        if abs(quantity_delta) <= _EPSILON:
            continue

        live_row = live_by_ric.get(ric)
        live_ticker = _normalize_ticker(live_row.get("ticker")) if live_row else None
        requested_ticker = _normalize_ticker(raw.get("ticker"))
        if live_row and requested_ticker and live_ticker and requested_ticker != live_ticker:
            raise ValueError(
                f"Scenario row for ric {ric} specified ticker {requested_ticker}, "
                f"but holdings account {account_id!r} currently maps that ric to ticker {live_ticker}."
            )
        if live_row is None and requested_ticker is None:
            raise ValueError(
                f"Scenario row for ric {ric} is not in holdings account {account_id!r} and requires ticker."
            )

        current_quantity = float(live_row.get("quantity") or 0.0) if live_row else 0.0
        normalized.append(
            {
                "ric": ric,
                "ticker": requested_ticker or live_ticker,
                "current_quantity": current_quantity,
                "quantity_delta": float(quantity_delta),
                "hypothetical_quantity": float(current_quantity + quantity_delta),
            }
        )
    if not normalized:
        raise ValueError("At least one non-zero cPAR what-if scenario row is required.")
    return normalized


def _apply_scenario_rows(
    *,
    account_id: str,
    live_positions: list[dict[str, Any]],
    scenario_rows: list[dict[str, object]],
) -> list[dict[str, Any]]:
    positions_by_ric = {
        _normalize_ric(str(row.get("ric") or "")): dict(row)
        for row in live_positions
        if _normalize_ric(str(row.get("ric") or ""))
    }
    for scenario in scenario_rows:
        ric = str(scenario["ric"])
        hypothetical_quantity = float(scenario["hypothetical_quantity"])
        if abs(hypothetical_quantity) <= _EPSILON:
            positions_by_ric.pop(ric, None)
            continue
        updated = dict(positions_by_ric.get(ric) or {})
        updated["account_id"] = str(updated.get("account_id") or account_id)
        updated["ric"] = ric
        updated["ticker"] = scenario.get("ticker")
        updated["quantity"] = hypothetical_quantity
        updated["source"] = str(updated.get("source") or "cpar_whatif_preview")
        updated["updated_at"] = updated.get("updated_at")
        positions_by_ric[ric] = updated
    return sorted(
        positions_by_ric.values(),
        key=lambda row: (
            str(row.get("ticker") or row.get("ric") or ""),
            str(row.get("ric") or ""),
        ),
    )


def _build_scenario_preview_rows(
    *,
    scenario_rows: list[dict[str, object]],
    current_payload: dict[str, object],
    hypothetical_payload: dict[str, object],
) -> list[dict[str, object]]:
    current_rows = {
        _normalize_ric(str(row.get("ric") or "")): dict(row)
        for row in list(current_payload.get("positions") or [])
        if _normalize_ric(str(row.get("ric") or ""))
    }
    hypothetical_rows = {
        _normalize_ric(str(row.get("ric") or "")): dict(row)
        for row in list(hypothetical_payload.get("positions") or [])
        if _normalize_ric(str(row.get("ric") or ""))
    }
    preview_rows: list[dict[str, object]] = []
    for scenario in scenario_rows:
        ric = str(scenario["ric"])
        current_row = current_rows.get(ric, {})
        hypothetical_row = hypothetical_rows.get(ric, {})
        removed = abs(float(scenario["hypothetical_quantity"])) <= _EPSILON and not hypothetical_row
        current_market_value = current_row.get("market_value")
        hypothetical_market_value = None if removed else hypothetical_row.get("market_value")
        current_mv = float(current_market_value or 0.0)
        hypothetical_mv = float(hypothetical_market_value or 0.0)
        preview_rows.append(
            {
                "ric": ric,
                "ticker": scenario.get("ticker") or hypothetical_row.get("ticker") or current_row.get("ticker"),
                "display_name": hypothetical_row.get("display_name") or current_row.get("display_name"),
                "quantity_delta": float(scenario["quantity_delta"]),
                "current_quantity": float(scenario["current_quantity"]),
                "hypothetical_quantity": float(scenario["hypothetical_quantity"]),
                "price": hypothetical_row.get("price") if hypothetical_row else current_row.get("price"),
                "price_date": hypothetical_row.get("price_date") if hypothetical_row else current_row.get("price_date"),
                "price_field_used": (
                    hypothetical_row.get("price_field_used")
                    if hypothetical_row
                    else current_row.get("price_field_used")
                ),
                "market_value_delta": float(hypothetical_mv - current_mv),
                "fit_status": hypothetical_row.get("fit_status") if hypothetical_row else current_row.get("fit_status"),
                "warnings": list(hypothetical_row.get("warnings") or current_row.get("warnings") or []),
                "coverage": (
                    hypothetical_row.get("coverage")
                    or current_row.get("coverage")
                    or "missing_cpar_fit"
                ),
                "coverage_reason": (
                    "Position removed by the cPAR what-if scenario."
                    if removed
                    else hypothetical_row.get("coverage_reason") or current_row.get("coverage_reason")
                ),
            }
        )
    preview_rows.sort(
        key=lambda row: (
            -abs(float(row.get("market_value_delta") or 0.0)),
            str(row.get("ticker") or row.get("ric") or ""),
        ),
    )
    return preview_rows


def load_cpar_portfolio_whatif_payload(
    *,
    account_id: str,
    mode: str,
    scenario_rows: list[dict[str, Any]],
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
    data_db=None,
) -> dict[str, object]:
    package, account, live_positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_account_context(
        account_id=account_id,
        allowed_account_ids=allowed_account_ids,
        data_db=data_db,
    )
    normalized_rows = _normalize_scenario_rows(
        account_id=str(account.get("account_id") or account_id),
        live_positions=live_positions,
        scenario_rows=scenario_rows,
    )
    hypothetical_positions = _apply_scenario_rows(
        account_id=str(account.get("account_id") or account_id),
        live_positions=live_positions,
        scenario_rows=normalized_rows,
    )
    rics = sorted(
        {
            *(_normalize_ric(str(row.get("ric") or "")) for row in live_positions),
            *(_normalize_ric(str(row.get("ric") or "")) for row in normalized_rows),
        }
    )
    rics = [ric for ric in rics if ric]
    fit_by_ric, price_by_ric, classification_by_ric, covariance_rows = cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows(
        rics=rics,
        package_run_id=str(package["package_run_id"]),
        package_date=str(package["package_date"]),
        positions=[*live_positions, *normalized_rows],
        data_db=data_db,
    )
    for row in normalized_rows:
        if float(row["current_quantity"]) == 0.0 and str(row["ric"]) not in fit_by_ric:
            raise ValueError(
                f"RIC {row['ric']} is not present in the active cPAR package. "
                "Stage additions from active-package cPAR search results only."
            )
    current_payload = cpar_portfolio_snapshot_service.build_cpar_portfolio_hedge_snapshot(
        package=package,
        account=account,
        positions=live_positions,
        mode=mode,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covariance_rows=covariance_rows,
    )
    hypothetical_payload = cpar_portfolio_snapshot_service.build_cpar_portfolio_hedge_snapshot(
        package=package,
        account=account,
        positions=hypothetical_positions,
        mode=mode,
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric=classification_by_ric,
        covariance_rows=covariance_rows,
    )
    return {
        **cpar_meta_service.package_meta_payload(package),
        "account_id": str(account.get("account_id") or account_id),
        "account_name": str(account.get("account_name") or account.get("account_id") or account_id),
        "mode": str(mode),
        "scenario_row_count": int(len(normalized_rows)),
        "changed_positions_count": int(len(normalized_rows)),
        "scenario_rows": _build_scenario_preview_rows(
            scenario_rows=normalized_rows,
            current_payload=current_payload,
            hypothetical_payload=hypothetical_payload,
        ),
        "current": current_payload,
        "hypothetical": hypothetical_payload,
        "_preview_only": True,
    }
