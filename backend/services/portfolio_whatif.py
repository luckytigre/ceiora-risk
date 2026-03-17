"""Read-only portfolio what-if preview using current model state."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import math

import pandas as pd

from backend import config
from backend.analytics.contracts import (
    ComponentSharesPayload,
    ExposureModesPayload,
    FactorDetailPayload,
    RiskSharesPayload,
)
from backend.analytics.services.risk_views import (
    build_positions_from_snapshot,
    compute_exposures_modes,
    compute_position_risk_mix,
    compute_position_total_risk_contributions,
    specific_risk_by_ticker_view,
)
from backend.data.serving_outputs import load_current_payload
from backend.data.sqlite import cache_get, cache_get_live
from backend.risk_model.risk_attribution import risk_decomposition
from backend.services import holdings_service


def _normalize_account_id(raw: str | None) -> str:
    return str(raw or "").strip().lower()


def _normalize_ticker(raw: str | None) -> str:
    return str(raw or "").strip().upper()


def _normalize_ric(raw: str | None) -> str:
    return str(raw or "").strip().upper()


def _scenario_key(account_id: str, ric: str | None, ticker: str | None) -> str:
    ident = _normalize_ticker(ticker) or _normalize_ric(ric)
    return f"{_normalize_account_id(account_id)}::{ident}"


def _load_serving_or_runtime_cache(
    payload_name: str,
    *,
    fallback_loader,
):
    payload = load_current_payload(payload_name)
    if payload is not None:
        return payload
    if not config.serving_outputs_cache_fallback_enabled():
        return None
    return fallback_loader(payload_name)


def _load_universe_loadings() -> dict[str, Any]:
    data = _load_serving_or_runtime_cache("universe_loadings", fallback_loader=cache_get)
    if not isinstance(data, dict) or not isinstance(data.get("by_ticker"), dict):
        raise RuntimeError("Universe loadings are not ready. Run refresh before using what-if preview.")
    return data


def _load_covariance_frame() -> pd.DataFrame:
    cov, _ = _load_covariance_frame_with_source()
    return cov


def _load_covariance_frame_with_source() -> tuple[pd.DataFrame, bool]:
    serving_payload = load_current_payload("risk_engine_cov")
    payload = serving_payload
    if payload is None and config.serving_outputs_cache_fallback_enabled():
        payload = cache_get_live("risk_engine_cov") or cache_get("risk_engine_cov")
    if not isinstance(payload, dict):
        raise RuntimeError("Risk engine covariance is not ready. Run refresh before using what-if preview.")
    factors = [str(x) for x in (payload.get("factors") or [])]
    matrix = payload.get("matrix") or []
    if not factors or not matrix:
        raise RuntimeError("Risk engine covariance is empty.")
    return pd.DataFrame(matrix, index=factors, columns=factors, dtype=float), isinstance(serving_payload, dict)


def _load_specific_risk_by_ticker() -> dict[str, dict[str, Any]]:
    specific_risk, _ = _load_specific_risk_by_ticker_with_source()
    return specific_risk


def _load_specific_risk_by_ticker_with_source() -> tuple[dict[str, dict[str, Any]], bool]:
    serving_payload = load_current_payload("risk_engine_specific_risk")
    payload = serving_payload
    if payload is None and config.serving_outputs_cache_fallback_enabled():
        payload = cache_get_live("risk_engine_specific_risk") or cache_get("risk_engine_specific_risk")
    payload = payload or {}
    if not isinstance(payload, dict):
        payload = {}
    return specific_risk_by_ticker_view(payload), isinstance(serving_payload, dict)


def _normalize_scenario_rows(scenario_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []
    seen_scenario_keys: set[str] = set()
    for raw in list(scenario_rows or []):
        account_id = _normalize_account_id(raw.get("account_id"))
        ticker = _normalize_ticker(raw.get("ticker"))
        ric = _normalize_ric(raw.get("ric"))
        delta_quantity = float(raw.get("quantity") or 0.0)
        if not account_id:
            raise ValueError("Each what-if row requires account_id.")
        if not ticker:
            raise ValueError("Each what-if row requires ticker.")
        if not math.isfinite(delta_quantity):
            raise ValueError("Each what-if row requires a finite quantity.")
        key = _scenario_key(account_id, ric, ticker)
        if key in seen_scenario_keys:
            raise ValueError(f"Duplicate what-if row for account {account_id} ticker {ticker}.")
        seen_scenario_keys.add(key)
        normalized_rows.append(
            {
                "account_id": account_id,
                "ticker": ticker,
                "ric": ric,
                "quantity": delta_quantity,
                "source": str(raw.get("source") or "what_if"),
            }
        )
    return normalized_rows


def _build_holdings_snapshot(
    scenario_rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    live_rows = holdings_service.load_holdings_positions(account_id=None)
    current_by_key: dict[str, dict[str, Any]] = {}
    for raw in live_rows:
        account_id = _normalize_account_id(raw.get("account_id"))
        ticker = _normalize_ticker(raw.get("ticker"))
        ric = _normalize_ric(raw.get("ric"))
        if not account_id or not (ticker or ric):
            continue
        key = _scenario_key(account_id, ric, ticker)
        quantity = float(raw.get("quantity") or 0.0)
        existing = current_by_key.get(key)
        if existing is None:
            current_by_key[key] = {
                "account_id": account_id,
                "ticker": ticker,
                "ric": ric,
                "quantity": quantity,
                "source": str(raw.get("source") or "neon_holdings"),
            }
            continue
        existing["quantity"] = float(existing.get("quantity") or 0.0) + quantity
        if not existing.get("ric"):
            existing["ric"] = ric

    hypothetical_by_key = {key: dict(value) for key, value in current_by_key.items()}
    for raw in scenario_rows:
        account_id = _normalize_account_id(raw.get("account_id"))
        ticker = _normalize_ticker(raw.get("ticker"))
        ric = _normalize_ric(raw.get("ric"))
        delta_quantity = float(raw.get("quantity") or 0.0)
        key = _scenario_key(account_id, ric, ticker)
        current_quantity = float((current_by_key.get(key) or {}).get("quantity") or 0.0)
        hypothetical_quantity = current_quantity + delta_quantity
        normalized = {
            "account_id": account_id,
            "ticker": ticker,
            "ric": ric,
            "quantity": hypothetical_quantity,
            "source": str(raw.get("source") or "what_if"),
        }
        if abs(hypothetical_quantity) <= 1e-12:
            hypothetical_by_key.pop(key, None)
        else:
            hypothetical_by_key[key] = dict(normalized)

    deltas: list[dict[str, Any]] = []
    all_keys = sorted(set(current_by_key) | set(hypothetical_by_key))
    for key in all_keys:
        current = current_by_key.get(key)
        hypothetical = hypothetical_by_key.get(key)
        current_qty = float((current or {}).get("quantity") or 0.0)
        hypothetical_qty = float((hypothetical or {}).get("quantity") or 0.0)
        if abs(current_qty - hypothetical_qty) <= 1e-12:
            continue
        base = hypothetical or current or {}
        deltas.append({
            "account_id": str(base.get("account_id") or ""),
            "ticker": str(base.get("ticker") or ""),
            "ric": str(base.get("ric") or ""),
            "current_quantity": current_qty,
            "hypothetical_quantity": hypothetical_qty,
            "delta_quantity": hypothetical_qty - current_qty,
        })

    return (
        list(current_by_key.values()),
        list(hypothetical_by_key.values()),
        deltas,
    )


def _rows_to_snapshot(
    rows: list[dict[str, Any]],
    *,
    sleeve_label: str,
    default_source: str,
) -> tuple[dict[str, float], dict[str, dict[str, str]]]:
    qty_by_ticker: dict[str, float] = defaultdict(float)
    accounts_by_ticker: dict[str, set[str]] = defaultdict(set)
    source_by_ticker: dict[str, str] = {}
    for row in rows:
        ticker = _normalize_ticker(row.get("ticker"))
        if not ticker:
            continue
        qty = float(row.get("quantity") or 0.0)
        if abs(qty) <= 0.0:
            continue
        qty_by_ticker[ticker] += qty
        account_id = _normalize_account_id(row.get("account_id"))
        if account_id:
            accounts_by_ticker[ticker].add(account_id)
        source_txt = str(row.get("source") or "").strip().upper()
        if source_txt:
            source_by_ticker[ticker] = source_txt

    shares = {ticker: float(qty) for ticker, qty in qty_by_ticker.items() if abs(float(qty)) > 0.0}
    meta: dict[str, dict[str, str]] = {}
    for ticker in shares:
        accounts = sorted(accounts_by_ticker.get(ticker) or [])
        account = accounts[0] if len(accounts) == 1 else ("MULTI" if len(accounts) > 1 else "MAIN")
        meta[ticker] = {
            "account": str(account).upper(),
            "sleeve": sleeve_label if len(accounts) > 0 else "NEON HOLDINGS",
            "source": source_by_ticker.get(ticker) or default_source,
        }
    return shares, meta


def _build_preview_payload(
    *,
    holdings_rows: list[dict[str, Any]],
    sleeve_label: str,
    default_source: str,
    universe_loadings: dict[str, Any],
    cov: pd.DataFrame,
    specific_risk_by_ticker: dict[str, dict[str, Any]],
    coverage_date: str | None,
    factor_coverage: dict[str, Any],
) -> dict[str, Any]:
    shares_map, dynamic_meta = _rows_to_snapshot(
        holdings_rows,
        sleeve_label=sleeve_label,
        default_source=default_source,
    )
    positions, total_value = build_positions_from_snapshot(
        universe_loadings["by_ticker"],
        shares_map,
        dynamic_meta=dynamic_meta,
    )
    raw_risk_shares, raw_component_shares, raw_factor_details = risk_decomposition(
        cov=cov,
        positions=positions,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )
    risk_shares: RiskSharesPayload = {
        "market": float(raw_risk_shares.get("market", 0.0)),
        "industry": float(raw_risk_shares.get("industry", 0.0)),
        "style": float(raw_risk_shares.get("style", 0.0)),
        "idio": float(raw_risk_shares.get("idio", 0.0)),
    }
    component_shares: ComponentSharesPayload = {
        "market": float(raw_component_shares.get("market", 0.0)),
        "industry": float(raw_component_shares.get("industry", 0.0)),
        "style": float(raw_component_shares.get("style", 0.0)),
    }
    factor_details: list[FactorDetailPayload] = [dict(row) for row in raw_factor_details]
    position_risk_mix = compute_position_risk_mix(
        positions=positions,
        cov=cov,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )
    position_risk_contrib = compute_position_total_risk_contributions(
        positions=positions,
        cov=cov,
        specific_risk_by_ticker=specific_risk_by_ticker,
    )
    for pos in positions:
        ticker = str(pos.get("ticker", "")).upper()
        pos["risk_contrib_pct"] = float(position_risk_contrib.get(ticker, 0.0))
        pos["risk_mix"] = dict(position_risk_mix.get(ticker, {
            "market": 0.0,
            "industry": 0.0,
            "style": 0.0,
            "idio": 0.0,
        }))
    exposure_modes: ExposureModesPayload = compute_exposures_modes(
        positions,
        cov,
        factor_details,
        factor_coverage=factor_coverage,
        coverage_date=coverage_date,
    )
    return {
        "positions": [dict(pos) for pos in positions],
        "total_value": round(float(total_value), 2),
        "position_count": len(positions),
        "risk_shares": risk_shares,
        "component_shares": component_shares,
        "factor_details": factor_details,
        "exposure_modes": exposure_modes,
        "factor_catalog": list(universe_loadings.get("factor_catalog") or []),
    }


def _factor_delta_rows(
    current_modes: dict[str, Any],
    hypothetical_modes: dict[str, Any],
    *,
    mode: str,
) -> list[dict[str, Any]]:
    current_map = {
        str(row.get("factor_id")): float(row.get("value") or 0.0)
        for row in (current_modes.get(mode) or [])
    }
    hypothetical_map = {
        str(row.get("factor_id")): float(row.get("value") or 0.0)
        for row in (hypothetical_modes.get(mode) or [])
    }
    rows: list[dict[str, Any]] = []
    for factor in sorted(set(current_map) | set(hypothetical_map)):
        current_value = float(current_map.get(factor, 0.0))
        hypothetical_value = float(hypothetical_map.get(factor, 0.0))
        delta_value = hypothetical_value - current_value
        rows.append({
            "factor_id": factor,
            "current": round(current_value, 6),
            "hypothetical": round(hypothetical_value, 6),
            "delta": round(delta_value, 6),
        })
    rows.sort(key=lambda row: abs(float(row["delta"])), reverse=True)
    return rows


def preview_portfolio_whatif(
    *,
    scenario_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    normalized_scenario_rows = _normalize_scenario_rows(scenario_rows)
    universe_loadings = _load_universe_loadings()
    cov, cov_from_serving = _load_covariance_frame_with_source()
    specific_risk, specific_risk_from_serving = _load_specific_risk_by_ticker_with_source()
    current_portfolio_payload = _load_serving_or_runtime_cache("portfolio", fallback_loader=cache_get) or {}
    source_dates = (
        (current_portfolio_payload or {}).get("source_dates")
        or universe_loadings.get("source_dates")
        or {}
    )
    coverage_date = str(source_dates.get("exposures_served_asof") or source_dates.get("exposures_asof") or "").strip() or None
    factor_coverage: dict[str, Any] = {}
    current_rows, hypothetical_rows, deltas = _build_holdings_snapshot(normalized_scenario_rows)
    current_preview = _build_preview_payload(
        holdings_rows=current_rows,
        sleeve_label="LIVE HOLDINGS",
        default_source="NEON_HOLDINGS",
        universe_loadings=universe_loadings,
        cov=cov,
        specific_risk_by_ticker=specific_risk,
        coverage_date=coverage_date,
        factor_coverage=factor_coverage,
    )
    hypothetical_preview = _build_preview_payload(
        holdings_rows=hypothetical_rows,
        sleeve_label="WHAT IF",
        default_source="WHAT_IF",
        universe_loadings=universe_loadings,
        cov=cov,
        specific_risk_by_ticker=specific_risk,
        coverage_date=coverage_date,
        factor_coverage=factor_coverage,
    )

    risk_share_deltas = {
        bucket: round(
            float(hypothetical_preview["risk_shares"].get(bucket, 0.0))
            - float(current_preview["risk_shares"].get(bucket, 0.0)),
            2,
        )
        for bucket in ("market", "industry", "style", "idio")
    }
    factor_deltas = {
        mode: _factor_delta_rows(
            current_preview["exposure_modes"],
            hypothetical_preview["exposure_modes"],
            mode=mode,
        )[:20]
        for mode in ("raw", "sensitivity", "risk_contribution")
    }
    serving_snapshot = {
        "run_id": (
            (current_portfolio_payload or {}).get("run_id")
            or universe_loadings.get("run_id")
        ),
        "snapshot_id": (
            (current_portfolio_payload or {}).get("snapshot_id")
            or universe_loadings.get("snapshot_id")
        ),
        "refresh_started_at": (
            (current_portfolio_payload or {}).get("refresh_started_at")
            or universe_loadings.get("refresh_started_at")
        ),
    }
    truth_surface = (
        "live_holdings_projected_through_current_served_model"
        if cov_from_serving and specific_risk_from_serving
        else "live_holdings_projected_through_current_loadings_and_live_risk_cache"
    )
    return {
        "scenario_rows": [
            {
                "account_id": _normalize_account_id(row.get("account_id")),
                "ticker": _normalize_ticker(row.get("ticker")),
                "ric": _normalize_ric(row.get("ric")),
                "quantity": float(row.get("quantity") or 0.0),
                "source": str(row.get("source") or "what_if"),
            }
            for row in normalized_scenario_rows
        ],
        "holding_deltas": deltas,
        "current": current_preview,
        "hypothetical": hypothetical_preview,
        "diff": {
            "total_value": round(
                float(hypothetical_preview["total_value"]) - float(current_preview["total_value"]),
                2,
            ),
            "position_count": int(hypothetical_preview["position_count"]) - int(current_preview["position_count"]),
            "risk_shares": risk_share_deltas,
            "factor_deltas": factor_deltas,
        },
        "source_dates": source_dates,
        "serving_snapshot": serving_snapshot,
        "truth_surface": truth_surface,
        "_preview_only": True,
    }
