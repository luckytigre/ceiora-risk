"""cUSE4 read-only portfolio what-if preview using current model state.

This module is tied to the current cUSE4 serving surfaces, covariance, and
specific-risk semantics. It is not the future owner of any cPAR what-if flow.
Prefer importing `backend.services.cuse4_portfolio_whatif` from cUSE4 routes.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
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
from backend.data import serving_outputs as serving_outputs_store
from backend.data.serving_outputs import load_current_payload
from backend.data.sqlite import cache_get, cache_get_live_first
from backend.risk_model.risk_attribution import risk_decomposition
from backend.services import holdings_service


@dataclass(frozen=True)
class PortfolioWhatIfDependencies:
    current_payload_loader: Callable[[str], Any]
    runtime_cache_loader: Callable[[str], Any]
    live_cache_loader: Callable[[str], Any]
    holdings_loader: Callable[[str | None], list[dict[str, Any]]]
    universe_loader: Callable[..., dict[str, Any]]
    covariance_loader: Callable[..., tuple[pd.DataFrame, bool]]
    specific_risk_loader: Callable[..., tuple[dict[str, dict[str, Any]], bool]]


def get_portfolio_whatif_dependencies() -> PortfolioWhatIfDependencies:
    return PortfolioWhatIfDependencies(
        current_payload_loader=load_current_payload,
        runtime_cache_loader=cache_get,
        live_cache_loader=cache_get_live_first,
        holdings_loader=holdings_service.load_holdings_positions,
        universe_loader=_load_universe_loadings_from_current_payload,
        covariance_loader=_load_covariance_frame_from_current_payload,
        specific_risk_loader=_load_specific_risk_by_ticker_from_current_payload,
    )


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
    current_payload_loader: Callable[[str], Any] = load_current_payload,
    fallback_loader,
):
    payload = current_payload_loader(payload_name)
    if payload is not None:
        return payload
    if not config.serving_outputs_cache_fallback_enabled():
        return None
    return fallback_loader(payload_name)


def _load_current_serving_payloads(
    *payload_names: str,
    current_payload_loader: Callable[[str], Any] = load_current_payload,
) -> dict[str, Any | None]:
    if current_payload_loader is serving_outputs_store.load_current_payload:
        return serving_outputs_store.load_current_payloads(payload_names)
    return {
        payload_name: current_payload_loader(payload_name)
        for payload_name in payload_names
    }


def _load_universe_loadings(
    *,
    current_payload: Any | None = None,
    current_payload_loader: Callable[[str], Any] = load_current_payload,
    fallback_loader: Callable[[str], Any] = cache_get,
) -> dict[str, Any]:
    data = current_payload
    if data is None:
        data = _load_serving_or_runtime_cache(
            "universe_loadings",
            current_payload_loader=current_payload_loader,
            fallback_loader=fallback_loader,
        )
    if not isinstance(data, dict) or not isinstance(data.get("by_ticker"), dict):
        raise RuntimeError("Universe loadings are not ready. Run refresh before using what-if preview.")
    return data


def _load_covariance_frame() -> pd.DataFrame:
    cov, _ = _load_covariance_frame_with_source()
    return cov


def _load_covariance_frame_with_source(
    *,
    current_payload: Any | None = None,
    current_payload_loader: Callable[[str], Any] = load_current_payload,
    live_cache_loader: Callable[[str], Any] = cache_get_live_first,
) -> tuple[pd.DataFrame, bool]:
    serving_payload = (
        current_payload if current_payload is not None else current_payload_loader("risk_engine_cov")
    )
    payload = serving_payload
    if payload is None and config.serving_outputs_cache_fallback_enabled():
        payload = live_cache_loader("risk_engine_cov")
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


def _load_specific_risk_by_ticker_with_source(
    *,
    current_payload: Any | None = None,
    current_payload_loader: Callable[[str], Any] = load_current_payload,
    live_cache_loader: Callable[[str], Any] = cache_get_live_first,
) -> tuple[dict[str, dict[str, Any]], bool]:
    serving_payload = (
        current_payload
        if current_payload is not None
        else current_payload_loader("risk_engine_specific_risk")
    )
    payload = serving_payload
    if payload is None and config.serving_outputs_cache_fallback_enabled():
        payload = live_cache_loader("risk_engine_specific_risk")
    payload = payload or {}
    if not isinstance(payload, dict):
        payload = {}
    return specific_risk_by_ticker_view(payload), isinstance(serving_payload, dict)


def _load_universe_loadings_from_current_payload(
    current_payload: Any | None,
    *,
    current_payload_loader: Callable[[str], Any] = load_current_payload,
    fallback_loader: Callable[[str], Any] = cache_get,
) -> dict[str, Any]:
    if current_payload is None:
        return _load_universe_loadings(
            current_payload_loader=current_payload_loader,
            fallback_loader=fallback_loader,
        )
    if not isinstance(current_payload, dict) or not isinstance(current_payload.get("by_ticker"), dict):
        raise RuntimeError("Universe loadings are not ready. Run refresh before using what-if preview.")
    return current_payload


def _load_covariance_frame_from_current_payload(
    current_payload: Any | None,
    *,
    current_payload_loader: Callable[[str], Any] = load_current_payload,
    live_cache_loader: Callable[[str], Any] = cache_get_live_first,
) -> tuple[pd.DataFrame, bool]:
    if current_payload is None:
        return _load_covariance_frame_with_source(
            current_payload_loader=current_payload_loader,
            live_cache_loader=live_cache_loader,
        )
    factors = [str(x) for x in (current_payload.get("factors") or [])]
    matrix = current_payload.get("matrix") or []
    if not factors or not matrix:
        raise RuntimeError("Risk engine covariance is empty.")
    return pd.DataFrame(matrix, index=factors, columns=factors, dtype=float), True


def _load_specific_risk_by_ticker_from_current_payload(
    current_payload: Any | None,
    *,
    current_payload_loader: Callable[[str], Any] = load_current_payload,
    live_cache_loader: Callable[[str], Any] = cache_get_live_first,
) -> tuple[dict[str, dict[str, Any]], bool]:
    if current_payload is None:
        return _load_specific_risk_by_ticker_with_source(
            current_payload_loader=current_payload_loader,
            live_cache_loader=live_cache_loader,
        )
    payload = current_payload or {}
    if not isinstance(payload, dict):
        payload = {}
    return specific_risk_by_ticker_view(payload), True


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
    *,
    holdings_loader: Callable[[str | None], list[dict[str, Any]]] = holdings_service.load_holdings_positions,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    live_rows = holdings_loader(account_id=None)
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
    served_loadings_asof: str | None,
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
        factor_coverage_asof=served_loadings_asof,
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
    dependencies: PortfolioWhatIfDependencies | None = None,
) -> dict[str, Any]:
    deps = dependencies or get_portfolio_whatif_dependencies()
    normalized_scenario_rows = _normalize_scenario_rows(scenario_rows)
    current_payloads = _load_current_serving_payloads(
        "portfolio",
        "universe_loadings",
        "risk_engine_cov",
        "risk_engine_specific_risk",
        current_payload_loader=deps.current_payload_loader,
    )
    universe_loadings = deps.universe_loader(
        current_payloads.get("universe_loadings"),
        current_payload_loader=deps.current_payload_loader,
        fallback_loader=deps.runtime_cache_loader,
    )
    cov, cov_from_serving = deps.covariance_loader(
        current_payloads.get("risk_engine_cov"),
        current_payload_loader=deps.current_payload_loader,
        live_cache_loader=deps.live_cache_loader,
    )
    specific_risk, specific_risk_from_serving = deps.specific_risk_loader(
        current_payloads.get("risk_engine_specific_risk"),
        current_payload_loader=deps.current_payload_loader,
        live_cache_loader=deps.live_cache_loader,
    )
    current_portfolio_payload = current_payloads.get("portfolio")
    if current_portfolio_payload is None:
        current_portfolio_payload = _load_serving_or_runtime_cache(
            "portfolio",
            current_payload_loader=deps.current_payload_loader,
            fallback_loader=deps.runtime_cache_loader,
        ) or {}
    source_dates = (
        (current_portfolio_payload or {}).get("source_dates")
        or universe_loadings.get("source_dates")
        or {}
    )
    served_loadings_asof = str(
        source_dates.get("exposures_served_asof")
        or source_dates.get("exposures_latest_available_asof")
        or source_dates.get("exposures_asof")
        or ""
    ).strip() or None
    factor_coverage: dict[str, Any] = {}
    current_rows, hypothetical_rows, deltas = _build_holdings_snapshot(
        normalized_scenario_rows,
        holdings_loader=deps.holdings_loader,
    )
    current_preview = _build_preview_payload(
        holdings_rows=current_rows,
        sleeve_label="LIVE HOLDINGS",
        default_source="NEON_HOLDINGS",
        universe_loadings=universe_loadings,
        cov=cov,
        specific_risk_by_ticker=specific_risk,
        served_loadings_asof=served_loadings_asof,
        factor_coverage=factor_coverage,
    )
    hypothetical_preview = _build_preview_payload(
        holdings_rows=hypothetical_rows,
        sleeve_label="WHAT IF",
        default_source="WHAT_IF",
        universe_loadings=universe_loadings,
        cov=cov,
        specific_risk_by_ticker=specific_risk,
        served_loadings_asof=served_loadings_asof,
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


__all__ = [
    "PortfolioWhatIfDependencies",
    "get_portfolio_whatif_dependencies",
    "preview_portfolio_whatif",
]
