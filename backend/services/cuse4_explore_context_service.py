"""Explicit cUSE4 owner for Explore held-position context payloads."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from backend.analytics.services.risk_views import build_positions_from_snapshot
from backend.services import cuse4_dashboard_payload_service, cuse4_holdings_service, cuse4_portfolio_whatif


def _normalize_account_id(raw: str | None) -> str:
    return str(raw or "").strip().lower()


def _normalize_ticker(raw: str | None) -> str:
    return str(raw or "").strip().upper()


def _rows_to_snapshot(
    holdings_rows: list[dict[str, Any]],
) -> tuple[dict[str, float], dict[str, dict[str, str]]]:
    quantity_by_ticker: dict[str, float] = defaultdict(float)
    accounts_by_ticker: dict[str, set[str]] = defaultdict(set)
    source_by_ticker: dict[str, str] = {}
    for row in holdings_rows:
        ticker = _normalize_ticker(row.get("ticker"))
        if not ticker:
            continue
        quantity = float(row.get("quantity") or 0.0)
        if abs(quantity) <= 0.0:
            continue
        quantity_by_ticker[ticker] += quantity
        account_id = _normalize_account_id(row.get("account_id"))
        if account_id:
            accounts_by_ticker[ticker].add(account_id)
        source = str(row.get("source") or "").strip().upper()
        if source:
            source_by_ticker[ticker] = source

    shares_map = {
        ticker: float(quantity)
        for ticker, quantity in quantity_by_ticker.items()
        if abs(float(quantity)) > 0.0
    }
    dynamic_meta: dict[str, dict[str, str]] = {}
    for ticker in shares_map:
        accounts = sorted(accounts_by_ticker.get(ticker) or [])
        account_label = accounts[0] if len(accounts) == 1 else ("MULTI" if len(accounts) > 1 else "MAIN")
        dynamic_meta[ticker] = {
            "account": str(account_label).upper(),
            "sleeve": "LIVE HOLDINGS",
            "source": source_by_ticker.get(ticker) or "NEON_HOLDINGS",
        }
    return shares_map, dynamic_meta


def _held_positions_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "ticker": str(row.get("ticker") or "").upper(),
            "shares": float(row.get("shares") or 0.0),
            "weight": float(row.get("weight") or 0.0),
            "market_value": float(row.get("market_value") or 0.0),
            "long_short": str(row.get("long_short") or "LONG"),
            "price": float(row.get("price") or 0.0),
        }
        for row in rows
        if str(row.get("ticker") or "").strip()
    ]


def load_cuse_explore_context_payload(
    *,
    account_id: str | None = None,
    allowed_account_ids: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    normalized_account_id = _normalize_account_id(account_id) or None
    if normalized_account_id is None:
        portfolio_payload = cuse4_dashboard_payload_service.load_portfolio_response()
        return {
            "held_positions": _held_positions_payload(list(portfolio_payload.get("positions") or [])),
            "source_dates": portfolio_payload.get("source_dates") or {},
            "run_id": portfolio_payload.get("run_id"),
            "snapshot_id": portfolio_payload.get("snapshot_id"),
            "refresh_started_at": portfolio_payload.get("refresh_started_at"),
            "_cached": bool(portfolio_payload.get("_cached")),
            "_account_scoped": False,
            "account_id": None,
        }

    deps = cuse4_portfolio_whatif.get_portfolio_whatif_dependencies()
    try:
        universe_loadings = deps.universe_loader(
            None,
            current_payload_loader=deps.current_payload_loader,
            fallback_loader=deps.runtime_cache_loader,
        )
    except RuntimeError as exc:
        raise cuse4_dashboard_payload_service.DashboardPayloadNotReady(
            cache_key="portfolio",
            message=str(exc),
        ) from exc

    holdings_rows = cuse4_holdings_service.load_holdings_positions(
        normalized_account_id,
        allowed_account_ids=allowed_account_ids,
    )
    shares_map, dynamic_meta = _rows_to_snapshot(holdings_rows)
    positions, _ = build_positions_from_snapshot(
        universe_loadings["by_ticker"],
        shares_map,
        dynamic_meta=dynamic_meta,
    )

    current_portfolio_payload = deps.current_payload_loader("portfolio") or {}
    source_dates = (
        dict((current_portfolio_payload or {}).get("source_dates") or {})
        or dict(universe_loadings.get("source_dates") or {})
    )
    return {
        "held_positions": _held_positions_payload(positions),
        "source_dates": source_dates,
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
        "_cached": False,
        "_account_scoped": True,
        "account_id": normalized_account_id,
    }


__all__ = ["load_cuse_explore_context_payload"]
