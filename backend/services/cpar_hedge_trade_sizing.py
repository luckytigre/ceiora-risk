"""Package-pinned cPAR hedge trade pricing and sizing helpers."""

from __future__ import annotations

from typing import Any

from backend.cpar.factor_registry import build_cpar1_factor_registry
from backend.data import cpar_source_reads
from backend.services import cpar_meta_service


def _factor_spec_index() -> dict[str, dict[str, object]]:
    return {
        str(spec.factor_id): {
            "factor_id": str(spec.factor_id),
            "label": spec.label,
            "group": spec.group,
            "display_order": int(spec.display_order),
        }
        for spec in build_cpar1_factor_registry()
    }


def _select_price(row: dict[str, Any] | None) -> tuple[float | None, str | None, str | None, str | None]:
    if not row:
        return None, None, None, None
    if row.get("adj_close") is not None:
        return float(row["adj_close"]), "adj_close", str(row.get("date") or "") or None, str(row.get("currency") or "") or None
    if row.get("close") is not None:
        return float(row["close"]), "close", str(row.get("date") or "") or None, str(row.get("currency") or "") or None
    return None, None, str(row.get("date") or "") or None, str(row.get("currency") or "") or None


def load_factor_proxy_price_context(
    factor_ids: list[str],
    *,
    package_date: str,
    data_db=None,
) -> dict[str, dict[str, object]]:
    clean_factor_ids = sorted({str(factor_id or "").strip().upper() for factor_id in factor_ids if str(factor_id or "").strip()})
    if not clean_factor_ids:
        return {}
    try:
        proxy_rows = cpar_source_reads.resolve_factor_proxy_rows(clean_factor_ids, data_db=data_db)
        price_rows = cpar_source_reads.load_latest_price_rows(
            [str(row.get("ric") or "") for row in proxy_rows],
            as_of_date=str(package_date),
            data_db=data_db,
        )
    except cpar_source_reads.CparSourceReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(f"Shared-source hedge pricing read failed: {exc}") from exc

    spec_index = _factor_spec_index()
    proxy_by_ticker = {str(row.get("ticker") or "").strip().upper(): row for row in proxy_rows}
    price_by_ric = {str(row.get("ric") or "").strip().upper(): row for row in price_rows}
    out: dict[str, dict[str, object]] = {}
    missing: list[str] = []
    for factor_id in clean_factor_ids:
        proxy_row = proxy_by_ticker.get(factor_id)
        if proxy_row is None:
            missing.append(factor_id)
            continue
        proxy_ric = str(proxy_row.get("ric") or "").strip().upper()
        price_row = price_by_ric.get(proxy_ric)
        price, price_field_used, price_date, currency = _select_price(price_row)
        if price is None:
            missing.append(factor_id)
            continue
        spec = spec_index.get(factor_id, {})
        out[factor_id] = {
            "factor_id": factor_id,
            "label": spec.get("label"),
            "group": spec.get("group"),
            "display_order": spec.get("display_order"),
            "proxy_ric": proxy_ric,
            "proxy_ticker": str(proxy_row.get("ticker") or factor_id),
            "price": float(price),
            "price_field_used": price_field_used,
            "price_date": price_date,
            "currency": currency,
        }
    if missing:
        rendered = ", ".join(sorted(missing))
        raise cpar_meta_service.CparReadUnavailable(
            f"Missing package-date hedge ETF price coverage for: {rendered}"
        )
    return out


def sized_trade_rows_from_hedge_weights(
    hedge_weights: dict[str, float],
    *,
    base_notional: float,
    factor_proxy_price_context: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for factor_id, trade_weight in hedge_weights.items():
        context = factor_proxy_price_context.get(str(factor_id))
        if context is None:
            continue
        price = float(context["price"])
        dollar_notional = float(trade_weight) * float(base_notional)
        quantity = dollar_notional / price if abs(price) > 1e-12 else None
        rows.append(
            {
                "factor_id": str(factor_id),
                "label": context.get("label"),
                "group": context.get("group"),
                "display_order": context.get("display_order"),
                "proxy_ric": context.get("proxy_ric"),
                "proxy_ticker": context.get("proxy_ticker"),
                "price": price,
                "price_field_used": context.get("price_field_used"),
                "price_date": context.get("price_date"),
                "currency": context.get("currency"),
                "trade_weight": float(trade_weight),
                "dollar_notional": float(dollar_notional),
                "quantity": None if quantity is None else float(quantity),
            }
        )
    rows.sort(
        key=lambda row: (
            row.get("factor_id") != "SPY",
            -abs(float(row.get("dollar_notional") or 0.0)),
            str(row.get("factor_id") or ""),
        )
    )
    return rows
