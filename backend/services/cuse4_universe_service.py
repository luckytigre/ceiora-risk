"""Concrete cUSE4 owner for default universe/search/detail route semantics."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from backend import config
from backend.data.history_queries import load_price_history_rows
from backend.data import registry_quote_reads
from backend.data.serving_outputs import load_runtime_payload, load_runtime_payload_field
from backend.data.sqlite import cache_get


DATA_DB = Path(config.DATA_DB_PATH)


@dataclass(frozen=True)
class UniversePayloadNotReady(RuntimeError):
    cache_key: str
    message: str
    refresh_profile: str = "serve-refresh"


PayloadLoader = Callable[..., Any]
HistoryLoader = Callable[..., tuple[Any, list[tuple[Any, Any]]]]
RowNormalizer = Callable[[dict[str, Any]], dict[str, Any]]


def _bool_flag(row: dict[str, Any], key: str) -> bool:
    return bool(int(row.get(key) or 0) == 1)


def _normalize_registry_ticker_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "ticker": str(row.get("ticker") or row.get("ric") or "").upper().strip(),
        "ric": str(row.get("ric") or "").upper().strip() or None,
        "name": (
            str(row.get("common_name") or "").strip()
            or str(row.get("ticker") or row.get("ric") or "").upper().strip()
        ),
        "trbc_economic_sector_short": str(row.get("trbc_economic_sector") or "").strip(),
        "trbc_industry_group": str(row.get("trbc_industry_group") or "").strip() or None,
        "market_cap": None,
        "price": (
            float(row["price"])
            if row.get("price") is not None
            else None
        ),
        "exposures": {},
        "sensitivities": {},
        "risk_loading": None,
        "specific_var": None,
        "specific_vol": None,
        "model_status": (
            "core_estimated"
            if _bool_flag(row, "allow_cuse_native_core")
            else ("projected_only" if (
                _bool_flag(row, "allow_cuse_fundamental_projection")
                or _bool_flag(row, "allow_cuse_returns_projection")
            ) else "ineligible")
        ),
        "model_status_reason": "registry_runtime_only",
        "eligibility_reason": "registry_runtime_only",
        "model_warning": "No live cUSE factor payload is published for this name yet; showing registry/runtime coverage only.",
        "as_of_date": (
            str(row.get("price_date") or row.get("classification_as_of_date") or row.get("observation_as_of_date") or "").strip()
            or None
        ),
        "exposure_origin": (
            "native"
            if _bool_flag(row, "allow_cuse_native_core")
            else ("projected_fundamental" if _bool_flag(row, "allow_cuse_fundamental_projection")
                  else ("projected_returns" if _bool_flag(row, "allow_cuse_returns_projection") else None))
        ),
        "projection_method": None,
        "projection_r_squared": None,
        "projection_obs_count": None,
        "projection_asof": None,
    }


def cuse_row_model_readiness(
    row: dict[str, Any],
    *,
    quote_source: str,
) -> tuple[bool, str, str]:
    model_status = str(row.get("model_status") or "").strip()
    exposure_origin = str(row.get("exposure_origin") or "").strip()
    has_factor_exposures = bool(dict(row.get("exposures") or {}))
    served_exposure_available = bool(row.get("served_exposure_available", has_factor_exposures))
    projection_output_status = str(row.get("projection_output_status") or "").strip()

    if (
        quote_source == "served_payload"
        and (
            has_factor_exposures
            or served_exposure_available
            or (
                exposure_origin in {"projected_returns", "projected_fundamental"}
                and projection_output_status == "available"
            )
        )
        and model_status != "ineligible"
    ):
        return (
            True,
            "Preview Ready",
            "This security has a currently published cUSE modeled surface and can be staged into what-if preview.",
        )
    if quote_source == "served_payload" and exposure_origin == "projected_returns" and not served_exposure_available:
        return (
            False,
            "Not Preview Ready",
            "This security is admitted to the returns-projection path, but the current published cUSE snapshot does not contain served projected loadings for it.",
        )
    if quote_source == "served_payload" and exposure_origin == "projected_fundamental" and not served_exposure_available:
        return (
            False,
            "Not Preview Ready",
            "This security is admitted to the fundamental-projection path, but the current published cUSE snapshot does not contain served projected loadings for it.",
        )
    if quote_source != "served_payload":
        return (
            False,
            "Not Preview Ready",
            "This security is searchable through registry/runtime authority, but the current published cUSE snapshot does not contain a modeled surface for what-if preview.",
        )
    return (
        False,
        "Not Preview Ready",
        "This security does not currently have a published cUSE modeled surface that what-if preview can use.",
    )


def _cuse_risk_tier(row: dict[str, Any]) -> tuple[str, str, str]:
    projection_output_status = str(row.get("projection_output_status") or "").strip()
    served_exposure_available = bool(row.get("served_exposure_available"))
    if (
        projection_output_status == "unavailable"
        or (
            str(row.get("exposure_origin") or "").strip().startswith("projected")
            and not served_exposure_available
        )
    ):
        if str(row.get("exposure_origin") or "").strip() == "projected_fundamental":
            return (
                "fundamental_projection_candidate",
                "Projected (Fundamental Candidate)",
                "cUSE policy admits this security to the fundamental-projection path, but no served projected loadings are currently available.",
            )
        if str(row.get("exposure_origin") or "").strip() == "projected_returns":
            return (
                "returns_projection_candidate",
                "Projected (Returns Candidate)",
                "cUSE policy admits this security to the returns-projection path, but no served projected loadings are currently available.",
            )
    if str(row.get("model_status") or "").strip() == "core_estimated" and str(row.get("quote_source") or "") == "served_payload":
        return (
            "live_core",
            "Core",
            "Native cUSE core exposures are currently published for this security.",
        )
    if str(row.get("exposure_origin") or "").strip() == "projected_fundamental":
        return (
            "fundamental_projection",
            "Projected (Fundamental)",
            "cUSE treats this security through the fundamental-projection path.",
        )
    if str(row.get("exposure_origin") or "").strip() == "projected_returns":
        return (
            "returns_projection",
            "Projected (Returns)",
            "cUSE treats this security through the returns-projection path.",
        )
    if _bool_flag(row, "allow_cuse_native_core"):
        return (
            "core_candidate",
            "Core Candidate",
            "Registry policy admits this security to the native cUSE core path when a live factor payload is available.",
        )
    if _bool_flag(row, "allow_cuse_fundamental_projection"):
        return (
            "fundamental_projection_candidate",
            "Projected (Fundamental)",
            "Registry policy admits this security to the cUSE fundamental-projection path.",
        )
    if _bool_flag(row, "allow_cuse_returns_projection"):
        return (
            "returns_projection_candidate",
            "Projected (Returns)",
            "Registry policy admits this security to the cUSE returns-projection path.",
        )
    return (
        "limited_info",
        "Limited Info",
        "This security is tracked in the registry, but it is not currently admitted to a cUSE estimation or projection path.",
    )


def _decorate_cuse_row(
    row: dict[str, Any],
    *,
    quote_source: str,
) -> dict[str, Any]:
    enriched = {**row, "quote_source": quote_source}
    tier, label, detail = _cuse_risk_tier(enriched)
    whatif_ready, whatif_ready_label, whatif_ready_detail = cuse_row_model_readiness(
        enriched,
        quote_source=quote_source,
    )
    source_label = "Live cUSE Payload" if quote_source == "served_payload" else "Registry Runtime"
    source_detail = (
        "This quote is coming from the current published cUSE payload."
        if quote_source == "served_payload"
        else "This quote is coming from registry/runtime authority because the current published cUSE snapshot does not contain this ticker."
    )
    return {
        **enriched,
        "risk_tier": tier,
        "risk_tier_label": label,
        "risk_tier_detail": detail,
        "quote_source": quote_source,
        "quote_source_label": source_label,
        "quote_source_detail": source_detail,
        "whatif_ready": whatif_ready,
        "whatif_ready_label": whatif_ready_label,
        "whatif_ready_detail": whatif_ready_detail,
    }


def _registry_search_rows(
    *,
    q: str,
    limit: int,
) -> list[dict[str, Any]]:
    rows = registry_quote_reads.search_registry_quote_rows(
        q,
        limit=max(int(limit) * 8, int(limit)),
        data_db=DATA_DB,
    )
    return [
        _decorate_cuse_row(
            _normalize_registry_ticker_row(row),
            quote_source="registry_runtime",
        )
        for row in rows
        if str(row.get("ticker") or "").strip()
    ]


def _registry_ticker_row(ticker: str) -> dict[str, Any] | None:
    rows = registry_quote_reads.load_registry_quote_rows_for_tickers(
        [ticker],
        data_db=DATA_DB,
    )
    if not rows:
        return None
    ranked = sorted(
        (_decorate_cuse_row(_normalize_registry_ticker_row(row), quote_source="registry_runtime") for row in rows),
        key=lambda row: (
            0 if str(row.get("risk_tier") or "").startswith("live_core") else (
                1 if str(row.get("risk_tier") or "").startswith("core") else (
                    2 if "fundamental" in str(row.get("risk_tier") or "") else (
                        3 if "returns" in str(row.get("risk_tier") or "") else 9
                    )
                )
            ),
            str(row.get("ric") or ""),
        ),
    )
    return ranked[0] if ranked else None


def _published_portfolio_overlay_rows(
    *,
    payload_loader: PayloadLoader,
    fallback_loader,
) -> dict[str, dict[str, Any]]:
    portfolio_payload = payload_loader("portfolio", fallback_loader=fallback_loader)
    positions = (portfolio_payload or {}).get("positions") if isinstance(portfolio_payload, dict) else None
    if not isinstance(positions, list):
        return {}

    overlay: dict[str, dict[str, Any]] = {}
    for raw in positions:
        if not isinstance(raw, dict):
            continue
        ticker = str(raw.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        exposures = dict(raw.get("exposures") or {})
        row: dict[str, Any] = {
            "ticker": ticker,
            "name": str(raw.get("name") or ticker).strip() or ticker,
            "trbc_economic_sector_short": str(raw.get("trbc_economic_sector_short") or "").strip(),
            "trbc_industry_group": str(raw.get("trbc_industry_group") or "").strip() or None,
            "price": raw.get("price"),
            "exposures": exposures,
            "specific_var": raw.get("specific_var"),
            "specific_vol": raw.get("specific_vol"),
            "model_status": str(raw.get("model_status") or "").strip(),
            "model_status_reason": str(raw.get("model_status_reason") or raw.get("eligibility_reason") or "").strip(),
            "eligibility_reason": str(raw.get("eligibility_reason") or raw.get("model_status_reason") or "").strip(),
            "exposure_origin": str(raw.get("exposure_origin") or "").strip() or None,
            "projection_method": raw.get("projection_method"),
            "projection_r_squared": raw.get("projection_r_squared"),
            "projection_obs_count": raw.get("projection_obs_count"),
            "projection_asof": raw.get("projection_asof"),
            "projection_output_status": raw.get("projection_output_status"),
            "served_exposure_available": bool(raw.get("served_exposure_available", bool(exposures))),
        }
        ric = str(raw.get("ric") or "").upper().strip()
        if ric:
            row["ric"] = ric
        prior = overlay.get(ticker) or {}
        overlay[ticker] = {**prior, **{key: value for key, value in row.items() if value not in (None, "", {})}}
        if exposures:
            overlay[ticker]["exposures"] = exposures
    return overlay


def _search_rank(row: dict[str, Any], needle: str) -> tuple[int, int, str]:
    ticker = str(row.get("ticker", "")).upper()
    name = str(row.get("name", "")).upper()
    ric = str(row.get("ric", "")).upper()

    if ticker == needle:
        return (0, 0, ticker)
    if ric == needle:
        return (0, 1, ticker)
    if ticker.startswith(needle):
        return (1, len(ticker), ticker)
    if ric.startswith(needle):
        return (1, len(ric), ticker)
    if needle in ticker:
        return (2, ticker.find(needle), ticker)
    if needle in ric:
        return (2, ric.find(needle), ticker)
    if name.startswith(needle):
        return (3, len(name), ticker)
    return (4, name.find(needle), ticker)


def _week_ending_friday(day: date) -> date:
    return day + timedelta(days=(4 - day.weekday()))


def _load_universe_payload(
    payload_name: str,
    *,
    payload_loader: PayloadLoader,
    fallback_loader,
) -> dict[str, Any]:
    payload = payload_loader(payload_name, fallback_loader=fallback_loader)
    if payload is None:
        raise UniversePayloadNotReady(
            cache_key=payload_name,
            message=(
                "Universe cache is not ready yet. Run refresh and try again."
                if payload_name == "universe_loadings"
                else "Universe factor cache is not ready yet."
            ),
        )
    return payload


def load_universe_payload() -> dict[str, Any]:
    return _load_universe_payload(
        "universe_loadings",
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
    )


def load_universe_factors_payload() -> dict[str, Any]:
    return _load_universe_payload(
        "universe_factors",
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
    )


def _load_universe_ticker_payload(
    ticker: str,
    *,
    payload_loader: PayloadLoader,
    fallback_loader,
    row_normalizer: RowNormalizer,
) -> dict[str, Any]:
    data = _load_universe_payload(
        "universe_loadings",
        payload_loader=payload_loader,
        fallback_loader=fallback_loader,
    )
    by_ticker = data.get("by_ticker") or {}
    overlay_by_ticker = _published_portfolio_overlay_rows(
        payload_loader=payload_loader,
        fallback_loader=fallback_loader,
    )
    clean_ticker = str(ticker).upper().strip()
    item = (
        {**dict(by_ticker.get(clean_ticker) or {}), **dict(overlay_by_ticker.get(clean_ticker) or {})}
        if clean_ticker in by_ticker or clean_ticker in overlay_by_ticker
        else None
    )
    if item is None:
        try:
            fallback = _registry_ticker_row(ticker)
        except registry_quote_reads.RegistryQuoteReadError:
            fallback = None
        if fallback is None:
            raise HTTPException(status_code=404, detail="Ticker not found in cached universe")
        return {"item": row_normalizer(dict(fallback)), "_cached": False}
    return {
        "item": row_normalizer(_decorate_cuse_row(dict(item), quote_source="served_payload")),
        "_cached": True,
    }


def load_universe_ticker_payload(
    ticker: str,
    *,
    row_normalizer: RowNormalizer,
) -> dict[str, Any]:
    return _load_universe_ticker_payload(
        ticker,
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
        row_normalizer=row_normalizer,
    )


def _load_universe_ticker_history_payload(
    ticker: str,
    *,
    years: int,
    payload_loader: PayloadLoader,
    fallback_loader,
    history_loader: HistoryLoader,
    data_db: Path,
) -> dict[str, Any]:
    data = _load_universe_payload(
        "universe_loadings",
        payload_loader=payload_loader,
        fallback_loader=fallback_loader,
    )
    clean_ticker = str(ticker).upper().strip()
    item = (data.get("by_ticker") or {}).get(clean_ticker)
    if item is None:
        try:
            item = _registry_ticker_row(clean_ticker)
        except registry_quote_reads.RegistryQuoteReadError:
            item = None
        if item is None:
            raise HTTPException(status_code=404, detail="Ticker not found in cached universe")
    ric = str(item.get("ric") or "").strip()
    if not ric:
        raise HTTPException(status_code=404, detail="RIC mapping unavailable for ticker")

    latest_date, rows = history_loader(
        data_db,
        ric=ric,
        years=int(years),
    )
    if not latest_date:
        raise HTTPException(status_code=404, detail="No price history found for ticker")

    week_close: dict[str, float] = {}
    for d_raw, close_raw in rows:
        if d_raw is None or close_raw is None:
            continue
        d_txt = str(d_raw).strip()
        try:
            day = datetime.fromisoformat(d_txt).date()
            close = float(close_raw)
        except (TypeError, ValueError):
            continue
        week_end = _week_ending_friday(day).isoformat()
        week_close[week_end] = close

    points = [
        {"date": week_end, "close": round(float(close), 4)}
        for week_end, close in sorted(week_close.items())
    ]
    return {
        "ticker": clean_ticker,
        "ric": ric,
        "years": int(years),
        "points": points,
        "_cached": True,
    }


def load_universe_ticker_history_payload(ticker: str, *, years: int) -> dict[str, Any]:
    return _load_universe_ticker_history_payload(
        ticker,
        years=years,
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
        history_loader=load_price_history_rows,
        data_db=DATA_DB,
    )


def _search_universe_payload(
    *,
    q: str,
    limit: int,
    payload_loader: PayloadLoader,
    fallback_loader,
    row_normalizer: RowNormalizer,
) -> dict[str, Any]:
    index = load_runtime_payload_field(
        "universe_loadings",
        "index",
        fallback_loader=fallback_loader,
    )
    if index is None:
        data = _load_universe_payload(
            "universe_loadings",
            payload_loader=payload_loader,
            fallback_loader=fallback_loader,
        )
        index = data.get("index") or []
    if not isinstance(index, list):
        index = []
    needle = str(q).strip().upper()
    if not needle:
        return {"query": q, "results": [], "total": 0, "_cached": True}

    full_payload: dict[str, Any] | None = None
    by_ticker: dict[str, Any] = {}
    ranked: list[tuple[tuple[int, int, str], int, dict[str, Any]]] = []
    for row in index:
        ticker = str(row.get("ticker", "")).upper()
        name = str(row.get("name", "")).upper()
        ric = str(row.get("ric", "")).upper()
        if needle in ticker or needle in name or needle in ric:
            served_row = dict(row)
            if (
                not served_row.get("exposures")
                and "served_exposure_available" not in served_row
                and "projection_output_status" not in served_row
            ):
                if full_payload is None:
                    full_payload = _load_universe_payload(
                        "universe_loadings",
                        payload_loader=payload_loader,
                        fallback_loader=fallback_loader,
                    )
                    by_ticker = full_payload.get("by_ticker") or {}
                served_row = {
                    **served_row,
                    **dict(by_ticker.get(ticker) or {}),
                }
            normalized = row_normalizer(
                _decorate_cuse_row(
                    served_row,
                    quote_source="served_payload",
                )
            )
            ranked.append((_search_rank(normalized, needle), 0, normalized))

    overlay_by_ticker = _published_portfolio_overlay_rows(
        payload_loader=payload_loader,
        fallback_loader=fallback_loader,
    )
    existing_tickers = {
        str(row.get("ticker") or "").upper().strip()
        for _, _, row in ranked
        if str(row.get("ticker") or "").strip()
    }
    for ticker, raw_row in overlay_by_ticker.items():
        if not ticker or ticker in existing_tickers:
            continue
        name = str(raw_row.get("name") or "").upper()
        ric = str(raw_row.get("ric") or "").upper()
        if needle not in ticker and needle not in name and needle not in ric:
            continue
        normalized = row_normalizer(
            _decorate_cuse_row(
                dict(raw_row),
                quote_source="served_payload",
            )
        )
        ranked.append((_search_rank(normalized, needle), 0, normalized))
        existing_tickers.add(ticker)

    registry_rows: list[dict[str, Any]] = []
    if len(ranked) < limit:
        try:
            registry_rows = _registry_search_rows(q=q, limit=limit)
        except registry_quote_reads.RegistryQuoteReadError:
            registry_rows = []
    for row in registry_rows:
        clean_ticker = str(row.get("ticker") or "").upper().strip()
        if not clean_ticker or clean_ticker in existing_tickers:
            continue
        normalized = row_normalizer(dict(row))
        ranked.append((_search_rank(normalized, needle), 1, normalized))
        existing_tickers.add(clean_ticker)

    ranked.sort(key=lambda item: (item[0], item[1]))
    hits = [row for _, _, row in ranked[:limit]]
    return {"query": q, "results": hits, "total": len(hits), "_cached": True}


def search_universe_payload(
    *,
    q: str,
    limit: int,
    row_normalizer: RowNormalizer,
) -> dict[str, Any]:
    return _search_universe_payload(
        q=q,
        limit=limit,
        payload_loader=load_runtime_payload,
        fallback_loader=cache_get,
        row_normalizer=row_normalizer,
    )


__all__ = [
    "DATA_DB",
    "UniversePayloadNotReady",
    "cache_get",
    "load_price_history_rows",
    "load_runtime_payload",
    "load_universe_factors_payload",
    "load_universe_payload",
    "load_universe_ticker_history_payload",
    "load_universe_ticker_payload",
    "search_universe_payload",
]
