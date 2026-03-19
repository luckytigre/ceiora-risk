"""Read-only cPAR ticker detail payload service."""

from __future__ import annotations

from typing import Any

from backend.cpar.factor_registry import build_cpar1_factor_registry
from backend.data import cpar_outputs, cpar_queries
from backend.services import cpar_meta_service


def _factor_rows(loadings: dict[str, Any]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for spec in build_cpar1_factor_registry():
        if spec.factor_id not in loadings:
            continue
        rows.append(
            {
                "factor_id": spec.factor_id,
                "label": spec.label,
                "group": spec.group,
                "display_order": int(spec.display_order),
                "beta": float(loadings[spec.factor_id]),
            }
        )
    return rows


def load_cpar_ticker_payload(
    ticker: str,
    *,
    ric: str | None = None,
    data_db=None,
) -> dict[str, object]:
    package = cpar_meta_service.require_active_package(data_db=data_db)
    try:
        fit = cpar_outputs.load_package_instrument_fit(
            ticker=ticker,
            package_run_id=str(package["package_run_id"]),
            ric=ric,
            data_db=data_db,
        )
    except cpar_outputs.CparPackageNotReady as exc:
        raise cpar_meta_service.CparReadNotReady(str(exc)) from exc
    except cpar_outputs.CparAuthorityReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc
    except cpar_queries.CparAmbiguousInstrumentFit as exc:
        raise cpar_meta_service.CparTickerAmbiguous(str(exc)) from exc
    if fit is None:
        raise cpar_meta_service.CparTickerNotFound(
            f"Ticker {str(ticker).upper().strip()} was not found in the active cPAR package."
        )
    return {
        **cpar_meta_service.package_meta_payload(package),
        "ticker": fit.get("ticker"),
        "ric": fit.get("ric"),
        "display_name": fit.get("display_name"),
        "fit_status": fit.get("fit_status"),
        "warnings": list(fit.get("warnings") or []),
        "observed_weeks": int(fit.get("observed_weeks") or 0),
        "lookback_weeks": int(fit.get("lookback_weeks") or 0),
        "longest_gap_weeks": int(fit.get("longest_gap_weeks") or 0),
        "price_field_used": fit.get("price_field_used"),
        "hq_country_code": fit.get("hq_country_code"),
        "market_step_alpha": fit.get("market_step_alpha"),
        "beta_market_step1": fit.get("market_step_beta"),
        "block_alpha": fit.get("block_alpha"),
        "beta_spy_trade": fit.get("spy_trade_beta_raw"),
        "raw_loadings": _factor_rows(dict(fit.get("raw_loadings") or {})),
        "thresholded_loadings": _factor_rows(dict(fit.get("thresholded_loadings") or {})),
        "pre_hedge_factor_variance_proxy": fit.get("factor_variance_proxy"),
        "pre_hedge_factor_volatility_proxy": fit.get("factor_volatility_proxy"),
    }
