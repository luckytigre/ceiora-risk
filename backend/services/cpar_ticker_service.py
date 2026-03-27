"""Read-only cPAR single-name explore payload service."""

from __future__ import annotations

from typing import Any

from backend.data import cpar_outputs, cpar_source_reads
from backend.services import cpar_display_loadings, cpar_meta_service


class CparTickerNotFound(LookupError):
    """Raised when a ticker is not present in the active cPAR package."""


def _factor_rows(loadings: dict[str, Any] | None) -> list[dict[str, object]]:
    return cpar_display_loadings.ordered_factor_rows(dict(loadings or {}))


def load_cpar_ticker_payload(
    *,
    ticker: str,
    ric: str | None = None,
    data_db=None,
) -> dict[str, object]:
    package = cpar_meta_service.require_active_package(data_db=data_db)
    clean_ticker = str(ticker or "").strip().upper()
    clean_ric = str(ric or "").strip().upper() or None
    if not clean_ticker:
        raise CparTickerNotFound("Ticker is required.")

    try:
        fit = cpar_outputs.load_active_package_instrument_fit(
            clean_ticker,
            ric=clean_ric,
            data_db=data_db,
        )
    except cpar_outputs.CparPackageNotReady as exc:
        raise cpar_meta_service.CparReadNotReady(str(exc)) from exc
    except cpar_outputs.CparAuthorityReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc

    if fit is None:
        raise CparTickerNotFound(
            f"{clean_ticker} is not present in the active cPAR package."
        )

    resolved_ric = str(fit.get("ric") or "").strip().upper()
    if not resolved_ric:
        raise CparTickerNotFound(f"{clean_ticker} is missing a valid cPAR RIC mapping.")

    try:
        price_rows = cpar_source_reads.load_latest_price_rows(
            [resolved_ric],
            as_of_date=str(package["package_date"]),
            data_db=data_db,
        )
        classification_rows = cpar_source_reads.load_latest_classification_rows(
            [resolved_ric],
            as_of_date=str(package["package_date"]),
            data_db=data_db,
        )
        common_name_rows = cpar_source_reads.load_latest_common_name_rows(
            [resolved_ric],
            as_of_date=str(package["package_date"]),
            data_db=data_db,
        )
    except cpar_source_reads.CparSourceReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(f"Shared-source read failed: {exc}") from exc

    price_row = next(iter(price_rows), None)
    classification_row = next(iter(classification_rows), None)
    common_name_row = next(iter(common_name_rows), None)

    latest_price = None
    price_field_used = None
    if price_row is not None:
        if price_row.get("adj_close") is not None:
            latest_price = float(price_row["adj_close"])
            price_field_used = "adj_close"
        elif price_row.get("close") is not None:
            latest_price = float(price_row["close"])
            price_field_used = "close"

    display_name = (
        fit.get("display_name")
        or (common_name_row or {}).get("common_name")
        or resolved_ric
    )

    return {
        **cpar_meta_service.package_meta_payload(package),
        "ticker": fit.get("ticker") or clean_ticker,
        "ric": resolved_ric,
        "display_name": display_name,
        "target_scope": fit.get("target_scope"),
        "fit_family": fit.get("fit_family"),
        "price_on_package_date_status": fit.get("price_on_package_date_status"),
        "fit_row_status": fit.get("fit_row_status") or "present",
        "fit_quality_status": fit.get("fit_quality_status") or fit.get("fit_status"),
        "portfolio_use_status": fit.get("portfolio_use_status"),
        "ticker_detail_use_status": fit.get("ticker_detail_use_status") or "available",
        "hedge_use_status": fit.get("hedge_use_status"),
        "reason_code": fit.get("reason_code"),
        "quality_label": fit.get("quality_label"),
        "fit_status": fit.get("fit_status"),
        "warnings": list(fit.get("warnings") or []),
        "observed_weeks": int(fit.get("observed_weeks") or 0),
        "lookback_weeks": int(fit.get("lookback_weeks") or 0),
        "longest_gap_weeks": int(fit.get("longest_gap_weeks") or 0),
        "price_field_used": price_field_used or fit.get("price_field_used"),
        "hq_country_code": (
            fit.get("hq_country_code")
            or (classification_row or {}).get("hq_country_code")
        ),
        "market_step_alpha": fit.get("market_step_alpha"),
        "beta_market_step1": fit.get("market_step_beta"),
        "block_alpha": fit.get("block_alpha"),
        "beta_spy_trade": fit.get("spy_trade_beta_raw"),
        "display_loadings": cpar_display_loadings.ordered_factor_rows(
            cpar_display_loadings.display_loadings_from_fit(fit),
        ),
        "raw_loadings": _factor_rows(fit.get("raw_loadings")),
        "thresholded_loadings": _factor_rows(fit.get("thresholded_loadings")),
        "pre_hedge_factor_variance_proxy": fit.get("factor_variance_proxy"),
        "pre_hedge_factor_volatility_proxy": fit.get("factor_volatility_proxy"),
        "source_context": {
            "status": "ok",
            "reason": None,
            "latest_common_name": (
                None
                if common_name_row is None
                else {
                    "value": common_name_row.get("common_name"),
                    "as_of_date": common_name_row.get("as_of_date"),
                }
            ),
            "classification_snapshot": (
                None
                if classification_row is None
                else {
                    "as_of_date": classification_row.get("as_of_date"),
                    "trbc_economic_sector": classification_row.get("trbc_economic_sector"),
                    "trbc_business_sector": classification_row.get("trbc_business_sector"),
                    "trbc_industry_group": classification_row.get("trbc_industry_group"),
                    "trbc_industry": classification_row.get("trbc_industry"),
                    "trbc_activity": classification_row.get("trbc_activity"),
                }
            ),
            "latest_price_context": (
                None
                if price_row is None
                else {
                    "price": latest_price,
                    "price_date": price_row.get("date"),
                    "price_field_used": price_field_used,
                    "currency": price_row.get("currency"),
                }
            ),
        },
    }
