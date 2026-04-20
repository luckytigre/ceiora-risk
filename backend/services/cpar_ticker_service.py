"""Read-only cPAR single-name explore payload service."""

from __future__ import annotations

from typing import Any

from backend.data import cpar_outputs, cpar_source_reads, registry_quote_reads
from backend.services import cpar_display_loadings, cpar_meta_service


class CparTickerNotFound(LookupError):
    """Raised when a ticker is not present in the active cPAR package."""


def _factor_rows(loadings: dict[str, Any] | None) -> list[dict[str, object]]:
    return cpar_display_loadings.ordered_factor_rows(dict(loadings or {}))


def _bool_flag(row: dict[str, Any], key: str) -> bool:
    return bool(int(row.get(key) or 0) == 1)


def _active_cpar_tier(fit: dict[str, Any]) -> tuple[str, str, str]:
    fit_status = str(fit.get("fit_status") or "").strip()
    target_scope = str(fit.get("target_scope") or "").strip().lower()
    if fit_status == "limited_history":
        return (
            "active_package_limited",
            "Active Package (Limited)",
            "The active cPAR package has a usable fit for this security, but history depth or continuity is weaker than ideal.",
        )
    if fit_status == "insufficient_history":
        return (
            "active_package_insufficient",
            "Active Package (Insufficient)",
            "The active cPAR package tracks this security, but it does not have enough history to expose loadings.",
        )
    if "core" in target_scope:
        return (
            "active_package_core",
            "Active Core",
            "This security is covered directly in the active cPAR package core target set.",
        )
    return (
        "active_package_extended",
        "Active Extended",
        "This security is covered in the active cPAR package extended target set.",
    )


def _registry_cpar_tier(row: dict[str, Any]) -> tuple[str, str, str]:
    if _bool_flag(row, "allow_cpar_core_target"):
        return (
            "registry_core_target",
            "Core Target",
            "Registry policy admits this security to the cPAR core target path, but it is not present in the active package.",
        )
    if _bool_flag(row, "allow_cpar_extended_target"):
        return (
            "registry_extended_target",
            "Extended Target",
            "Registry policy admits this security to the cPAR extended target path, but it is not present in the active package.",
        )
    return (
        "limited_info",
        "Limited Info",
        "This security is tracked in the registry, but it is not currently admitted to an active cPAR target path.",
    )


def _load_registry_row(
    *,
    ticker: str,
    ric: str | None,
    package_date: str,
    data_db=None,
) -> dict[str, Any] | None:
    rows = []
    if ric:
        rows = registry_quote_reads.load_registry_quote_rows_for_rics(
            [ric],
            as_of_date=package_date,
            data_db=data_db,
        )
    if not rows:
        rows = registry_quote_reads.load_registry_quote_rows_for_tickers(
            [ticker],
            as_of_date=package_date,
            data_db=data_db,
        )
    if not rows:
        return None
    ranked = sorted(
        rows,
        key=lambda row: (
            0 if _bool_flag(row, "allow_cpar_core_target") else (1 if _bool_flag(row, "allow_cpar_extended_target") else 9),
            0 if str(row.get("ticker") or "").upper().strip() == ticker else 1,
            0 if ric and str(row.get("ric") or "").upper().strip() == ric else 1,
            str(row.get("ric") or ""),
        ),
    )
    return ranked[0] if ranked else None


def resolve_cpar_ticker_identity(
    *,
    ticker: str,
    ric: str | None = None,
    data_db=None,
) -> tuple[dict[str, object], dict[str, Any] | None, dict[str, Any] | None, str, str]:
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

    try:
        registry_row = _load_registry_row(
            ticker=clean_ticker,
            ric=clean_ric,
            package_date=str(package["package_date"]),
            data_db=data_db,
        )
    except registry_quote_reads.RegistryQuoteReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(str(exc)) from exc

    if fit is None and registry_row is None:
        raise CparTickerNotFound(
            f"{clean_ticker} is not present in the active cPAR package or registry."
        )

    resolved_ric = str((fit or {}).get("ric") or (registry_row or {}).get("ric") or "").strip().upper()
    if not resolved_ric:
        raise CparTickerNotFound(f"{clean_ticker} is missing a valid cPAR RIC mapping.")

    return package, fit, registry_row, clean_ticker, resolved_ric


def load_cpar_ticker_payload(
    *,
    ticker: str,
    ric: str | None = None,
    data_db=None,
) -> dict[str, object]:
    package, fit, registry_row, clean_ticker, resolved_ric = resolve_cpar_ticker_identity(
        ticker=ticker,
        ric=ric,
        data_db=data_db,
    )

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
        (fit or {}).get("display_name")
        or (common_name_row or {}).get("common_name")
        or (registry_row or {}).get("common_name")
        or resolved_ric
    )

    if fit is not None:
        risk_tier, risk_tier_label, risk_tier_detail = _active_cpar_tier(fit)
        scenario_stage_supported = bool(str(fit.get("ticker") or "").strip()) and str(fit.get("ticker_detail_use_status") or "available") == "available"
        quote_source = "active_package"
        quote_source_label = "Active cPAR Package"
        quote_source_detail = "This quote is backed by the active published cPAR package."
        ticker_detail_use_status = fit.get("ticker_detail_use_status") or "available"
        portfolio_use_status = fit.get("portfolio_use_status")
        hedge_use_status = fit.get("hedge_use_status")
        reason_code = fit.get("reason_code")
        quality_label = fit.get("quality_label")
        fit_status = fit.get("fit_status")
        fit_row_status = fit.get("fit_row_status") or "present"
        fit_quality_status = fit.get("fit_quality_status") or fit.get("fit_status")
        target_scope = fit.get("target_scope")
        fit_family = fit.get("fit_family")
        price_on_package_date_status = fit.get("price_on_package_date_status")
        warnings = list(fit.get("warnings") or [])
        observed_weeks = int(fit.get("observed_weeks") or 0)
        lookback_weeks = int(fit.get("lookback_weeks") or 0)
        longest_gap_weeks = int(fit.get("longest_gap_weeks") or 0)
        market_step_alpha = fit.get("market_step_alpha")
        beta_market_step1 = fit.get("market_step_beta")
        block_alpha = fit.get("block_alpha")
        beta_spy_trade = fit.get("spy_trade_beta_raw")
        display_loadings = cpar_display_loadings.ordered_factor_rows(
            cpar_display_loadings.display_loadings_from_fit(fit),
        )
        raw_loadings = _factor_rows(fit.get("raw_loadings"))
        thresholded_loadings = _factor_rows(fit.get("thresholded_loadings"))
        pre_hedge_factor_variance_proxy = fit.get("factor_variance_proxy")
        pre_hedge_factor_volatility_proxy = fit.get("factor_volatility_proxy")
        scenario_stage_detail = (
            None if scenario_stage_supported else
            "cPAR what-if staging stays limited to active-package rows with a resolved ticker."
        )
    else:
        risk_tier, risk_tier_label, risk_tier_detail = _registry_cpar_tier(registry_row or {})
        scenario_stage_supported = False
        scenario_stage_detail = "cPAR what-if staging stays limited to active-package names."
        quote_source = "registry_runtime"
        quote_source_label = "Registry Runtime"
        quote_source_detail = "This quote is backed by registry/runtime authority because the active cPAR package does not contain a fit row for it."
        ticker_detail_use_status = "registry_only"
        portfolio_use_status = "missing_cpar_fit"
        hedge_use_status = "hedge_unavailable"
        reason_code = "not_in_active_package"
        quality_label = "registry_only"
        fit_status = None
        fit_row_status = "missing"
        fit_quality_status = None
        target_scope = None
        fit_family = None
        price_on_package_date_status = "present" if price_row is not None else "missing"
        warnings = []
        observed_weeks = 0
        lookback_weeks = 0
        longest_gap_weeks = 0
        market_step_alpha = None
        beta_market_step1 = None
        block_alpha = None
        beta_spy_trade = None
        display_loadings = []
        raw_loadings = []
        thresholded_loadings = []
        pre_hedge_factor_variance_proxy = None
        pre_hedge_factor_volatility_proxy = None

    return {
        **cpar_meta_service.package_meta_payload(package),
        "ticker": (fit or {}).get("ticker") or (registry_row or {}).get("ticker") or clean_ticker,
        "ric": resolved_ric,
        "display_name": display_name,
        "target_scope": target_scope,
        "fit_family": fit_family,
        "price_on_package_date_status": price_on_package_date_status,
        "fit_row_status": fit_row_status,
        "fit_quality_status": fit_quality_status,
        "portfolio_use_status": portfolio_use_status,
        "ticker_detail_use_status": ticker_detail_use_status,
        "hedge_use_status": hedge_use_status,
        "reason_code": reason_code,
        "quality_label": quality_label,
        "fit_status": fit_status,
        "warnings": warnings,
        "observed_weeks": observed_weeks,
        "lookback_weeks": lookback_weeks,
        "longest_gap_weeks": longest_gap_weeks,
        "price_field_used": price_field_used or (fit or {}).get("price_field_used"),
        "hq_country_code": (
            (fit or {}).get("hq_country_code")
            or (classification_row or {}).get("hq_country_code")
            or (registry_row or {}).get("hq_country_code")
        ),
        "market_step_alpha": market_step_alpha,
        "beta_market_step1": beta_market_step1,
        "block_alpha": block_alpha,
        "beta_spy_trade": beta_spy_trade,
        "display_loadings": display_loadings,
        "raw_loadings": raw_loadings,
        "thresholded_loadings": thresholded_loadings,
        "pre_hedge_factor_variance_proxy": pre_hedge_factor_variance_proxy,
        "pre_hedge_factor_volatility_proxy": pre_hedge_factor_volatility_proxy,
        "risk_tier": risk_tier,
        "risk_tier_label": risk_tier_label,
        "risk_tier_detail": risk_tier_detail,
        "quote_source": quote_source,
        "quote_source_label": quote_source_label,
        "quote_source_detail": quote_source_detail,
        "scenario_stage_supported": scenario_stage_supported,
        "scenario_stage_detail": scenario_stage_detail,
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
