from __future__ import annotations

import pytest

from backend.services import cpar_meta_service, cpar_ticker_history_service, cpar_ticker_service


def _package() -> dict[str, object]:
    return {
        "package_run_id": "run_curr",
        "package_date": "2026-03-14",
        "profile": "cpar-weekly",
        "method_version": "cPAR1",
        "factor_registry_version": "cPAR1_registry_v1",
        "data_authority": "neon",
        "lookback_weeks": 52,
        "half_life_weeks": 26,
        "min_observations": 39,
        "source_prices_asof": "2026-03-14",
        "classification_asof": "2026-03-14",
        "universe_count": 10,
        "fit_ok_count": 8,
        "fit_limited_count": 2,
        "fit_insufficient_count": 0,
    }


def _fit() -> dict[str, object]:
    return {
        "ric": "AAPL.OQ",
        "ticker": "AAPL",
        "display_name": "Apple Inc.",
        "target_scope": "core_us_equity",
        "fit_family": "returns_regression_weekly",
        "price_on_package_date_status": "present",
        "fit_row_status": "present",
        "fit_quality_status": "ok",
        "portfolio_use_status": "covered",
        "ticker_detail_use_status": "available",
        "hedge_use_status": "usable",
        "reason_code": "ok",
        "quality_label": "ok",
        "fit_status": "ok",
        "warnings": [],
        "observed_weeks": 52,
        "lookback_weeks": 52,
        "longest_gap_weeks": 0,
        "price_field_used": "adj_close",
        "hq_country_code": "US",
        "market_step_alpha": 0.01,
        "market_step_beta": 0.98,
        "block_alpha": None,
        "spy_trade_beta_raw": 1.15,
        "raw_loadings": {"SPY": 0.98, "XLK": 0.32},
        "thresholded_loadings": {"SPY": 0.98, "XLK": 0.32},
        "factor_variance_proxy": 0.2,
        "factor_volatility_proxy": 0.4472,
    }


def test_cpar_ticker_service_builds_quote_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_ticker_service.cpar_outputs,
        "load_active_package_instrument_fit",
        lambda *args, **kwargs: _fit(),
    )
    monkeypatch.setattr(
        cpar_ticker_service.cpar_source_reads,
        "load_latest_price_rows",
        lambda *args, **kwargs: [{"ric": "AAPL.OQ", "date": "2026-03-14", "adj_close": 210.0, "close": 210.0, "currency": "USD"}],
    )
    monkeypatch.setattr(
        cpar_ticker_service.cpar_source_reads,
        "load_latest_classification_rows",
        lambda *args, **kwargs: [{"ric": "AAPL.OQ", "as_of_date": "2026-03-14", "trbc_economic_sector": "Technology", "trbc_business_sector": "Technology Equipment", "trbc_industry_group": "Computers", "trbc_industry": "Computers", "trbc_activity": "Hardware", "hq_country_code": "US"}],
    )
    monkeypatch.setattr(
        cpar_ticker_service.cpar_source_reads,
        "load_latest_common_name_rows",
        lambda *args, **kwargs: [{"ric": "AAPL.OQ", "as_of_date": "2026-03-14", "common_name": "Apple Inc."}],
    )

    payload = cpar_ticker_service.load_cpar_ticker_payload(ticker="AAPL")

    assert payload["ticker"] == "AAPL"
    assert payload["ric"] == "AAPL.OQ"
    assert payload["source_context"]["latest_price_context"]["price"] == 210.0
    assert payload["source_context"]["classification_snapshot"]["trbc_industry_group"] == "Computers"
    assert payload["display_loadings"][0]["factor_id"] == "SPY"
    assert payload["display_loadings"][0]["beta"] == pytest.approx(0.98)
    assert payload["thresholded_loadings"][0]["factor_id"] == "SPY"
    assert payload["target_scope"] == "core_us_equity"
    assert payload["fit_family"] == "returns_regression_weekly"
    assert payload["portfolio_use_status"] == "covered"
    assert payload["ticker_detail_use_status"] == "available"


def test_cpar_ticker_service_maps_source_failures_to_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_ticker_service.cpar_outputs,
        "load_active_package_instrument_fit",
        lambda *args, **kwargs: _fit(),
    )
    monkeypatch.setattr(
        cpar_ticker_service.cpar_source_reads,
        "load_latest_price_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(cpar_ticker_service.cpar_source_reads.CparSourceReadError("prices unavailable")),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="Shared-source read failed"):
        cpar_ticker_service.load_cpar_ticker_payload(ticker="AAPL")


def test_cpar_ticker_history_service_builds_weekly_points(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cpar_ticker_history_service.cpar_ticker_service,
        "load_cpar_ticker_payload",
        lambda **kwargs: {"ticker": "AAPL", "ric": "AAPL.OQ", "package_date": "2026-03-14"},
    )
    monkeypatch.setattr(
        cpar_ticker_history_service.cpar_source_reads,
        "load_price_rows_for_rics",
        lambda *args, **kwargs: [
            {"ric": "AAPL.OQ", "date": "2026-03-10", "adj_close": 100.0, "close": 100.0},
            {"ric": "AAPL.OQ", "date": "2026-03-12", "adj_close": 102.0, "close": 102.0},
            {"ric": "AAPL.OQ", "date": "2026-03-17", "adj_close": 105.0, "close": 105.0},
        ],
    )

    payload = cpar_ticker_history_service.load_cpar_ticker_history_payload(ticker="AAPL", years=5)

    assert payload["ticker"] == "AAPL"
    assert payload["points"] == [
        {"date": "2026-03-13", "close": 102.0},
        {"date": "2026-03-20", "close": 105.0},
    ]
