from __future__ import annotations

from backend.services import cpar_portfolio_snapshot_service


def test_persisted_runtime_coverage_overrides_late_price_inference() -> None:
    package = {
        "package_run_id": "run_curr",
        "package_date": "2026-03-14",
        "profile": "cpar-weekly",
        "method_version": "cPAR1",
        "factor_registry_version": "cPAR1_registry_v1",
        "data_authority": "sqlite",
        "lookback_weeks": 52,
        "half_life_weeks": 26,
        "min_observations": 39,
        "source_prices_asof": "2026-03-14",
        "classification_asof": "2026-03-14",
        "universe_count": 1,
        "fit_ok_count": 1,
        "fit_limited_count": 0,
        "fit_insufficient_count": 0,
    }
    account = {"account_id": "acct_main", "account_name": "Main"}
    positions = [{"account_id": "acct_main", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0}]
    fit_by_ric = {
        "AAPL.OQ": {
            "package_run_id": "run_curr",
            "package_date": "2026-03-14",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "display_name": "Apple Inc.",
            "fit_status": "ok",
            "warnings": [],
            "thresholded_loadings": {"SPY": 1.0},
            "raw_loadings": {"SPY": 1.0},
            "specific_variance_proxy": 0.04,
            "specific_volatility_proxy": 0.2,
            "target_scope": "core_us_equity",
            "fit_family": "returns_regression_weekly",
            "price_on_package_date_status": "missing",
            "fit_row_status": "present",
            "fit_quality_status": "ok",
            "portfolio_use_status": "missing_price",
            "ticker_detail_use_status": "available",
            "hedge_use_status": "missing_price",
            "reason_code": "missing_price_on_or_before_package_date",
            "quality_label": "missing_price",
        }
    }
    price_by_ric = {
        "AAPL.OQ": {"ric": "AAPL.OQ", "date": "2026-03-14", "adj_close": 210.0, "close": 210.0}
    }

    payload = cpar_portfolio_snapshot_service.build_cpar_portfolio_hedge_snapshot(
        package=package,
        account=account,
        positions=positions,
        mode="factor_neutral",
        fit_by_ric=fit_by_ric,
        price_by_ric=price_by_ric,
        classification_by_ric={},
        covariance_rows=[],
    )

    assert payload["portfolio_status"] == "unavailable"
    assert payload["coverage_breakdown"]["missing_price"]["positions_count"] == 1
    position = payload["positions"][0]
    assert position["coverage"] == "missing_price"
    assert position["market_value"] is None
    assert position["price"] is None
    assert position["portfolio_use_status"] == "missing_price"
    assert position["hedge_use_status"] == "missing_price"
