from __future__ import annotations

import math

from backend.services import cpar_portfolio_hedge_recommendation_service


def test_cpar_portfolio_hedge_recommendation_service_returns_sized_trade_package(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_portfolio_hedge_recommendation_service,
        "_load_scope_context",
        lambda **kwargs: (
            {
                "package_run_id": "run_curr",
                "package_date": "2026-04-18",
                "profile": "weekly",
                "started_at": "2026-04-18T00:00:00Z",
                "completed_at": "2026-04-18T01:00:00Z",
                "method_version": "cpar1",
                "factor_registry_version": "v1",
                "data_authority": "neon",
                "lookback_weeks": 52,
                "half_life_weeks": 26,
                "min_observations": 26,
                "source_prices_asof": "2026-04-18",
                "classification_asof": "2026-04-18",
                "universe_count": 100,
                "fit_ok_count": 90,
                "fit_limited_count": 5,
                "fit_insufficient_count": 5,
            },
            None,
            [{"account_id": "all_accounts", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0}],
            [{"account_id": "acct-main", "account_name": "Main"}],
        ),
    )
    monkeypatch.setattr(
        cpar_portfolio_hedge_recommendation_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_support_rows",
        lambda **kwargs: (
            {
                "AAPL.OQ": {
                    "ric": "AAPL.OQ",
                    "fit_status": "ok",
                    "portfolio_use_status": "covered",
                    "hedge_use_status": "usable",
                    "spy_trade_beta_raw": 0.25,
                    "thresholded_loadings": {"XLK": 0.4, "XLF": -0.1},
                    "specific_variance_proxy": 0.1,
                    "specific_volatility_proxy": 0.3,
                },
            },
            {"AAPL.OQ": {"ric": "AAPL.OQ", "adj_close": 100.0, "date": "2026-04-18", "currency": "USD"}},
            {"AAPL.OQ": {"ric": "AAPL.OQ", "trbc_industry_group": "Technology"}},
            [
                {"factor_id": "SPY", "factor_id_2": "SPY", "covariance": 1.0},
                {"factor_id": "SPY", "factor_id_2": "XLK", "covariance": 0.2},
                {"factor_id": "XLK", "factor_id_2": "SPY", "covariance": 0.2},
                {"factor_id": "XLK", "factor_id_2": "XLK", "covariance": 1.0},
                {"factor_id": "XLF", "factor_id_2": "XLF", "covariance": 1.0},
                {"factor_id": "SPY", "factor_id_2": "XLF", "covariance": 0.1},
                {"factor_id": "XLF", "factor_id_2": "SPY", "covariance": 0.1},
                {"factor_id": "XLK", "factor_id_2": "XLF", "covariance": 0.1},
                {"factor_id": "XLF", "factor_id_2": "XLK", "covariance": 0.1},
            ],
        ),
    )
    monkeypatch.setattr(
        cpar_portfolio_hedge_recommendation_service.cpar_display_covariance,
        "load_package_display_covariance_rows",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        cpar_portfolio_hedge_recommendation_service,
        "_build_current_snapshot",
        lambda **kwargs: {"scope": "all_permitted_accounts", "display_factor_chart": [], "positions": []},
    )
    monkeypatch.setattr(
        cpar_portfolio_hedge_recommendation_service.cpar_hedge_trade_sizing,
        "load_factor_proxy_price_context",
        lambda *args, **kwargs: {
            "SPY": {
                "factor_id": "SPY",
                "label": "Market",
                "group": "market",
                "display_order": 0,
                "proxy_ric": "SPY",
                "proxy_ticker": "SPY",
                "price": 400.0,
                "price_field_used": "adj_close",
                "price_date": "2026-04-18",
                "currency": "USD",
            },
            "XLK": {
                "factor_id": "XLK",
                "label": "Technology",
                "group": "sector",
                "display_order": 1,
                "proxy_ric": "XLK",
                "proxy_ticker": "XLK",
                "price": 200.0,
                "price_field_used": "adj_close",
                "price_date": "2026-04-18",
                "currency": "USD",
            },
            "XLF": {
                "factor_id": "XLF",
                "label": "Financials",
                "group": "sector",
                "display_order": 2,
                "proxy_ric": "XLF",
                "proxy_ticker": "XLF",
                "price": 50.0,
                "price_field_used": "adj_close",
                "price_date": "2026-04-18",
                "currency": "USD",
            },
        },
    )

    payload = cpar_portfolio_hedge_recommendation_service.load_cpar_portfolio_hedge_recommendation_payload(
        scope="all_permitted_accounts",
    )

    assert payload["scope"] == "all_permitted_accounts"
    assert payload["hedge_recommendation"]["mode"] == "factor_neutral"
    assert payload["hedge_recommendation"]["max_hedge_legs"] == 10
    assert payload["hedge_recommendation"]["base_notional"] == 1000.0
    trade_rows = payload["hedge_recommendation"]["trade_rows"]
    assert [row["proxy_ticker"] for row in trade_rows[:2]] == ["SPY", "XLK"]
    assert math.isclose(float(trade_rows[0]["dollar_notional"]), -250.0, rel_tol=0.0, abs_tol=1e-12)
    assert math.isclose(float(trade_rows[1]["dollar_notional"]), -400.0, rel_tol=0.0, abs_tol=1e-12)


def test_cpar_portfolio_hedge_recommendation_service_returns_unavailable_package_for_empty_scope(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_portfolio_hedge_recommendation_service,
        "_load_scope_context",
        lambda **kwargs: (
            {
                "package_run_id": "run_curr",
                "package_date": "2026-04-18",
                "profile": "weekly",
                "started_at": "2026-04-18T00:00:00Z",
                "completed_at": "2026-04-18T01:00:00Z",
                "method_version": "cpar1",
                "factor_registry_version": "v1",
                "data_authority": "neon",
                "lookback_weeks": 52,
                "half_life_weeks": 26,
                "min_observations": 26,
                "source_prices_asof": "2026-04-18",
                "classification_asof": "2026-04-18",
                "universe_count": 100,
                "fit_ok_count": 90,
                "fit_limited_count": 5,
                "fit_insufficient_count": 5,
            },
            None,
            [],
            [],
        ),
    )
    monkeypatch.setattr(
        cpar_portfolio_hedge_recommendation_service.cpar_display_covariance,
        "load_package_display_covariance_rows",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        cpar_portfolio_hedge_recommendation_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_support_rows",
        lambda **kwargs: ({}, {}, {}, []),
    )
    monkeypatch.setattr(
        cpar_portfolio_hedge_recommendation_service,
        "_build_current_snapshot",
        lambda **kwargs: {"scope": "all_permitted_accounts", "portfolio_status": "unavailable", "display_factor_chart": [], "positions": []},
    )

    payload = cpar_portfolio_hedge_recommendation_service.load_cpar_portfolio_hedge_recommendation_payload(
        scope="all_permitted_accounts",
    )

    assert payload["hedge_recommendation"]["hedge_status"] == "hedge_unavailable"
    assert payload["hedge_recommendation"]["hedge_reason"] == "no_covered_positions_in_selected_scope"
