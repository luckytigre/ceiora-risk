from __future__ import annotations

import math

from backend.services import cpar_position_hedge_service


def test_cpar_position_hedge_service_sizes_single_row_packages(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_position_hedge_service,
        "_resolve_scope_context",
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
            [{"account_id": "acct-1", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0}],
        ),
    )
    monkeypatch.setattr(
        cpar_position_hedge_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_support_rows",
        lambda **kwargs: (
            {
                "AAPL.OQ": {
                    "ric": "AAPL.OQ",
                    "fit_status": "ok",
                    "portfolio_use_status": "covered",
                    "hedge_use_status": "usable",
                    "spy_trade_beta_raw": 0.2,
                    "thresholded_loadings": {"XLK": 0.4},
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
            ],
        ),
    )
    monkeypatch.setattr(
        cpar_position_hedge_service.cpar_hedge_trade_sizing,
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
        },
    )

    payload = cpar_position_hedge_service.load_cpar_position_hedge_payload(
        ric="AAPL.OQ",
        scope="all_permitted_accounts",
    )

    assert payload["position"]["base_notional"] == 1000.0
    assert payload["packages"]["market_neutral"]["mode"] == "market_neutral"
    assert payload["packages"]["factor_neutral"]["mode"] == "factor_neutral"
    factor_rows = payload["packages"]["factor_neutral"]["trade_rows"]
    assert [row["proxy_ticker"] for row in factor_rows] == ["SPY", "XLK"]
    assert math.isclose(float(factor_rows[0]["dollar_notional"]), -200.0, rel_tol=0.0, abs_tol=1e-12)
    assert math.isclose(float(factor_rows[0]["quantity"]), -0.5, rel_tol=0.0, abs_tol=1e-12)
    assert math.isclose(float(factor_rows[1]["dollar_notional"]), -400.0, rel_tol=0.0, abs_tol=1e-12)
    assert math.isclose(float(factor_rows[1]["quantity"]), -2.0, rel_tol=0.0, abs_tol=1e-12)
