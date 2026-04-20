from __future__ import annotations

from backend.services import cpar_explore_context_service


def test_cpar_explore_context_service_builds_position_only_payload(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_explore_context_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_aggregate_context",
        lambda **kwargs: (
            {
                "package_run_id": "run_curr",
                "package_date": "2026-04-18",
                "profile": "weekly",
                "method_version": "v1",
                "factor_registry_version": "f1",
                "data_authority": "neon",
                "lookback_weeks": 52,
                "half_life_weeks": 26,
                "min_observations": 26,
                "universe_count": 2,
                "fit_ok_count": 2,
                "fit_limited_count": 0,
                "fit_insufficient_count": 0,
            },
            [{"account_id": "acct_a"}],
            [{"ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 3.0}],
        ),
    )
    monkeypatch.setattr(
        cpar_explore_context_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_support_rows_without_covariance",
        lambda **kwargs: (
            {"AAPL.OQ": {"fit_status": "ok"}},
            {"AAPL.OQ": {"price": 200.0, "price_field_used": "close", "date": "2026-04-18"}},
            {"AAPL.OQ": {"trbc_industry_group": "Technology"}},
        ),
    )
    monkeypatch.setattr(
        cpar_explore_context_service.cpar_portfolio_snapshot_service,
        "_build_position_rows",
        lambda *, covered_gross_market_value, **kwargs: [
            {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "quantity": 3.0,
                "price": 200.0,
                "market_value": 600.0,
                "portfolio_weight": 1.0 if covered_gross_market_value else None,
                "fit_status": "ok",
                "coverage": "covered",
            },
        ],
    )
    monkeypatch.setattr(
        cpar_explore_context_service.cpar_portfolio_snapshot_service,
        "_aggregate_loadings",
        lambda rows, *, loadings_by_ric: ({}, 600.0, 600.0),
    )

    payload = cpar_explore_context_service.load_cpar_explore_context_payload()

    assert payload["portfolio_status"] == "ok"
    assert payload["scope"] == "all_accounts"
    assert payload["positions_count"] == 1
    assert payload["covered_positions_count"] == 1
    assert payload["held_positions"] == [
        {
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 3.0,
            "price": 200.0,
            "market_value": 600.0,
            "portfolio_weight": 1.0,
            "long_short": "LONG",
            "fit_status": "ok",
            "coverage": "covered",
        },
    ]


def test_cpar_explore_context_service_marks_restricted_scope(monkeypatch) -> None:
    monkeypatch.setattr(
        cpar_explore_context_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_aggregate_context",
        lambda **kwargs: (
            {
                "package_run_id": "run_curr",
                "package_date": "2026-04-18",
                "profile": "weekly",
                "method_version": "v1",
                "factor_registry_version": "f1",
                "data_authority": "neon",
                "lookback_weeks": 52,
                "half_life_weeks": 26,
                "min_observations": 26,
                "universe_count": 1,
                "fit_ok_count": 1,
                "fit_limited_count": 0,
                "fit_insufficient_count": 0,
            },
            [{"account_id": "acct_a"}],
            [],
        ),
    )

    payload = cpar_explore_context_service.load_cpar_explore_context_payload(
        allowed_account_ids=("acct_a",),
    )

    assert payload["scope"] == "restricted_accounts"
