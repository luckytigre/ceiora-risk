from __future__ import annotations

import pytest

from backend.cpar.factor_registry import CPAR1_METHOD_VERSION
from backend.services import cpar_explore_whatif_service, cpar_meta_service


def _package() -> dict[str, object]:
    return {
        "package_run_id": "run_curr",
        "package_date": "2026-03-14",
        "profile": "cpar-weekly",
        "method_version": CPAR1_METHOD_VERSION,
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


def _accounts() -> list[dict[str, object]]:
    return [
        {"account_id": "acct_a", "account_name": "Account A", "positions_count": 1},
        {"account_id": "acct_b", "account_name": "Account B", "positions_count": 1},
    ]


def _live_positions() -> list[dict[str, object]]:
    return [
        {"account_id": "acct_a", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0, "source": "seed", "updated_at": None},
        {"account_id": "acct_b", "ric": "MSFT.OQ", "ticker": "MSFT", "quantity": 5.0, "source": "seed", "updated_at": None},
    ]


def _current_snapshot() -> dict[str, object]:
    return {
        "scope": "restricted_accounts",
        "positions_count": 1,
        "gross_market_value": 2000.0,
        "portfolio_status": "ok",
        "portfolio_reason": None,
        "positions": [
            {"account_id": "acct_a", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0, "fit_status": "ok", "coverage": "covered", "warnings": [], "market_value": 2000.0, "portfolio_weight": 1.0, "thresholded_contributions": []},
        ],
        "factor_variance_contributions": [
            {"factor_id": "SPY", "group": "market", "variance_share": 0.7},
            {"factor_id": "XLK", "group": "sector", "variance_share": 0.2},
            {"factor_id": "QUAL", "group": "style", "variance_share": 0.1},
        ],
        "factor_chart": [
            {
                "factor_id": "SPY",
                "label": "Market",
                "group": "market",
                "display_order": 0,
                "aggregate_beta": 1.1,
                "sensitivity_beta": 0.4,
                "risk_contribution_pct": 70.0,
                "factor_volatility": 0.3,
                "drilldown": [{"ric": "AAPL.OQ", "ticker": "AAPL", "portfolio_weight": 1.0, "factor_beta": 1.0, "vol_scaled_loading": 0.3, "contribution_beta": 1.0, "vol_scaled_contribution": 0.3, "risk_contribution_pct": 70.0, "fit_status": "ok", "coverage": "covered"}],
            },
        ],
        "display_factor_chart": [
            {
                "factor_id": "SPY",
                "label": "Market",
                "group": "market",
                "display_order": 0,
                "aggregate_beta": 0.9,
                "sensitivity_beta": 0.33,
                "risk_contribution_pct": 70.0,
                "factor_volatility": 0.3,
                "drilldown": [{"ric": "AAPL.OQ", "ticker": "AAPL", "portfolio_weight": 1.0, "factor_beta": 0.8, "vol_scaled_loading": 0.24, "contribution_beta": 0.8, "vol_scaled_contribution": 0.24, "risk_contribution_pct": 70.0, "fit_status": "ok", "coverage": "covered"}],
            },
        ],
    }


def _hypothetical_snapshot() -> dict[str, object]:
    return {
        "scope": "restricted_accounts",
        "positions_count": 2,
        "gross_market_value": 2900.0,
        "portfolio_status": "ok",
        "portfolio_reason": None,
        "positions": [
            {"account_id": "acct_a", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0, "fit_status": "ok", "coverage": "covered", "warnings": [], "market_value": 2000.0, "portfolio_weight": 0.69, "thresholded_contributions": []},
            {"account_id": "acct_a", "ric": "NVDA.OQ", "ticker": "NVDA", "quantity": 6.0, "fit_status": "ok", "coverage": "covered", "warnings": [], "market_value": 900.0, "portfolio_weight": 0.31, "thresholded_contributions": []},
        ],
        "factor_variance_contributions": [
            {"factor_id": "SPY", "group": "market", "variance_share": 0.6},
            {"factor_id": "XLK", "group": "sector", "variance_share": 0.25},
            {"factor_id": "QUAL", "group": "style", "variance_share": 0.15},
        ],
        "factor_chart": [
            {
                "factor_id": "SPY",
                "label": "Market",
                "group": "market",
                "display_order": 0,
                "aggregate_beta": 1.4,
                "sensitivity_beta": 0.45,
                "risk_contribution_pct": 60.0,
                "factor_volatility": 0.3,
                "drilldown": [{"ric": "NVDA.OQ", "ticker": "NVDA", "portfolio_weight": 0.31, "factor_beta": 1.3, "vol_scaled_loading": 0.39, "contribution_beta": 0.403, "vol_scaled_contribution": 0.1209, "risk_contribution_pct": 18.0, "fit_status": "ok", "coverage": "covered"}],
            },
        ],
        "display_factor_chart": [
            {
                "factor_id": "SPY",
                "label": "Market",
                "group": "market",
                "display_order": 0,
                "aggregate_beta": 1.05,
                "sensitivity_beta": 0.36,
                "risk_contribution_pct": 60.0,
                "factor_volatility": 0.3,
                "drilldown": [{"ric": "NVDA.OQ", "ticker": "NVDA", "portfolio_weight": 0.31, "factor_beta": 1.0, "vol_scaled_loading": 0.3, "contribution_beta": 0.31, "vol_scaled_contribution": 0.093, "risk_contribution_pct": 18.0, "fit_status": "ok", "coverage": "covered"}],
            },
        ],
    }


def test_cpar_explore_whatif_service_builds_aggregate_preview(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cpar_explore_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_holdings_context",
        lambda **kwargs: (_package(), _accounts(), _live_positions()),
    )
    monkeypatch.setattr(
        cpar_explore_whatif_service.cpar_portfolio_snapshot_service,
        "aggregate_cpar_positions_across_accounts",
        lambda positions: (
            ([{"account_id": "acct_a", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0}] if len(positions) == 1 else [{"account_id": "acct_a", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0}, {"account_id": "acct_a", "ric": "NVDA.OQ", "ticker": "NVDA", "quantity": 6.0}]),
            [{"account_id": "acct_a"}],
        ),
    )
    monkeypatch.setattr(
        cpar_explore_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_support_rows",
        lambda **kwargs: (
            {"AAPL.OQ": {}, "MSFT.OQ": {}, "NVDA.OQ": {}},
            {"AAPL.OQ": {}, "MSFT.OQ": {}, "NVDA.OQ": {}},
            {"AAPL.OQ": {}, "MSFT.OQ": {}, "NVDA.OQ": {}},
            [],
        ),
    )

    def _build_snapshot(*, positions, **kwargs):
        return _current_snapshot() if len(positions) == 1 else _hypothetical_snapshot()

    monkeypatch.setattr(
        cpar_explore_whatif_service.cpar_aggregate_risk_service,
        "build_cpar_risk_snapshot",
        _build_snapshot,
    )

    payload = cpar_explore_whatif_service.load_cpar_explore_whatif_payload(
        scenario_rows=[{"account_id": "acct_a", "ric": "NVDA.OQ", "ticker": "NVDA", "quantity": 6.0}],
    )

    assert payload["_preview_only"] is True
    assert payload["preview_scope"]["account_ids"] == ["acct_a"]
    assert payload["scenario_rows"][0]["account_id"] == "acct_a"
    assert payload["holding_deltas"][0]["delta_quantity"] == 6.0
    assert payload["current"]["scope"] == "restricted_accounts"
    assert payload["hypothetical"]["scope"] == "restricted_accounts"
    assert [row["ticker"] for row in payload["current"]["positions"]] == ["AAPL"]
    assert [row["ticker"] for row in payload["hypothetical"]["positions"]] == ["AAPL", "NVDA"]
    assert payload["current"]["risk_shares"]["market"] == 70.0
    assert payload["hypothetical"]["risk_shares"]["style"] == 15.0
    assert payload["diff"]["risk_shares"]["market"] == -10.0
    assert payload["diff"]["factor_deltas"]["raw"][0]["factor_id"] == "SPY"
    assert payload["current"]["display_exposure_modes"]["raw"][0]["value"] == pytest.approx(0.9)
    assert payload["diff"]["display_factor_deltas"]["raw"][0]["current"] == pytest.approx(0.9)
    assert payload["diff"]["display_factor_deltas"]["raw"][0]["hypothetical"] == pytest.approx(1.05)


def test_cpar_explore_whatif_service_rejects_unknown_accounts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cpar_explore_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_holdings_context",
        lambda **kwargs: (_package(), _accounts(), _live_positions()),
    )

    with pytest.raises(ValueError, match="was not found"):
        cpar_explore_whatif_service.load_cpar_explore_whatif_payload(
            scenario_rows=[{"account_id": "acct_missing", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 1.0}],
        )


def test_cpar_explore_whatif_service_propagates_typed_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cpar_explore_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_holdings_context",
        lambda **kwargs: (_ for _ in ()).throw(cpar_meta_service.CparReadUnavailable("Holdings read failed")),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="Holdings read failed"):
        cpar_explore_whatif_service.load_cpar_explore_whatif_payload(
            scenario_rows=[{"account_id": "acct_a", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 1.0}],
        )
