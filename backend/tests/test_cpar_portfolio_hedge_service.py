from __future__ import annotations

import pytest

from backend.cpar.factor_registry import CPAR1_METHOD_VERSION
from backend.data import cpar_outputs
from backend.services import (
    cpar_meta_service,
    cpar_portfolio_hedge_service,
    cpar_portfolio_snapshot_service,
)


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
        "universe_count": 2,
        "fit_ok_count": 2,
        "fit_limited_count": 0,
        "fit_insufficient_count": 0,
    }


def _fit_row(
    *,
    ric: str,
    ticker: str,
    fit_status: str = "ok",
    warnings: list[str] | None = None,
    market_step_beta: float | None = None,
    raw_loadings: dict[str, float] | None = None,
    thresholded_loadings: dict[str, float] | None = None,
) -> dict[str, object]:
    loadings = dict(thresholded_loadings or {"SPY": 1.0})
    raw = dict(raw_loadings or loadings)
    return {
        "package_run_id": "run_curr",
        "package_date": "2026-03-14",
        "ric": ric,
        "ticker": ticker,
        "display_name": f"{ticker} Corp",
        "fit_status": fit_status,
        "warnings": list(warnings or []),
        "observed_weeks": 52,
        "lookback_weeks": 52,
        "longest_gap_weeks": 0,
        "price_field_used": "adj_close",
        "hq_country_code": "US",
        "market_step_alpha": 0.0,
        "market_step_beta": raw.get("SPY", 0.0) if market_step_beta is None else float(market_step_beta),
        "block_alpha": 0.0,
        "spy_trade_beta_raw": raw.get("SPY", 0.0),
        "raw_loadings": raw,
        "thresholded_loadings": dict(loadings),
        "factor_variance_proxy": 0.2,
        "factor_volatility_proxy": 0.447,
        "specific_variance_proxy": 0.04,
        "specific_volatility_proxy": 0.2,
    }


def _covariance_rows() -> list[dict[str, object]]:
    return [
        {
            "factor_id": "SPY",
            "factor_id_2": "SPY",
            "covariance": 1.0,
            "correlation": 1.0,
            "package_run_id": "run_curr",
            "updated_at": "2026-03-14T00:00:00Z",
        },
        {
            "factor_id": "SPY",
            "factor_id_2": "XLK",
            "covariance": 0.2,
            "correlation": 0.2,
            "package_run_id": "run_curr",
            "updated_at": "2026-03-14T00:00:00Z",
        },
        {
            "factor_id": "XLK",
            "factor_id_2": "SPY",
            "covariance": 0.2,
            "correlation": 0.2,
            "package_run_id": "run_curr",
            "updated_at": "2026-03-14T00:00:00Z",
        },
        {
            "factor_id": "XLK",
            "factor_id_2": "XLK",
            "covariance": 1.0,
            "correlation": 1.0,
            "package_run_id": "run_curr",
            "updated_at": "2026-03-14T00:00:00Z",
        },
    ]


def test_portfolio_hedge_service_returns_partial_account_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: [{"account_id": "acct_main", "account_name": "Main", "positions_count": 3}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_positions",
        lambda **kwargs: [
            {"account_id": "acct_main", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0, "source": "seed", "updated_at": None},
            {"account_id": "acct_main", "ric": "MSFT.OQ", "ticker": "MSFT", "quantity": 5.0, "source": "seed", "updated_at": None},
            {"account_id": "acct_main", "ric": "EMPTY.OQ", "ticker": "EMPTY", "quantity": 2.0, "source": "seed", "updated_at": None},
        ],
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fits_for_rics",
        lambda rics, **kwargs: [
            _fit_row(
                ric="AAPL.OQ",
                ticker="AAPL",
                market_step_beta=0.9,
                raw_loadings={"SPY": 0.9, "XLK": 0.3},
                thresholded_loadings={"SPY": 0.9, "XLK": 0.3},
            ),
            _fit_row(ric="MSFT.OQ", ticker="MSFT", fit_status="insufficient_history", thresholded_loadings={"SPY": 1.0, "XLK": 0.2}),
        ],
    )
    monkeypatch.setattr(cpar_outputs, "load_package_covariance_rows", lambda *args, **kwargs: _covariance_rows())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.cpar_source_reads,
        "load_latest_price_rows",
        lambda rics, **kwargs: [
            {"ric": "AAPL.OQ", "date": "2026-03-14", "close": 200.0, "adj_close": 201.0},
            {"ric": "MSFT.OQ", "date": "2026-03-14", "close": 100.0, "adj_close": 101.0},
        ],
    )

    payload = cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
        account_id="acct_main",
        mode="factor_neutral",
    )

    assert payload["account_id"] == "acct_main"
    assert payload["portfolio_status"] == "partial"
    assert payload["covered_positions_count"] == 1
    assert payload["excluded_positions_count"] == 2
    assert payload["coverage_breakdown"] == {
        "covered": {"positions_count": 1, "gross_market_value": pytest.approx(2010.0)},
        "missing_price": {"positions_count": 0, "gross_market_value": pytest.approx(0.0)},
        "missing_cpar_fit": {"positions_count": 1, "gross_market_value": pytest.approx(0.0)},
        "insufficient_history": {"positions_count": 1, "gross_market_value": pytest.approx(505.0)},
    }
    assert payload["hedge_status"] == "hedge_ok"
    assert payload["aggregate_thresholded_loadings"][0]["factor_id"] == "SPY"
    assert payload["aggregate_display_loadings"][0] == {
        "factor_id": "SPY",
        "label": "Market",
        "group": "market",
        "display_order": 0,
        "beta": pytest.approx(0.9),
    }
    assert [row["factor_id"] for row in payload["factor_variance_contributions"]] == ["SPY", "XLK"]
    assert payload["factor_variance_contributions"][0] == {
        "factor_id": "SPY",
        "label": "Market",
        "group": "market",
        "display_order": 0,
        "beta": pytest.approx(0.9),
        "variance_contribution": pytest.approx(0.864),
        "variance_share": pytest.approx(0.864 / 1.048),
    }
    assert payload["factor_variance_contributions"][1]["label"] == "Technology"
    assert payload["factor_variance_contributions"][1]["group"] == "sector"
    assert payload["factor_variance_contributions"][1]["display_order"] > 0
    assert payload["factor_variance_contributions"][1]["beta"] == pytest.approx(0.3)
    assert payload["factor_variance_contributions"][1]["variance_contribution"] == pytest.approx(0.144)
    assert payload["factor_variance_contributions"][1]["variance_share"] == pytest.approx(0.144 / 1.048)
    assert [row["factor_id"] for row in payload["factor_chart"]] == ["SPY", "XLK"]
    assert payload["factor_chart"][0]["aggregate_beta"] == pytest.approx(0.9)
    assert payload["factor_chart"][0]["factor_volatility"] == pytest.approx(1.0)
    assert payload["factor_chart"][0]["covariance_adjustment"] == pytest.approx(0.96)
    assert payload["factor_chart"][0]["sensitivity_beta"] == pytest.approx(0.9)
    assert payload["factor_chart"][0]["risk_contribution_pct"] == pytest.approx((0.864 / 1.048) * 100.0)
    assert payload["factor_chart"][0]["positive_contribution_beta"] == pytest.approx(0.9)
    assert payload["factor_chart"][0]["negative_contribution_beta"] == pytest.approx(0.0)
    assert payload["factor_chart"][0]["variance_share"] == pytest.approx(0.864 / 1.048)
    assert payload["factor_chart"][0]["drilldown"][0]["factor_beta"] == pytest.approx(0.9)
    assert payload["factor_chart"][0]["drilldown"][0]["contribution_beta"] == pytest.approx(0.9)
    assert payload["factor_chart"][0]["drilldown"][0]["vol_scaled_loading"] == pytest.approx(0.9)
    assert payload["factor_chart"][0]["drilldown"][0]["vol_scaled_contribution"] == pytest.approx(0.9)
    assert payload["factor_chart"][0]["drilldown"][0]["covariance_adjusted_loading"] == pytest.approx(0.864)
    assert payload["factor_chart"][0]["drilldown"][0]["risk_contribution_pct"] == pytest.approx((0.864 / 1.048) * 100.0)
    assert [row["factor_id"] for row in payload["display_factor_chart"]] == ["SPY", "XLK"]
    assert payload["display_factor_chart"][0]["aggregate_beta"] == pytest.approx(0.9)
    assert payload["display_factor_chart"][0]["drilldown"][0]["factor_beta"] == pytest.approx(0.9)
    covered_row = next(row for row in payload["positions"] if row["ric"] == "AAPL.OQ")
    assert covered_row["thresholded_contributions"][0] == {
        "factor_id": "SPY",
        "label": "Market",
        "group": "market",
        "display_order": 0,
        "beta": pytest.approx(0.9),
    }
    assert covered_row["thresholded_contributions"][1]["factor_id"] == "XLK"
    assert covered_row["thresholded_contributions"][1]["label"] == "Technology"
    assert covered_row["thresholded_contributions"][1]["group"] == "sector"
    assert covered_row["thresholded_contributions"][1]["display_order"] > 0
    assert covered_row["thresholded_contributions"][1]["beta"] == pytest.approx(0.3)
    assert covered_row["display_contributions"][0]["beta"] == pytest.approx(0.9)
    assert covered_row["display_contributions"][1]["beta"] == pytest.approx(0.3)
    excluded_rows = [row for row in payload["positions"] if row["ric"] != "AAPL.OQ"]
    assert all(row["thresholded_contributions"] == [] for row in excluded_rows)
    reconciled = {}
    for row in payload["positions"]:
        for contribution in row["thresholded_contributions"]:
            factor_id = contribution["factor_id"]
            reconciled[factor_id] = float(reconciled.get(factor_id, 0.0) + float(contribution["beta"]))
    assert reconciled == {"SPY": pytest.approx(0.9), "XLK": pytest.approx(0.3)}
    assert {row["coverage"] for row in payload["positions"]} == {"covered", "insufficient_history", "missing_cpar_fit"}


def test_portfolio_hedge_service_factor_chart_preserves_positive_and_negative_contribution_legs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: [{"account_id": "acct_long_short", "account_name": "Long Short", "positions_count": 2}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_positions",
        lambda **kwargs: [
            {"account_id": "acct_long_short", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0, "source": "seed", "updated_at": None},
            {"account_id": "acct_long_short", "ric": "SHRT.OQ", "ticker": "SHRT", "quantity": -5.0, "source": "seed", "updated_at": None},
        ],
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fits_for_rics",
        lambda rics, **kwargs: [
            _fit_row(ric="AAPL.OQ", ticker="AAPL", thresholded_loadings={"SPY": 1.0}),
            _fit_row(ric="SHRT.OQ", ticker="SHRT", thresholded_loadings={"SPY": 0.8}),
        ],
    )
    monkeypatch.setattr(cpar_outputs, "load_package_covariance_rows", lambda *args, **kwargs: _covariance_rows())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.cpar_source_reads,
        "load_latest_price_rows",
        lambda rics, **kwargs: [
            {"ric": "AAPL.OQ", "date": "2026-03-14", "close": 100.0, "adj_close": 100.0},
            {"ric": "SHRT.OQ", "date": "2026-03-14", "close": 100.0, "adj_close": 100.0},
        ],
    )

    payload = cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
        account_id="acct_long_short",
        mode="factor_neutral",
    )

    factor_row = payload["factor_chart"][0]
    assert factor_row["factor_id"] == "SPY"
    assert factor_row["aggregate_beta"] == pytest.approx(0.4)
    assert factor_row["positive_contribution_beta"] == pytest.approx(10.0 / 15.0)
    assert factor_row["negative_contribution_beta"] == pytest.approx(-4.0 / 15.0)
    assert [row["ric"] for row in factor_row["drilldown"]] == ["AAPL.OQ", "SHRT.OQ"]
    assert factor_row["drilldown"][0]["contribution_beta"] == pytest.approx(10.0 / 15.0)
    assert factor_row["drilldown"][1]["contribution_beta"] == pytest.approx(-4.0 / 15.0)


def test_portfolio_hedge_service_keeps_zero_net_factor_rows_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: [{"account_id": "acct_flat", "account_name": "Flat", "positions_count": 2}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_positions",
        lambda **kwargs: [
            {"account_id": "acct_flat", "ric": "LONG.OQ", "ticker": "LONG", "quantity": 10.0, "source": "seed", "updated_at": None},
            {"account_id": "acct_flat", "ric": "SHRT.OQ", "ticker": "SHRT", "quantity": -10.0, "source": "seed", "updated_at": None},
        ],
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fits_for_rics",
        lambda rics, **kwargs: [
            _fit_row(ric="LONG.OQ", ticker="LONG", thresholded_loadings={"SPY": 1.0}),
            _fit_row(ric="SHRT.OQ", ticker="SHRT", thresholded_loadings={"SPY": 1.0}),
        ],
    )
    monkeypatch.setattr(cpar_outputs, "load_package_covariance_rows", lambda *args, **kwargs: _covariance_rows())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.cpar_source_reads,
        "load_latest_price_rows",
        lambda rics, **kwargs: [
            {"ric": "LONG.OQ", "date": "2026-03-14", "close": 100.0, "adj_close": 100.0},
            {"ric": "SHRT.OQ", "date": "2026-03-14", "close": 100.0, "adj_close": 100.0},
        ],
    )

    payload = cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
        account_id="acct_flat",
        mode="factor_neutral",
    )

    assert payload["portfolio_status"] == "ok"
    assert payload["aggregate_thresholded_loadings"] == []
    assert payload["hedge_status"] == "hedge_ok"
    assert payload["hedge_reason"] == "no_material_factor_exposures"
    assert payload["factor_chart"][0]["factor_id"] == "SPY"
    assert payload["factor_chart"][0]["aggregate_beta"] == pytest.approx(0.0)
    assert payload["factor_chart"][0]["positive_contribution_beta"] == pytest.approx(0.5)
    assert payload["factor_chart"][0]["negative_contribution_beta"] == pytest.approx(-0.5)
    assert payload["factor_chart"][0]["variance_share"] == pytest.approx(0.0)


def test_portfolio_hedge_service_returns_empty_payload_for_account_without_positions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: [{"account_id": "acct_empty", "account_name": "Empty", "positions_count": 0}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_positions",
        lambda **kwargs: [],
    )

    payload = cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
        account_id="acct_empty",
        mode="factor_neutral",
    )

    assert payload["portfolio_status"] == "empty"
    assert payload["hedge_status"] is None
    assert payload["coverage_breakdown"] == {
        "covered": {"positions_count": 0, "gross_market_value": 0.0},
        "missing_price": {"positions_count": 0, "gross_market_value": 0.0},
        "missing_cpar_fit": {"positions_count": 0, "gross_market_value": 0.0},
        "insufficient_history": {"positions_count": 0, "gross_market_value": 0.0},
    }
    assert payload["factor_variance_contributions"] == []
    assert payload["positions"] == []


def test_portfolio_hedge_service_returns_unavailable_payload_when_no_rows_are_covered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: [{"account_id": "acct_unavailable", "account_name": "Unavailable", "positions_count": 2}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_positions",
        lambda **kwargs: [
            {"account_id": "acct_unavailable", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0, "source": "seed", "updated_at": None},
            {"account_id": "acct_unavailable", "ric": "MISS.OQ", "ticker": "MISS", "quantity": 2.0, "source": "seed", "updated_at": None},
        ],
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fits_for_rics",
        lambda rics, **kwargs: [
            _fit_row(ric="AAPL.OQ", ticker="AAPL", fit_status="insufficient_history", thresholded_loadings={"SPY": 1.0}),
        ],
    )
    monkeypatch.setattr(cpar_outputs, "load_package_covariance_rows", lambda *args, **kwargs: _covariance_rows())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.cpar_source_reads,
        "load_latest_price_rows",
        lambda rics, **kwargs: [
            {"ric": "AAPL.OQ", "date": "2026-03-14", "close": 200.0, "adj_close": 201.0},
        ],
    )

    payload = cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
        account_id="acct_unavailable",
        mode="factor_neutral",
    )

    assert payload["portfolio_status"] == "unavailable"
    assert payload["portfolio_reason"] == (
        "No holdings rows in this account have both price coverage and a usable persisted cPAR fit in the active package."
    )
    assert payload["hedge_status"] is None
    assert payload["aggregate_thresholded_loadings"] == []
    assert payload["factor_variance_contributions"] == []
    assert payload["coverage_breakdown"] == {
        "covered": {"positions_count": 0, "gross_market_value": pytest.approx(0.0)},
        "missing_price": {"positions_count": 0, "gross_market_value": pytest.approx(0.0)},
        "missing_cpar_fit": {"positions_count": 1, "gross_market_value": pytest.approx(0.0)},
        "insufficient_history": {"positions_count": 1, "gross_market_value": pytest.approx(2010.0)},
    }
    assert {row["coverage"] for row in payload["positions"]} == {"insufficient_history", "missing_cpar_fit"}
    assert all(row["thresholded_contributions"] == [] for row in payload["positions"])


def test_portfolio_hedge_service_raises_when_account_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: [{"account_id": "acct_main", "account_name": "Main", "positions_count": 1}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_positions",
        lambda **kwargs: [],
    )

    with pytest.raises(cpar_portfolio_hedge_service.CparPortfolioAccountNotFound, match="acct_missing"):
        cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
            account_id="acct_missing",
            mode="factor_neutral",
        )


def test_portfolio_hedge_service_pins_one_package_for_fit_and_covariance(monkeypatch: pytest.MonkeyPatch) -> None:
    package = _package()
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: package)
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: [{"account_id": "acct_main", "account_name": "Main", "positions_count": 1}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_positions",
        lambda **kwargs: [
            {"account_id": "acct_main", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0, "source": "seed", "updated_at": None},
        ],
    )
    observed: dict[str, list[str]] = {"fit": [], "covariance": []}

    def _fits(rics, *, package_run_id: str, **kwargs):
        observed["fit"].append(package_run_id)
        return [_fit_row(ric="AAPL.OQ", ticker="AAPL", thresholded_loadings={"SPY": 1.1})]

    def _covariance(package_run_id: str, **kwargs):
        observed["covariance"].append(package_run_id)
        return _covariance_rows()

    monkeypatch.setattr(cpar_outputs, "load_package_instrument_fits_for_rics", _fits)
    monkeypatch.setattr(cpar_outputs, "load_package_covariance_rows", _covariance)
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.cpar_source_reads,
        "load_latest_price_rows",
        lambda rics, **kwargs: [{"ric": "AAPL.OQ", "date": "2026-03-14", "close": 200.0, "adj_close": 201.0}],
    )

    payload = cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
        account_id="acct_main",
        mode="market_neutral",
    )

    assert observed["fit"] == ["run_curr"]
    assert observed["covariance"] == ["run_curr"]
    assert payload["package_run_id"] == "run_curr"
    assert payload["mode"] == "market_neutral"


def test_portfolio_hedge_service_maps_holdings_failures_to_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: (_ for _ in ()).throw(cpar_portfolio_snapshot_service.holdings_reads.HoldingsReadError("neon unavailable")),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="Holdings read failed"):
        cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
            account_id="acct_main",
            mode="factor_neutral",
        )


def test_portfolio_hedge_service_does_not_swallow_unexpected_holdings_bugs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: (_ for _ in ()).throw(ValueError("bad holdings row shape")),
    )

    with pytest.raises(ValueError, match="bad holdings row shape"):
        cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
            account_id="acct_main",
            mode="factor_neutral",
        )


def test_portfolio_hedge_service_maps_typed_source_failures_to_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: [{"account_id": "acct_main", "account_name": "Main", "positions_count": 1}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_positions",
        lambda **kwargs: [
            {"account_id": "acct_main", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0, "source": "seed", "updated_at": None},
        ],
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fits_for_rics",
        lambda *args, **kwargs: [_fit_row(ric="AAPL.OQ", ticker="AAPL")],
    )
    monkeypatch.setattr(cpar_outputs, "load_package_covariance_rows", lambda *args, **kwargs: _covariance_rows())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.cpar_source_reads,
        "load_latest_price_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            cpar_portfolio_snapshot_service.cpar_source_reads.CparSourceReadError("prices unavailable")
        ),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="Shared-source read failed"):
        cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
            account_id="acct_main",
            mode="factor_neutral",
        )


def test_portfolio_hedge_service_does_not_swallow_unexpected_shared_source_bugs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: [{"account_id": "acct_main", "account_name": "Main", "positions_count": 1}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_positions",
        lambda **kwargs: [
            {"account_id": "acct_main", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0, "source": "seed", "updated_at": None},
        ],
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fits_for_rics",
        lambda *args, **kwargs: [_fit_row(ric="AAPL.OQ", ticker="AAPL")],
    )
    monkeypatch.setattr(cpar_outputs, "load_package_covariance_rows", lambda *args, **kwargs: _covariance_rows())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.cpar_source_reads,
        "load_latest_price_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("unexpected price row shape")),
    )

    with pytest.raises(ValueError, match="unexpected price row shape"):
        cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
            account_id="acct_main",
            mode="factor_neutral",
        )
