from __future__ import annotations

import pytest

from backend.data import cpar_outputs
from backend.services import cpar_meta_service, cpar_portfolio_hedge_service


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
    thresholded_loadings: dict[str, float] | None = None,
) -> dict[str, object]:
    loadings = dict(thresholded_loadings or {"SPY": 1.0})
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
        "market_step_beta": loadings.get("SPY", 0.0),
        "block_alpha": 0.0,
        "spy_trade_beta_raw": loadings.get("SPY", 0.0),
        "raw_loadings": dict(loadings),
        "thresholded_loadings": dict(loadings),
        "factor_variance_proxy": 0.2,
        "factor_volatility_proxy": 0.447,
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
        cpar_portfolio_hedge_service.holdings_reads,
        "load_holdings_accounts",
        lambda: [{"account_id": "acct_main", "account_name": "Main", "positions_count": 3}],
    )
    monkeypatch.setattr(
        cpar_portfolio_hedge_service.holdings_reads,
        "load_holdings_positions",
        lambda *, account_id: [
            {"account_id": "acct_main", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0, "source": "seed", "updated_at": None},
            {"account_id": "acct_main", "ric": "MSFT.OQ", "ticker": "MSFT", "quantity": 5.0, "source": "seed", "updated_at": None},
            {"account_id": "acct_main", "ric": "EMPTY.OQ", "ticker": "EMPTY", "quantity": 2.0, "source": "seed", "updated_at": None},
        ],
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fits_for_rics",
        lambda rics, **kwargs: [
            _fit_row(ric="AAPL.OQ", ticker="AAPL", thresholded_loadings={"SPY": 1.1, "XLK": 0.3}),
            _fit_row(ric="MSFT.OQ", ticker="MSFT", fit_status="insufficient_history", thresholded_loadings={"SPY": 1.0, "XLK": 0.2}),
        ],
    )
    monkeypatch.setattr(cpar_outputs, "load_package_covariance_rows", lambda *args, **kwargs: _covariance_rows())
    monkeypatch.setattr(
        cpar_portfolio_hedge_service.cpar_source_reads,
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
    assert payload["hedge_status"] == "hedge_ok"
    assert payload["aggregate_thresholded_loadings"][0]["factor_id"] == "SPY"
    assert {row["coverage"] for row in payload["positions"]} == {"covered", "insufficient_history", "missing_price"}


def test_portfolio_hedge_service_returns_empty_payload_for_account_without_positions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_hedge_service.holdings_reads,
        "load_holdings_accounts",
        lambda: [{"account_id": "acct_empty", "account_name": "Empty", "positions_count": 0}],
    )
    monkeypatch.setattr(
        cpar_portfolio_hedge_service.holdings_reads,
        "load_holdings_positions",
        lambda *, account_id: [],
    )

    payload = cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
        account_id="acct_empty",
        mode="factor_neutral",
    )

    assert payload["portfolio_status"] == "empty"
    assert payload["hedge_status"] is None
    assert payload["positions"] == []


def test_portfolio_hedge_service_raises_when_account_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_hedge_service.holdings_reads,
        "load_holdings_accounts",
        lambda: [{"account_id": "acct_main", "account_name": "Main", "positions_count": 1}],
    )
    monkeypatch.setattr(
        cpar_portfolio_hedge_service.holdings_reads,
        "load_holdings_positions",
        lambda *, account_id: [],
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
        cpar_portfolio_hedge_service.holdings_reads,
        "load_holdings_accounts",
        lambda: [{"account_id": "acct_main", "account_name": "Main", "positions_count": 1}],
    )
    monkeypatch.setattr(
        cpar_portfolio_hedge_service.holdings_reads,
        "load_holdings_positions",
        lambda *, account_id: [
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
        cpar_portfolio_hedge_service.cpar_source_reads,
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
        cpar_portfolio_hedge_service.holdings_reads,
        "load_holdings_accounts",
        lambda: (_ for _ in ()).throw(RuntimeError("neon unavailable")),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="Holdings read failed"):
        cpar_portfolio_hedge_service.load_cpar_portfolio_hedge_payload(
            account_id="acct_main",
            mode="factor_neutral",
        )
