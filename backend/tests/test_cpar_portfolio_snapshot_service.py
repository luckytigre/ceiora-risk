from __future__ import annotations

import pytest

from backend.cpar.factor_registry import CPAR1_METHOD_VERSION
from backend.data import cpar_outputs, cpar_source_reads
from backend.services import (
    cpar_aggregate_risk_service,
    cpar_meta_service,
    cpar_portfolio_account_snapshot_service,
    cpar_portfolio_snapshot_service,
)


def _package() -> dict[str, object]:
    return {
        "package_run_id": "run_curr",
        "package_date": "2026-03-14",
        "profile": "cpar-weekly",
        "started_at": "2026-03-14T00:00:00Z",
        "completed_at": "2026-03-14T00:01:00Z",
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
        "fit_limited_count": 1,
        "fit_insufficient_count": 1,
    }


def test_account_context_maps_typed_holdings_failures_to_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: (_ for _ in ()).throw(cpar_portfolio_snapshot_service.holdings_reads.HoldingsReadError("neon unavailable")),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="Holdings read failed"):
        cpar_portfolio_snapshot_service.load_cpar_portfolio_account_context(account_id="acct_main")


def test_snapshot_service_forwards_hedge_snapshot_builder_to_account_snapshot_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = {"forwarded": True}

    monkeypatch.setattr(
        cpar_portfolio_account_snapshot_service,
        "build_cpar_portfolio_hedge_snapshot",
        lambda **kwargs: sentinel,
    )

    payload = cpar_portfolio_snapshot_service.build_cpar_portfolio_hedge_snapshot(
        package=_package(),
        account={"account_id": "acct_main", "account_name": "Main"},
        positions=[],
        mode="factor_neutral",
        fit_by_ric={},
        price_by_ric={},
        classification_by_ric={},
        covariance_rows=[],
    )

    assert payload is sentinel


def test_account_context_does_not_swallow_unexpected_holdings_bugs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: (_ for _ in ()).throw(ValueError("bad holdings row shape")),
    )

    with pytest.raises(ValueError, match="bad holdings row shape"):
        cpar_portfolio_snapshot_service.load_cpar_portfolio_account_context(account_id="acct_main")


def test_account_context_matches_accounts_after_normalization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: [{"account_id": " ACCT_MAIN ", "account_name": "Main", "positions_count": 1}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_positions",
        lambda **kwargs: [{"account_id": "acct_main", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 1.0}],
    )

    package, account, positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_account_context(
        account_id="acct_main"
    )

    assert package["package_run_id"] == "run_curr"
    assert account["account_id"] == " ACCT_MAIN "
    assert positions[0]["ric"] == "AAPL.OQ"


def test_holdings_context_preserves_raw_live_position_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda **kwargs: [{"account_id": "acct_a", "account_name": "Account A", "positions_count": 1}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_all_holdings_positions",
        lambda **kwargs: [{"account_id": "acct_a", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0, "source": "seed", "updated_at": "2026-03-14T10:00:00Z"}],
    )

    package, accounts, live_positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_holdings_context()

    assert package["package_run_id"] == "run_curr"
    assert accounts[0]["account_id"] == "acct_a"
    assert live_positions == [
        {
            "account_id": "acct_a",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 10.0,
            "source": "seed",
            "updated_at": "2026-03-14T10:00:00Z",
        }
    ]


def test_aggregate_context_aggregates_positions_across_accounts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_contributing_holdings_accounts",
        lambda **kwargs: [
            {"account_id": "acct_a", "account_name": "Account A"},
            {"account_id": "acct_b", "account_name": "Account B"},
        ],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_aggregate_holdings_positions",
        lambda **kwargs: [
            {"account_id": "all_accounts", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 6.0, "source": "aggregate", "updated_at": "2026-03-14T11:00:00Z"},
            {"account_id": "all_accounts", "ric": "MSFT.OQ", "ticker": "MSFT", "quantity": 5.0, "source": "aggregate", "updated_at": "2026-03-14T09:00:00Z"},
        ],
    )

    package, accounts, positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_aggregate_context()

    assert package["package_run_id"] == "run_curr"
    assert [row["account_id"] for row in accounts] == ["acct_a", "acct_b"]
    assert positions == [
        {
            "account_id": "all_accounts",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 6.0,
            "source": "aggregate",
            "updated_at": "2026-03-14T11:00:00Z",
        },
        {
            "account_id": "all_accounts",
            "ric": "MSFT.OQ",
            "ticker": "MSFT",
            "quantity": 5.0,
            "source": "aggregate",
            "updated_at": "2026-03-14T09:00:00Z",
        },
    ]


def test_aggregate_context_maps_typed_holdings_failures_to_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_contributing_holdings_accounts",
        lambda **kwargs: (_ for _ in ()).throw(cpar_portfolio_snapshot_service.holdings_reads.HoldingsReadError("neon unavailable")),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="Holdings read failed"):
        cpar_portfolio_snapshot_service.load_cpar_portfolio_aggregate_context()


def test_holdings_context_forwards_allowed_account_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    def _accounts_loader(**kwargs):
        captured["accounts_kwargs"] = dict(kwargs)
        return []
    def _positions_loader(**kwargs):
        captured["positions_kwargs"] = dict(kwargs)
        return []
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        _accounts_loader,
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_all_holdings_positions",
        _positions_loader,
    )

    cpar_portfolio_snapshot_service.load_cpar_portfolio_holdings_context(
        allowed_account_ids=("acct_a", "acct_b"),
    )

    assert captured["accounts_kwargs"] == {"allowed_account_ids": ("acct_a", "acct_b")}
    assert captured["positions_kwargs"] == {"allowed_account_ids": ("acct_a", "acct_b")}


def test_aggregate_context_forwards_allowed_account_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    def _accounts_loader(**kwargs):
        captured["accounts_kwargs"] = dict(kwargs)
        return []
    def _positions_loader(**kwargs):
        captured["positions_kwargs"] = dict(kwargs)
        return []
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_contributing_holdings_accounts",
        _accounts_loader,
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_aggregate_holdings_positions",
        _positions_loader,
    )

    cpar_portfolio_snapshot_service.load_cpar_portfolio_aggregate_context(
        allowed_account_ids=("acct_a",),
    )

    assert captured["accounts_kwargs"] == {"allowed_account_ids": ("acct_a",)}
    assert captured["positions_kwargs"] == {"allowed_account_ids": ("acct_a",)}


def test_support_rows_map_typed_package_authority_failures_to_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fits_for_rics",
        lambda *args, **kwargs: (_ for _ in ()).throw(cpar_outputs.CparAuthorityReadError("neon down")),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="neon down"):
        cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows(
            rics=["AAPL.OQ"],
            package_run_id="run_curr",
            package_date="2026-03-14",
        )


def test_support_rows_map_prefetched_alias_resolution_failures_to_typed_read_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service,
        "_resolve_support_fit_aliases",
        lambda **kwargs: (_ for _ in ()).throw(cpar_outputs.CparPackageNotReady("prefetch unavailable")),
    )

    with pytest.raises(cpar_meta_service.CparReadNotReady, match="prefetch unavailable"):
        cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows(
            rics=["AAPL.OQ"],
            package_run_id="run_curr",
            package_date="2026-03-14",
            positions=[{"ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 1.0}],
        )


def test_support_rows_map_typed_source_failures_to_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fits_for_rics",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_covariance_rows",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.cpar_source_reads,
        "load_latest_price_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(cpar_source_reads.CparSourceReadError("prices unavailable")),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="Shared-source read failed"):
        cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows(
            rics=["AAPL.OQ"],
            package_run_id="run_curr",
            package_date="2026-03-14",
        )


def test_support_rows_treats_classification_failures_as_degraded_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fits_for_rics",
        lambda *args, **kwargs: [{
            "ric": "AAPL.OQ",
            "fit_status": "ok",
            "thresholded_loadings": {"SPY": 1.0},
            "specific_variance_proxy": 0.04,
            "specific_volatility_proxy": 0.2,
        }],
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_covariance_rows",
        lambda *args, **kwargs: [{"factor_id": "SPY", "factor_id_2": "SPY", "covariance": 1.0, "correlation": 1.0}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.cpar_source_reads,
        "load_latest_price_rows",
        lambda *args, **kwargs: [{"ric": "AAPL.OQ", "adj_close": 100.0, "date": "2026-03-14"}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.cpar_source_reads,
        "load_latest_classification_rows",
        lambda *args, **kwargs: (_ for _ in ()).throw(cpar_source_reads.CparSourceReadError("classifications unavailable")),
    )

    fit_by_ric, price_by_ric, classification_by_ric, covariance_rows = cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows(
        rics=["AAPL.OQ"],
        package_run_id="run_curr",
        package_date="2026-03-14",
    )

    assert fit_by_ric["AAPL.OQ"]["fit_status"] == "ok"
    assert price_by_ric["AAPL.OQ"]["adj_close"] == 100.0
    assert classification_by_ric == {}
    assert covariance_rows[0]["factor_id"] == "SPY"


def test_support_rows_aliases_holdings_ric_to_unique_active_package_ticker_fit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fits_for_rics",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fits_for_tickers",
        lambda *args, **kwargs: [{
            "ric": "IBKR.OQ",
            "ticker": "IBKR",
            "display_name": "Interactive Brokers Group Inc",
            "fit_status": "ok",
            "warnings": [],
            "specific_variance_proxy": 0.04,
            "specific_volatility_proxy": 0.2,
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
        }],
    )
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_covariance_rows",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.cpar_source_reads,
        "load_latest_price_rows",
        lambda *args, **kwargs: [{"ric": "IBKR.OQ", "adj_close": 210.0, "date": "2026-03-14"}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.cpar_source_reads,
        "load_latest_classification_rows",
        lambda *args, **kwargs: [{"ric": "IBKR.OQ", "trbc_industry_group": "Investment Banking & Investment Services"}],
    )

    fit_by_ric, price_by_ric, classification_by_ric, covariance_rows = cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows(
        rics=["IBKR.O"],
        positions=[{"account_id": "acct_main", "ric": "IBKR.O", "ticker": "IBKR", "quantity": 1.0}],
        package_run_id="run_curr",
        package_date="2026-03-14",
    )

    assert fit_by_ric["IBKR.O"]["ric"] == "IBKR.OQ"
    assert price_by_ric["IBKR.O"]["ric"] == "IBKR.OQ"
    assert classification_by_ric["IBKR.O"]["ric"] == "IBKR.OQ"
    assert covariance_rows == []


def test_support_rows_does_not_swallow_unexpected_output_decode_bugs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_outputs,
        "load_package_instrument_fits_for_rics",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("bad fit row shape")),
    )

    with pytest.raises(ValueError, match="bad fit row shape"):
        cpar_portfolio_snapshot_service.load_cpar_portfolio_support_rows(
            rics=["AAPL.OQ"],
            package_run_id="run_curr",
            package_date="2026-03-14",
        )


def test_aggregate_risk_builder_uses_display_covariance_for_display_analytics() -> None:
    payload = cpar_aggregate_risk_service.build_cpar_risk_snapshot(
        package=_package(),
        accounts=[{"account_id": "acct_a", "account_name": "Account A"}],
        positions=[{"account_id": "all_accounts", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 1.0}],
        fit_by_ric={
            "AAPL.OQ": {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "display_name": "Apple Inc.",
                "fit_status": "ok",
                "warnings": [],
                "market_step_beta": 1.0,
                "spy_trade_beta_raw": 1.0,
                "raw_loadings": {"SPY": 1.0, "XLK": 1.0},
                "thresholded_loadings": {"SPY": 1.0, "XLK": 1.0},
                "specific_variance_proxy": 0.25,
                "specific_volatility_proxy": 0.5,
            }
        },
        price_by_ric={"AAPL.OQ": {"ric": "AAPL.OQ", "adj_close": 100.0, "date": "2026-03-14"}},
        classification_by_ric={"AAPL.OQ": {"ric": "AAPL.OQ", "trbc_industry_group": "Technology Hardware"}},
        covariance_rows=[
            {"factor_id": "SPY", "factor_id_2": "SPY", "covariance": 1.0, "correlation": 1.0},
            {"factor_id": "SPY", "factor_id_2": "XLK", "covariance": 0.8, "correlation": 0.8},
            {"factor_id": "XLK", "factor_id_2": "SPY", "covariance": 0.8, "correlation": 0.8},
            {"factor_id": "XLK", "factor_id_2": "XLK", "covariance": 1.0, "correlation": 1.0},
        ],
        display_covariance_rows=[
            {"factor_id": "SPY", "factor_id_2": "SPY", "covariance": 1.0, "correlation": 1.0},
            {"factor_id": "SPY", "factor_id_2": "XLK", "covariance": 0.0, "correlation": 0.0},
            {"factor_id": "XLK", "factor_id_2": "SPY", "covariance": 0.0, "correlation": 0.0},
            {"factor_id": "XLK", "factor_id_2": "XLK", "covariance": 1.0, "correlation": 1.0},
        ],
    )

    raw_xlk = next(row for row in payload["factor_chart"] if row["factor_id"] == "XLK")
    display_xlk = next(row for row in payload["display_factor_chart"] if row["factor_id"] == "XLK")
    xlk_index = payload["display_cov_matrix"]["factors"].index("XLK")
    raw_cov_xlk_index = payload["cov_matrix"]["factors"].index("XLK")

    assert payload["cov_matrix"]["correlation"][0][raw_cov_xlk_index] == pytest.approx(0.8)
    assert payload["display_cov_matrix"]["correlation"][0][xlk_index] == pytest.approx(0.0)
    assert raw_xlk["covariance_adjustment"] == pytest.approx(1.0)
    assert display_xlk["covariance_adjustment"] == pytest.approx(1.0)
    assert payload["idio_variance_proxy"] == pytest.approx(0.25)
    assert payload["total_variance_proxy"] == pytest.approx(payload["factor_variance_proxy"] + payload["idio_variance_proxy"])
    assert payload["risk_shares"]["idio"] > 0
    assert payload["vol_scaled_shares"]["style"] >= 0
    assert payload["vol_scaled_shares"]["idio"] > 0
    assert pytest.approx(sum(payload["vol_scaled_shares"].values()), abs=0.05) == 100.0
    assert payload["positions"][0]["risk_mix"]["idio"] > 0


def test_aggregate_risk_builder_labels_fit_miss_as_missing_cpar_fit() -> None:
    payload = cpar_aggregate_risk_service.build_cpar_risk_snapshot(
        package=_package(),
        accounts=[{"account_id": "acct_a", "account_name": "Account A"}],
        positions=[{"account_id": "all_accounts", "ric": "IBKR.O", "ticker": "IBKR", "quantity": 2.0}],
        fit_by_ric={},
        price_by_ric={},
        classification_by_ric={},
        covariance_rows=[],
        display_covariance_rows=[],
    )

    assert payload["portfolio_status"] == "unavailable"
    assert payload["coverage_breakdown"]["missing_cpar_fit"]["positions_count"] == 1
    assert payload["coverage_breakdown"]["missing_price"]["positions_count"] == 0
    position = payload["positions"][0]
    assert position["ric"] == "IBKR.O"
    assert position["coverage"] == "missing_cpar_fit"
    assert position["coverage_reason"] == "No persisted cPAR fit row exists for this RIC in the active package."
