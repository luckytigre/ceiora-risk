from __future__ import annotations

import pytest

from backend.services import cpar_meta_service, cpar_portfolio_whatif_service


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
        "universe_count": 3,
        "fit_ok_count": 3,
        "fit_limited_count": 0,
        "fit_insufficient_count": 0,
    }


def _account() -> dict[str, object]:
    return {
        "account_id": "acct_main",
        "account_name": "Main Account",
        "positions_count": 2,
    }


def _live_positions() -> list[dict[str, object]]:
    return [
        {"account_id": "acct_main", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0, "source": "seed", "updated_at": None},
        {"account_id": "acct_main", "ric": "MSFT.OQ", "ticker": "MSFT", "quantity": 5.0, "source": "seed", "updated_at": None},
    ]


def _fit_by_ric() -> dict[str, dict[str, object]]:
    return {
        "AAPL.OQ": {
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "display_name": "Apple Inc.",
            "fit_status": "ok",
            "warnings": [],
            "spy_trade_beta_raw": 1.1,
            "thresholded_loadings": {"SPY": 1.1, "XLK": 0.30},
        },
        "MSFT.OQ": {
            "ric": "MSFT.OQ",
            "ticker": "MSFT",
            "display_name": "Microsoft Corp.",
            "fit_status": "ok",
            "warnings": [],
            "spy_trade_beta_raw": 0.9,
            "thresholded_loadings": {"SPY": 0.9, "XLK": 0.20},
        },
        "NVDA.OQ": {
            "ric": "NVDA.OQ",
            "ticker": "NVDA",
            "display_name": "NVIDIA Corp.",
            "fit_status": "ok",
            "warnings": [],
            "spy_trade_beta_raw": 1.3,
            "thresholded_loadings": {"SPY": 1.3, "XLK": 0.45},
        },
    }


def _price_by_ric() -> dict[str, dict[str, object]]:
    return {
        "AAPL.OQ": {"ric": "AAPL.OQ", "date": "2026-03-14", "adj_close": 200.0, "close": 200.0},
        "MSFT.OQ": {"ric": "MSFT.OQ", "date": "2026-03-14", "adj_close": 100.0, "close": 100.0},
        "NVDA.OQ": {"ric": "NVDA.OQ", "date": "2026-03-14", "adj_close": 150.0, "close": 150.0},
    }


def _covariance_rows() -> list[dict[str, object]]:
    return [
        {"factor_id": "SPY", "factor_id_2": "SPY", "covariance": 1.0},
        {"factor_id": "SPY", "factor_id_2": "XLK", "covariance": 0.2},
        {"factor_id": "XLK", "factor_id_2": "SPY", "covariance": 0.2},
        {"factor_id": "XLK", "factor_id_2": "XLK", "covariance": 1.0},
    ]


def test_portfolio_whatif_service_supports_new_active_package_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_portfolio_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_account_context",
        lambda **kwargs: (_package(), _account(), _live_positions()),
    )
    observed_rics: list[str] = []

    def _support_rows(*, rics, **kwargs):
        observed_rics.extend(sorted(rics))
        return _fit_by_ric(), _price_by_ric(), _covariance_rows()

    monkeypatch.setattr(
        cpar_portfolio_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_support_rows",
        _support_rows,
    )

    payload = cpar_portfolio_whatif_service.load_cpar_portfolio_whatif_payload(
        account_id="acct_main",
        mode="factor_neutral",
        scenario_rows=[{"ric": "NVDA.OQ", "ticker": "NVDA", "quantity_delta": 6.0}],
    )

    assert observed_rics == ["AAPL.OQ", "MSFT.OQ", "NVDA.OQ"]
    assert payload["_preview_only"] is True
    assert payload["package_run_id"] == "run_curr"
    assert payload["current"]["positions_count"] == 2
    assert payload["hypothetical"]["positions_count"] == 3
    assert payload["scenario_rows"][0]["ric"] == "NVDA.OQ"
    assert payload["scenario_rows"][0]["current_quantity"] == 0.0
    assert payload["scenario_rows"][0]["hypothetical_quantity"] == 6.0
    assert payload["scenario_rows"][0]["market_value_delta"] == 900.0
    assert payload["scenario_rows"][0]["coverage"] == "covered"


def test_portfolio_whatif_service_supports_position_removals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_portfolio_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_account_context",
        lambda **kwargs: (_package(), _account(), _live_positions()),
    )
    monkeypatch.setattr(
        cpar_portfolio_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_support_rows",
        lambda **kwargs: (_fit_by_ric(), _price_by_ric(), _covariance_rows()),
    )

    payload = cpar_portfolio_whatif_service.load_cpar_portfolio_whatif_payload(
        account_id="acct_main",
        mode="factor_neutral",
        scenario_rows=[{"ric": "MSFT.OQ", "ticker": "MSFT", "quantity_delta": -5.0}],
    )

    assert payload["hypothetical"]["positions_count"] == 1
    assert payload["scenario_rows"][0]["hypothetical_quantity"] == 0.0
    assert "removed" in str(payload["scenario_rows"][0]["coverage_reason"]).lower()


def test_portfolio_whatif_service_rejects_zero_only_scenarios(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_portfolio_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_account_context",
        lambda **kwargs: (_package(), _account(), _live_positions()),
    )

    with pytest.raises(ValueError, match="At least one non-zero"):
        cpar_portfolio_whatif_service.load_cpar_portfolio_whatif_payload(
            account_id="acct_main",
            mode="factor_neutral",
            scenario_rows=[{"ric": "AAPL.OQ", "ticker": "AAPL", "quantity_delta": 0.0}],
        )


def test_portfolio_whatif_service_rejects_ticker_ric_mismatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_portfolio_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_account_context",
        lambda **kwargs: (_package(), _account(), _live_positions()),
    )

    with pytest.raises(ValueError, match="currently maps that ric to ticker"):
        cpar_portfolio_whatif_service.load_cpar_portfolio_whatif_payload(
            account_id="acct_main",
            mode="factor_neutral",
            scenario_rows=[{"ric": "AAPL.OQ", "ticker": "MSFT", "quantity_delta": 1.0}],
        )


def test_portfolio_whatif_service_rejects_new_rows_outside_active_package(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_portfolio_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_account_context",
        lambda **kwargs: (_package(), _account(), _live_positions()),
    )
    monkeypatch.setattr(
        cpar_portfolio_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_support_rows",
        lambda **kwargs: (_fit_by_ric(), _price_by_ric(), _covariance_rows()),
    )

    with pytest.raises(ValueError, match="active cPAR package"):
        cpar_portfolio_whatif_service.load_cpar_portfolio_whatif_payload(
            account_id="acct_main",
            mode="factor_neutral",
            scenario_rows=[{"ric": "AMD.OQ", "ticker": "AMD", "quantity_delta": 4.0}],
        )


def test_portfolio_whatif_service_propagates_typed_snapshot_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_portfolio_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_account_context",
        lambda **kwargs: (_ for _ in ()).throw(cpar_meta_service.CparReadUnavailable("Holdings read failed")),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="Holdings read failed"):
        cpar_portfolio_whatif_service.load_cpar_portfolio_whatif_payload(
            account_id="acct_main",
            mode="factor_neutral",
            scenario_rows=[{"ric": "AAPL.OQ", "ticker": "AAPL", "quantity_delta": 1.0}],
        )


def test_portfolio_whatif_service_does_not_swallow_unexpected_snapshot_bugs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpar_portfolio_whatif_service.cpar_portfolio_snapshot_service,
        "load_cpar_portfolio_account_context",
        lambda **kwargs: (_ for _ in ()).throw(ValueError("bad snapshot state")),
    )

    with pytest.raises(ValueError, match="bad snapshot state"):
        cpar_portfolio_whatif_service.load_cpar_portfolio_whatif_payload(
            account_id="acct_main",
            mode="factor_neutral",
            scenario_rows=[{"ric": "AAPL.OQ", "ticker": "AAPL", "quantity_delta": 1.0}],
        )
