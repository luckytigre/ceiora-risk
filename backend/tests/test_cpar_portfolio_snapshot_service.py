from __future__ import annotations

import pytest

from backend.data import cpar_outputs, cpar_source_reads
from backend.services import cpar_meta_service, cpar_portfolio_snapshot_service


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
    }


def test_account_context_maps_typed_holdings_failures_to_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda: (_ for _ in ()).throw(cpar_portfolio_snapshot_service.holdings_reads.HoldingsReadError("neon unavailable")),
    )

    with pytest.raises(cpar_meta_service.CparReadUnavailable, match="Holdings read failed"):
        cpar_portfolio_snapshot_service.load_cpar_portfolio_account_context(account_id="acct_main")


def test_account_context_does_not_swallow_unexpected_holdings_bugs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda: (_ for _ in ()).throw(ValueError("bad holdings row shape")),
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
        lambda: [{"account_id": " ACCT_MAIN ", "account_name": "Main", "positions_count": 1}],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_positions",
        lambda *, account_id: [{"account_id": "acct_main", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 1.0}],
    )

    package, account, positions = cpar_portfolio_snapshot_service.load_cpar_portfolio_account_context(
        account_id="acct_main"
    )

    assert package["package_run_id"] == "run_curr"
    assert account["account_id"] == " ACCT_MAIN "
    assert positions[0]["ric"] == "AAPL.OQ"


def test_aggregate_context_aggregates_positions_across_accounts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpar_meta_service, "require_active_package", lambda **kwargs: _package())
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_holdings_accounts",
        lambda: [
            {"account_id": "acct_a", "account_name": "Account A", "positions_count": 2},
            {"account_id": "acct_b", "account_name": "Account B", "positions_count": 2},
        ],
    )
    monkeypatch.setattr(
        cpar_portfolio_snapshot_service.holdings_reads,
        "load_all_holdings_positions",
        lambda: [
            {"account_id": "acct_a", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": 10.0, "updated_at": "2026-03-14T10:00:00Z"},
            {"account_id": "acct_b", "ric": "AAPL.OQ", "ticker": "AAPL", "quantity": -4.0, "updated_at": "2026-03-14T11:00:00Z"},
            {"account_id": "acct_b", "ric": "MSFT.OQ", "ticker": "MSFT", "quantity": 5.0, "updated_at": "2026-03-14T09:00:00Z"},
            {"account_id": "acct_a", "ric": "FLAT.OQ", "ticker": "FLAT", "quantity": 1.0, "updated_at": None},
            {"account_id": "acct_b", "ric": "FLAT.OQ", "ticker": "FLAT", "quantity": -1.0, "updated_at": None},
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
