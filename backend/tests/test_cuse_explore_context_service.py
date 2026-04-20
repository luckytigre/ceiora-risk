from __future__ import annotations

from types import SimpleNamespace

from backend.services import cuse4_explore_context_service


def test_cuse_explore_context_service_builds_scoped_positions_from_holdings_and_loadings(monkeypatch) -> None:
    dependencies = SimpleNamespace(
        current_payload_loader=lambda payload_name: {
            "portfolio": {
                "source_dates": {"prices_asof": "2026-04-18"},
                "run_id": "run_live",
                "snapshot_id": "snap_live",
                "refresh_started_at": "2026-04-18T09:30:00Z",
            },
        }.get(payload_name),
        runtime_cache_loader=lambda payload_name: None,
        universe_loader=lambda *args, **kwargs: {
            "by_ticker": {
                "AAPL": {"ticker": "AAPL", "price": 200.0},
                "MSFT": {"ticker": "MSFT", "price": 100.0},
            },
            "run_id": "run_live",
            "snapshot_id": "snap_live",
            "refresh_started_at": "2026-04-18T09:30:00Z",
        },
    )
    monkeypatch.setattr(
        cuse4_explore_context_service.cuse4_portfolio_whatif,
        "get_portfolio_whatif_dependencies",
        lambda: dependencies,
    )
    monkeypatch.setattr(
        cuse4_explore_context_service.cuse4_holdings_service,
        "load_holdings_positions",
        lambda account_id, allowed_account_ids=None: [
            {"account_id": "acct_a", "ticker": "AAPL", "quantity": 3.0, "source": "manual"},
            {"account_id": "acct_a", "ticker": "MSFT", "quantity": -2.0, "source": "manual"},
        ],
    )

    payload = cuse4_explore_context_service.load_cuse_explore_context_payload(
        account_id="acct_a",
        allowed_account_ids=("acct_a",),
    )

    assert payload["_account_scoped"] is True
    assert payload["account_id"] == "acct_a"
    assert payload["_cached"] is False
    assert payload["run_id"] == "run_live"
    assert payload["held_positions"] == [
        {
            "ticker": "AAPL",
            "shares": 3.0,
            "weight": 0.75,
            "market_value": 600.0,
            "long_short": "LONG",
            "price": 200.0,
        },
        {
            "ticker": "MSFT",
            "shares": -2.0,
            "weight": -0.25,
            "market_value": -200.0,
            "long_short": "SHORT",
            "price": 100.0,
        },
    ]

