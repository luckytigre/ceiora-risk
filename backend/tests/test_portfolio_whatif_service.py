from __future__ import annotations

from dataclasses import replace

import pandas as pd

from backend.services import cuse4_portfolio_whatif
from backend.services import portfolio_whatif
from backend.services import neon_holdings


def _deps(**overrides) -> portfolio_whatif.PortfolioWhatIfDependencies:
    return replace(
        portfolio_whatif.get_portfolio_whatif_dependencies(),
        **overrides,
    )


def test_portfolio_whatif_legacy_shim_reexports_supported_contract() -> None:
    assert portfolio_whatif.PortfolioWhatIfDependencies is cuse4_portfolio_whatif.PortfolioWhatIfDependencies
    assert portfolio_whatif.config is cuse4_portfolio_whatif.config
    assert sorted(portfolio_whatif.__all__) == [
        "PortfolioWhatIfDependencies",
        "config",
        "get_portfolio_whatif_dependencies",
        "preview_portfolio_whatif",
    ]


def test_portfolio_whatif_legacy_shim_preview_uses_legacy_default_dependencies(
    monkeypatch,
) -> None:
    sentinel_dependencies = _deps(
        current_payload_loader=lambda key: {
            "portfolio": {
                "source_dates": {"exposures_served_asof": "2026-03-03"},
                "snapshot_id": "shim_snap",
                "run_id": "shim_run",
            },
            "universe_loadings": {
                "by_ticker": {
                    "AAA": {
                        "ticker": "AAA",
                        "name": "AAA",
                        "price": 10.0,
                        "exposures": {"Beta": 1.0},
                        "specific_var": 0.01,
                        "specific_vol": 0.1,
                        "model_status": "core_estimated",
                        "eligibility_reason": "",
                        "trbc_economic_sector_short": "Technology",
                        "trbc_economic_sector_short_abbr": "Tech",
                        "trbc_industry_group": "Software",
                    },
                },
                "source_dates": {"exposures_served_asof": "2026-03-03"},
                "snapshot_id": "shim_snap",
                "run_id": "shim_run",
            },
            "risk_engine_cov": {"factors": ["Beta"], "matrix": [[0.04]]},
            "risk_engine_specific_risk": {"AAA.OQ": {"ticker": "AAA", "specific_var": 0.01}},
        }.get(key),
        holdings_loader=lambda account_id=None: [
            {"account_id": "acct_a", "ticker": "AAA", "ric": "AAA.N", "quantity": 10.0, "source": "ui_edit"},
        ],
    )
    monkeypatch.setattr(
        portfolio_whatif,
        "get_portfolio_whatif_dependencies",
        lambda: sentinel_dependencies,
    )

    out = portfolio_whatif.preview_portfolio_whatif(
        scenario_rows=[{"account_id": "acct_a", "ticker": "AAA", "quantity": 5.0}],
    )

    assert out["serving_snapshot"]["snapshot_id"] == "shim_snap"
    assert out["source_dates"]["exposures_served_asof"] == "2026-03-03"
    assert out["current"]["position_count"] == 1


def test_preview_portfolio_whatif_projects_current_and_hypothetical_without_writes() -> None:
    universe_loadings = {
        "by_ticker": {
            "AAA": {
                "ticker": "AAA",
                "name": "AAA",
                "price": 10.0,
                "exposures": {"Beta": 1.0, "Book-to-Price": 0.5},
                "specific_var": 0.01,
                "specific_vol": 0.1,
                "model_status": "core_estimated",
                "eligibility_reason": "",
                "trbc_economic_sector_short": "Technology",
                "trbc_economic_sector_short_abbr": "Tech",
                "trbc_industry_group": "Software",
            },
            "BBB": {
                "ticker": "BBB",
                "name": "BBB",
                "price": 20.0,
                "exposures": {"Beta": 0.8, "Book-to-Price": -0.2},
                "specific_var": 0.02,
                "specific_vol": 0.1414,
                "model_status": "core_estimated",
                "eligibility_reason": "",
                "trbc_economic_sector_short": "Financials",
                "trbc_economic_sector_short_abbr": "Fins",
                "trbc_industry_group": "Banks",
            },
        },
        "source_dates": {
            "prices_asof": "2026-03-03",
            "fundamentals_asof": "2026-03-03",
            "classification_asof": "2026-03-03",
            "exposures_asof": "2026-03-03",
        },
    }
    cov = pd.DataFrame(
        [[0.04, 0.01], [0.01, 0.09]],
        index=["Beta", "Book-to-Price"],
        columns=["Beta", "Book-to-Price"],
    )
    live_rows = [
        {"account_id": "acct_a", "ticker": "AAA", "ric": "AAA.N", "quantity": 10.0, "source": "ui_edit"},
        {"account_id": "acct_b", "ticker": "BBB", "ric": "BBB.N", "quantity": 5.0, "source": "ui_edit"},
    ]

    out = portfolio_whatif.preview_portfolio_whatif(
        scenario_rows=[
            {"account_id": "acct_a", "ticker": "AAA", "quantity": 20.0},
            {"account_id": "acct_b", "ticker": "BBB", "quantity": -5.0},
        ],
        dependencies=_deps(
            current_payload_loader=lambda key: {
                "portfolio": {
                    "source_dates": {
                        "prices_asof": "2026-03-03",
                        "exposures_asof": "2026-03-04",
                        "exposures_latest_available_asof": "2026-03-04",
                        "exposures_served_asof": "2026-03-03",
                    },
                    "run_id": "run_meta_1",
                    "snapshot_id": "snap_meta_1",
                    "refresh_started_at": "2026-03-15T14:00:00+00:00",
                },
                "risk_engine_cov": {
                    "factors": ["Beta", "Book-to-Price"],
                    "matrix": [[0.04, 0.01], [0.01, 0.09]],
                },
                "risk_engine_specific_risk": {
                    "AAA.OQ": {"ticker": "AAA", "specific_var": 0.01},
                    "BBB.OQ": {"ticker": "BBB", "specific_var": 0.02},
                },
            }.get(key),
            holdings_loader=lambda account_id=None: live_rows,
            universe_loader=lambda current_payload, **kwargs: universe_loadings,
            covariance_loader=lambda current_payload, **kwargs: (cov, True),
            specific_risk_loader=lambda current_payload, **kwargs: (
                {
                    "AAA": {"specific_var": 0.01},
                    "BBB": {"specific_var": 0.02},
                },
                True,
            ),
        ),
    )

    assert out["_preview_only"] is True
    assert out["current"]["position_count"] == 2
    assert out["hypothetical"]["position_count"] == 1
    assert out["current"]["total_value"] == 200.0
    assert out["hypothetical"]["total_value"] == 300.0
    assert len(out["holding_deltas"]) == 2
    assert out["holding_deltas"][0]["account_id"] == "acct_a"
    assert out["holding_deltas"][0]["hypothetical_quantity"] == 30.0
    assert "raw" in out["hypothetical"]["exposure_modes"]
    assert "risk_contribution" in out["diff"]["factor_deltas"]
    assert out["serving_snapshot"]["snapshot_id"] == "snap_meta_1"
    assert out["source_dates"]["exposures_served_asof"] == "2026-03-03"
    assert out["truth_surface"] == "live_holdings_projected_through_current_served_model"


def test_preview_portfolio_whatif_prefers_published_risk_payloads() -> None:
    published_payloads = {
        "portfolio": {
            "source_dates": {"exposures_served_asof": "2026-03-03"},
            "snapshot_id": "snap_meta_2",
            "run_id": "run_meta_2",
        },
        "universe_loadings": {
            "by_ticker": {
                "AAA": {
                    "ticker": "AAA",
                    "name": "AAA",
                    "price": 10.0,
                    "exposures": {"Beta": 1.0},
                    "specific_var": 0.01,
                    "specific_vol": 0.1,
                    "model_status": "core_estimated",
                    "eligibility_reason": "",
                    "trbc_economic_sector_short": "Technology",
                    "trbc_economic_sector_short_abbr": "Tech",
                    "trbc_industry_group": "Software",
                },
            },
            "source_dates": {"exposures_served_asof": "2026-03-03"},
            "snapshot_id": "snap_meta_2",
            "run_id": "run_meta_2",
        },
        "risk_engine_cov": {"factors": ["Beta"], "matrix": [[0.04]]},
        "risk_engine_specific_risk": {"AAA.OQ": {"ticker": "AAA", "specific_var": 0.01}},
    }

    out = portfolio_whatif.preview_portfolio_whatif(
        scenario_rows=[{"account_id": "acct_a", "ticker": "AAA", "quantity": 5.0}],
        dependencies=_deps(
            current_payload_loader=lambda key: published_payloads.get(key),
            live_cache_loader=lambda key: {
                "factors": ["Wrong"],
                "matrix": [[9.0]],
            } if key == "risk_engine_cov" else {
                "AAA.OQ": {"ticker": "AAA", "specific_var": 9.0},
            },
            runtime_cache_loader=lambda key: {},
            holdings_loader=lambda account_id=None: [
                {"account_id": "acct_a", "ticker": "AAA", "ric": "AAA.N", "quantity": 10.0, "source": "ui_edit"},
            ],
        ),
    )

    assert out["serving_snapshot"]["snapshot_id"] == "snap_meta_2"
    assert out["source_dates"]["exposures_served_asof"] == "2026-03-03"
    assert out["current"]["position_count"] == 1
    assert out["current"]["positions"][0]["ticker"] == "AAA"
    assert out["truth_surface"] == "live_holdings_projected_through_current_served_model"


def test_preview_portfolio_whatif_labels_live_risk_cache_fallback(monkeypatch) -> None:
    monkeypatch.setattr(portfolio_whatif.config, "DATA_BACKEND", "sqlite")
    out = portfolio_whatif.preview_portfolio_whatif(
        scenario_rows=[{"account_id": "acct_a", "ticker": "AAA", "quantity": 5.0}],
        dependencies=_deps(
            current_payload_loader=lambda key: {
                "portfolio": {
                    "source_dates": {"exposures_served_asof": "2026-03-03"},
                    "snapshot_id": "snap_meta_3",
                    "run_id": "run_meta_3",
                },
                "universe_loadings": {
                    "by_ticker": {
                        "AAA": {
                            "ticker": "AAA",
                            "name": "AAA",
                            "price": 10.0,
                            "exposures": {"Beta": 1.0},
                            "specific_var": 0.01,
                            "specific_vol": 0.1,
                            "model_status": "core_estimated",
                            "eligibility_reason": "",
                            "trbc_economic_sector_short": "Technology",
                            "trbc_economic_sector_short_abbr": "Tech",
                            "trbc_industry_group": "Software",
                        },
                    },
                    "source_dates": {"exposures_served_asof": "2026-03-03"},
                    "snapshot_id": "snap_meta_3",
                    "run_id": "run_meta_3",
                },
            }.get(key),
            live_cache_loader=lambda key: {
                "factors": ["Beta"],
                "matrix": [[0.04]],
            } if key == "risk_engine_cov" else {
                "AAA.OQ": {"ticker": "AAA", "specific_var": 0.01},
            },
            runtime_cache_loader=lambda key: {},
            holdings_loader=lambda account_id=None: [
                {"account_id": "acct_a", "ticker": "AAA", "ric": "AAA.N", "quantity": 10.0, "source": "ui_edit"},
            ],
        ),
    )

    assert out["truth_surface"] == "live_holdings_projected_through_current_loadings_and_live_risk_cache"


def test_preview_portfolio_whatif_requires_account_id() -> None:
    try:
        portfolio_whatif.preview_portfolio_whatif(
            scenario_rows=[{"ticker": "AAA", "quantity": 10.0}],
        )
    except ValueError as exc:
        assert str(exc) == "Each what-if row requires account_id."
        return
    raise AssertionError("preview_portfolio_whatif should reject missing account_id")


def test_preview_portfolio_whatif_aggregates_duplicate_live_rows_by_account_and_ticker() -> None:
    universe_loadings = {
        "by_ticker": {
            "AAA": {
                "ticker": "AAA",
                "name": "AAA",
                "price": 10.0,
                "exposures": {"Beta": 1.0},
                "specific_var": 0.01,
                "specific_vol": 0.1,
                "model_status": "core_estimated",
                "eligibility_reason": "",
                "trbc_economic_sector_short": "Technology",
                "trbc_economic_sector_short_abbr": "Tech",
                "trbc_industry_group": "Software",
            },
        },
        "source_dates": {"prices_asof": "2026-03-03"},
    }
    cov = pd.DataFrame([[0.04]], index=["Beta"], columns=["Beta"])
    live_rows = [
        {"account_id": "acct_a", "ticker": "AAA", "ric": "AAA.N", "quantity": 10.0, "source": "ui_edit"},
        {"account_id": "acct_a", "ticker": "AAA", "ric": "AAA.K", "quantity": 5.0, "source": "ui_edit"},
    ]

    out = portfolio_whatif.preview_portfolio_whatif(
        scenario_rows=[{"account_id": "acct_a", "ticker": "AAA", "quantity": 20.0}],
        dependencies=_deps(
            holdings_loader=lambda account_id=None: live_rows,
            universe_loader=lambda current_payload, **kwargs: universe_loadings,
            covariance_loader=lambda current_payload, **kwargs: (cov, True),
            specific_risk_loader=lambda current_payload, **kwargs: ({"AAA": {"specific_var": 0.01}}, True),
        ),
    )

    assert out["current"]["position_count"] == 1
    assert out["current"]["positions"][0]["shares"] == 15.0
    assert out["holding_deltas"][0]["current_quantity"] == 15.0
    assert out["holding_deltas"][0]["hypothetical_quantity"] == 35.0
    assert out["current"]["positions"][0]["sleeve"] == "LIVE HOLDINGS"


def test_preview_portfolio_whatif_rejects_duplicate_scenario_rows() -> None:
    universe_loadings = {
        "by_ticker": {
            "AAA": {
                "ticker": "AAA",
                "name": "AAA",
                "price": 10.0,
                "exposures": {"Beta": 1.0},
                "specific_var": 0.01,
                "specific_vol": 0.1,
                "model_status": "core_estimated",
                "eligibility_reason": "",
                "trbc_economic_sector_short": "Technology",
                "trbc_economic_sector_short_abbr": "Tech",
                "trbc_industry_group": "Software",
            },
        },
        "source_dates": {"prices_asof": "2026-03-03"},
    }
    cov = pd.DataFrame([[0.04]], index=["Beta"], columns=["Beta"])

    try:
        portfolio_whatif.preview_portfolio_whatif(
            scenario_rows=[
                {"account_id": "acct_a", "ticker": "AAA", "quantity": 10.0},
                {"account_id": "acct_a", "ticker": "AAA", "quantity": 20.0},
            ],
            dependencies=_deps(
                holdings_loader=lambda account_id=None: [],
                universe_loader=lambda current_payload, **kwargs: universe_loadings,
                covariance_loader=lambda current_payload, **kwargs: (cov, True),
                specific_risk_loader=lambda current_payload, **kwargs: ({"AAA": {"specific_var": 0.01}}, True),
            ),
        )
    except ValueError as exc:
        assert "Duplicate what-if row" in str(exc)
        return
    raise AssertionError("duplicate scenario rows should be rejected")


class _FakeConn:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_apply_ticker_bucket_scenario_applies_delta_to_full_ticker_bucket(monkeypatch) -> None:
    conn = _FakeConn()
    deletes: list[str] = []
    upserts: list[tuple[str, float]] = []
    events: list[str] = []

    monkeypatch.setattr(neon_holdings, "_ensure_account", lambda *args, **kwargs: None)
    monkeypatch.setattr(neon_holdings, "_insert_batch", lambda *args, **kwargs: "batch_1")
    monkeypatch.setattr(neon_holdings, "_resolve_ticker_to_ric", lambda *args, **kwargs: ("AAA.N", ["AAA.K"]))
    monkeypatch.setattr(
        neon_holdings,
        "_load_current_positions_for_ticker",
        lambda *args, **kwargs: [
            {"account_id": "acct_a", "ric": "AAA.N", "ticker": "AAA", "quantity": 10.0, "source": "ui_edit"},
            {"account_id": "acct_a", "ric": "AAA.K", "ticker": "AAA", "quantity": 5.0, "source": "ui_edit"},
        ],
    )
    monkeypatch.setattr(
        neon_holdings,
        "_delete_position",
        lambda _conn, *, account_id, ric: deletes.append(f"{account_id}:{ric}"),
    )
    monkeypatch.setattr(
        neon_holdings,
        "_upsert_position",
        lambda _conn, *, account_id, ric, ticker, quantity, source, import_batch_id: upserts.append((f"{account_id}:{ric}:{ticker}:{source}:{import_batch_id}", float(quantity))),
    )
    monkeypatch.setattr(
        neon_holdings,
        "_insert_event",
        lambda *args, **kwargs: events.append(str(kwargs.get("event_type"))),
    )

    out = neon_holdings.apply_ticker_bucket_scenario(
        conn,
        scenario_rows=[{"account_id": "acct_a", "ticker": "AAA", "quantity": 20.0}],
        requested_by="tester",
    )

    assert out["status"] == "ok"
    assert out["accepted_rows"] == 1
    assert out["applied_deletes"] == 2
    assert out["applied_upserts"] == 1
    assert deletes == ["acct_a:AAA.N", "acct_a:AAA.K"]
    assert upserts == [("acct_a:AAA.N:AAA:what_if:batch_1", 35.0)]
    assert events.count("replace_ticker_bucket_delete") == 2
    assert events.count("replace_ticker_bucket_set") == 1
    assert conn.commits == 1


def test_apply_ticker_bucket_scenario_rejects_duplicate_payload_rows_without_mutation(monkeypatch) -> None:
    conn = _FakeConn()
    monkeypatch.setattr(neon_holdings, "_resolve_ticker_to_ric", lambda *args, **kwargs: ("AAA.N", []))
    delete_calls: list[str] = []
    monkeypatch.setattr(
        neon_holdings,
        "_delete_position",
        lambda _conn, *, account_id, ric: delete_calls.append(f"{account_id}:{ric}"),
    )

    out = neon_holdings.apply_ticker_bucket_scenario(
        conn,
        scenario_rows=[
            {"account_id": "acct_a", "ticker": "AAA", "quantity": 10.0},
            {"account_id": "acct_a", "ticker": "AAA", "quantity": 20.0},
        ],
        requested_by="tester",
    )

    assert out["status"] == "rejected"
    assert out["accepted_rows"] == 1
    assert out["rejected_rows"] == 1
    assert out["rejection_counts"]["duplicate_row_in_file"] == 1
    assert delete_calls == []
    assert conn.commits == 0
