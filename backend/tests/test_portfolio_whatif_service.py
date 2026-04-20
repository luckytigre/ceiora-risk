from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest

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


def test_preview_portfolio_whatif_scopes_current_book_to_staged_accounts() -> None:
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
            "BBB": {
                "ticker": "BBB",
                "name": "BBB",
                "price": 20.0,
                "exposures": {"Beta": 0.8},
                "specific_var": 0.02,
                "specific_vol": 0.1414,
                "model_status": "core_estimated",
                "eligibility_reason": "",
                "trbc_economic_sector_short": "Financials",
                "trbc_economic_sector_short_abbr": "Fins",
                "trbc_industry_group": "Banks",
            },
        },
    }
    cov = pd.DataFrame([[0.04]], index=["Beta"], columns=["Beta"])
    live_rows = [
        {"account_id": "acct_a", "ticker": "AAA", "ric": "AAA.N", "quantity": 10.0, "source": "ui_edit"},
        {"account_id": "acct_b", "ticker": "BBB", "ric": "BBB.N", "quantity": 5.0, "source": "ui_edit"},
    ]

    out = portfolio_whatif.preview_portfolio_whatif(
        scenario_rows=[{"account_id": "acct_a", "ticker": "AAA", "quantity": 5.0}],
        dependencies=_deps(
            current_payload_loader=lambda key: {
                "portfolio": {
                    "source_dates": {"exposures_served_asof": "2026-03-03"},
                    "run_id": "run_meta_scope",
                    "snapshot_id": "snap_meta_scope",
                },
                "risk_engine_cov": {"factors": ["Beta"], "matrix": [[0.04]]},
                "risk_engine_specific_risk": {
                    "AAA.OQ": {"ticker": "AAA", "specific_var": 0.01},
                    "BBB.OQ": {"ticker": "BBB", "specific_var": 0.02},
                },
            }.get(key),
            holdings_loader=lambda account_id=None: live_rows,
            universe_loader=lambda current_payload, **kwargs: universe_loadings,
            covariance_loader=lambda current_payload, **kwargs: (cov, True),
            specific_risk_loader=lambda current_payload, **kwargs: (
                {"AAA": {"specific_var": 0.01}, "BBB": {"specific_var": 0.02}},
                True,
            ),
        ),
    )

    assert out["preview_scope"]["kind"] == "staged_accounts"
    assert out["preview_scope"]["account_ids"] == ["acct_a"]
    assert out["current"]["position_count"] == 1
    assert [row["ticker"] for row in out["current"]["positions"]] == ["AAA"]
    assert out["hypothetical"]["position_count"] == 1
    assert out["holding_deltas"][0]["account_id"] == "acct_a"


def test_preview_portfolio_whatif_can_limit_exposure_modes() -> None:
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
    }
    cov = pd.DataFrame([[0.04]], index=["Beta"], columns=["Beta"])

    out = portfolio_whatif.preview_portfolio_whatif(
        scenario_rows=[],
        requested_exposure_modes=("raw",),
        dependencies=_deps(
            current_payload_loader=lambda key: {
                "portfolio": {
                    "source_dates": {"exposures_served_asof": "2026-03-03"},
                    "run_id": "run_meta_3",
                    "snapshot_id": "snap_meta_3",
                },
                "risk_engine_cov": {"factors": ["Beta"], "matrix": [[0.04]]},
                "risk_engine_specific_risk": {
                    "AAA.OQ": {"ticker": "AAA", "specific_var": 0.01},
                },
            }.get(key),
            holdings_loader=lambda account_id=None: [
                {"account_id": "acct_a", "ticker": "AAA", "ric": "AAA.N", "quantity": 10.0, "source": "ui_edit"},
            ],
            universe_loader=lambda current_payload, **kwargs: universe_loadings,
            covariance_loader=lambda current_payload, **kwargs: (cov, True),
            specific_risk_loader=lambda current_payload, **kwargs: (
                {"AAA": {"specific_var": 0.01}},
                True,
            ),
        ),
    )

    assert sorted(out["current"]["exposure_modes"].keys()) == ["raw"]
    assert sorted(out["hypothetical"]["exposure_modes"].keys()) == ["raw"]
    assert out["diff"]["factor_deltas"]["raw"][0]["factor_id"] == "Beta"
    assert out["diff"]["factor_deltas"]["raw"][0]["delta"] == 0.0
    assert out["diff"]["factor_deltas"]["sensitivity"] == []
    assert out["diff"]["factor_deltas"]["risk_contribution"] == []


def test_preview_portfolio_whatif_includes_vol_scaled_shares() -> None:
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
    }
    cov = pd.DataFrame([[0.04]], index=["Beta"], columns=["Beta"])

    out = portfolio_whatif.preview_portfolio_whatif(
        scenario_rows=[],
        dependencies=_deps(
            current_payload_loader=lambda key: {
                "portfolio": {
                    "source_dates": {"exposures_served_asof": "2026-03-03"},
                    "run_id": "run_meta_vs",
                    "snapshot_id": "snap_meta_vs",
                },
                "risk_engine_cov": {"factors": ["Beta"], "matrix": [[0.04]]},
                "risk_engine_specific_risk": {
                    "AAA.OQ": {"ticker": "AAA", "specific_var": 0.01},
                },
            }.get(key),
            holdings_loader=lambda account_id=None: [
                {"account_id": "acct_a", "ticker": "AAA", "ric": "AAA.N", "quantity": 10.0, "source": "ui_edit"},
            ],
            universe_loader=lambda current_payload, **kwargs: universe_loadings,
            covariance_loader=lambda current_payload, **kwargs: (cov, True),
            specific_risk_loader=lambda current_payload, **kwargs: (
                {"AAA": {"specific_var": 0.01}},
                True,
            ),
        ),
    )

    current_shares = out["current"]["vol_scaled_shares"]
    assert pytest.approx(sum(current_shares.values()), abs=0.05) == 100.0
    assert current_shares["style"] > 0.0
    assert current_shares["idio"] > 0.0


def test_preview_portfolio_whatif_scoped_snapshot_and_lazy_modes_stay_populated() -> None:
    universe_loadings = {
        "by_ticker": {
            "AAA": {
                "ticker": "AAA",
                "name": "AAA",
                "price": 10.0,
                "exposures": {"Beta": 1.0, "Value": 0.5},
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
                "exposures": {"Beta": 0.8, "Value": -0.2},
                "specific_var": 0.02,
                "specific_vol": 0.1414,
                "model_status": "core_estimated",
                "eligibility_reason": "",
                "trbc_economic_sector_short": "Financials",
                "trbc_economic_sector_short_abbr": "Fins",
                "trbc_industry_group": "Banks",
            },
        },
        "factor_catalog": [
            {"factor_id": "Beta", "factor_name": "Beta"},
            {"factor_id": "Value", "factor_name": "Value"},
        ],
        "source_dates": {"exposures_served_asof": "2026-03-03"},
        "snapshot_id": "snap_scope",
        "run_id": "run_scope",
    }
    cov = pd.DataFrame(
        [[0.04, 0.01], [0.01, 0.09]],
        index=["Beta", "Value"],
        columns=["Beta", "Value"],
    )
    deps = _deps(
        current_payload_loader=lambda key: {
            "portfolio": {
                "source_dates": {"exposures_served_asof": "2026-03-03"},
                "run_id": "run_scope",
                "snapshot_id": "snap_scope",
            },
            "universe_loadings": universe_loadings,
            "risk_engine_cov": {"factors": ["Beta", "Value"], "matrix": [[0.04, 0.01], [0.01, 0.09]]},
            "risk_engine_specific_risk": {
                "AAA.OQ": {"ticker": "AAA", "specific_var": 0.01},
                "BBB.OQ": {"ticker": "BBB", "specific_var": 0.02},
            },
        }.get(key),
        runtime_cache_loader=lambda key: {},
        live_cache_loader=lambda key: {},
        holdings_loader=lambda account_id=None, allowed_account_ids=None: [
            {"account_id": "acct_a", "ticker": "AAA", "ric": "AAA.N", "quantity": 10.0, "source": "ui_edit"},
            {"account_id": "acct_a", "ticker": "BBB", "ric": "BBB.N", "quantity": 5.0, "source": "ui_edit"},
        ],
        universe_loader=lambda current_payload, **kwargs: universe_loadings,
        covariance_loader=lambda current_payload, **kwargs: (cov, True),
        specific_risk_loader=lambda current_payload, **kwargs: (
            {"AAA": {"specific_var": 0.01}, "BBB": {"specific_var": 0.02}},
            True,
        ),
    )

    summary = cuse4_portfolio_whatif.preview_portfolio_whatif(
        scenario_rows=[],
        account_id="acct_a",
        allowed_account_ids=("acct_a",),
        requested_exposure_modes=("raw",),
        dependencies=deps,
    )
    sensitivity = cuse4_portfolio_whatif.preview_portfolio_whatif(
        scenario_rows=[],
        account_id="acct_a",
        allowed_account_ids=("acct_a",),
        requested_exposure_modes=("sensitivity",),
        dependencies=deps,
    )
    risk_contribution = cuse4_portfolio_whatif.preview_portfolio_whatif(
        scenario_rows=[],
        account_id="acct_a",
        allowed_account_ids=("acct_a",),
        requested_exposure_modes=("risk_contribution",),
        dependencies=deps,
    )

    current_shares = summary["current"]["vol_scaled_shares"]
    assert pytest.approx(sum(current_shares.values()), abs=0.05) == 100.0
    assert current_shares["style"] > 0.0
    assert current_shares["idio"] > 0.0
    assert len(summary["current"]["exposure_modes"]["raw"]) == 2
    assert len(sensitivity["current"]["exposure_modes"]["sensitivity"]) == 2
    assert len(risk_contribution["current"]["exposure_modes"]["risk_contribution"]) == 2


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


def test_preview_portfolio_whatif_rejects_tickers_without_published_modeled_surface() -> None:
    universe_loadings = {
        "by_ticker": {
            "SPY": {
                "ticker": "SPY",
                "name": "SPY",
                "price": 610.0,
                "exposures": {},
                "model_status": "projected_only",
                "exposure_origin": "projected_returns",
                "served_exposure_available": False,
                "projection_output_status": "unavailable",
            },
        },
        "source_dates": {"prices_asof": "2026-03-03"},
    }
    cov = pd.DataFrame([[0.04]], index=["Beta"], columns=["Beta"])

    try:
        portfolio_whatif.preview_portfolio_whatif(
            scenario_rows=[{"account_id": "acct_a", "ticker": "SPY", "quantity": 10.0}],
            dependencies=_deps(
                holdings_loader=lambda account_id=None: [],
                universe_loader=lambda current_payload, **kwargs: universe_loadings,
                covariance_loader=lambda current_payload, **kwargs: (cov, True),
                specific_risk_loader=lambda current_payload, **kwargs: ({}, True),
            ),
        )
    except ValueError as exc:
        assert "currently published cUSE modeled surface" in str(exc)
        assert "SPY" in str(exc)
        return
    raise AssertionError("preview_portfolio_whatif should reject registry-visible but unmodeled names")


def test_preview_portfolio_whatif_accepts_held_name_from_published_portfolio_overlay() -> None:
    universe_loadings = {
        "by_ticker": {},
        "source_dates": {"prices_asof": "2026-03-03"},
    }
    portfolio_payload = {
        "positions": [
            {
                "account": "acct_a",
                "ticker": "COST",
                "name": "Costco Wholesale Corp",
                "price": 100.0,
                "shares": 100.0,
                "market_value": 10000.0,
                "weight": 1.0,
                "exposures": {"market": 1.0, "style_growth_score": -0.5},
                "specific_var": 0.04,
                "specific_vol": 0.2,
                "model_status": "core_estimated",
                "eligibility_reason": "",
                "model_status_reason": "",
                "exposure_origin": "native",
                "served_exposure_available": True,
                "trbc_economic_sector_short": "Consumer Cyclicals",
                "trbc_economic_sector_short_abbr": "ConsCyc",
                "trbc_industry_group": "Retail",
            }
        ],
        "source_dates": {"exposures_served_asof": "2026-03-03"},
        "snapshot_id": "snap_held",
        "run_id": "run_held",
    }
    cov = pd.DataFrame(
        [[0.04, 0.01], [0.01, 0.09]],
        index=["market", "style_growth_score"],
        columns=["market", "style_growth_score"],
    )
    live_rows = [
        {"account_id": "acct_a", "ticker": "COST", "quantity": 100.0, "source": "neon_holdings"},
    ]

    out = portfolio_whatif.preview_portfolio_whatif(
        scenario_rows=[{"account_id": "acct_a", "ticker": "COST", "quantity": -50.0}],
        dependencies=_deps(
            current_payload_loader=lambda key: {
                "portfolio": portfolio_payload,
                "universe_loadings": universe_loadings,
                "risk_engine_cov": {
                    "factors": ["market", "style_growth_score"],
                    "matrix": [[0.04, 0.01], [0.01, 0.09]],
                },
                "risk_engine_specific_risk": {
                    "COST": {"ticker": "COST", "specific_var": 0.04},
                },
            }.get(key),
            holdings_loader=lambda account_id=None, allowed_account_ids=None: list(live_rows),
            universe_loader=lambda current_payload, **kwargs: current_payload,
            covariance_loader=lambda current_payload, **kwargs: (cov, True),
            specific_risk_loader=lambda current_payload, **kwargs: ({"COST": {"specific_var": 0.04}}, True),
        ),
    )

    assert out["current"]["position_count"] == 1
    assert out["hypothetical"]["position_count"] == 1
    assert out["current"]["positions"][0]["ticker"] == "COST"
    assert out["holding_deltas"][0]["ticker"] == "COST"
    assert out["current"]["exposure_modes"]["raw"][0]["factor_id"] == "market"


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


def test_apply_ticker_bucket_scenario_rejects_zero_delta_rows_without_mutation(monkeypatch) -> None:
    conn = _FakeConn()
    delete_calls: list[str] = []
    monkeypatch.setattr(
        neon_holdings,
        "_delete_position",
        lambda _conn, *, account_id, ric: delete_calls.append(f"{account_id}:{ric}"),
    )

    out = neon_holdings.apply_ticker_bucket_scenario(
        conn,
        scenario_rows=[{"account_id": "acct_a", "ticker": "AAA", "quantity": 0.0}],
        requested_by="tester",
    )

    assert out["status"] == "rejected"
    assert out["accepted_rows"] == 0
    assert out["rejected_rows"] == 1
    assert out["rejection_counts"]["zero_quantity"] == 1
    assert delete_calls == []
    assert conn.commits == 0


def test_apply_ticker_bucket_scenario_rejects_alias_rows_that_resolve_to_same_ric(monkeypatch) -> None:
    conn = _FakeConn()
    delete_calls: list[str] = []

    def _resolve(*_args, **kwargs):
        ticker = kwargs.get("ticker") if "ticker" in kwargs else _args[1]
        return ("AAA.N", []) if ticker in {"AAA", "AAA.US"} else (None, [])

    monkeypatch.setattr(neon_holdings, "_resolve_ticker_to_ric", _resolve)
    monkeypatch.setattr(
        neon_holdings,
        "_delete_position",
        lambda _conn, *, account_id, ric: delete_calls.append(f"{account_id}:{ric}"),
    )

    out = neon_holdings.apply_ticker_bucket_scenario(
        conn,
        scenario_rows=[
            {"account_id": "acct_a", "ticker": "AAA", "quantity": 10.0},
            {"account_id": "acct_a", "ticker": "AAA.US", "quantity": 20.0},
        ],
        requested_by="tester",
    )

    assert out["status"] == "rejected"
    assert out["accepted_rows"] == 1
    assert out["rejected_rows"] == 1
    assert out["rejection_counts"]["duplicate_resolved_instrument"] == 1
    assert delete_calls == []
    assert conn.commits == 0


def test_apply_ticker_bucket_scenario_rejects_mismatched_ticker_and_ric(monkeypatch) -> None:
    conn = _FakeConn()
    delete_calls: list[str] = []

    monkeypatch.setattr(
        neon_holdings,
        "_ric_exists",
        lambda *_args, **_kwargs: (True, "AAA"),
    )
    monkeypatch.setattr(
        neon_holdings,
        "_delete_position",
        lambda _conn, *, account_id, ric: delete_calls.append(f"{account_id}:{ric}"),
    )

    out = neon_holdings.apply_ticker_bucket_scenario(
        conn,
        scenario_rows=[
            {"account_id": "acct_a", "ticker": "BBB", "ric": "AAA.N", "quantity": 10.0},
        ],
        requested_by="tester",
    )

    assert out["status"] == "rejected"
    assert out["accepted_rows"] == 0
    assert out["rejected_rows"] == 1
    assert out["rejection_counts"]["identifier_mismatch"] == 1
    assert delete_calls == []
    assert conn.commits == 0
