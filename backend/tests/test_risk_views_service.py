from __future__ import annotations

from backend.analytics.services.risk_views import build_positions_from_snapshot, compute_exposures_modes


def test_build_positions_from_snapshot_downgrades_empty_exposure_rows() -> None:
    positions, total_value = build_positions_from_snapshot(
        universe_by_ticker={
            "LAZ": {
                "ticker": "LAZ",
                "name": "Lazard",
                "price": 48.39,
                "model_status": "core_estimated",
                "eligibility_reason": "",
                "exposures": {},
                "specific_var": 0.01,
                "specific_vol": 0.1,
            }
        },
        shares_map={"LAZ": -200.0},
    )

    assert total_value == -9678.0
    assert len(positions) == 1
    assert positions[0]["model_status"] == "ineligible"
    assert positions[0]["model_status_reason"] == "missing_factor_exposures"
    assert positions[0]["eligibility_reason"] == "missing_factor_exposures"
    assert positions[0]["exposures"] == {}


def test_compute_exposures_modes_emits_canonical_factor_coverage_asof_alias() -> None:
    out = compute_exposures_modes(
        positions=[
            {
                "ticker": "AAPL",
                "weight": 1.0,
                "exposures": {"style_beta_score": 1.25},
            }
        ],
        cov=None,
        factor_details=[
            {
                "factor_id": "style_beta_score",
                "exposure": 1.25,
                "factor_vol": 0.2,
                "sensitivity": 0.25,
                "marginal_var_contrib": 0.0,
                "pct_of_total": 5.0,
            }
        ],
        factor_coverage={
            "style_beta_score": {
                "cross_section_n": 3000,
                "eligible_n": 2800,
                "coverage_pct": 0.9333,
            }
        },
        factor_coverage_asof="2026-03-13",
    )

    raw_row = out["raw"][0]
    assert raw_row["factor_coverage_asof"] == "2026-03-13"
    assert raw_row["coverage_date"] == "2026-03-13"
