from __future__ import annotations

from backend.analytics.services.risk_views import build_positions_from_snapshot


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
    assert positions[0]["eligibility_reason"] == "missing_factor_exposures"
    assert positions[0]["exposures"] == {}
