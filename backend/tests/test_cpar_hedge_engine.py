import math

import pytest

from backend.cpar.hedge_engine import (
    build_factor_neutral_hedge,
    build_factor_neutral_recommendation,
    build_market_neutral_hedge,
)


def _identity_covariance(factor_ids: list[str]) -> dict[tuple[str, str], float]:
    covariance: dict[tuple[str, str], float] = {}
    for left in factor_ids:
        for right in factor_ids:
            covariance[(left, right)] = 1.0 if left == right else 0.0
    return covariance


def test_market_neutral_hedge_skips_small_market_beta() -> None:
    preview = build_market_neutral_hedge(
        {"SPY": 0.09, "XLK": 0.25},
        _identity_covariance(["SPY", "XLK"]),
        fit_status="ok",
    )

    assert preview.status == "hedge_ok"
    assert preview.reason == "below_market_materiality_threshold"
    assert preview.hedge_legs == ()


def test_factor_neutral_hedge_is_deterministic_after_pruning_and_leg_cap() -> None:
    covariance = _identity_covariance(["SPY", "XLK", "XLY", "MTUM", "VLUE", "QUAL", "USMV"])
    covariance[("XLK", "XLY")] = 0.95
    covariance[("XLY", "XLK")] = 0.95
    preview = build_factor_neutral_hedge(
        {
            "SPY": 0.20,
            "XLK": 0.40,
            "XLY": 0.30,
            "MTUM": 0.20,
            "VLUE": -0.10,
            "QUAL": 0.08,
            "USMV": 0.06,
        },
        covariance,
        fit_status="ok",
        previous_hedge_weights={"SPY": -0.20, "XLK": -0.40, "VLUE": 0.10},
    )

    assert preview.status == "hedge_ok"
    assert [leg.factor_id for leg in preview.hedge_legs] == ["SPY", "XLK", "MTUM", "VLUE", "QUAL"]
    assert preview.hedge_weights["SPY"] == -0.20
    assert "XLY" not in preview.hedge_weights
    assert "USMV" not in preview.hedge_weights
    assert preview.stability.leg_overlap_ratio == 0.6


def test_factor_neutral_hedge_marks_degraded_when_leg_cap_leaves_too_much_residual() -> None:
    factor_ids = ["SPY", "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV"]
    preview = build_factor_neutral_hedge(
        {
            "SPY": 0.20,
            "XLB": 0.10,
            "XLC": 0.10,
            "XLE": 0.10,
            "XLF": 0.10,
            "XLI": 0.10,
            "XLK": 0.10,
            "XLP": 0.10,
            "XLRE": 0.10,
            "XLU": 0.10,
            "XLV": 0.10,
        },
        _identity_covariance(factor_ids),
        fit_status="ok",
    )

    assert preview.status == "hedge_degraded"
    assert math.isclose(float(preview.non_market_reduction_ratio or 0.0), 0.4, rel_tol=0.0, abs_tol=1e-12)


def test_factor_neutral_hedge_respects_explicit_leg_cap() -> None:
    preview = build_factor_neutral_hedge(
        {
            "SPY": 0.30,
            "XLK": 0.25,
            "XLF": -0.20,
            "XLV": 0.15,
        },
        _identity_covariance(["SPY", "XLK", "XLF", "XLV"]),
        fit_status="ok",
        max_hedge_legs=2,
    )

    assert [leg.factor_id for leg in preview.hedge_legs] == ["SPY", "XLK"]
    assert "XLF" not in preview.hedge_weights


def test_factor_neutral_recommendation_uses_top_magnitude_candidates_up_to_ten() -> None:
    preview = build_factor_neutral_recommendation(
        {
            "SPY": 0.30,
            "XLK": 0.25,
            "XLF": -0.20,
            "XLV": 0.15,
        },
        _identity_covariance(["SPY", "XLK", "XLF", "XLV"]),
        fit_status="ok",
        max_hedge_legs=3,
    )

    assert preview.status == "hedge_ok"
    assert [leg.factor_id for leg in preview.hedge_legs] == ["SPY", "XLK", "XLF"]
    assert math.isclose(float(preview.hedge_weights["SPY"]), -0.30, rel_tol=0.0, abs_tol=1e-12)
    assert preview.reason is None


def test_hedge_is_unavailable_for_insufficient_history() -> None:
    preview = build_factor_neutral_hedge(
        {"SPY": 0.20, "XLK": 0.40},
        _identity_covariance(["SPY", "XLK"]),
        fit_status="insufficient_history",
    )

    assert preview.status == "hedge_unavailable"
    assert preview.hedge_legs == ()


def test_factor_neutral_hedge_fails_closed_on_incomplete_covariance_surface() -> None:
    with pytest.raises(ValueError, match="Incomplete covariance coverage"):
        build_factor_neutral_hedge(
            {"SPY": 0.20, "XLK": 0.40, "MTUM": -0.10},
            {
                ("SPY", "SPY"): 1.0,
                ("XLK", "XLK"): 1.0,
                ("MTUM", "MTUM"): 1.0,
                ("SPY", "XLK"): 0.2,
                ("XLK", "SPY"): 0.2,
            },
            fit_status="ok",
        )
