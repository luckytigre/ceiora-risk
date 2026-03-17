import pandas as pd

from backend.risk_model.eligibility import EligibilityContext
from backend.risk_model.regression_frame import RegressionFrameBuilder


def _build_context() -> EligibilityContext:
    exposure_date = "2026-03-03"
    exposure_snapshot = pd.DataFrame(
        {
            "ticker": ["AAA", "BBB", "CCC"],
            "beta_score": [0.3, -0.2, 0.1],
            "momentum_score": [0.1, 0.2, -0.3],
            "size_score": [0.4, 0.1, -0.1],
            "nonlinear_size_score": [0.0, 0.1, -0.1],
            "short_term_reversal_score": [0.1, -0.1, 0.2],
            "resid_vol_score": [0.3, 0.1, -0.2],
            "liquidity_score": [0.2, 0.2, 0.2],
            "book_to_price_score": [0.1, 0.1, 0.2],
            "earnings_yield_score": [0.2, 0.3, 0.1],
            "leverage_score": [0.1, -0.1, 0.0],
            "growth_score": [0.3, 0.0, -0.2],
            "profitability_score": [0.2, 0.4, 0.1],
            "investment_score": [0.1, 0.1, 0.1],
            "dividend_yield_score": [0.0, 0.2, 0.1],
            "trbc_business_sector": ["Technology Equipment", "Industrial Goods", "Technology Equipment"],
        },
        index=pd.Index(["AAA.O", "BBB.O", "CCC.T"], name="ric"),
    )

    dates = ["2026-03-04"]
    market_cap_panel = pd.DataFrame(
        {
            "AAA.O": [100.0],
            "BBB.O": [200.0],
            "CCC.T": [150.0],
        },
        index=dates,
    )
    sector_panel = pd.DataFrame(
        {
            "AAA.O": ["Technology"],
            "BBB.O": ["Industrials"],
            "CCC.T": ["Technology"],
        },
        index=dates,
    )
    business_panel = pd.DataFrame(
        {
            "AAA.O": ["Technology Equipment"],
            "BBB.O": ["Industrial Goods"],
            "CCC.T": ["Technology Equipment"],
        },
        index=dates,
    )
    industry_panel = pd.DataFrame(
        {
            "AAA.O": ["Semiconductors"],
            "BBB.O": ["Capital Goods"],
            "CCC.T": ["Hardware"],
        },
        index=dates,
    )
    country_panel = pd.DataFrame(
        {
            "AAA.O": ["US"],
            "BBB.O": ["US"],
            "CCC.T": ["JP"],
        },
        index=dates,
    )

    return EligibilityContext(
        exposure_dates=[exposure_date],
        exposure_snapshots={exposure_date: exposure_snapshot},
        market_cap_panel=market_cap_panel,
        trbc_economic_sector_short_panel=sector_panel,
        trbc_business_sector_panel=business_panel,
        trbc_industry_panel=industry_panel,
        hq_country_code_panel=country_panel,
        dates=dates,
    )


def test_regression_frame_builder_builds_full_projectable_frame() -> None:
    daily_returns = pd.DataFrame(
        {
            "AAA.O": [0.01],
            "BBB.O": [0.02],
            "CCC.T": [0.03],
        },
        index=["2026-03-04"],
    )

    builder = RegressionFrameBuilder(
        daily_returns=daily_returns,
        eligibility_ctx=_build_context(),
        lag_days=0,
        returns_winsor_pct=0.0,
    )

    result = builder.build(date="2026-03-04", eligibility_date="2026-03-04")

    assert result.skip_reason is None
    assert result.frame is not None
    assert result.summary.exposure_n == 3
    assert result.summary.structural_eligible_n == 3
    assert result.summary.projectable_n == 3
    assert result.summary.regression_member_n == 3
    assert list(result.frame.projectable_index) == ["AAA.O", "BBB.O", "CCC.T"]
    assert sorted(result.frame.industry_names) == ["Industrial Goods", "Technology Equipment"]
    assert list(result.frame.industry_dummies.columns) == ["Industrial Goods", "Technology Equipment"]
    assert "Beta" in result.frame.style_names
    assert result.frame.style_matrix is not None
    assert result.frame.projectable_style_matrix is not None


def test_regression_frame_builder_supports_us_core_with_projected_non_us() -> None:
    daily_returns = pd.DataFrame(
        {
            "AAA.O": [0.01],
            "BBB.O": [0.02],
            "CCC.T": [0.03],
        },
        index=["2026-03-04"],
    )

    builder = RegressionFrameBuilder(
        daily_returns=daily_returns,
        eligibility_ctx=_build_context(),
        lag_days=0,
        returns_winsor_pct=0.0,
        core_country_codes={"US"},
    )

    result = builder.build(date="2026-03-04", eligibility_date="2026-03-04")

    assert result.skip_reason is None
    assert result.frame is not None
    assert result.summary.structural_eligible_n == 3
    assert result.summary.core_structural_eligible_n == 2
    assert result.summary.projectable_n == 3
    assert result.summary.projected_only_n == 1
    assert result.summary.regression_member_n == 2
    assert list(result.frame.regression_index) == ["AAA.O", "BBB.O"]
    assert list(result.frame.projectable_index) == ["AAA.O", "BBB.O", "CCC.T"]
    assert result.frame.projectable_style_matrix is not None
    assert result.frame.projectable_style_matrix.shape[0] == 3
    assert list(result.frame.projectable_industry_dummies.columns) == list(result.frame.industry_dummies.columns)
    assert list(result.frame.hq_country_series) == ["US", "US"]
    assert list(result.frame.projectable_hq_country_series) == ["US", "US", "JP"]


def test_regression_frame_builder_reports_missing_return_row() -> None:
    builder = RegressionFrameBuilder(
        daily_returns=pd.DataFrame(columns=["AAA.O"]),
        eligibility_ctx=_build_context(),
        lag_days=0,
        returns_winsor_pct=0.0,
    )

    result = builder.build(date="2026-03-04", eligibility_date="2026-03-04")

    assert result.frame is None
    assert result.skip_reason == "missing_return_row"
