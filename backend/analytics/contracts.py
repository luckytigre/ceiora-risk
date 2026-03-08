"""Typed contracts for analytics cache and API-adjacent payloads."""

from __future__ import annotations

from typing import Any, NotRequired, TypedDict


class SourceDatesPayload(TypedDict, total=False):
    fundamentals_asof: str | None
    exposures_asof: str | None
    prices_asof: str | None
    classification_asof: str | None


class SnapshotBuildPayload(TypedDict, total=False):
    status: str
    reason: str
    mode: str


class RiskEngineMetaPayload(TypedDict, total=False):
    status: str
    method_version: str
    last_recompute_date: str
    factor_returns_latest_date: str | None
    cross_section_min_age_days: int
    recompute_interval_days: int
    lookback_days: int
    specific_risk_ticker_count: int
    latest_r2: float
    recompute_reason: str


class RiskEngineStatePayload(TypedDict):
    status: str
    method_version: str
    last_recompute_date: str
    factor_returns_latest_date: str | None
    cross_section_min_age_days: int
    recompute_interval_days: int
    lookback_days: int
    specific_risk_ticker_count: int
    recomputed_this_refresh: bool
    recompute_reason: str


class PositionRiskMixPayload(TypedDict):
    country: float
    industry: float
    style: float
    idio: float


class PositionPayload(TypedDict, total=False):
    ticker: str
    name: str
    long_short: str
    shares: float
    price: float
    market_value: float
    weight: float
    trbc_economic_sector_short: str
    trbc_economic_sector_short_abbr: str
    account: str
    sleeve: str
    source: str
    trbc_industry_group: str
    exposures: dict[str, float]
    specific_var: float | None
    specific_vol: float | None
    risk_contrib_pct: float
    eligible_for_model: bool
    eligibility_reason: str
    risk_mix: PositionRiskMixPayload


class RiskSharesPayload(TypedDict):
    country: float
    industry: float
    style: float
    idio: float


class ComponentSharesPayload(TypedDict):
    country: float
    industry: float
    style: float


class FactorDetailPayload(TypedDict, total=False):
    factor: str
    category: str
    exposure: float
    factor_vol: float
    sensitivity: float
    marginal_var_contrib: float
    pct_of_total: float
    pct_of_systematic: NotRequired[float]


class CovariancePayload(TypedDict):
    factors: list[str]
    matrix: list[list[float]]


class CovarianceMatrixPayload(TypedDict, total=False):
    factors: list[str]
    correlation: list[list[float]]
    matrix: list[list[float]]


class FactorCoveragePayload(TypedDict):
    cross_section_n: int
    eligible_n: int
    coverage_pct: float


class ExposureDrilldownPayload(TypedDict, total=False):
    ticker: str
    weight: float
    exposure: float
    sensitivity: float
    contribution: float


class ExposureFactorPayload(TypedDict, total=False):
    factor: str
    value: float
    factor_vol: float
    cross_section_n: int
    eligible_n: int
    coverage_pct: float
    coverage_date: str | None
    drilldown: list[ExposureDrilldownPayload]


class ExposureModesPayload(TypedDict):
    raw: list[ExposureFactorPayload]
    sensitivity: list[ExposureFactorPayload]
    risk_contribution: list[ExposureFactorPayload]


class UniverseTickerPayload(TypedDict, total=False):
    ticker: str
    ric: str | None
    name: str
    trbc_economic_sector_short: str
    trbc_economic_sector_short_abbr: str
    trbc_business_sector: str
    trbc_industry_group: str
    market_cap: float | None
    price: float
    exposures: dict[str, float]
    sensitivities: dict[str, float]
    risk_loading: float | None
    specific_var: float | None
    specific_vol: float | None
    eligible_for_model: bool
    eligibility_reason: str
    model_warning: str
    as_of_date: str


class UniverseLoadingsPayload(TypedDict, total=False):
    ticker_count: int
    eligible_ticker_count: int
    factor_count: int
    factors: list[str]
    factor_vols: dict[str, float]
    index: list[dict[str, Any]]
    by_ticker: dict[str, UniverseTickerPayload]
    risk_engine: RiskEngineStatePayload
    refresh_started_at: str
    source_dates: SourceDatesPayload


class UniverseFactorsPayload(TypedDict, total=False):
    factors: list[str]
    factor_vols: dict[str, float]
    r_squared: float
    condition_number: float
    ticker_count: int
    eligible_ticker_count: int
    risk_engine: RiskEngineStatePayload
    refresh_started_at: str


class EligibilitySummaryPayload(TypedDict, total=False):
    status: str
    date: str
    exp_date: str | None
    exposure_n: int
    structural_eligible_n: int
    regression_member_n: int
    structural_coverage: float
    regression_coverage: float
    drop_pct_from_prev: float
    alert_level: str
    selection_mode: str
    max_regression_member_n: int
    coverage_threshold_n: int
    latest_available_date: str | None
    selected_well_covered: bool
    used_older_than_latest: bool


class ModelSanityChecksPayload(TypedDict):
    factor_sign_mismatch_count: int
    latest_regression_coverage_pct: float
    latest_structural_eligible_n: int
    country_risk_share_pct: float
    industry_risk_share_pct: float
    style_risk_share_pct: float
    idio_risk_share_pct: float


class ModelSanityPayload(TypedDict):
    status: str
    warnings: list[str]
    checks: ModelSanityChecksPayload
    coverage_date: NotRequired[str | None]
    latest_available_date: NotRequired[str | None]
    selection_mode: NotRequired[str]
    update_available: NotRequired[bool]


class SpecificRiskPayload(TypedDict, total=False):
    ticker: str
    ric: str
    specific_var: float
    specific_vol: float
    obs: int


class RefreshMetaPayload(TypedDict, total=False):
    status: str
    mode: str
    run_id: str
    snapshot_id: str
    refresh_started_at: str
    source_dates: SourceDatesPayload
    cross_section_snapshot: SnapshotBuildPayload
    risk_engine: RiskEngineStatePayload
    model_sanity_status: str
    cuse4_foundation: dict[str, Any]
    health_refreshed: bool


class StageRefreshSnapshotResult(TypedDict):
    snapshot_id: str
    risk_engine_state: RiskEngineStatePayload
    sanity: ModelSanityPayload
    health_refreshed: bool
