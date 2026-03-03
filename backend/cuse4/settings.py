"""Versioned cUSE4 engine settings and profile policies."""

from __future__ import annotations

from dataclasses import dataclass
import os


ENGINE_METHOD_VERSION = "cuse4_v0_foundation_2026_03_03"


@dataclass(frozen=True)
class EstuPolicy:
    min_price_history_days: int = 252
    min_price_floor: float = 2.0
    min_market_cap: float = 250_000_000.0
    min_adv_20d: float = 1_000_000.0


@dataclass(frozen=True)
class CovarianceProfile:
    name: str
    factor_vol_half_life: float
    factor_corr_half_life: float
    vra_half_life: float


CUSE4_S = CovarianceProfile(
    name="cUSE4-S",
    factor_vol_half_life=84.0,
    factor_corr_half_life=504.0,
    vra_half_life=42.0,
)

CUSE4_L = CovarianceProfile(
    name="cUSE4-L",
    factor_vol_half_life=252.0,
    factor_corr_half_life=504.0,
    vra_half_life=168.0,
)


def default_profile() -> CovarianceProfile:
    profile = os.getenv("CUSE4_PROFILE", "cUSE4-L").strip().lower()
    if profile in {"cuse4-s", "s", "short"}:
        return CUSE4_S
    return CUSE4_L


def estu_policy_from_env() -> EstuPolicy:
    return EstuPolicy(
        min_price_history_days=max(20, int(os.getenv("CUSE4_ESTU_MIN_PRICE_HISTORY_DAYS", "252"))),
        min_price_floor=float(os.getenv("CUSE4_ESTU_MIN_PRICE_FLOOR", "2.0")),
        min_market_cap=float(os.getenv("CUSE4_ESTU_MIN_MARKET_CAP", "250000000")),
        min_adv_20d=float(os.getenv("CUSE4_ESTU_MIN_ADV20D", "1000000")),
    )
