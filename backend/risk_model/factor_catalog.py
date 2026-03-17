"""Factor identity and catalog helpers for the risk model."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Iterable, Literal

from backend.risk_model.descriptors import FULL_STYLE_FACTORS

FactorFamily = Literal["market", "industry", "style"]
FactorBlock = Literal["core_structural", "core_style", "coverage_only"]

MARKET_FACTOR = "Market"
MARKET_FACTOR_ID = "market"

# Column name -> human label for style factors from raw cross-section history
STYLE_COLUMN_TO_LABEL: dict[str, str] = {
    "beta_score": "Beta",
    "momentum_score": "Momentum",
    "size_score": "Size",
    "nonlinear_size_score": "Nonlinear Size",
    "short_term_reversal_score": "Short-Term Reversal",
    "resid_vol_score": "Residual Volatility",
    "liquidity_score": "Liquidity",
    "book_to_price_score": "Book-to-Price",
    "earnings_yield_score": "Earnings Yield",
    "leverage_score": "Leverage",
    "growth_score": "Growth",
    "profitability_score": "Profitability",
    "investment_score": "Investment",
    "dividend_yield_score": "Dividend Yield",
}

@dataclass(frozen=True)
class FactorCatalogEntry:
    factor_id: str
    factor_name: str
    short_label: str
    family: FactorFamily
    block: FactorBlock
    source_column: str | None = None
    display_order: int = 0
    covariance_display: bool = True
    exposure_publish: bool = True
    active: bool = True
    method_version: str = ""


def _slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower())
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "unknown"


def factor_id_for_name(
    factor_name: str,
    *,
    family: FactorFamily,
    source_column: str | None = None,
) -> str:
    if family == "style" and source_column:
        return f"style_{_slugify(source_column)}"
    if family == "market":
        return MARKET_FACTOR_ID
    if family == "industry":
        return f"industry_{_slugify(factor_name)}"
    return _slugify(factor_name)


STYLE_SCORE_COLS = list(STYLE_COLUMN_TO_LABEL.keys())
STYLE_FACTOR_NAMES = set(FULL_STYLE_FACTORS.keys())
STYLE_LABEL_TO_COLUMN = {label: column for column, label in STYLE_COLUMN_TO_LABEL.items()}
STYLE_ID_TO_LABEL = {
    factor_id_for_name(label, family="style", source_column=source_column): label
    for source_column, label in STYLE_COLUMN_TO_LABEL.items()
}


def _best_effort_industry_name_from_id(factor_id: str) -> str:
    slug = str(factor_id or "").strip()
    if slug.startswith("industry_"):
        slug = slug[len("industry_") :]
    parts = [part for part in slug.split("_") if part]
    if not parts:
        return str(factor_id or "").strip()
    return " ".join(part.capitalize() for part in parts)


def factor_name_from_token(
    factor_token: str,
    *,
    known_factor_names: Iterable[str] | None = None,
) -> str:
    token = str(factor_token or "").strip()
    if not token:
        return ""
    if token in {MARKET_FACTOR, MARKET_FACTOR_ID}:
        return MARKET_FACTOR
    if token in STYLE_FACTOR_NAMES:
        return token
    if token in STYLE_ID_TO_LABEL:
        return STYLE_ID_TO_LABEL[token]

    known_names = [str(name or "").strip() for name in (known_factor_names or []) if str(name or "").strip()]
    if known_names:
        for candidate in known_names:
            if candidate == token and not (
                candidate == MARKET_FACTOR_ID or candidate.startswith(("style_", "industry_"))
            ):
                return candidate
            family = infer_factor_family(candidate)
            candidate_id = factor_id_for_name(
                candidate,
                family=family,
                source_column=STYLE_LABEL_TO_COLUMN.get(candidate),
            )
            if token == candidate_id:
                return candidate

    if token.startswith("industry_"):
        return _best_effort_industry_name_from_id(token)
    if token.startswith("style_"):
        return STYLE_ID_TO_LABEL.get(token, token)
    return token


def infer_factor_family(
    factor_name: str,
    *,
    structural_factor_names: Iterable[str] | None = None,
) -> FactorFamily:
    factor = str(factor_name or "").strip()
    structural = {
        str(name or "").strip()
        for name in (structural_factor_names or (MARKET_FACTOR, MARKET_FACTOR_ID))
    }
    structural.update({MARKET_FACTOR, MARKET_FACTOR_ID})
    if factor in STYLE_FACTOR_NAMES or factor in STYLE_ID_TO_LABEL or factor.startswith("style_"):
        return "style"
    if factor in structural:
        return "market"
    if factor.startswith("industry_"):
        return "industry"
    return "industry"


def build_factor_catalog(
    *,
    market_factor_name: str = MARKET_FACTOR,
    industry_names: Iterable[str] | None = None,
    method_version: str = "",
) -> dict[str, FactorCatalogEntry]:
    """Build a catalog keyed by factor display name."""
    catalog: dict[str, FactorCatalogEntry] = {}

    if market_factor_name:
        catalog[market_factor_name] = FactorCatalogEntry(
            factor_id=factor_id_for_name(market_factor_name, family="market"),
            factor_name=market_factor_name,
            short_label=market_factor_name,
            family="market",
            block="core_structural",
            display_order=0,
            method_version=method_version,
        )

    for idx, (source_column, factor_name) in enumerate(STYLE_COLUMN_TO_LABEL.items(), start=1000):
        catalog[factor_name] = FactorCatalogEntry(
            factor_id=factor_id_for_name(factor_name, family="style", source_column=source_column),
            factor_name=factor_name,
            short_label=factor_name,
            family="style",
            block="core_style",
            source_column=source_column,
            display_order=idx,
            method_version=method_version,
        )

    for idx, factor_name in enumerate(sorted({str(name).strip() for name in (industry_names or []) if str(name).strip()}), start=100):
        catalog[factor_name] = FactorCatalogEntry(
            factor_id=factor_id_for_name(factor_name, family="industry"),
            factor_name=factor_name,
            short_label=factor_name,
            family="industry",
            block="core_structural",
            display_order=idx,
            method_version=method_version,
        )

    return catalog


def build_factor_catalog_for_factors(
    factor_names: Iterable[str],
    *,
    method_version: str = "",
) -> dict[str, FactorCatalogEntry]:
    """Build a catalog for the exact live factor set."""
    catalog: dict[str, FactorCatalogEntry] = {}
    known_factor_names = [
        str(name or "").strip()
        for name in factor_names
        if str(name or "").strip()
    ]
    for token in known_factor_names:
        factor_name = factor_name_from_token(token, known_factor_names=known_factor_names)
        if not factor_name:
            continue
        family = infer_factor_family(token or factor_name)
        source_column = STYLE_LABEL_TO_COLUMN.get(factor_name)
        factor_id = (
            str(token)
            if str(token).strip().startswith(("style_", "industry_")) or str(token).strip() == MARKET_FACTOR_ID
            else factor_id_for_name(factor_name, family=family, source_column=source_column)
        )
        if family == "market":
            display_order = 0
            block: FactorBlock = "core_structural"
        elif family == "industry":
            display_order = 100 + len([entry for entry in catalog.values() if entry.family == "industry"])
            block = "core_structural"
        elif family == "style":
            display_order = 1000 + len([entry for entry in catalog.values() if entry.family == "style"])
            block = "core_style"
        else:
            display_order = 5000
            block = "coverage_only"
        catalog[factor_name] = FactorCatalogEntry(
            factor_id=factor_id,
            factor_name=factor_name,
            short_label=factor_name,
            family=family,
            block=block,
            source_column=source_column,
            display_order=display_order,
            method_version=method_version,
        )
    return catalog


def factor_name_to_id_map(entries: dict[str, FactorCatalogEntry]) -> dict[str, str]:
    return {
        str(entry.factor_name): str(entry.factor_id)
        for entry in entries.values()
    }


def factor_id_to_entry_map(entries: dict[str, FactorCatalogEntry]) -> dict[str, FactorCatalogEntry]:
    return {
        str(entry.factor_id): entry
        for entry in entries.values()
    }


def serialize_factor_catalog(entries: dict[str, FactorCatalogEntry]) -> list[dict[str, object]]:
    return [asdict(entry) for entry in sorted(entries.values(), key=lambda entry: (entry.display_order, entry.factor_name))]
