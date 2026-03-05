from __future__ import annotations

from backend.api.routes.presenters import normalize_trbc_sector_fields


def test_normalize_trbc_sector_fields_prefers_existing_short_and_abbr() -> None:
    row = {
        "ticker": "ABC",
        "trbc_economic_sector_short": "Technology",
        "trbc_economic_sector_short_abbr": "Tech",
    }
    out = normalize_trbc_sector_fields(row)
    assert out["trbc_economic_sector_short"] == "Technology"
    assert out["trbc_economic_sector_short_abbr"] == "Tech"


def test_normalize_trbc_sector_fields_falls_back_to_aliases() -> None:
    row = {
        "ticker": "ABC",
        "trbc_sector": "Financials",
    }
    out = normalize_trbc_sector_fields(row)
    assert out["trbc_economic_sector_short"] == "Financials"
    assert out["trbc_economic_sector_short_abbr"] == "Fins"


def test_normalize_trbc_sector_fields_handles_missing_sector() -> None:
    out = normalize_trbc_sector_fields({"ticker": "ABC"})
    assert out["trbc_economic_sector_short"] == ""
    assert out["trbc_economic_sector_short_abbr"] == ""
