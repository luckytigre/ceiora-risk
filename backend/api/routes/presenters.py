"""Shared response shaping helpers for API routes."""

from __future__ import annotations

from typing import Any

from backend.analytics.trbc_economic_sector_short import abbreviate_trbc_economic_sector_short


def normalize_trbc_sector_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize sector field aliases and ensure short abbreviation is present."""
    trbc_economic_sector_short = str(
        payload.get("trbc_economic_sector_short")
        or payload.get("trbc_sector")
        or payload.get("sector")
        or ""
    )
    return {
        **payload,
        "trbc_economic_sector_short": trbc_economic_sector_short,
        "trbc_economic_sector_short_abbr": str(
            payload.get("trbc_economic_sector_short_abbr")
            or payload.get("trbc_sector_abbr")
            or abbreviate_trbc_economic_sector_short(trbc_economic_sector_short)
        ),
    }
