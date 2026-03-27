"""Shared source-classification rules for universe taxonomy and eligibility."""

from __future__ import annotations


# Instruments in these TRBC economic sectors are excluded from the core equity model.
# They may still participate as projection-only instruments with returns-based estimation.
NON_EQUITY_ECONOMIC_SECTORS = {
    "Exchange Traded Fund",
    "Digital Asset",
}
