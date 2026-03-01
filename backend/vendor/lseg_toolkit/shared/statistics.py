"""
Summary statistics utilities for pipeline reports.

These functions use standard pandas operations (mean, median, nunique, value_counts)
and provide a common interface for calculating report statistics.
"""

from typing import Any

import pandas as pd


def calculate_summary_statistics(
    df: pd.DataFrame,
    sector_col: str | None = None,
    market_cap_col: str | None = None,
    market_cap_divisor: float = 1.0,
) -> dict[str, Any]:
    """
    Calculate summary statistics for a DataFrame.

    Uses pandas built-in aggregation functions:
    - len() for count
    - Series.nunique() for unique sector count
    - Series.mean() / Series.median() for market cap stats

    Args:
        df: DataFrame to analyze
        sector_col: Column name for sector (optional)
        market_cap_col: Column name for market cap (optional)
        market_cap_divisor: Divisor for market cap display (e.g., 1_000_000 for millions)

    Returns:
        Dictionary with statistics:
        - Total Companies: int
        - Total Sectors: int (if sector_col provided)
        - Avg Market Cap (M): float (if market_cap_col provided)
        - Median Market Cap (M): float (if market_cap_col provided)
    """
    stats: dict[str, Any] = {
        "Total Companies": len(df),
    }

    if sector_col and sector_col in df.columns:
        stats["Total Sectors"] = df[sector_col].nunique()

    if market_cap_col and market_cap_col in df.columns:
        market_caps = df[market_cap_col].dropna()
        if len(market_caps) > 0:
            stats["Avg Market Cap (M)"] = round(
                market_caps.mean() / market_cap_divisor, 2
            )
            stats["Median Market Cap (M)"] = round(
                market_caps.median() / market_cap_divisor, 2
            )

    return stats


def calculate_sector_breakdown(
    df: pd.DataFrame,
    sector_col: str,
    market_cap_col: str | None = None,
    market_cap_divisor: float = 1.0,
) -> dict[str, dict[str, Any]]:
    """
    Calculate sector breakdown with counts, percentages, and market cap statistics.

    Uses pandas built-in aggregation functions:
    - Series.value_counts() for sector counts
    - Series.mean() / Series.median() for market cap stats per sector

    Args:
        df: DataFrame to analyze
        sector_col: Column name for sector grouping
        market_cap_col: Column name for market cap (optional)
        market_cap_divisor: Divisor for market cap display (e.g., 1_000_000 for millions)

    Returns:
        Dictionary mapping sector names to:
        - count: int
        - percentage: float
        - avg_market_cap: float (if market_cap_col provided)
        - median_market_cap: float (if market_cap_col provided)
    """
    if sector_col not in df.columns or len(df) == 0:
        return {}

    total = len(df)
    sector_counts = df[sector_col].value_counts()

    breakdown: dict[str, dict[str, Any]] = {}
    for sector, count in sector_counts.items():
        sector_str = str(sector)

        # Handle empty/blank/nan sector names
        if not sector_str or sector_str.strip() == "" or sector_str.lower() == "nan":
            sector_str = "Unknown"

        sector_df = df[df[sector_col] == sector]

        breakdown[sector_str] = {
            "count": int(count),
            "percentage": (count / total) * 100,
        }

        # Add market cap statistics if column provided and available
        if market_cap_col and market_cap_col in df.columns:
            market_caps = sector_df[market_cap_col].dropna() / market_cap_divisor
            if len(market_caps) > 0:
                breakdown[sector_str]["avg_market_cap"] = round(market_caps.mean(), 2)
                breakdown[sector_str]["median_market_cap"] = round(
                    market_caps.median(), 2
                )

    return breakdown
