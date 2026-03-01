"""
Index constituent retrieval and management.

Handles fetching constituent lists for global indices with optional market cap filtering.
"""

import lseg.data as rd

from ..exceptions import DataRetrievalError

# Verified global indices available via LSEG API
AVAILABLE_INDICES = {
    # US Indices
    "SPX": {
        "name": "S&P 500",
        "region": "US",
        "description": "Large cap US equities - 500 companies",
    },
    "SPCY": {
        "name": "S&P SmallCap 600",
        "region": "US",
        "description": "Small cap US equities - 600 companies",
    },
    "NDX": {
        "name": "Nasdaq 100",
        "region": "US",
        "description": "Top 100 non-financial Nasdaq stocks",
    },
    "DJI": {
        "name": "Dow Jones Industrial Average",
        "region": "US",
        "description": "30 large cap US blue chip companies",
    },
    # European Indices
    "STOXX": {
        "name": "STOXX Europe 600",
        "region": "Europe",
        "description": "Broad European equity index - 600 companies",
    },
    "STOXX50E": {
        "name": "EURO STOXX 50",
        "region": "Europe",
        "description": "Blue chip Eurozone stocks - 50 companies",
    },
    "FTSE": {
        "name": "FTSE 100",
        "region": "UK",
        "description": "Top 100 companies on London Stock Exchange",
    },
    "GDAXI": {
        "name": "DAX",
        "region": "Germany",
        "description": "Top 40 German companies",
    },
    "FCHI": {
        "name": "CAC 40",
        "region": "France",
        "description": "Top 40 French companies",
    },
    "AEX": {
        "name": "AEX",
        "region": "Netherlands",
        "description": "Top 25 Dutch companies",
    },
    # Asian Indices
    "N225": {
        "name": "Nikkei 225",
        "region": "Japan",
        "description": "Top 225 Japanese companies",
    },
    "HSI": {
        "name": "Hang Seng",
        "region": "Hong Kong",
        "description": "Top Hong Kong companies",
    },
    # Other Regions
    "GSPTSE": {
        "name": "S&P/TSX Composite",
        "region": "Canada",
        "description": "Top Canadian companies - ~200 stocks",
    },
}


def get_available_indices() -> dict[str, dict[str, str]]:
    """
    Get list of commonly used indices available for querying.

    Returns:
        Dictionary mapping index codes to their details:
            {
                'SPX': {
                    'name': 'S&P 500',
                    'region': 'US',
                    'description': 'Large cap US equities'
                },
                ...
            }
    """
    return AVAILABLE_INDICES


def get_index_constituents(
    index: str, min_market_cap: float | None = None, max_market_cap: float | None = None
) -> list[str]:
    """
    Get list of tickers for an index.

    Args:
        index: Index symbol (e.g., 'SPX', 'NDX', 'DAX')
        min_market_cap: Minimum market cap filter (in millions)
        max_market_cap: Maximum market cap filter (in millions)

    Returns:
        List of RIC ticker symbols

    Note:
        Market cap filtering requires 2-step process:
        1. Get all RICs from index
        2. Query market caps separately and filter
    """
    # Normalize index to RIC format (add . prefix if missing)
    index_ric = index if index.startswith(".") else f".{index}"

    try:
        # Step 1: Get all constituents from index
        df = rd.get_data(universe=index_ric, fields=["TR.IndexConstituentRIC"])

        if df is None or df.empty:
            return []

        # Extract RICs
        ric_col = (
            "Constituent RIC" if "Constituent RIC" in df.columns else df.columns[1]
        )
        rics = df[ric_col].dropna().unique().tolist()

        # Step 2: If market cap filtering requested, get market caps separately
        if min_market_cap is not None or max_market_cap is not None:
            # Query market caps for all RICs
            cap_df = rd.get_data(universe=rics, fields=["TR.CompanyMarketCap"])

            if cap_df is not None and not cap_df.empty:
                # Convert millions to actual value for filtering
                min_cap = min_market_cap * 1_000_000 if min_market_cap else 0
                max_cap = max_market_cap * 1_000_000 if max_market_cap else float("inf")

                # Filter by market cap
                if "Company Market Cap" in cap_df.columns:
                    # Filter and extract RICs that meet criteria
                    filtered_df = cap_df[
                        (cap_df["Company Market Cap"] >= min_cap)
                        & (cap_df["Company Market Cap"] <= max_cap)
                    ]
                    rics = filtered_df["Instrument"].dropna().unique().tolist()

        return rics

    except Exception as e:
        raise DataRetrievalError(
            f"Failed to get constituents for index {index_ric}: {e}"
        ) from e
