"""
Company data retrieval.

Handles fetching basic company information including name, sector, price, and market cap.
"""

import lseg.data as rd
import pandas as pd

from ..exceptions import DataRetrievalError


def get_company_data(
    tickers: list[str], fields: list[str] | None = None, as_of_date: str | None = None
) -> pd.DataFrame:
    """
    Fetch basic company data for given tickers.

    Args:
        tickers: List of ticker symbols (RICs)
        fields: Optional list of specific fields to retrieve.
               If None, retrieves default set: name, sector, price, market cap
        as_of_date: Optional date string (YYYY-MM-DD) to get historical data

    Returns:
        DataFrame with columns:
            - Instrument: RIC ticker
            - Company Common Name: Company name
            - TRBC Economic Sector Name: Sector
            - Price Close: Share price (current or as of snapshot date)
            - Company Market Cap: Market capitalization (in actual value)

    Examples:
        >>> get_company_data(['AAPL.O', 'MSFT.O'])
        # Returns basic info for Apple and Microsoft

        >>> get_company_data(['AAPL.O'], as_of_date='2025-10-26')
        # Returns data as of October 26, 2025
    """
    if fields is None:
        # Default fields: basic company info
        fields = [
            "TR.CommonName",
            "TR.TRBCEconomicSector",
            "TR.PriceClose",
            "TR.CompanyMarketCap",
        ]

    parameters = {}
    if as_of_date:
        # Point-in-time data
        parameters["SDate"] = as_of_date
        parameters["EDate"] = as_of_date

    try:
        df = rd.get_data(
            universe=tickers,
            fields=fields,
            parameters=parameters if parameters else None,
        )

        if df is None or df.empty:
            return pd.DataFrame()

        return df

    except Exception as e:
        raise DataRetrievalError(f"Failed to get company data: {e}") from e
