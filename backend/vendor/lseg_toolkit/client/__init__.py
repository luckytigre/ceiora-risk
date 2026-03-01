"""
LSEG API Client - Modular wrapper for accessing LSEG/Refinitiv data.

This package provides a modular client for financial data retrieval.

Refactored Structure:
- session.py: Session management
- constituents.py: Index constituent retrieval
- company.py: Company data
- _legacy.py: Legacy methods (earnings, financial, consensus) - to be extracted later

The LsegClient class provides the same interface as before, ensuring backwards compatibility.
"""

import pandas as pd

from .company import get_company_data
from .consensus import _calculate_fiscal_period_label_simple, get_consensus_estimates
from .constituents import (
    AVAILABLE_INDICES,
    get_available_indices,
    get_index_constituents,
)
from .earnings import get_earnings_data, get_since_last_earnings_return
from .financial import get_financial_ratios
from .session import SessionManager


class LsegClient(SessionManager):
    """
    Main client for accessing LSEG/Refinitiv financial data.

    Provides methods for:
    - Session management (inherited from SessionManager)
    - Fetching company data by index constituents
    - Retrieving market data (prices, market caps)
    - Getting earnings data and estimates
    - Accessing financial ratios and analyst targets

    The client is modular - refactored methods use focused submodules,
    while some methods remain in legacy code temporarily.
    """

    def __init__(self, auto_open: bool = True):
        """
        Initialize LSEG client.

        Args:
            auto_open: Automatically open LSEG session (default: True)
        """
        super().__init__(auto_open=auto_open)

    # ========================================================================
    # REFACTORED METHODS - Using new modular structure
    # ========================================================================

    @staticmethod
    def get_available_indices() -> dict[str, dict[str, str]]:
        """Get list of commonly used indices available for querying."""
        return get_available_indices()

    def get_index_constituents(
        self,
        index: str,
        min_market_cap: float | None = None,
        max_market_cap: float | None = None,
    ) -> list[str]:
        """Get list of tickers for an index."""
        return get_index_constituents(index, min_market_cap, max_market_cap)

    def get_company_data(
        self,
        tickers: list[str],
        fields: list[str] | None = None,
        as_of_date: str | None = None,
    ) -> pd.DataFrame:
        """Fetch basic company data for given tickers."""
        return get_company_data(tickers, fields, as_of_date)

    # ========================================================================
    # REFACTORED METHODS - Using new modular functions
    # ========================================================================

    def get_earnings_data(
        self,
        tickers: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        convert_timezone: str | None = None,
    ) -> pd.DataFrame:
        """Get earnings release dates and times for companies."""
        return get_earnings_data(tickers, start_date, end_date, convert_timezone)

    def get_financial_ratios(
        self,
        tickers: list[str],
        include_estimates: bool = True,
        as_of_date: str | None = None,
    ) -> pd.DataFrame:
        """Get comprehensive financial ratios and valuation metrics."""
        return get_financial_ratios(tickers, include_estimates, as_of_date)

    def get_since_last_earnings_return(
        self, tickers: list[str], as_of_date: str | None = None
    ) -> pd.DataFrame:
        """Calculate return since last earnings release."""
        return get_since_last_earnings_return(tickers, as_of_date)

    def get_consensus_estimates(
        self, tickers: list[str], period: str = "NTM", as_of_date: str | None = None
    ) -> pd.DataFrame:
        """Get consensus estimates for revenue, EBITDA, and EPS."""
        return get_consensus_estimates(tickers, period, as_of_date)

    def _calculate_fiscal_period_label_simple(
        self, period_end_date: pd.Timestamp
    ) -> str:
        """Calculate fiscal period label from period end date."""
        return _calculate_fiscal_period_label_simple(period_end_date)


# Re-export for convenience
__all__ = [
    "LsegClient",
    "SessionManager",
    "get_available_indices",
    "get_index_constituents",
    "get_company_data",
    "AVAILABLE_INDICES",
]
