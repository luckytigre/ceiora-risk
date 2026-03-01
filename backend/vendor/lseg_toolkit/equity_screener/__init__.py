"""
Equity Screener module for screening stocks by financial criteria.

This module provides functionality to screen US stocks based on:
- Market capitalization range
- Financial ratios (P/E, EV/EBITDA, P/FCF, P/B)
- Performance metrics (returns, dividend yield)
- Activist campaign history
"""

from .config import EquityScreenerConfig
from .pipeline import EquityScreenerPipeline

__all__ = ["EquityScreenerConfig", "EquityScreenerPipeline"]
