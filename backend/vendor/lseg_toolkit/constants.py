"""
Application-wide constants for jl-lseg-toolkit.

This module centralizes magic numbers and configuration values used across
the codebase to improve maintainability and make adjustments easier.
"""

# =============================================================================
# API Request Configuration
# =============================================================================

# Maximum number of concurrent API requests in ThreadPoolExecutor.
# Value of 11 is based on empirical testing with LSEG API - higher values
# don't improve performance and may trigger rate limiting. Lower values
# are slower due to underutilized parallelism. 11 provides optimal balance
# for typical queries (5-10 unique earnings dates + 1 reference price fetch).
MAX_WORKERS = 11

# Maximum depth to search parent directories for local config file.
# Prevents infinite loops while allowing reasonable project nesting.
MAX_PARENT_SEARCH_DEPTH = 10


# =============================================================================
# Time Periods (in days)
# =============================================================================

# Lookback period for finding previous earnings dates.
# 180 days (~6 months) covers 2 quarterly earnings cycles, ensuring we find
# the most recent past earnings for most companies.
EARNINGS_LOOKBACK_DAYS = 180

# Period for calculating 2-year total return.
# Used because LSEG doesn't provide a built-in TR.TotalReturn2Yr field,
# so we calculate it manually using TR.TotalReturn with date range.
TWO_YEARS_DAYS = 730

# Return period thresholds for validation (in days).
# Stocks that IPO'd more recently than these periods have their returns
# set to NaN as the metric is not meaningful.
RETURN_PERIODS = {
    "1_month": 30,
    "3_months": 90,
    "6_months": 180,
    "1_year": 365,
    "2_years": 730,
    "3_years": 1095,
    "5_years": 1825,
}


# =============================================================================
# Excel Formatting
# =============================================================================

# Default font for Excel exports
EXCEL_DEFAULT_FONT = "Book Antiqua"
EXCEL_DEFAULT_FONT_SIZE = 10

# Number of decimal places for numeric values in Excel
EXCEL_DECIMAL_PLACES = 2

# Maximum length for Excel sheet names (Excel limitation)
EXCEL_MAX_SHEET_NAME_LENGTH = 31


# =============================================================================
# Market Cap Conversion
# =============================================================================

# Divisor to convert raw market cap (in dollars) to millions
MARKET_CAP_DIVISOR_MILLIONS = 1_000_000


# =============================================================================
# Parallel Processing
# =============================================================================

# Number of workers for parallel data fetching in earnings pipeline
EARNINGS_PIPELINE_MAX_WORKERS = 5
