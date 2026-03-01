"""
Configuration for equity screener.

Defines screening criteria and output settings.
"""

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from ..exceptions import ConfigurationError

logger = logging.getLogger(__name__)


@dataclass
class EquityScreenerConfig:
    """
    Configuration for equity screening.

    Attributes:
        screen_date: Date to use for screening data (YYYY-MM-DD format)
        index: Index code to screen constituents (e.g., 'SPX', 'NDX'). Default: 'SPX'
        country: Country code for headquarters filter (e.g., 'US', 'GB', 'CA'). Default: 'US'
        min_mkt_cap: Minimum market cap in millions (default: None = unrestricted)
        max_mkt_cap: Maximum market cap in millions (default: None = unrestricted)
        output_dir: Directory for output files (default: 'exports')
    """

    screen_date: str | None = None
    index: str | None = "SPX"  # Default to S&P 500
    country: str | None = "US"  # Default to US
    min_mkt_cap: float | None = None  # Unrestricted by default
    max_mkt_cap: float | None = None  # Unrestricted by default
    output_dir: str = "exports"

    def __post_init__(self):
        """Validate and set defaults after initialization."""
        # Default to today if no date provided
        if self.screen_date is None:
            self.screen_date = date.today().strftime("%Y-%m-%d")

        # Validate date format
        try:
            parsed_date = datetime.strptime(self.screen_date, "%Y-%m-%d")

            # Warn if future date
            if parsed_date.date() > date.today():
                logger.warning(
                    f"Screening date {self.screen_date} is in the future. Using today's date."
                )
                self.screen_date = date.today().strftime("%Y-%m-%d")

            # Ensure correct format
            self.screen_date = parsed_date.strftime("%Y-%m-%d")

        except ValueError:
            raise ConfigurationError(
                f"Invalid date format '{self.screen_date}'. "
                "Please use YYYY-MM-DD format (e.g., 2024-12-31)"
            )

        # Validate market cap range (if specified)
        if self.min_mkt_cap is not None and self.min_mkt_cap <= 0:
            raise ConfigurationError("Minimum market cap must be positive")

        if self.max_mkt_cap is not None and self.max_mkt_cap <= 0:
            raise ConfigurationError("Maximum market cap must be positive")

        if (
            self.min_mkt_cap is not None
            and self.max_mkt_cap is not None
            and self.min_mkt_cap >= self.max_mkt_cap
        ):
            raise ConfigurationError(
                "Minimum market cap must be less than maximum market cap"
            )

        # Validate index code (if specified)
        if self.index is not None:
            self.index = self.index.upper()  # Normalize to uppercase

        # Validate country code (if specified)
        if self.country is not None:
            self.country = self.country.upper()  # Normalize to uppercase

    def to_dict(self) -> dict[str, Any]:
        """
        Convert config to dictionary for display.

        Returns:
            Dictionary of configuration parameters
        """

        # Format market cap for display
        def format_mkt_cap(value_m: float | None) -> str:
            """Format market cap value (e.g., '$2B' or '$500M')"""
            if value_m is None:
                return "No restriction"
            if value_m >= 1000:
                return f"${value_m / 1000:.1f}B".rstrip("0").rstrip(".")
            return f"${value_m:.0f}M"

        result = {"Screen Date": self.screen_date, "Output Directory": self.output_dir}

        # Add index if specified
        if self.index is not None:
            result["Index"] = self.index

        # Add country if specified
        if self.country is not None:
            result["Country"] = self.country

        # Add market cap filters if specified
        if self.min_mkt_cap is not None or self.max_mkt_cap is not None:
            result["Min Market Cap"] = format_mkt_cap(self.min_mkt_cap)
            result["Max Market Cap"] = format_mkt_cap(self.max_mkt_cap)

        return result

    def get_mkt_cap_range(self) -> tuple[float | None, float | None]:
        """
        Get market cap range in actual values (not millions).

        Returns:
            Tuple of (min_cap, max_cap) in actual dollar values, or None if unrestricted
        """
        min_cap = self.min_mkt_cap * 1_000_000 if self.min_mkt_cap is not None else None
        max_cap = self.max_mkt_cap * 1_000_000 if self.max_mkt_cap is not None else None
        return (min_cap, max_cap)
