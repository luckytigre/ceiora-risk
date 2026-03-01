"""
Configuration for earnings report generation.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta


def _default_start_date() -> datetime:
    """Get default start date (Monday of current week)."""
    today = datetime.now()
    return today - timedelta(days=today.weekday())


def _default_end_date() -> datetime:
    """Get default end date (Sunday of current week)."""
    today = datetime.now()
    start = today - timedelta(days=today.weekday())
    return start + timedelta(days=6)


@dataclass
class EarningsConfig:
    """
    Configuration for earnings report generation.

    Attributes:
        index: Index symbol (SPX, NDX, R2K, DAX, ESTX50, etc.)
        min_market_cap: Minimum market cap filter in millions (None = no limit)
        max_market_cap: Maximum market cap filter in millions (None = no limit)
        start_date: Start date for earnings releases (defaults to Monday of current week)
        end_date: End date for earnings releases (defaults to Sunday of current week)
        timezone: Timezone for earnings times (default: US/Eastern)
        output_dir: Directory for exported reports
        consensus_date: Date for consensus estimate snapshot (set automatically in __post_init__)
    """

    index: str = "SPX"
    min_market_cap: float | None = None
    max_market_cap: float | None = None
    start_date: datetime = field(default_factory=_default_start_date)
    end_date: datetime = field(default_factory=_default_end_date)
    timezone: str = "US/Eastern"
    output_dir: str = "exports"
    consensus_date: datetime = field(init=False)

    def __post_init__(self):
        """Calculate consensus snapshot date."""
        # Set consensus snapshot date
        self.consensus_date = self._calculate_consensus_date()

    def _calculate_consensus_date(self) -> datetime:
        """
        Calculate the date to use for consensus estimate snapshot.

        Logic:
        - For weekly reports (Mon-Sun): Use Sunday before start_date
        - For custom date ranges: Use day before start_date
        - IMPORTANT: Cap to today's date (can't get future estimates)

        This ensures estimates are captured BEFORE any companies report.

        Returns:
            datetime object for consensus snapshot
        """
        # Normalize start_date to date (handles both datetime and date types)
        start_as_date = (
            self.start_date.date()
            if isinstance(self.start_date, datetime)
            else self.start_date
        )

        # Calculate ideal consensus date (day before start)
        if start_as_date.weekday() == 0:  # Monday
            # Use Sunday before the week starts
            ideal_date = start_as_date - timedelta(days=1)
        else:
            # Custom date range - use day before start
            ideal_date = start_as_date - timedelta(days=1)

        # Cap to today - cannot get future estimates
        today = datetime.now().date()
        if ideal_date > today:
            return datetime.combine(today, datetime.min.time())
        else:
            return datetime.combine(ideal_date, datetime.min.time())

    def to_dict(self):
        """Convert config to dictionary for summary sheet."""
        return {
            "Index": self.index,
            "Min Market Cap (M)": self.min_market_cap or "None",
            "Max Market Cap (M)": self.max_market_cap or "None",
            "Start Date": self.start_date.strftime("%Y-%m-%d"),
            "End Date": self.end_date.strftime("%Y-%m-%d"),
            "Consensus Estimates As Of": self.consensus_date.strftime("%Y-%m-%d"),
            "Timezone": self.timezone,
        }
