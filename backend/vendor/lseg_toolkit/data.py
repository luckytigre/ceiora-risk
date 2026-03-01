"""
Data processing utilities for LSEG financial data.

Common transformations and calculations used across projects.
"""

import logging
from collections.abc import Sequence

import numpy as np
import pandas as pd

from .exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class DataProcessor:
    """
    Utility class for processing and transforming financial data.

    Provides methods for:
    - Data cleaning and validation
    - Common calculations (growth rates, averages)
    - Timezone conversions
    - Data aggregation and grouping
    """

    @staticmethod
    def calculate_yoy_growth(current: pd.Series, prior: pd.Series) -> pd.Series:
        """
        Calculate year-over-year growth rates.

        Args:
            current: Current period values
            prior: Prior period values

        Returns:
            Series with YoY growth percentages
        """
        return ((current - prior) / prior * 100).round(2)

    @staticmethod
    def format_market_cap(values: pd.Series) -> pd.Series:
        """
        Convert market cap values to millions with 2 decimal places.

        Args:
            values: Market cap values

        Returns:
            Series with values in millions, rounded to 2 decimals
        """
        return (values / 1_000_000).round(2)

    @staticmethod
    def convert_timezone(
        timestamps: pd.Series, target_tz: str = "US/Eastern"
    ) -> pd.Series:
        """
        Convert timestamps to target timezone.

        Args:
            timestamps: Series of datetime values
            target_tz: Target timezone string (e.g., 'US/Eastern', 'Europe/London')

        Returns:
            Series with converted timestamps

        Raises:
            ConfigurationError: If target_tz is not a valid timezone
        """
        try:
            return pd.to_datetime(timestamps).dt.tz_convert(target_tz)
        except Exception as e:
            raise ConfigurationError(f"Invalid timezone '{target_tz}': {e}") from e

    @staticmethod
    def aggregate_by_sector(
        df: pd.DataFrame,
        sector_col: str = "sector",
        value_cols: Sequence[str] | None = None,
        agg_funcs: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Aggregate data by sector.

        Args:
            df: Input DataFrame
            sector_col: Name of sector column
            value_cols: Columns to aggregate
            agg_funcs: Aggregation functions (e.g., ['mean', 'median', 'count'])

        Returns:
            DataFrame with sector-level aggregations
        """
        if agg_funcs is None:
            agg_funcs = ["mean", "median", "count"]

        if value_cols is None:
            value_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        # pandas.DataFrame.agg accepts list[str] but mypy expects AggFuncType
        return df.groupby(sector_col)[value_cols].agg(agg_funcs)  # type: ignore[arg-type]
