"""
Timezone utilities for LSEG data.

IMPORTANT: LSEG returns all earnings times in GMT/UTC.
This module provides conversion from GMT to user's desired timezone.
"""

import logging
from datetime import datetime

import pandas as pd
import pytz

logger = logging.getLogger(__name__)


def convert_datetime_to_timezone(
    dt: datetime, from_tz: str = "GMT", to_tz: str = "US/Eastern"
) -> datetime:
    """
    Convert a datetime from one timezone to another.

    LSEG returns all times in GMT/UTC, so from_tz defaults to 'GMT'.

    Args:
        dt: Datetime object (naive or aware)
        from_tz: Source timezone string (default: 'GMT')
        to_tz: Target timezone string (e.g., 'US/Eastern')

    Returns:
        Datetime object in target timezone

    Examples:
        >>> dt = datetime(2025, 10, 30, 20, 0, 0)  # 8:00 PM GMT
        >>> convert_datetime_to_timezone(dt, 'GMT', 'US/Eastern')
        # Returns 2025-10-30 16:00:00 EDT (4:00 PM Eastern)
    """
    # Get timezone objects
    source_tz = pytz.timezone(from_tz)
    target_tz = pytz.timezone(to_tz)

    # If dt is naive, localize it to source timezone
    if dt.tzinfo is None:
        dt_aware = source_tz.localize(dt)
    else:
        dt_aware = dt

    # Convert to target timezone
    dt_converted = dt_aware.astimezone(target_tz)

    return dt_converted


def add_timezone_converted_columns(
    df: pd.DataFrame,
    target_tz: str = "US/Eastern",
    datetime_col: str = "Event Start Date Time",
) -> pd.DataFrame:
    """
    Add timezone-converted datetime columns to earnings DataFrame.

    LSEG returns all times in GMT/UTC. This function converts them to the target timezone.

    Args:
        df: DataFrame with earnings data
        target_tz: Target timezone to convert to (default: US/Eastern)
        datetime_col: Name of datetime column to convert

    Returns:
        DataFrame with added columns:
            - Event Start Date Time (GMT): Original GMT time with timezone info
            - Event Start Date Time ({target_tz}): Converted time
            - Event Time ({target_tz}): Just the time portion for easier reading

    Examples:
        >>> df = get_earnings_data(['AAPL.O', 'BP.L'])
        >>> df = add_timezone_converted_columns(df, 'US/Eastern')
        # Now has converted times in US/Eastern
    """
    if df is None or df.empty:
        return df

    if datetime_col not in df.columns:
        return df

    # Create a copy to avoid modifying original
    df = df.copy()

    # Function to convert a single row
    def convert_row(row):
        # Skip if no datetime
        if pd.isna(row[datetime_col]):
            return pd.Series(
                {
                    f"{datetime_col} (GMT)": None,
                    f"{datetime_col} ({target_tz})": None,
                    f"Event Time ({target_tz})": None,
                }
            )

        # Convert the datetime
        original_dt = pd.to_datetime(row[datetime_col])

        # Skip timezone conversion if time is 00:00:00
        # This indicates LSEG has a date but no announced time yet.
        # Converting midnight GMT would show wrong date in other timezones.
        if (
            original_dt.hour == 0
            and original_dt.minute == 0
            and original_dt.second == 0
        ):
            return pd.Series(
                {
                    f"{datetime_col} (GMT)": original_dt,
                    f"{datetime_col} ({target_tz})": original_dt,
                    f"Event Time ({target_tz})": None,
                }
            )

        try:
            # Convert from GMT to target timezone
            converted_dt = convert_datetime_to_timezone(
                original_dt, from_tz="GMT", to_tz=target_tz
            )

            # Localize original to GMT as well
            gmt_tz = pytz.timezone("GMT")
            original_dt_aware = gmt_tz.localize(original_dt)

            # Extract just the time portion for display
            time_str = converted_dt.strftime("%I:%M %p %Z")  # e.g., "04:00 PM EDT"

            return pd.Series(
                {
                    f"{datetime_col} (GMT)": original_dt_aware,
                    f"{datetime_col} ({target_tz})": converted_dt,
                    f"Event Time ({target_tz})": time_str,
                }
            )
        except (ValueError, pytz.exceptions.UnknownTimeZoneError) as e:
            # If conversion fails (invalid timezone, etc.), log and return original
            logger.warning(f"Timezone conversion failed for {original_dt}: {e}")
            return pd.Series(
                {
                    f"{datetime_col} (GMT)": original_dt,
                    f"{datetime_col} ({target_tz})": original_dt,
                    f"Event Time ({target_tz})": None,
                }
            )

    # Apply conversion to all rows
    converted = df.apply(convert_row, axis=1)

    # Add the new columns to the DataFrame
    df = pd.concat([df, converted], axis=1)

    return df
