"""
Earnings data retrieval.

Handles fetching earnings release dates, times, and return calculations.
"""

import logging
from datetime import datetime, timedelta

import lseg.data as rd
import pandas as pd

from ..constants import EARNINGS_LOOKBACK_DAYS, MAX_WORKERS
from ..exceptions import DataRetrievalError

logger = logging.getLogger(__name__)


def get_earnings_data(
    tickers: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    convert_timezone: str | None = None,
) -> pd.DataFrame:
    """
    Get earnings release dates and times for companies.

    Args:
        tickers: List of ticker symbols (RICs)
        start_date: Start date for filtering (YYYY-MM-DD format)
        end_date: End date for filtering (YYYY-MM-DD format)
        convert_timezone: Optional timezone to convert times to (e.g., 'US/Eastern', 'Europe/London')
                        If None, returns times in GMT (as returned by LSEG)

    Returns:
        DataFrame with columns:
            - Instrument: RIC ticker
            - Company Common Name: Company name
            - Event Start Date: Earnings release date
            - Event Start Time: Earnings release time (GMT/UTC)
            - Event Start Date Time: Combined date and time (GMT/UTC)
            - Event Type: Type of event (EarningsReleases)
            - Event Title: Title of earnings release (e.g., "Q4 2025 Apple Inc Earnings Release")

        If convert_timezone is specified, adds additional columns:
            - Event Start Date Time (GMT): Original GMT time
            - Event Start Date Time ({timezone}): Converted time
            - Event Time ({timezone}): Time portion for display

    Note:
        LSEG returns all times in GMT/UTC.
        Empty times indicate the specific time has not been announced yet.
        Common patterns (in GMT):
            - ~12:00-13:00: US Before Market Open (BMO) - 8-9 AM ET
            - ~20:00-21:00: US After Market Close (AMC) - 4-5 PM ET
            - ~07:00-08:00: UK/Europe Before Market Open
            - ~05:00-06:00: Japan afternoon (2-3 PM JST)
    """
    try:
        fields = [
            "TR.CommonName",
            "TR.EventStartDate",
            "TR.EventStartTime",
            "TR.EventStartDateTime",
            "TR.EventType",
            "TR.EventTitle",
        ]

        # Add date filtering parameters if provided
        parameters = None
        if start_date or end_date:
            parameters = {}
            if start_date:
                parameters["SDate"] = start_date
            if end_date:
                parameters["EDate"] = end_date

        df = rd.get_data(universe=tickers, fields=fields, parameters=parameters)

        if df is None or df.empty:
            return pd.DataFrame()

        # Filter for earnings releases only (if EventType column exists)
        if "Company Event Type" in df.columns:
            df = df[df["Company Event Type"] == "EarningsReleases"]

        # Convert timezone if requested
        if convert_timezone:
            from lseg_toolkit.timezone_utils import add_timezone_converted_columns

            df = add_timezone_converted_columns(df, target_tz=convert_timezone)

        return df

    except Exception as e:
        raise DataRetrievalError(f"Failed to get earnings data: {e}") from e


def get_since_last_earnings_return(
    tickers: list[str], as_of_date: str | None = None
) -> pd.DataFrame:
    """
    Calculate stock returns since each company's last earnings release.

    For each ticker, this function finds the most recent past earnings date
    (within a 180-day lookback window) and calculates the percentage return
    from that earnings date to the reference date.

    Args:
        tickers (list[str]): List of ticker symbols (RICs) to process.
            Example: ["AAPL.O", "MSFT.O", "GOOGL.O"]
        as_of_date (str | None): Optional date string in YYYY-MM-DD format.
            If provided, calculates returns as of this date (using that day's
            closing price). If None, uses current/today's price.
            Useful for historical analysis and ensuring consistency with
            consensus estimate snapshots.

    Returns:
        pd.DataFrame: DataFrame with columns:
            - Instrument: RIC ticker symbol
            - Last Earnings Date: Date of the previous earnings release
            - Price at Last Earnings: Closing price on the earnings date
            - Since Last Earnings Return: Percentage return from earnings
              date to reference date (positive = gain, negative = loss)

    Raises:
        DataRetrievalError: If the LSEG API call fails or returns an error.

    Implementation Notes:
        **Performance Optimization:**
        This function is optimized to minimize API calls by:
        1. Fetching all earnings dates in a single batch query
        2. Grouping tickers by their earnings date (typically 5-10 unique dates)
        3. Fetching historical prices for each date group in parallel
        4. Fetching reference (current/as_of) prices in parallel with historical

        Uses ThreadPoolExecutor with max_workers=11 for concurrent API calls.
        This reduces total API calls from N (one per ticker) to ~M+1 (one per
        unique earnings date + one for reference prices).

        **Lookback Period:**
        Searches for earnings within the past 180 days from the reference date.
        Companies without earnings in this window return empty results.

        **Return Calculation:**
        return_pct = ((reference_price - earnings_price) / earnings_price) * 100

    Example:
        >>> returns_df = get_since_last_earnings_return(["AAPL.O", "MSFT.O"])
        >>> print(returns_df)
           Instrument Last Earnings Date  Price at Last Earnings  Since Last Earnings Return
        0     AAPL.O         2025-01-30                  185.50                        12.34
        1     MSFT.O         2025-01-28                  415.20                         8.76
    """
    try:
        # Determine reference date (as_of_date or today)
        reference_date = (
            datetime.strptime(as_of_date, "%Y-%m-%d").date()
            if as_of_date
            else datetime.now().date()
        )

        # Look back for previous earnings (default: ~6 months / 2 quarterly cycles)
        end_date = reference_date.strftime("%Y-%m-%d")
        start_date = (reference_date - timedelta(days=EARNINGS_LOOKBACK_DAYS)).strftime(
            "%Y-%m-%d"
        )

        # Step 1: Get historical earnings dates (batch call)
        earnings_df = rd.get_data(
            universe=tickers,
            fields=[
                "TR.EventStartDate",
                "TR.EventType",
            ],
            parameters={
                "SDate": start_date,
                "EDate": end_date,
            },
        )

        if earnings_df is None or earnings_df.empty:
            # No earnings found - return empty with expected columns
            return pd.DataFrame(
                columns=[
                    "Instrument",
                    "Last Earnings Date",
                    "Price at Last Earnings",
                    "Since Last Earnings Return",
                ]
            )

        # Filter for earnings releases only
        if "Company Event Type" in earnings_df.columns:
            earnings_df = earnings_df[
                earnings_df["Company Event Type"] == "EarningsReleases"
            ]

        # Get the most recent earnings date for each ticker
        earnings_df = earnings_df.sort_values(
            ["Instrument", "Event Start Date"], ascending=[True, False]
        )
        latest_earnings = earnings_df.groupby("Instrument").first().reset_index()

        if latest_earnings.empty:
            return pd.DataFrame(
                columns=[
                    "Instrument",
                    "Last Earnings Date",
                    "Price at Last Earnings",
                    "Since Last Earnings Return",
                ]
            )

        # Step 2: Group tickers by earnings date to batch API calls
        # This reduces API calls from N (one per ticker) to M (one per unique date)
        from collections import defaultdict

        tickers_by_date = defaultdict(list)
        for _, row in latest_earnings.iterrows():
            earnings_date = pd.to_datetime(row["Event Start Date"]).date()
            tickers_by_date[earnings_date].append(row["Instrument"])

        # Step 3 & 4: Fetch historical and reference prices in parallel
        from concurrent.futures import ThreadPoolExecutor, as_completed

        historical_prices = {}  # {(ticker, date): price}

        # Parallelize all API calls (historical by date + reference prices)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit reference price query first (runs in parallel with historical)
            ref_prices_future = executor.submit(
                rd.get_data,
                universe=tickers,
                fields=["TR.PriceClose"],
                parameters={"SDate": end_date, "EDate": end_date} if as_of_date else {},
            )

            # Submit all date-based historical price queries
            futures = {}
            for earnings_date, ticker_list in tickers_by_date.items():
                earnings_date_str = earnings_date.strftime("%Y-%m-%d")
                future = executor.submit(
                    rd.get_data,
                    universe=ticker_list,
                    fields=["TR.PriceClose"],
                    parameters={"SDate": earnings_date_str, "EDate": earnings_date_str},
                )
                futures[future] = (earnings_date, ticker_list)

            # Collect historical price results as they complete
            for future in as_completed(futures):
                earnings_date, ticker_list = futures[future]
                try:
                    hist_df = future.result()

                    if (
                        hist_df is not None
                        and not hist_df.empty
                        and "Price Close" in hist_df.columns
                    ):
                        for _, row in hist_df.iterrows():
                            ticker = row["Instrument"]
                            price = row["Price Close"]
                            if pd.notna(price):
                                historical_prices[(ticker, earnings_date)] = price
                except Exception as e:
                    # Log and skip this batch - individual batch failures are acceptable
                    logger.warning(
                        f"Failed to fetch historical prices for {len(ticker_list)} tickers: {e}"
                    )
                    continue

            # Get reference prices (wait for completion)
            reference_prices = ref_prices_future.result()

        # Build reference price lookup
        ref_price_dict = {}
        if reference_prices is not None and not reference_prices.empty:
            for _, row in reference_prices.iterrows():
                ticker = row["Instrument"]
                price = row.get("Price Close")
                if pd.notna(price):
                    ref_price_dict[ticker] = price

        # Step 5: Calculate returns from historical and reference prices
        results = []
        for _, row in latest_earnings.iterrows():
            ticker = row["Instrument"]
            earnings_date = pd.to_datetime(row["Event Start Date"]).date()

            # Look up historical and reference prices
            hist_price = historical_prices.get((ticker, earnings_date))
            ref_price = ref_price_dict.get(ticker)

            if hist_price is not None and ref_price is not None and hist_price > 0:
                return_pct = ((ref_price - hist_price) / hist_price) * 100

                results.append(
                    {
                        "Instrument": ticker,
                        "Last Earnings Date": row["Event Start Date"],
                        "Price at Last Earnings": hist_price,
                        "Since Last Earnings Return": return_pct,
                    }
                )

        return pd.DataFrame(results)

    except Exception as e:
        raise DataRetrievalError(
            f"Failed to get since-last-earnings returns: {e}"
        ) from e
