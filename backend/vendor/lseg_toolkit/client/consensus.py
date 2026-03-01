"""
Consensus estimates retrieval.

Handles fetching analyst consensus estimates for revenue, EBITDA, and EPS
across various time periods.
"""

import lseg.data as rd
import pandas as pd

from ..exceptions import ConfigurationError, DataRetrievalError


def _calculate_fiscal_period_label_simple(period_end_date: pd.Timestamp) -> str:
    """
    Calculate fiscal period label from period end date alone.

    Args:
        period_end_date: End date of the fiscal period

    Returns:
        String like "Q3 2025" for quarters or "FY2025" for full years

    Logic:
    - Standard quarterly periods end on: Mar 31, Jun 30, Sept 30, Dec 31
    - Maps to calendar quarters: Q1 (Mar), Q2 (Jun), Q3 (Sep), Q4 (Dec)
    - For non-standard dates, return just the year
    - Full year periods (ending Dec 31) labeled as "FY{year}"
    """
    if pd.isna(period_end_date):
        return "N/A"

    month = period_end_date.month
    day = period_end_date.day
    year = period_end_date.year

    # Standard quarter-end dates
    quarter_map = {
        (3, 31): ("Q1", year),
        (6, 30): ("Q2", year),
        (9, 30): ("Q3", year),
        (12, 31): ("Q4", year),
    }

    if (month, day) in quarter_map:
        quarter, q_year = quarter_map[(month, day)]
        # If it's Q4 (Dec 31), call it a full fiscal year
        if quarter == "Q4":
            return f"FY{q_year}"
        else:
            return f"{quarter} {q_year}"
    else:
        # Non-standard fiscal period - just show date
        return period_end_date.strftime("%b %Y")


def get_consensus_estimates(
    tickers: list[str], period: str = "NTM", as_of_date: str | None = None
) -> pd.DataFrame:
    """
    Get analyst consensus estimates for companies.

    Args:
        tickers: List of ticker symbols (RICs)
        period: Estimation period:
               - 'NTM': Next Twelve Months (rolling forward)
               - 'FY1': Current Fiscal Year
               - 'FY2': Next Fiscal Year
               - 'FQ1': Next Fiscal Quarter
               - 'FQ2': Quarter After Next
        as_of_date: Optional date string (YYYY-MM-DD) to get historical estimates
                   If None, returns current estimates

    Returns:
        DataFrame with columns:
            - Instrument: RIC ticker
            - Company Common Name: Company name
            - Revenue - Mean: Revenue consensus estimate
            - EBITDA - Mean: EBITDA consensus estimate
            - Earnings Per Share - Mean: EPS consensus estimate

    Examples:
        >>> client.get_consensus_estimates(['AAPL.O', 'MSFT.O'])
        # Returns current NTM consensus estimates

        >>> client.get_consensus_estimates(['AAPL.O'], period='FQ1')
        # Returns next quarter estimates

        >>> client.get_consensus_estimates(['AAPL.O'], period='FQ1', as_of_date='2025-10-26')
        # Returns FQ1 estimates as they were on Oct 26
    """
    # Map periods to actual field strings (must not use f-strings as LSEG uppercases them)
    # Include period end date to identify fiscal year/quarter
    period_map = {
        "NTM": [
            "TR.RevenueMean(Period=NTM)",
            "TR.EBITDAMean(Period=NTM)",
            "TR.EPSMean(Period=NTM)",
        ],
        "FY1": [
            "TR.RevenueMean(Period=FY1)",
            "TR.RevenueMean(Period=FY1).periodenddate",
            "TR.EBITDAMean(Period=FY1)",
            "TR.EPSMean(Period=FY1)",
        ],
        "FY2": [
            "TR.RevenueMean(Period=FY2)",
            "TR.RevenueMean(Period=FY2).periodenddate",
            "TR.EBITDAMean(Period=FY2)",
            "TR.EPSMean(Period=FY2)",
        ],
        "FQ1": [
            "TR.RevenueMean(Period=FQ1)",
            "TR.RevenueMean(Period=FQ1).periodenddate",
            "TR.EBITDAMean(Period=FQ1)",
            "TR.EPSMean(Period=FQ1)",
        ],
        "FQ2": [
            "TR.RevenueMean(Period=FQ2)",
            "TR.RevenueMean(Period=FQ2).periodenddate",
            "TR.EBITDAMean(Period=FQ2)",
            "TR.EPSMean(Period=FQ2)",
        ],
    }

    if period not in period_map:
        raise ConfigurationError(
            f"Invalid period: {period}. Must be one of: {list(period_map.keys())}"
        )

    fields = ["TR.CommonName"] + period_map[period]

    try:
        # Build parameters dict
        parameters = {}
        if as_of_date:
            # Use same date for SDate and EDate to get point-in-time snapshot
            parameters["SDate"] = as_of_date
            parameters["EDate"] = as_of_date

        df = rd.get_data(
            universe=tickers,
            fields=fields,
            parameters=parameters if parameters else None,
        )

        if df is None or df.empty:
            return pd.DataFrame()

        # Add fiscal period label for quarterly/yearly periods (not NTM)
        if period != "NTM" and "Period End Date" in df.columns:
            # Convert to datetime if not already
            if not pd.api.types.is_datetime64_any_dtype(df["Period End Date"]):
                df["Period End Date"] = pd.to_datetime(df["Period End Date"])

            # Calculate fiscal period label for each row
            df["Fiscal Period"] = df["Period End Date"].apply(
                _calculate_fiscal_period_label_simple
            )

        return df

    except Exception as e:
        raise DataRetrievalError(f"Failed to get consensus estimates: {e}") from e
