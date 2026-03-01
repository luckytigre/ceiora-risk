"""
Financial ratios and valuation metrics retrieval.

Handles fetching comprehensive financial data including valuation ratios,
debt metrics, performance indicators, and analyst metrics.
"""

import logging

import lseg.data as rd
import pandas as pd

from ..constants import TWO_YEARS_DAYS
from ..exceptions import DataRetrievalError

logger = logging.getLogger(__name__)


def get_financial_ratios(
    tickers: list[str], include_estimates: bool = True, as_of_date: str | None = None
) -> pd.DataFrame:
    """
    Get comprehensive financial ratios and valuation metrics for a list of companies.

    This function retrieves a wide range of financial data from LSEG and performs
    several derived calculations. It supports point-in-time historical snapshots
    for consistent backtesting and consensus estimate analysis.

    Args:
        tickers (list[str]): List of ticker symbols (RICs) to fetch data for.
            Example: ["AAPL.O", "MSFT.O", "GOOGL.O"]
        include_estimates (bool): If True, includes forward-looking consensus
            estimates (NTM = Next Twelve Months). Defaults to True.
        as_of_date (str | None): Optional date string in YYYY-MM-DD format for
            historical point-in-time snapshot. If None, returns current/live data.
            Example: "2025-01-15"

    Returns:
        pd.DataFrame: DataFrame indexed by Instrument with columns including:

            **Valuation Ratios:**
            - P/E (Daily Time Series Ratio): P/E LTM (Last Twelve Months)
            - Price Close: Share price (used for calculations)
            - Earnings Per Share - Mean: EPS consensus NTM
            - P/E NTM: Calculated forward P/E (Price / EPS consensus)
            - Enterprise Value To EBITDA: EV/EBITDA LTM
            - EV/EBITDA NTM: Forward EV/EBITDA
            - Price To Book Value Per Share: P/B ratio
            - P/FCF LTM: Price to Free Cash Flow (calculated)

            **Debt Metrics:**
            - Net Debt: Net debt position (Total Debt - Cash)
            - Enterprise Value: Market Cap + Net Debt
            - Net Debt to EBITDA NTM: Leverage ratio (calculated)
            - Net Debt % of EV: Net debt as percentage of enterprise value

            **Performance Metrics:**
            - 1 Month Total Return: 1-month total return %
            - 3 Month Total Return: 3-month total return %
            - 6 Month Total Return: 6-month total return %
            - YTD Total Return: Year-to-date total return %
            - 1 Year Total Return: 1-year total return %
            - 2 Year Total Return: 2-year total return % (calculated separately)
            - 3 Year Total Return: 3-year total return %
            - 5 Year Total Return: 5-year total return %
            - Dividend Yield: Dividend yield %

            **Analyst Metrics:**
            - Price / Intrinsic Value: StarMine P/IV ratio

    Raises:
        DataRetrievalError: If the LSEG API call fails or returns an error.

    Implementation Notes:
        Several ratios are calculated manually rather than using LSEG's pre-built
        fields for better precision and historical snapshot support:

        - **P/E NTM** = Price / EPS consensus
          (avoids LSEG datetime nanosecond conversion issues)
        - **Net Debt** = Total Debt - Cash
          (TR.F.NetDebt doesn't support historical snapshots)
        - **Enterprise Value** = Market Cap + Net Debt
          (TR.F.EV doesn't support historical snapshots)
        - **Free Cash Flow** = Operating Cash Flow - CapEx
          (TR.FreeCashFlow doesn't support historical snapshots)
        - **P/FCF** = Market Cap / Free Cash Flow
        - **Net Debt / EBITDA** = Net Debt / EBITDA NTM consensus

        **Financial Company Handling:**
        Financial companies (banks, insurance, asset managers) have fundamentally
        different balance sheets where "debt" includes customer deposits and
        liabilities. For these companies, Net Debt and FCF metrics are set to NaN
        as they are not meaningful.

        **Return Period Validation:**
        Returns are set to NaN for stocks that haven't traded long enough for
        the metric to be meaningful (based on IPO date).
    """
    fields = [
        # Valuation - LTM
        "TR.PE",  # P/E LTM
        "TR.EVToEBITDA",  # EV/EBITDA LTM
        "TR.PriceToBVPerShare",  # P/B
        # Components for calculated ratios
        "TR.PriceClose",  # Share price
        "TR.CompanyMarketCap",  # Market cap (for P/FCF and EV calculation)
        # Debt components (for Net Debt calculation - works with historical snapshots)
        "TR.TotalDebt",  # Total debt
        "TR.CashAndSTInvestments",  # Cash and short-term investments
        # Cash flow components (for FCF calculation - works with historical snapshots)
        "TR.CashFromOperatingActivities",  # Operating cash flow
        "TR.CapitalExpenditures",  # Capital expenditures
        # Company info for return validation and sector identification
        "TR.IPODate",  # IPO date (to validate return periods)
        "TR.TRBCEconomicSector",  # Sector (to identify financials)
        # Performance metrics
        "TR.TotalReturn1Mo",  # 1-month total return %
        "TR.TotalReturn3Mo",  # 3-month total return %
        "TR.TotalReturn6Mo",  # 6-month total return %
        "TR.TotalReturnYTD",  # Year-to-date total return %
        "TR.TotalReturn1Yr",  # 1-year total return %
        "TR.TotalReturn3Yr",  # 3-year total return %
        "TR.TotalReturn5Yr",  # 5-year total return %
        "TR.DividendYield",  # Dividend yield %
        # Analyst metrics
        "TR.IVPriceToIntrinsicValue",  # StarMine P/IV
    ]

    if include_estimates:
        # Add forward-looking estimates (NTM)
        fields.extend(
            [
                "TR.EPSMean(Period=NTM)",  # EPS consensus NTM
                "TR.EBITDAMean(Period=NTM)",  # EBITDA consensus NTM
                "TR.H.EV/TR.EBITDAMean(Period=NTM)",  # EV/EBITDA NTM (calculated field)
            ]
        )

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

        # Calculate 2-year return separately using TR.TotalReturn with date range
        # (LSEG doesn't provide TR.TotalReturn2Yr, but we can use TR.TotalReturn with dates)
        try:
            from datetime import datetime, timedelta

            two_years_ago = (datetime.now() - timedelta(days=TWO_YEARS_DAYS)).strftime(
                "%Y-%m-%d"
            )
            today = datetime.now().strftime("%Y-%m-%d")

            return_2y_df = rd.get_data(
                universe=tickers,
                fields=["TR.TotalReturn"],
                parameters={"SDate": two_years_ago, "EDate": today},
            )

            if not return_2y_df.empty and "Total Return" in return_2y_df.columns:
                # Merge 2Y return into main dataframe
                return_2y_df = return_2y_df.rename(
                    columns={"Total Return": "2 Year Total Return"}
                )
                df = df.merge(
                    return_2y_df[["Instrument", "2 Year Total Return"]],
                    on="Instrument",
                    how="left",
                )
        except Exception as e:
            # If 2Y return calculation fails, continue without it
            logger.warning(f"Could not calculate 2-year returns: {e}")

        # IMPORTANT: Convert all numeric columns to proper numeric types
        # Historical snapshots may return strings for some fields
        numeric_columns = [
            "Price Close",
            "Company Market Cap",
            "Total Debt",
            "Cash and Short Term Investments",
            "Free Cash Flow",
            "Earnings Per Share - Mean",
            "EBITDA - Mean",
            "P/E (Daily Time Series Ratio)",
            "Enterprise Value To EBITDA",
            "Price To Book Value Per Share",
            "1 Month Total Return",
            "3 Month Total Return",
            "6 Month Total Return",
            "YTD Total Return",
            "1 Year Total Return",
            "3 Year Total Return",
            "5 Year Total Return",
            "Dividend Yield",
            "Price / Intrinsic Value",
        ]

        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Calculate Net Debt from components (Total Debt - Cash)
        # This works with historical snapshots, unlike TR.F.NetDebt
        if (
            "Total Debt" in df.columns
            and "Cash and Short Term Investments" in df.columns
        ):
            df["Net Debt"] = df["Total Debt"] - df["Cash and Short Term Investments"]

        # Calculate Enterprise Value (Market Cap + Net Debt)
        # This works with historical snapshots, unlike TR.F.EV
        if "Company Market Cap" in df.columns and "Net Debt" in df.columns:
            df["Enterprise Value"] = df["Company Market Cap"] + df["Net Debt"]

        # Set Net Debt metrics to N/A for financial companies
        # Financial companies (banks, insurance, asset managers) have different balance sheets
        # where "debt" includes customer deposits/liabilities and Net Debt is not meaningful
        sector_col = None
        if "TRBC Economic Sector Name" in df.columns:
            sector_col = "TRBC Economic Sector Name"
        elif "TRBC Economic Sector" in df.columns:
            sector_col = "TRBC Economic Sector"

        if sector_col:
            is_financial = df[sector_col] == "Financials"
            if is_financial.any():
                # Set Net Debt-related metrics to NaN for financials
                debt_metrics = [
                    "Net Debt",
                    "Enterprise Value",
                    "Net Debt to EBITDA NTM",
                    "Net Debt % of EV",
                ]
                for metric in debt_metrics:
                    if metric in df.columns:
                        df.loc[is_financial, metric] = pd.NA

        # Calculate P/E NTM manually (avoids datetime nanosecond conversion issues)
        if "Price Close" in df.columns and "Earnings Per Share - Mean" in df.columns:
            df["P/E NTM"] = df["Price Close"] / df["Earnings Per Share - Mean"]

        # Calculate Net Debt / EBITDA NTM
        if "Net Debt" in df.columns and "EBITDA - Mean" in df.columns:
            df["Net Debt to EBITDA NTM"] = df["Net Debt"] / df["EBITDA - Mean"]

        # Calculate Free Cash Flow (OCF - CapEx)
        # Note: We calculate this manually because TR.FreeCashFlow doesn't support historical snapshots
        if (
            "Cash from Operating Activities" in df.columns
            and "Capital Expenditures, Cumulative" in df.columns
        ):
            df["Free Cash Flow"] = df["Cash from Operating Activities"] - abs(
                df["Capital Expenditures, Cumulative"]
            )

            # Set to N/A for financial companies (banks, insurance, etc.)
            # FCF is not meaningful for financial institutions
            is_financial = df["TRBC Economic Sector Name"] == "Financials"
            df.loc[is_financial, "Free Cash Flow"] = pd.NA

        # Calculate P/FCF LTM (Market Cap / Free Cash Flow)
        if "Company Market Cap" in df.columns and "Free Cash Flow" in df.columns:
            df["P/FCF LTM"] = df["Company Market Cap"] / df["Free Cash Flow"]

        # Calculate Net Debt % of EV
        if "Net Debt" in df.columns and "Enterprise Value" in df.columns:
            df["Net Debt % of EV"] = (
                df["Net Debt"] / df["Enterprise Value"] * 100
            ).round(2)

        # Fix datetime conversion issue with EV/EBITDA NTM and rename
        ev_ebitda_ntm_col = "TR.H.EV/TR.EBITDAMEAN(PERIOD=NTM)"
        if ev_ebitda_ntm_col in df.columns:
            # If it's datetime type, extract numeric value from nanosecond component
            if df[ev_ebitda_ntm_col].dtype == "datetime64[ns]":
                df[ev_ebitda_ntm_col] = df[ev_ebitda_ntm_col].dt.nanosecond
            # Always rename to friendly name
            df = df.rename(columns={ev_ebitda_ntm_col: "EV/EBITDA NTM"})

        # Set returns to NaN if stock hasn't traded long enough
        if "IPO Date" in df.columns:
            from datetime import datetime, timedelta

            # Convert IPO Date to datetime if it's not already
            df["IPO Date"] = pd.to_datetime(df["IPO Date"], errors="coerce")
            today_ts = pd.Timestamp(datetime.now())

            # Define minimum trading periods for each return metric
            return_periods = {
                "1 Month Total Return": timedelta(days=30),
                "3 Month Total Return": timedelta(days=90),
                "6 Month Total Return": timedelta(days=180),
                "YTD Total Return": timedelta(days=0),  # Always valid (current year)
                "1 Year Total Return": timedelta(days=365),
                "2 Year Total Return": timedelta(days=365 * 2),
                "3 Year Total Return": timedelta(days=365 * 3),
                "5 Year Total Return": timedelta(days=365 * 5),
            }

            # Set to NaN if IPO date is too recent
            for return_col, min_period in return_periods.items():
                if return_col in df.columns:
                    # Stocks that IPO'd less than min_period ago get NaN
                    too_recent = (today_ts - df["IPO Date"]) < min_period
                    df.loc[too_recent, return_col] = pd.NA

        return df

    except Exception as e:
        raise DataRetrievalError(f"Failed to get financial ratios: {e}") from e
