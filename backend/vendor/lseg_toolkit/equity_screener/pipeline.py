"""
Equity screener pipeline orchestration.

Handles the complete workflow:
1. Screen equities using LSEG screener API
2. Fetch detailed financial metrics
3. Process and calculate derived metrics
4. Export to formatted Excel workbook
"""

import logging
from pathlib import Path
from typing import Any

import lseg.data as rd
import pandas as pd

from ..client import LsegClient
from ..excel import ExcelExporter
from ..exceptions import ConfigurationError, DataRetrievalError, DataValidationError
from ..shared import calculate_sector_breakdown, calculate_summary_statistics
from .config import EquityScreenerConfig

logger = logging.getLogger(__name__)


class EquityScreenerPipeline:
    """
    Main pipeline for equity screening workflow.

    Orchestrates screening, data fetching, processing, and export.
    """

    def __init__(self, config: EquityScreenerConfig):
        """
        Initialize pipeline with configuration.

        Args:
            config: EquityScreenerConfig with screening parameters
        """
        self.config = config
        self.client = LsegClient()

    def run(self) -> Path:
        """
        Execute the complete screening pipeline.

        Returns:
            Path to the exported Excel file

        Raises:
            RuntimeError: If screening or data fetching fails
        """
        print(f"\n{'=' * 80}")
        print("EQUITY SCREENER PIPELINE")
        print(f"{'=' * 80}\n")

        # Display configuration
        print("Configuration:")
        for key, value in self.config.to_dict().items():
            print(f"  {key}: {value}")
        print()

        # Step 1: Run screener
        print("Step 1: Running equity screener...")
        rics = self._run_screener()

        if not rics:
            raise DataValidationError("No stocks found matching screening criteria")

        print(f"Found {len(rics)} stocks matching criteria\n")

        # Step 2: Fetch financial data
        print("Step 2: Fetching financial metrics...")
        df = self._fetch_financial_data(rics)

        if df.empty:
            raise DataRetrievalError("Failed to fetch financial data")

        print(f"Retrieved data for {len(df)} stocks\n")

        # Step 3: Fetch activism data
        print("Step 3: Fetching activism campaign data...")
        activism_df = self._fetch_activism_data(rics)
        print("Activism data retrieved\n")

        # Step 4: Process data
        print("Step 4: Processing and calculating metrics...")
        df = self._process_data(df, activism_df)
        print("Data processing complete\n")

        # Step 5: Export to Excel
        print("Step 5: Exporting to Excel...")
        output_path = self._export_to_excel(df)
        print(f"Export complete: {output_path}\n")

        print(f"{'=' * 80}")
        print("SCREENING COMPLETE")
        print(f"{'=' * 80}")
        print(f"\nTotal stocks: {len(df)}")
        print(f"Sectors: {df['Sector'].nunique()}")
        print(f"Output file: {output_path}\n")

        return output_path

    def _run_screener(self) -> list[str]:
        """
        Run LSEG screener to get list of RICs matching criteria.

        Uses either index constituents or country-based screening.

        Returns:
            List of RICs (ticker symbols)
        """
        # If index is specified, use index constituents
        if self.config.index is not None:
            return self._get_index_constituents()

        # Otherwise, use country-based screening
        return self._get_country_based_universe()

    def _get_index_constituents(self) -> list[str]:
        """
        Get index constituents, optionally filtered by market cap.

        Returns:
            List of RICs from index
        """
        index_code = f".{self.config.index}"  # Add dot prefix for LSEG API
        min_cap, max_cap = self.config.get_mkt_cap_range()

        try:
            # Get index constituents
            df = rd.get_data(universe=index_code, fields=["TR.IndexConstituentRIC"])

            if df is None or df.empty:
                raise DataRetrievalError(
                    f"No constituents found for index {self.config.index}"
                )

            rics = df["Constituent RIC"].dropna().unique().tolist()

            # Apply market cap filter if specified
            if min_cap is not None or max_cap is not None:
                rics = self._filter_by_market_cap(rics, min_cap, max_cap)

            # Apply country filter if specified
            if self.config.country is not None:
                rics = self._filter_by_country(rics, self.config.country)

            return rics

        except Exception as e:
            raise DataRetrievalError(f"Failed to get index constituents: {e}") from e

    def _get_country_based_universe(self) -> list[str]:
        """
        Get universe based on country and market cap screening.

        Returns:
            List of RICs matching criteria
        """
        min_cap, max_cap = self.config.get_mkt_cap_range()

        # Build screener expression
        filters = ["U(IN(Equity(active,public,primary)))"]

        # Add country filter if specified
        if self.config.country is not None:
            filters.append(f"IN(TR.HQCountryCode,'{self.config.country}')")

        # Add market cap filter if specified
        if min_cap is not None and max_cap is not None:
            filters.append(f"BETWEEN(TR.CompanyMarketCap,{min_cap:.0f},{max_cap:.0f})")
        elif min_cap is not None:
            filters.append(f"TR.CompanyMarketCap>={min_cap:.0f}")
        elif max_cap is not None:
            filters.append(f"TR.CompanyMarketCap<={max_cap:.0f}")

        screener_expr = ", ".join(filters)

        try:
            screener_obj = rd.discovery.Screener(expression=screener_expr)
            screener_df = (
                screener_obj.get_data()
                if hasattr(screener_obj, "get_data")
                else pd.DataFrame(screener_obj)
            )

            if screener_df is None or screener_df.empty:
                return []

            # Get instrument column (try common names)
            instrument_col = None
            for col in ["Instrument", "RIC", "instrument", "ric"]:
                if col in screener_df.columns:
                    instrument_col = col
                    break

            if instrument_col is None:
                # Use first column as fallback
                instrument_col = screener_df.columns[0]

            return screener_df[instrument_col].tolist()

        except Exception as e:
            raise DataRetrievalError(f"Screener failed: {e}") from e

    def _filter_by_market_cap(
        self, rics: list[str], min_cap: float | None, max_cap: float | None
    ) -> list[str]:
        """
        Filter RICs by market cap range.

        Args:
            rics: List of RICs to filter
            min_cap: Minimum market cap (or None)
            max_cap: Maximum market cap (or None)

        Returns:
            Filtered list of RICs
        """
        # Fetch market caps
        caps_df = rd.get_data(universe=rics, fields=["TR.CompanyMarketCap"])

        # Apply filters
        if min_cap is not None:
            caps_df = caps_df[caps_df["Company Market Cap"] >= min_cap]
        if max_cap is not None:
            caps_df = caps_df[caps_df["Company Market Cap"] <= max_cap]

        return caps_df["Instrument"].tolist()

    def _filter_by_country(self, rics: list[str], country: str) -> list[str]:
        """
        Filter RICs by headquarters country.

        Args:
            rics: List of RICs to filter
            country: Country code (e.g., 'US', 'GB')

        Returns:
            Filtered list of RICs
        """
        # Fetch country data
        country_df = rd.get_data(universe=rics, fields=["TR.HQCountryCode"])

        # Find the actual column name (LSEG API returns different variations)
        country_col = None
        for col in ["Headquarters Country Code", "HQ Country Code", "Country Code"]:
            if col in country_df.columns:
                country_col = col
                break

        if country_col is None:
            # If column not found, return all RICs (no filtering)
            logger.warning("Could not find country column, skipping country filter")
            return rics

        # Filter by country
        country_df = country_df[country_df[country_col] == country]

        return country_df["Instrument"].tolist()

    def _fetch_financial_data(self, rics: list[str]) -> pd.DataFrame:
        """
        Fetch detailed financial metrics for screened stocks.

        Args:
            rics: List of RIC ticker symbols

        Returns:
            DataFrame with financial data
        """
        # Define fields to fetch
        # Note: Using manual calculations for some metrics due to API limitations
        fields = [
            "TR.CommonName",
            "TR.TRBCEconomicSector",
            "TR.CompanyMarketCap",
            "TR.PE",  # P/E LTM
            "TR.PriceClose",  # For P/E NTM calculation
            "TR.EPSMean(Period=NTM)",  # For P/E NTM calculation
            "TR.EVToEBITDA",  # EV/EBITDA LTM
            "TR.EBITDAMean(Period=NTM)",  # For EV/EBITDA NTM and Net Debt / EBITDA calculation
            "TR.F.LEVEREDFOCF(Period=LTM)",  # For P/FCF calculation
            "TR.PriceToBVPerShare",  # P/B
            # Debt components (for Net Debt and EV calculation - more reliable than TR.F.NetDebt/TR.F.EV)
            "TR.TotalDebt",  # Total debt
            "TR.CashAndSTInvestments",  # Cash and short-term investments
            # Performance metrics - returns
            "TR.TotalReturn1Mo",  # 1-month return
            "TR.TotalReturn3Mo",  # 3-month return
            "TR.TotalReturn6Mo",  # 6-month return
            "TR.TotalReturnYTD",  # Year-to-date return
            "TR.TotalReturn1Yr",  # 1-year return
            "TR.DividendYield",
            "TR.IVPriceToIntrinsicValue",  # StarMine Price to Intrinsic Value
        ]

        try:
            df = rd.get_data(universe=rics, fields=fields)

            if df is None or df.empty:
                return pd.DataFrame()

            return df

        except Exception as e:
            raise DataRetrievalError(f"Failed to fetch financial data: {e}") from e

    def _fetch_activism_data(self, rics: list[str]) -> pd.DataFrame:
        """
        Fetch activist campaign data for stocks.

        Args:
            rics: List of RIC ticker symbols

        Returns:
            DataFrame with activism dates (empty if unavailable)
        """
        activism_fields = ["TR.PFAnnDate"]  # Campaign Announcement Date

        try:
            activism_df = rd.get_data(universe=rics, fields=activism_fields)

            if activism_df is None or activism_df.empty:
                return pd.DataFrame()

            # Get most recent campaign date per stock
            latest = (
                activism_df.groupby("Instrument")["Campaign Announcement Date"]
                .max()
                .reset_index()
            )
            latest = latest.rename(
                columns={"Campaign Announcement Date": "Latest Activism Date"}
            )

            # Replace NaT with None
            latest["Latest Activism Date"] = latest["Latest Activism Date"].replace(
                {pd.NaT: None}
            )

            # Count stocks with activism
            activism_count = latest["Latest Activism Date"].notna().sum()
            if activism_count > 0:
                print(f"  → {activism_count} stocks with activism history")

            return latest

        except Exception as e:
            logger.warning(f"Activism data retrieval failed (non-critical): {e}")
            return pd.DataFrame()

    def _process_data(
        self, df: pd.DataFrame, activism_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Process raw financial data and calculate derived valuation metrics.

        This method performs several key transformations:
        1. Calculates derived financial metrics (Net Debt, EV, P/E NTM, etc.)
        2. Handles special cases for financial companies
        3. Renames columns to user-friendly display names
        4. Merges activism campaign data
        5. Cleans up intermediate calculation columns

        Args:
            df (pd.DataFrame): Raw financial data from LSEG API with columns:
                - Instrument: RIC ticker symbol
                - Company Common Name: Company name
                - TRBC Economic Sector Name: Sector classification
                - Company Market Cap: Market capitalization (raw, in dollars)
                - Total Debt, Cash and Short Term Investments: For Net Debt calc
                - Price Close, Earnings Per Share - Mean: For P/E NTM calc
                - EBITDA - Mean: For EV/EBITDA NTM and leverage calc
                - Various return columns (1Mo, 3Mo, 6Mo, YTD, 1Yr)
            activism_df (pd.DataFrame): Activism campaign data with columns:
                - Instrument: RIC ticker symbol
                - Latest Activism Date: Most recent campaign announcement date
                May be empty DataFrame if no activism data available.

        Returns:
            pd.DataFrame: Processed DataFrame with:
                - Renamed columns for display (e.g., "RIC", "Company Name", "Mkt Cap ($M)")
                - Market cap converted to millions
                - Calculated ratios: P/E NTM, EV/EBITDA NTM, Net Debt to EBITDA NTM,
                  P/FCF LTM, Net Debt % of EV
                - Intermediate calculation columns removed
                - Sorted by Sector, then Company Name

        Implementation Notes:
            **Derived Metric Calculations:**
            - Net Debt = Total Debt - Cash and Short Term Investments
            - Enterprise Value = Market Cap + Net Debt
            - P/E NTM = Price Close / Earnings Per Share Mean (NTM consensus)
            - EV/EBITDA NTM = Enterprise Value / EBITDA Mean (NTM consensus)
            - Net Debt to EBITDA NTM = Net Debt / EBITDA Mean
            - P/FCF LTM = Market Cap / Free Cash Flow
            - Net Debt % of EV = (Net Debt / Enterprise Value) * 100

            **Financial Company Handling:**
            For companies in the "Financials" sector (banks, insurance, asset managers),
            the following metrics are set to NaN (displayed as blank/N/A):
            - Net Debt and Enterprise Value
            - Net Debt to EBITDA NTM
            - Net Debt % of EV
            - EV/EBITDA NTM

            This is because financial companies have fundamentally different balance
            sheets where "debt" includes customer deposits and policyholder liabilities,
            making traditional debt metrics not meaningful.

            **Column Cleanup:**
            Intermediate columns used only for calculations are dropped:
            - Earnings Per Share - Mean
            - EBITDA - Mean
            - Free Cash Flow
            - Net Debt, Enterprise Value
            - Total Debt, Cash and Short Term Investments
        """
        # Calculate Net Debt from components (Total Debt - Cash)
        # This is more reliable than TR.F.NetDebt
        if (
            "Total Debt" in df.columns
            and "Cash and Short Term Investments" in df.columns
        ):
            df["Net Debt"] = df["Total Debt"] - df["Cash and Short Term Investments"]

        # Calculate Enterprise Value (Market Cap + Net Debt)
        # This is more reliable than TR.F.EV
        if "Company Market Cap" in df.columns and "Net Debt" in df.columns:
            df["Enterprise Value"] = df["Company Market Cap"] + df["Net Debt"]

        # Set Net Debt metrics to N/A for financial companies
        # Financial companies (banks, insurance, asset managers) have different balance sheets
        # where "debt" includes customer deposits/liabilities and Net Debt is not meaningful
        if "TRBC Economic Sector Name" in df.columns:
            is_financial = df["TRBC Economic Sector Name"] == "Financials"
            if is_financial.any():
                # Set Net Debt-related metrics to NaN for financials
                debt_metrics = [
                    "Net Debt",
                    "Enterprise Value",
                    "Net Debt to EBITDA NTM",
                    "Net Debt % of EV",
                    "EV/EBITDA NTM",
                ]
                for metric in debt_metrics:
                    if metric in df.columns:
                        df.loc[is_financial, metric] = pd.NA

        # Calculate P/E NTM manually (API formula has precision issues)
        if "Price Close" in df.columns and "Earnings Per Share - Mean" in df.columns:
            df["P/E NTM"] = df["Price Close"] / df["Earnings Per Share - Mean"]

        # Calculate EV/EBITDA NTM manually (more reliable than TR.H.EV/TR.EBITDAMean)
        if "Enterprise Value" in df.columns and "EBITDA - Mean" in df.columns:
            df["EV/EBITDA NTM"] = df["Enterprise Value"] / df["EBITDA - Mean"]

        # Calculate Net Debt / EBITDA NTM
        if "Net Debt" in df.columns and "EBITDA - Mean" in df.columns:
            df["Net Debt to EBITDA NTM"] = df["Net Debt"] / df["EBITDA - Mean"]

        # Calculate P/FCF LTM
        if "Company Market Cap" in df.columns and "Free Cash Flow" in df.columns:
            df["P/FCF LTM"] = df["Company Market Cap"] / df["Free Cash Flow"]

        # Calculate Net Debt % of EV
        if "Net Debt" in df.columns and "Enterprise Value" in df.columns:
            df["Net Debt % of EV"] = (
                df["Net Debt"] / df["Enterprise Value"] * 100
            ).round(2)

        # Rename columns for clarity
        df = df.rename(
            columns={
                "Instrument": "RIC",
                "Company Common Name": "Company Name",
                "TRBC Economic Sector Name": "Sector",
                "Company Market Cap": "Mkt Cap ($M)",
                "P/E (Daily Time Series Ratio)": "P/E LTM",
                "Enterprise Value To EBITDA (Daily Time Series Ratio)": "EV/EBITDA LTM",
                "Price To Book Value Per Share (Daily Time Series Ratio)": "P/B",
                # Performance metrics
                "1 Month Total Return": "1 Mo Return (%)",
                "3 Month Total Return": "3 Mo Return (%)",
                "6 Month Total Return": "6 Mo Return (%)",
                "YTD Total Return": "YTD Return (%)",
                "1 Year Total Return": "1 Yr Return (%)",
                "Dividend yield": "Dividend Yield LTM (%)",
                "Price / Intrinsic Value": "StarMine P/IV",
                "Price Close": "Share Price",
            }
        )

        # Convert market cap to millions
        if "Mkt Cap ($M)" in df.columns:
            df["Mkt Cap ($M)"] = (df["Mkt Cap ($M)"] / 1_000_000).round(2)

        # Merge activism data
        if not activism_df.empty:
            df = df.merge(activism_df, left_on="RIC", right_on="Instrument", how="left")
            df = df.drop(columns=["Instrument"], errors="ignore")
        else:
            df["Latest Activism Date"] = None

        # Drop intermediate calculation columns
        cols_to_drop = [
            "Earnings Per Share - Mean",
            "EBITDA - Mean",
            "Free Cash Flow",
            "Net Debt",
            "Enterprise Value",
            "Total Debt",
            "Cash and Short Term Investments",
        ]
        df = df.drop(
            columns=[col for col in cols_to_drop if col in df.columns], errors="ignore"
        )

        # Sort by sector and company name
        if "Sector" in df.columns and "Company Name" in df.columns:
            df = df.sort_values(["Sector", "Company Name"])

        return df

    def _export_to_excel(self, df: pd.DataFrame) -> Path:
        """
        Export data to formatted Excel workbook.

        Args:
            df: Processed data

        Returns:
            Path to Excel file
        """
        # Generate output filename
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(exist_ok=True)

        # Generate filename with single date stamp (screen_date already in YYYY-MM-DD format)
        # Convert to YYYYMMDD format for consistency
        if self.config.screen_date is None:
            raise ConfigurationError(
                "screen_date must be set (should be set in __post_init__)"
            )
        date_str = self.config.screen_date.replace("-", "")
        filename = f"equity_screener_{date_str}.xlsx"
        output_path = output_dir / filename

        with ExcelExporter(output_path) as exporter:
            # Write summary sheet
            stats = self._calculate_statistics(df)
            sector_breakdown = self._calculate_sector_breakdown(df)
            exporter.write_summary_sheet(
                sheet_name="Summary",
                params=self.config.to_dict(),
                statistics=stats,
                sector_breakdown=sector_breakdown,
            )

            # Write sector sheets
            if "Sector" in df.columns:
                for sector in df["Sector"].dropna().unique():
                    sector_df = df[df["Sector"] == sector].copy()

                    # Select columns for export
                    export_columns = [
                        "RIC",
                        "Company Name",
                        "Mkt Cap ($M)",
                        "P/E LTM",
                        "P/E NTM",
                        "EV/EBITDA LTM",
                        "EV/EBITDA NTM",
                        "P/FCF LTM",
                        "P/B",
                        "Net Debt % of EV",
                        "Net Debt to EBITDA NTM",
                        # Performance metrics
                        "1 Mo Return (%)",
                        "3 Mo Return (%)",
                        "6 Mo Return (%)",
                        "YTD Return (%)",
                        "1 Yr Return (%)",
                        "Dividend Yield LTM (%)",
                        "StarMine P/IV",
                        "Share Price",
                        "Latest Activism Date",
                    ]
                    available_cols = [
                        col for col in export_columns if col in sector_df.columns
                    ]
                    sector_export = sector_df[available_cols]

                    # Create safe sheet name (max 31 chars)
                    sheet_name = sector[:31].replace("/", "-").replace("\\", "-")
                    if not sheet_name or sheet_name.strip() == "":
                        sheet_name = "Unknown Sector"

                    exporter.write_screener_dataframe(
                        df=sector_export, sheet_name=sheet_name, include_index=False
                    )
            else:
                # Write all data to single sheet if no sector column
                exporter.write_screener_dataframe(
                    df, sheet_name="Data", include_index=False
                )

        return output_path

    def _calculate_statistics(self, df: pd.DataFrame) -> dict[str, Any]:
        """Calculate summary statistics."""
        return calculate_summary_statistics(
            df,
            sector_col="Sector",
            market_cap_col="Mkt Cap ($M)",
            market_cap_divisor=1.0,  # Already in millions
        )

    def _calculate_sector_breakdown(
        self, df: pd.DataFrame
    ) -> dict[str, dict[str, Any]]:
        """Calculate sector breakdown with counts and market cap statistics."""
        return calculate_sector_breakdown(
            df,
            sector_col="Sector",
            market_cap_col="Mkt Cap ($M)",
            market_cap_divisor=1.0,  # Already in millions
        )
