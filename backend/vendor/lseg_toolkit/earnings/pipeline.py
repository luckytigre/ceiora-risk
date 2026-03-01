"""
Main pipeline for earnings report generation.
"""

import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pandas as pd

from lseg_toolkit.client import LsegClient
from lseg_toolkit.earnings.config import EarningsConfig
from lseg_toolkit.excel import ExcelExporter
from lseg_toolkit.shared import calculate_sector_breakdown, calculate_summary_statistics


@contextmanager
def timer(description: str, verbose: bool = True):
    """Context manager to time code blocks."""
    start = time.time()
    yield
    elapsed = time.time() - start
    if verbose:
        print(f"  {description}: {elapsed:.2f}s")


class EarningsReportPipeline:
    """
    Pipeline for generating earnings reports.

    Orchestrates the data extraction, processing, and export workflow.
    """

    def __init__(self, config: EarningsConfig):
        """
        Initialize earnings report pipeline.

        Args:
            config: Earnings report configuration
        """
        self.config = config
        self.client = LsegClient()

    def run(self) -> Path:
        """
        Execute the full earnings report pipeline.

        Returns:
            Path to generated Excel report

        Workflow:
            1. Get index constituents with market cap filters
            2. Fetch earnings data for date range
            3. Get financial data (estimates, ratios, analyst targets)
            4. Process and transform data
            5. Export to multi-sheet Excel workbook
        """
        total_start = time.time()
        print(f"Generating earnings report for {self.config.index}...")
        print("\nPERFORMANCE PROFILE")
        print("=" * 80)

        # Step 1: Get universe of companies
        with timer("Step 1: Get index constituents"):
            tickers = self._get_universe()
        print(f"Found {len(tickers)} companies matching criteria\n")

        # Step 2: Fetch earnings and financial data
        with timer("Step 2: Fetch all data"):
            earnings_df = self._fetch_earnings_data(tickers)
        print(f"Retrieved earnings data for {len(earnings_df)} companies\n")

        # Step 3: Process data
        with timer("Step 3: Process and transform data"):
            processed_df = self._process_data(earnings_df)

        # Step 4: Export to Excel
        with timer("Step 4: Export to Excel"):
            output_path = self._export_to_excel(processed_df)

        total_elapsed = time.time() - total_start
        print("=" * 80)
        print(f"TOTAL TIME: {total_elapsed:.2f}s")
        print("=" * 80)
        print(f"\nReport generated: {output_path}")

        return output_path

    def _get_universe(self) -> list[str]:
        """
        Get list of tickers matching index and market cap criteria.

        Returns:
            List of ticker symbols
        """
        return self.client.get_index_constituents(
            index=self.config.index,
            min_market_cap=self.config.min_market_cap,
            max_market_cap=self.config.max_market_cap,
        )

    def _fetch_earnings_data(self, tickers: list[str]) -> pd.DataFrame:
        """
        Fetch all required data for earnings report.

        Args:
            tickers: List of ticker symbols (all index constituents)

        Returns:
            DataFrame with comprehensive earnings and financial data
            (only for companies with earnings in the date range)
        """
        # Step 1: Get earnings dates for all tickers (lightweight query)
        with timer("  → Fetch earnings dates/times (all constituents)"):
            earnings = self.client.get_earnings_data(
                tickers=tickers,
                start_date=self.config.start_date.strftime("%Y-%m-%d"),
                end_date=self.config.end_date.strftime("%Y-%m-%d"),
                convert_timezone=self.config.timezone,  # Add timezone conversion
            )

        # Step 2: Filter to only tickers with earnings in this period
        if earnings.empty:
            return pd.DataFrame()

        earnings_tickers = earnings["Instrument"].unique().tolist()
        print(
            f"  {len(earnings_tickers)} companies have earnings (vs {len(tickers)} total)"
        )

        # Step 3: Fetch detailed financial data ONLY for companies with earnings
        # Use concurrent execution to fetch all data in parallel
        # IMPORTANT: Use consensus_date for snapshot consistency
        snapshot_date = self.config.consensus_date.strftime("%Y-%m-%d")

        with timer(
            "  → Fetch company data, ratios, estimates, and earnings returns (parallel)"
        ):
            with ThreadPoolExecutor(max_workers=5) as executor:
                # Submit all tasks with snapshot date
                company_future = executor.submit(
                    self.client.get_company_data,
                    earnings_tickers,
                    as_of_date=snapshot_date,
                )
                ratios_future = executor.submit(
                    self.client.get_financial_ratios,
                    earnings_tickers,
                    as_of_date=snapshot_date,
                )
                fq1_future = executor.submit(
                    self.client.get_consensus_estimates,
                    earnings_tickers,
                    "FQ1",
                    as_of_date=snapshot_date,
                )
                fy1_future = executor.submit(
                    self.client.get_consensus_estimates,
                    earnings_tickers,
                    "FY1",
                    as_of_date=snapshot_date,
                )
                last_earnings_future = executor.submit(
                    self.client.get_since_last_earnings_return,
                    earnings_tickers,
                    as_of_date=snapshot_date,
                )

                # Wait for all to complete
                company_data = company_future.result()
                ratios = ratios_future.result()
                fq1_estimates = fq1_future.result()
                fy1_estimates = fy1_future.result()
                last_earnings_returns = last_earnings_future.result()

        # Step 4: Merge datasets (all DataFrames use 'Instrument' column for ticker)
        with timer("  → Merge datasets"):
            # Company data should keep: Company Common Name (override), TRBC Economic Sector, Price Close, Market Cap
            company_cols_to_keep = [
                "Instrument",
                "Company Common Name",
                "TRBC Economic Sector Name",
                "Price Close",
                "Company Market Cap",
            ]
            company_data_clean = company_data[
                [col for col in company_cols_to_keep if col in company_data.columns]
            ]

            # Ratios also contains Price Close and Market Cap - drop them to avoid conflicts
            # We'll use the values from company_data instead (more reliable)
            ratios_cols_to_drop = [
                "Price Close",
                "Company Market Cap",
                "Company Common Name",
            ]
            ratios_clean = ratios.drop(
                columns=[col for col in ratios_cols_to_drop if col in ratios.columns],
                errors="ignore",
            )

            # Merge in order: earnings → company_data → ratios
            df = earnings.merge(
                company_data_clean, on="Instrument", how="left", suffixes=("_drop", "")
            )
            df = df.merge(
                ratios_clean, on="Instrument", how="left", suffixes=("_drop", "")
            )

            # Drop any columns with _drop suffix
            df = df.drop(
                columns=[col for col in df.columns if col.endswith("_drop")],
                errors="ignore",
            )

            # Merge estimates with prefixes to distinguish periods
            df = df.merge(
                fq1_estimates.add_prefix("FQ1_"),
                left_on="Instrument",
                right_on="FQ1_Instrument",
                how="left",
            )
            df = df.merge(
                fy1_estimates.add_prefix("FY1_"),
                left_on="Instrument",
                right_on="FY1_Instrument",
                how="left",
            )

            # Drop duplicate instrument columns from estimates
            df = df.drop(columns=["FQ1_Instrument", "FY1_Instrument"], errors="ignore")

            # Merge last earnings returns
            if not last_earnings_returns.empty:
                df = df.merge(last_earnings_returns, on="Instrument", how="left")

        return df

    def _process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and transform raw data.

        Args:
            df: Raw data DataFrame

        Returns:
            Processed DataFrame ready for export
        """
        # Round numeric columns to 2 decimal places
        numeric_cols = df.select_dtypes(include=["float64", "float32"]).columns
        df[numeric_cols] = df[numeric_cols].round(2)

        # Sort by sector and earnings date
        # The actual column names from LSEG are different than expected
        sector_col = "TRBC Economic Sector Name"
        date_col = "Event Start Date"

        if sector_col in df.columns and date_col in df.columns:
            df = df.sort_values([sector_col, date_col])

        return df

    def _export_to_excel(self, df: pd.DataFrame) -> Path:
        """
        Export processed data to formatted Excel workbook.

        Args:
            df: Processed DataFrame

        Returns:
            Path to exported Excel file
        """
        # Generate output filename
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(exist_ok=True)

        filename = ExcelExporter.generate_filename(
            f"earnings_report_{self.config.index.lower()}"
        )
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

            # Write data by sector with detailed grouping
            sector_col = "TRBC Economic Sector Name"
            if sector_col in df.columns:
                for sector in df[sector_col].dropna().unique():
                    sector_df = df[df[sector_col] == sector].copy()
                    # Sort by earnings date within sector (soonest first)
                    sector_df = sector_df.sort_values("Event Start Date")
                    sheet_name = sector[:31]  # Excel sheet name limit
                    exporter.write_sector_sheet_with_details(
                        sheet_name=sheet_name,
                        df=sector_df,
                        fiscal_period_col="Fiscal Period",
                    )
            else:
                # Write all data to single sheet if no sector column
                exporter.write_dataframe(df, sheet_name="Data", include_index=False)

        return output_path

    def _calculate_statistics(self, df: pd.DataFrame) -> dict[str, Any]:
        """Calculate summary statistics for report."""
        return calculate_summary_statistics(
            df,
            sector_col="TRBC Economic Sector Name",
            market_cap_col="Company Market Cap",
            market_cap_divisor=1_000_000,
        )

    def _calculate_sector_breakdown(
        self, df: pd.DataFrame
    ) -> dict[str, dict[str, Any]]:
        """Calculate sector breakdown with counts, percentages, and market cap statistics."""
        return calculate_sector_breakdown(
            df,
            sector_col="TRBC Economic Sector Name",
            market_cap_col="Company Market Cap",
            market_cap_divisor=1_000_000,
        )
