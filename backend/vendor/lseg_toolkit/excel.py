"""
Excel export utilities for financial reports.

Common Excel formatting and export functionality shared across projects.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import xlsxwriter

logger = logging.getLogger(__name__)


class ExcelExporter:
    """
    Utility class for exporting financial data to formatted Excel files.

    Features:
    - Standardized formatting (Book Antiqua font, 2 decimal places)
    - Multi-sheet workbooks
    - Automatic column width adjustment
    - Number formatting for financial data
    """

    DEFAULT_FONT = "Book Antiqua"
    DEFAULT_FONT_SIZE = 10
    DECIMAL_PLACES = 2

    def __init__(self, output_path: Path):
        """
        Initialize Excel exporter.

        Args:
            output_path: Path where Excel file will be saved
        """
        self.output_path = output_path
        self.writer: pd.ExcelWriter | None = None
        self.workbook: xlsxwriter.Workbook | None = None

    def __enter__(self):
        """Context manager entry."""
        self.writer = pd.ExcelWriter(
            self.output_path,
            engine="xlsxwriter",
            date_format="yyyy-mm-dd",
            datetime_format="yyyy-mm-dd hh:mm:ss",
        )
        self.workbook = self.writer.book
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.writer:
            self.writer.close()

    @property
    def _workbook(self) -> xlsxwriter.Workbook:
        """
        Get workbook instance, ensuring it's initialized.

        Returns:
            xlsxwriter Workbook instance

        Raises:
            RuntimeError: If used outside context manager
        """
        if self.workbook is None:
            raise RuntimeError("ExcelExporter must be used within context manager")
        return self.workbook

    @property
    def _writer(self) -> pd.ExcelWriter:
        """
        Get ExcelWriter instance, ensuring it's initialized.

        Returns:
            pandas ExcelWriter instance

        Raises:
            RuntimeError: If used outside context manager
        """
        if self.writer is None:
            raise RuntimeError("ExcelExporter must be used within context manager")
        return self.writer

    def get_standard_format(self, **kwargs) -> xlsxwriter.format.Format:
        """
        Get standard cell format with Book Antiqua font.

        Args:
            **kwargs: Additional format properties

        Returns:
            xlsxwriter Format object
        """
        format_props = {
            "font_name": self.DEFAULT_FONT,
            "font_size": self.DEFAULT_FONT_SIZE,
            "num_format": f"0.{self.DECIMAL_PLACES * '0'}",
        }
        format_props.update(kwargs)
        return self._workbook.add_format(format_props)

    def get_header_format(self) -> xlsxwriter.format.Format:
        """Get format for header rows."""
        return self.get_standard_format(bold=True, bg_color="#D3D3D3")

    def write_dataframe(
        self,
        df: pd.DataFrame,
        sheet_name: str,
        startrow: int = 0,
        startcol: int = 0,
        include_index: bool = False,
        auto_width: bool = True,
    ):
        """
        Write DataFrame to worksheet with standard formatting.

        Args:
            df: DataFrame to write
            sheet_name: Name of worksheet
            startrow: Starting row position
            startcol: Starting column position
            include_index: Whether to include DataFrame index
            auto_width: Whether to auto-adjust column widths
        """
        if df is None or df.empty:
            logger.warning(
                f"Empty DataFrame passed to write_dataframe for sheet '{sheet_name}'"
            )
            return

        df.to_excel(
            self._writer,
            sheet_name=sheet_name,
            startrow=startrow,
            startcol=startcol,
            index=include_index,
        )

        worksheet = self._writer.sheets[sheet_name]
        header_format = self.get_header_format()

        # Format header row
        for col_num, value in enumerate(df.columns):
            worksheet.write(startrow, startcol + col_num, value, header_format)

        # Auto-adjust column widths
        if auto_width:
            for col_num, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).str.len().max(), len(str(col))) + 2
                worksheet.set_column(startcol + col_num, startcol + col_num, max_len)

    def write_summary_sheet(
        self,
        sheet_name: str,
        params: dict[str, Any],
        statistics: dict[str, Any],
        sector_breakdown: dict[str, Any] | None = None,
    ):
        """
        Write a summary sheet with parameters and statistics.

        Args:
            sheet_name: Name of summary worksheet
            params: Dictionary of input parameters
            statistics: Dictionary of summary statistics
            sector_breakdown: Optional dictionary of sector counts and percentages
        """
        worksheet = self._workbook.add_worksheet(sheet_name)
        header_format = self.get_header_format()
        cell_format = self.get_standard_format()
        title_format = self.get_standard_format(bold=True, font_size=12)
        currency_format = self.get_standard_format(num_format="$#,##0.00")

        row = 0

        # Write report title and generation date
        worksheet.write(row, 0, "Earnings Report Summary", title_format)
        row += 1
        worksheet.write(row, 0, "Generated:", cell_format)
        worksheet.write(
            row, 1, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), cell_format
        )
        row += 2

        # Write parameters section
        worksheet.write(row, 0, "Report Parameters", header_format)
        row += 1
        for key, value in params.items():
            worksheet.write(row, 0, key, cell_format)
            worksheet.write(row, 1, str(value), cell_format)
            row += 1

        row += 1

        # Write summary statistics section
        worksheet.write(row, 0, "Summary Statistics", header_format)
        row += 1
        # Create format for integer counts (no decimals)
        integer_format = self.get_standard_format(num_format="0")

        for key, value in statistics.items():
            if not key.startswith("  "):  # Skip nested items (sector counts)
                worksheet.write(row, 0, key, cell_format)
                # Check if this is a market cap field (needs $ formatting)
                if "Market Cap" in key or "Mkt Cap" in key:
                    worksheet.write(row, 1, value, currency_format)
                # Format integers without decimals (counts, sector numbers)
                elif isinstance(value, (int, float)) and value == int(value):
                    worksheet.write(row, 1, int(value), integer_format)
                else:
                    worksheet.write(row, 1, value, cell_format)
                row += 1

        # Write sector breakdown section if provided
        if sector_breakdown:
            row += 1
            worksheet.write(row, 0, "Sector Breakdown", header_format)
            row += 1
            # Header row for sector table
            worksheet.write(row, 0, "Sector", header_format)
            worksheet.write(row, 1, "Count", header_format)
            worksheet.write(row, 2, "% of Total", header_format)
            worksheet.write(row, 3, "Avg Mkt Cap (M)", header_format)
            worksheet.write(row, 4, "Median Mkt Cap (M)", header_format)
            row += 1

            # Create integer format for count column
            integer_format = self.get_standard_format(num_format="0")

            for sector, data in sector_breakdown.items():
                worksheet.write(row, 0, sector, cell_format)
                worksheet.write(row, 1, data["count"], integer_format)
                worksheet.write(row, 2, f"{data['percentage']:.1f}%", cell_format)
                # Add avg and median market caps if available (in millions, format with $)
                worksheet.write(row, 3, data.get("avg_market_cap", 0), currency_format)
                worksheet.write(
                    row, 4, data.get("median_market_cap", 0), currency_format
                )
                row += 1

        # Auto-adjust column widths
        worksheet.set_column(
            0, 0, 30
        )  # First column (labels) - wider for longer labels
        worksheet.set_column(
            1, 1, 25
        )  # Second column (values) - wider for dates/values
        worksheet.set_column(2, 2, 15)  # Third column (percentages)
        worksheet.set_column(3, 4, 20)  # Market cap columns - wider for numbers

    def write_sector_sheet_with_details(
        self,
        sheet_name: str,
        df: pd.DataFrame,
        fiscal_period_col: str = "Fiscal Period",
    ):
        """
        Write a sector sheet with main rows and collapsible detail subrows.

        This method creates a hierarchical Excel layout where each company has:
        - A main summary row with key metrics (ticker, company, dates, returns)
        - Collapsible detail subrows with consensus estimates, valuation ratios,
          and financial metrics (hidden by default, expandable via Excel grouping)

        The layout is optimized for quick scanning of earnings data while allowing
        drill-down into detailed metrics when needed.

        Args:
            sheet_name (str): Name of the worksheet. Excel limits to 31 characters;
                longer names are truncated.
            df (pd.DataFrame): DataFrame with company data. Expected columns include:
                - Instrument: RIC ticker symbol
                - Company Common Name: Company name
                - Event Start Date: Earnings release date
                - Event Time (US/Eastern): Earnings release time
                - Company Market Cap: Market capitalization
                - Price Close: Current share price
                - Various return columns (YTD, 1Mo, 3Mo, 6Mo, 1Y, 2Y, 3Y, 5Y)
                - Consensus estimate columns (FQ1_*, FY1_*)
                - Valuation ratio columns (P/E, EV/EBITDA, P/B, etc.)
                - Financial metric columns (Net Debt, etc.)
                Should be pre-sorted by earnings date (soonest first).
            fiscal_period_col (str): Name of column containing fiscal period labels
                (e.g., "Q3 2025"). Used to label consensus estimate sections.
                Defaults to "Fiscal Period".

        Returns:
            None: Writes directly to the Excel worksheet.

        Implementation Notes:
            **Excel Formatting Applied:**
            - Header row with bold gray background (frozen)
            - First 4 columns frozen (Ticker, Company, Date, Time) for easy scrolling
            - Main rows styled with light gray background for visual separation
            - Subrows indented and italicized for hierarchy

            **Number Formats:**
            - Market cap: Displayed in millions with comma separators
            - Returns: Percentage format (value/100 displayed as X.XX%)
            - Valuation ratios: Displayed with "x" suffix (e.g., "15.2x")
            - Currency values: Dollar format with 2 decimal places

            **Collapsible Groups:**
            Each company's detail subrows are grouped at Excel outline level 1,
            hidden by default. Users can expand/collapse using Excel's built-in
            grouping controls (+/- buttons in the row margin).

            **NA Handling:**
            Missing values are displayed as "N/A" text for consistency.
            Financial metrics for financial companies (banks, insurance) show "N/A"
            as Net Debt and related metrics are not meaningful for these sectors.

            **Column Widths:**
            Columns are sized appropriately for their content type:
            - Ticker: 10 chars
            - Company/Metric: 35 chars
            - Date columns: 15 chars
            - Return percentages: 10 chars each
        """
        worksheet = self._workbook.add_worksheet(sheet_name)

        # Create formats
        header_format = self.get_standard_format(bold=True, bg_color="#D3D3D3")
        main_row_format = self.get_standard_format(bold=True, bg_color="#F2F2F2")
        subrow_label_format = self.get_standard_format(indent=1, italic=True)
        subrow_value_format = self.get_standard_format(indent=1)
        number_format = self.get_standard_format(num_format="#,##0.00")
        percent_format = self.get_standard_format(num_format="0.00%")
        ratio_format = self.get_standard_format(num_format='0.00"x"')
        currency_format = self.get_standard_format(num_format="$#,##0.00")
        date_format = self._workbook.add_format(
            {
                "font_name": self.DEFAULT_FONT,
                "font_size": self.DEFAULT_FONT_SIZE,
                "num_format": "ddd, mmm dd, yyyy",  # Wed, Oct 29, 2025
                "bold": True,
                "bg_color": "#F2F2F2",
            }
        )

        # Define main row columns
        main_headers = [
            "Ticker",
            "Company Name",
            "Earnings Date",
            "Earnings Time",
            "Mkt Cap (M)",
            "Share Price",
            "YTD %",
            "Since Last Earns %",
            "1 Mo %",
            "3 Mo %",
            "6 Mo %",
            "1Y %",
            "2Y %",
            "3Y %",
            "5Y %",
        ]

        # Write headers
        for col, header in enumerate(main_headers):
            worksheet.write(0, col, header, header_format)

        # Freeze top row and first 4 columns (Ticker, Company, Date, Time)
        worksheet.freeze_panes(1, 4)

        current_row = 1

        # Helper function to get value with NA handling
        def safe_get(data, key, default=0):
            """Get value from Series, converting NA to default."""
            try:
                val = data.get(key, default) if hasattr(data, "get") else data[key]
            except (KeyError, TypeError, IndexError):
                return default

            # Handle NA values
            if pd.isna(val):
                return default

            # Convert Series to scalar if needed
            if isinstance(val, pd.Series):
                val = val.iloc[0] if len(val) > 0 else default
                if pd.isna(val):
                    return default

            # Convert string to numeric if needed (for historical snapshots)
            if isinstance(val, str) and default == 0:
                val = pd.to_numeric(val, errors="coerce")
                if pd.isna(val):
                    return default

            return val

        # Helper function to write percentage with proper NA handling
        def write_percent(worksheet, row, col, data, key, format_obj):
            """Write percentage to Excel, showing 'N/A' for missing values (consistent with other metrics)."""
            val = safe_get(data, key, None)  # Use None as default instead of 0
            if val is None or pd.isna(val):
                worksheet.write(
                    row, col, "N/A", format_obj
                )  # Write "N/A" text for missing data
            else:
                worksheet.write(row, col, val / 100, format_obj)

        # Iterate through each company
        for _idx, row_data in df.iterrows():
            # Write main row with proper column names and formats
            worksheet.write(
                current_row, 0, safe_get(row_data, "Instrument", ""), main_row_format
            )
            worksheet.write(
                current_row,
                1,
                safe_get(row_data, "Company Common Name", ""),
                main_row_format,
            )
            # Date formatting - convert to date
            event_date = safe_get(row_data, "Event Start Date", "")
            if event_date and not pd.isna(event_date):
                worksheet.write_datetime(
                    current_row, 2, pd.to_datetime(event_date), date_format
                )
            else:
                worksheet.write(current_row, 2, "", date_format)
            worksheet.write(
                current_row,
                3,
                safe_get(row_data, "Event Time (US/Eastern)", ""),
                main_row_format,
            )
            worksheet.write(
                current_row,
                4,
                safe_get(row_data, "Company Market Cap", 0) / 1_000_000,
                number_format,
            )
            worksheet.write(
                current_row, 5, safe_get(row_data, "Price Close", 0), number_format
            )
            # Return columns - use write_percent to show "N/A" for missing data instead of 0.00%
            write_percent(
                worksheet, current_row, 6, row_data, "YTD Total Return", percent_format
            )
            write_percent(
                worksheet,
                current_row,
                7,
                row_data,
                "Since Last Earnings Return",
                percent_format,
            )
            write_percent(
                worksheet,
                current_row,
                8,
                row_data,
                "1 Month Total Return",
                percent_format,
            )
            write_percent(
                worksheet,
                current_row,
                9,
                row_data,
                "3 Month Total Return",
                percent_format,
            )
            write_percent(
                worksheet,
                current_row,
                10,
                row_data,
                "6 Month Total Return",
                percent_format,
            )
            write_percent(
                worksheet,
                current_row,
                11,
                row_data,
                "1 Year Total Return",
                percent_format,
            )
            write_percent(
                worksheet,
                current_row,
                12,
                row_data,
                "2 Year Total Return",
                percent_format,
            )
            write_percent(
                worksheet,
                current_row,
                13,
                row_data,
                "3 Year Total Return",
                percent_format,
            )
            write_percent(
                worksheet,
                current_row,
                14,
                row_data,
                "5 Year Total Return",
                percent_format,
            )

            current_row += 1
            subrow_start = current_row

            # Get fiscal period labels
            fq_period = safe_get(row_data, f"FQ1_{fiscal_period_col}", "This Quarter")
            fy_period = safe_get(row_data, f"FY1_{fiscal_period_col}", "This Year")

            # Write subrows organized by category
            # Format: (label, value, format_type)
            # format_type can be: 'currency', 'ratio', 'percent', 'number', 'header', 'blank'
            subrows = [
                # Consensus Estimates - Next Quarter (FQ1)
                (f"Consensus Estimates - Next Qtr (ending {fq_period})", "", "header"),
                (
                    "  Revenue",
                    safe_get(row_data, "FQ1_Revenue - Mean", 0) / 1_000_000,
                    "currency",
                ),
                (
                    "  EBITDA",
                    safe_get(row_data, "FQ1_EBITDA - Mean", 0) / 1_000_000,
                    "currency",
                ),
                (
                    "  EPS",
                    safe_get(row_data, "FQ1_Earnings Per Share - Mean", 0),
                    "currency",
                ),
                ("", "", "blank"),  # Blank row
                # Consensus Estimates - Current Fiscal Year (FY1)
                (
                    f"Consensus Estimates - Current FY (ending {fy_period})",
                    "",
                    "header",
                ),
                (
                    "  Revenue",
                    safe_get(row_data, "FY1_Revenue - Mean", 0) / 1_000_000,
                    "currency",
                ),
                (
                    "  EBITDA",
                    safe_get(row_data, "FY1_EBITDA - Mean", 0) / 1_000_000,
                    "currency",
                ),
                (
                    "  EPS",
                    safe_get(row_data, "FY1_Earnings Per Share - Mean", 0),
                    "currency",
                ),
                ("", "", "blank"),  # Blank row
                # Valuation Ratios
                ("Valuation Ratios", "", "header"),
                (
                    "  P/E (LTM)",
                    safe_get(row_data, "P/E (Daily Time Series Ratio)", 0),
                    "ratio",
                ),
                ("  P/E (NTM)", safe_get(row_data, "P/E NTM", 0), "ratio"),
                (
                    "  EV/EBITDA (LTM)",
                    safe_get(
                        row_data,
                        "Enterprise Value To EBITDA (Daily Time Series Ratio)",
                        0,
                    ),
                    "ratio",
                ),
                ("  EV/EBITDA (NTM)", safe_get(row_data, "EV/EBITDA NTM", 0), "ratio"),
                (
                    "  P/B",
                    safe_get(
                        row_data,
                        "Price To Book Value Per Share (Daily Time Series Ratio)",
                        0,
                    ),
                    "ratio",
                ),
                ("  P/FCF (LTM)", safe_get(row_data, "P/FCF LTM", 0), "ratio"),
                ("", "", "blank"),  # Blank row
                # Financial Metrics
                ("Financial Metrics", "", "header"),
                (
                    "  Net Debt (M)",
                    safe_get(row_data, "Net Debt", 0) / 1_000_000
                    if pd.notna(safe_get(row_data, "Net Debt", pd.NA))
                    else "N/A",
                    "currency",
                ),
                (
                    "  Net Debt / EBITDA (NTM)",
                    safe_get(row_data, "Net Debt to EBITDA NTM", 0)
                    if pd.notna(safe_get(row_data, "Net Debt to EBITDA NTM", pd.NA))
                    else "N/A",
                    "ratio",
                ),
                (
                    "  Net Debt % of EV",
                    safe_get(row_data, "Net Debt % of EV", 0) / 100
                    if pd.notna(safe_get(row_data, "Net Debt % of EV", pd.NA))
                    else "N/A",
                    "percent",
                ),
                (
                    "  Dividend Yield",
                    safe_get(row_data, "Dividend yield", 0) / 100,
                    "percent",
                ),
            ]

            # Write subrows
            for label, value, format_type in subrows:
                if format_type == "blank":  # Blank row
                    current_row += 1
                    continue

                worksheet.write(current_row, 1, label, subrow_label_format)

                # Handle "N/A" strings (for financial companies)
                if value == "N/A":
                    worksheet.write(current_row, 2, "N/A", subrow_value_format)
                    current_row += 1
                    continue

                # Check for valid non-empty values (handle NA/NaN properly)
                try:
                    is_na = pd.isna(value)
                except (TypeError, ValueError):
                    is_na = value is None or value == ""

                if not is_na and value != "" and format_type != "header":
                    # Choose format based on type
                    if format_type == "currency":
                        cell_format = currency_format
                    elif format_type == "ratio":
                        cell_format = ratio_format
                    elif format_type == "percent":
                        cell_format = percent_format
                    else:  # 'number' or default
                        cell_format = subrow_value_format

                    worksheet.write(current_row, 2, value, cell_format)
                current_row += 1

            subrow_end = current_row - 1

            # Group the subrows (level 1 = collapsible, hidden by default)
            for row in range(subrow_start, subrow_end + 1):
                worksheet.set_row(row, None, None, {"level": 1, "hidden": True})

        # Set column widths
        worksheet.set_column(0, 0, 10)  # Ticker
        worksheet.set_column(1, 1, 35)  # Company/Metric
        worksheet.set_column(2, 2, 15)  # Earnings Date/Value
        worksheet.set_column(3, 3, 15)  # Earnings Time
        worksheet.set_column(4, 4, 15)  # Mkt Cap
        worksheet.set_column(5, 5, 12)  # Share Price
        worksheet.set_column(
            6, 14, 10
        )  # Return %s (YTD, Since Last, 1Mo, 3Mo, 6Mo, 1Y, 2Y, 3Y, 5Y)

    def write_screener_dataframe(
        self, df: pd.DataFrame, sheet_name: str, include_index: bool = False
    ):
        """
        Write screener data with proper column-specific formatting.

        Applies custom formats based on column names:
        - Market cap: Currency format with millions
        - Ratios (P/E, EV/EBITDA, etc.): "0.00x" format
        - Percentages (returns, yield): "0.00%" format
        - Share price: Currency format
        - Dates: Date format

        Args:
            df: DataFrame with screener data
            sheet_name: Name of worksheet
            include_index: Whether to include DataFrame index
        """
        worksheet = self._workbook.add_worksheet(sheet_name)

        # Create formats
        header_format = self.get_header_format()
        text_format = self.get_standard_format(num_format="@")  # Text
        number_format = self.get_standard_format(num_format="#,##0.00")
        currency_format = self.get_standard_format(num_format="$#,##0.00")
        ratio_format = self.get_standard_format(num_format='0.00"x"')
        percent_display_format = self.get_standard_format(
            num_format='0.00"%"'
        )  # Displays value with % suffix (no division)
        date_format = self._workbook.add_format(
            {
                "font_name": self.DEFAULT_FONT,
                "font_size": self.DEFAULT_FONT_SIZE,
                "num_format": "yyyy-mm-dd",
            }
        )

        # Write headers
        for col_num, col_name in enumerate(df.columns):
            worksheet.write(0, col_num, col_name, header_format)

        # Determine format for each column based on name
        column_formats = []
        for col_name in df.columns:
            col_lower = col_name.lower()

            # RIC and Company Name (text) - check first
            if col_name in ["RIC", "Company Name", "Sector"]:
                column_formats.append(text_format)
            # Market cap columns (in millions)
            elif "mkt cap" in col_lower or "market cap" in col_lower:
                column_formats.append(currency_format)
            # Share price
            elif "share price" in col_lower or col_name == "Price Close":
                column_formats.append(currency_format)
            # Dates
            elif "date" in col_lower:
                column_formats.append(date_format)
            # Net Debt % of EV - special case (already calculated as percentage, just needs display)
            elif "% of ev" in col_lower or "net debt %" in col_lower:
                column_formats.append(percent_display_format)
            # Percentage columns (returns, yield) - check before ratios
            elif any(pct in col_lower for pct in ["%", "return", "yield"]):
                # Special case: "EV/EBITDA" is a ratio, not a percentage
                if "ev/ebitda" in col_lower or "ev / ebitda" in col_lower:
                    column_formats.append(ratio_format)
                else:
                    column_formats.append(percent_display_format)
            # Ratio columns (P/E, EV/EBITDA, P/B, P/FCF) - check after percentages
            elif any(
                ratio in col_lower
                for ratio in [
                    "p/e",
                    "ev/",
                    "p/b",
                    "p/fcf",
                    "debt to",
                    "p/iv",
                    "price / intrinsic",
                ]
            ):
                column_formats.append(ratio_format)
            # Default: numeric
            else:
                column_formats.append(number_format)

        # Write data rows with appropriate formats
        for row_num, (_idx, row_data) in enumerate(df.iterrows(), start=1):
            for col_num, (col_name, cell_format) in enumerate(
                zip(df.columns, column_formats, strict=False)
            ):
                value = row_data[col_name]

                # Handle NA/None values
                if pd.isna(value) or value is None:
                    worksheet.write(row_num, col_num, "", cell_format)
                # Handle datetime values
                elif isinstance(value, pd.Timestamp):
                    worksheet.write_datetime(
                        row_num, col_num, value.to_pydatetime(), cell_format
                    )
                # Write with format
                else:
                    worksheet.write(row_num, col_num, value, cell_format)

        # Set column widths based on column type and content
        for col_num, col_name in enumerate(df.columns):
            col_lower = col_name.lower()

            # Set specific widths for known column types
            if col_name == "RIC":
                width = 10
            elif col_name == "Company Name":
                width = 35
            elif "mkt cap" in col_lower or "market cap" in col_lower:
                width = 15
            elif any(
                ratio in col_lower
                for ratio in ["p/e", "p/b", "p/fcf", "p/iv", "stamine"]
            ):
                width = 10
            elif "ev/ebitda" in col_lower or "ev / ebitda" in col_lower:
                width = 14
            elif "net debt" in col_lower:
                width = 16
            elif "return" in col_lower or "yield" in col_lower:
                width = 12
            elif "share price" in col_lower:
                width = 12
            elif "date" in col_lower:
                width = 18
            elif col_name == "Sector":
                width = 25
            else:
                # Calculate based on content
                max_len = (
                    max(
                        df[col_name].astype(str).str.len().max() if len(df) > 0 else 0,
                        len(str(col_name)),
                    )
                    + 2
                )
                width = min(max(max_len, 10), 50)

            worksheet.set_column(col_num, col_num, width)

        # Freeze panes: Top row + first 2 columns (RIC and Company Name)
        worksheet.freeze_panes(1, 2)

        # Add autofilter to header row
        if len(df) > 0:
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)

    @staticmethod
    def generate_filename(base_name: str, include_date: bool = True) -> str:
        """
        Generate standardized filename with optional date stamp.

        Args:
            base_name: Base name for file
            include_date: Whether to append current date

        Returns:
            Filename string
        """
        if include_date:
            date_str = datetime.now().strftime("%Y%m%d")
            return f"{base_name}_{date_str}.xlsx"
        return f"{base_name}.xlsx"
