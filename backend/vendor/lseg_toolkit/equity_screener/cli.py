"""
Command-line interface for equity screener.

Provides user-friendly CLI for running equity screens with customizable parameters.
"""

import argparse
import sys
from datetime import date

from ..exceptions import ConfigurationError, LsegError
from .config import EquityScreenerConfig
from .pipeline import EquityScreenerPipeline


def create_parser() -> argparse.ArgumentParser:
    """
    Create argument parser for equity screener CLI.

    Returns:
        Configured ArgumentParser
    """
    parser = argparse.ArgumentParser(
        prog="lseg-screener",
        description="Screen US equities based on market cap and financial criteria",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                   # Default: S&P 500 (SPX), US stocks, no market cap restrictions
  %(prog)s --list-indices                     # List all available indices
  %(prog)s --index NDX                        # Nasdaq 100 constituents
  %(prog)s --index FTSE --country GB          # FTSE 100 UK stocks only
  %(prog)s --no-index --country CA            # All Canadian stocks (no index filter)
  %(prog)s --no-index --no-country            # Global stocks (no filters)
  %(prog)s --min-cap 10000 --max-cap 50000    # S&P 500 large-caps ($10B-$50B)
  %(prog)s --index NDX --min-cap 100000       # Nasdaq 100 mega-caps ($100B+)
  %(prog)s --date 2024-12-31                  # Historical screen

Index examples:
  --index SPX                        # S&P 500 (default)
  --index NDX                        # Nasdaq 100
  --index DJI                        # Dow Jones
  --index FTSE                       # FTSE 100 (UK)
  --index N225                       # Nikkei 225 (Japan)
  --no-index                         # No index filter

Country examples:
  --country US                       # US headquarters (default)
  --country GB                       # UK headquarters
  --country CA                       # Canada headquarters
  --no-country                       # All countries

Screening criteria:
  - Equity type: Active, public, primary listings (always applied)
  - Index: Defaults to SPX (S&P 500), can be changed or disabled
  - Country: Defaults to US, can be changed or disabled
  - Market cap: No restrictions by default

Financial metrics included:
  - Valuation: P/E (LTM & NTM), EV/EBITDA (LTM & NTM), P/B, P/FCF
  - Leverage: Net Debt / EBITDA, Net Debt %% of EV
  - Performance: 1Y Total Return, Dividend Yield
  - Analyst: StarMine Price to Intrinsic Value
  - Activism: Latest campaign announcement date

Output:
  - Excel workbook with summary + sector worksheets
  - Date-stamped filename (equity_screener_YYYYMMDD_YYYYMMDD.xlsx)
  - Summary sheet: sector breakdown with statistics
  - Sector sheets: detailed financial metrics per stock
        """,
    )

    # Date argument
    parser.add_argument(
        "screen_date",
        nargs="?",
        default=None,
        help="Screening date in YYYY-MM-DD format (default: today)",
    )

    parser.add_argument(
        "--date",
        "-d",
        dest="screen_date_flag",
        help="Screening date in YYYY-MM-DD format (alternative to positional argument)",
    )

    # Index filter
    parser.add_argument(
        "--index",
        "-i",
        dest="index",
        default="SPX",
        help="Index code to screen constituents (e.g., SPX, NDX, DJI). Default: SPX. Use --no-index to disable.",
    )

    parser.add_argument(
        "--no-index",
        action="store_true",
        help="Disable index filtering (screen entire market)",
    )

    # Country filter
    parser.add_argument(
        "--country",
        "-c",
        dest="country",
        default="US",
        help="Country code for headquarters filter (e.g., US, GB, CA). Default: US. Use --no-country to disable.",
    )

    parser.add_argument(
        "--no-country",
        action="store_true",
        help="Disable country filtering (all countries)",
    )

    # Market cap filters
    parser.add_argument(
        "--min-cap",
        "--min",
        dest="min_mkt_cap",
        type=float,
        default=None,
        help="Minimum market cap in millions (default: no restriction)",
    )

    parser.add_argument(
        "--max-cap",
        "--max",
        dest="max_mkt_cap",
        type=float,
        default=None,
        help="Maximum market cap in millions (default: no restriction)",
    )

    # Output directory
    parser.add_argument(
        "--output-dir",
        default="exports",
        help="Output directory for Excel files (default: exports)",
    )

    # List indices
    parser.add_argument(
        "--list-indices",
        action="store_true",
        help="List all available indices and exit",
    )

    return parser


def main():
    """Main entry point for equity screener CLI."""
    parser = create_parser()
    args = parser.parse_args()

    # Handle --list-indices
    if args.list_indices:
        from ..client import LsegClient

        indices = LsegClient.get_available_indices()
        print("\nAvailable Indices:")
        print("=" * 80)
        for code, info in sorted(indices.items()):
            print(f"  {code:12} - {info['name']:30} ({info['region']})")
        print("=" * 80)
        print(f"\nTotal: {len(indices)} indices")
        print("\nUsage: lseg-screener --index SPX")
        sys.exit(0)

    # Determine screen date
    screen_date = args.screen_date_flag if args.screen_date_flag else args.screen_date
    if screen_date is None:
        screen_date = date.today().strftime("%Y-%m-%d")

    # Handle --no-index and --no-country flags
    index = None if args.no_index else args.index
    country = None if args.no_country else args.country

    # Create configuration
    try:
        config = EquityScreenerConfig(
            screen_date=screen_date,
            index=index,
            country=country,
            min_mkt_cap=args.min_mkt_cap,
            max_mkt_cap=args.max_mkt_cap,
            output_dir=args.output_dir,
        )
    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)

    # Run pipeline
    try:
        pipeline = EquityScreenerPipeline(config)
        output_path = pipeline.run()

        print("Screening complete!")
        print(f"  Results exported to: {output_path}")

    except LsegError as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nScreening interrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"\nUnexpected error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
