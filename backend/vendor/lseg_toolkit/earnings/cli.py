"""
Command-line interface for earnings report generation.
"""

import argparse
import sys
from datetime import datetime, timedelta

from lseg_toolkit.client import LsegClient
from lseg_toolkit.earnings.config import EarningsConfig
from lseg_toolkit.earnings.pipeline import EarningsReportPipeline
from lseg_toolkit.exceptions import ConfigurationError, LsegError


def parse_date(date_str: str) -> datetime:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD"
        )


def get_date_range_for_timeframe(timeframe: str) -> tuple[datetime, datetime]:
    """
    Convert timeframe string to start/end dates.

    Args:
        timeframe: One of 'week', 'month', 'today', 'tomorrow'

    Returns:
        Tuple of (start_date, end_date)
    """
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    if timeframe == "today":
        return (today, today)
    elif timeframe == "tomorrow":
        tomorrow = today + timedelta(days=1)
        return (tomorrow, tomorrow)
    elif timeframe == "week":
        # Current week: Monday to Sunday
        start_of_week = today - timedelta(days=today.weekday())
        end_of_week = start_of_week + timedelta(days=6)
        return (start_of_week, end_of_week)
    elif timeframe == "next-week":
        # Next week: Monday to Sunday
        start_of_week = today - timedelta(days=today.weekday()) + timedelta(days=7)
        end_of_week = start_of_week + timedelta(days=6)
        return (start_of_week, end_of_week)
    elif timeframe == "month":
        # Current month
        start_of_month = today.replace(day=1)
        # Last day of month
        if today.month == 12:
            end_of_month = today.replace(day=31)
        else:
            next_month = today.replace(month=today.month + 1, day=1)
            end_of_month = next_month - timedelta(days=1)
        return (start_of_month, end_of_month)
    else:
        raise ConfigurationError(f"Unknown timeframe: {timeframe}")


def list_indices():
    """Display list of available indices."""
    indices = LsegClient.get_available_indices()

    print("=" * 80)
    print("AVAILABLE INDICES")
    print("=" * 80)
    print()

    # Group by region
    by_region = {}
    for code, info in indices.items():
        region = info["region"]
        if region not in by_region:
            by_region[region] = []
        by_region[region].append((code, info))

    for region in sorted(by_region.keys()):
        print(f"{region}:")
        print("-" * 40)
        for code, info in sorted(by_region[region], key=lambda x: x[0]):
            print(f"  {code:12s} - {info['name']}")
            print(f"               {info['description']}")
        print()

    print("Usage: lseg-earnings --index <code>")
    print("Example: lseg-earnings --index SPX")
    print()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Generate earnings report from LSEG data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # This week's S&P 500 earnings (default)
  lseg-earnings

  # Nasdaq 100 earnings for next week
  lseg-earnings --index NDX --timeframe next-week

  # DAX earnings for a specific date range
  lseg-earnings --index DAX --start-date 2025-11-01 --end-date 2025-11-07

  # With market cap filter (millions of CCY)
  lseg-earnings --min-cap 10000 --max-cap 50000

  # List all available indices
  lseg-earnings --list-indices
        """,
    )

    # Special actions
    parser.add_argument(
        "--list-indices",
        action="store_true",
        help="List all available indices and exit",
    )

    # Index selection
    parser.add_argument(
        "--index",
        default="SPX",
        help="Index symbol (use --list-indices to see all) [default: SPX]",
    )

    # Market cap filters
    parser.add_argument(
        "--min-cap",
        type=float,
        help="Minimum market cap in millions (e.g., 10000 for $10B)",
    )

    parser.add_argument(
        "--max-cap",
        type=float,
        help="Maximum market cap in millions (e.g., 50000 for $50B)",
    )

    # Time frame selection (mutually exclusive with explicit dates)
    time_group = parser.add_mutually_exclusive_group()

    time_group.add_argument(
        "--timeframe",
        choices=["today", "tomorrow", "week", "next-week", "month"],
        help="Predefined time frame (overrides start/end dates)",
    )

    time_group.add_argument(
        "--start-date",
        type=parse_date,
        help="Start date for earnings (YYYY-MM-DD), will assume 1-day if no --end-date specified",
    )

    parser.add_argument(
        "--end-date",
        type=parse_date,
        help="End date for earnings (YYYY-MM-DD, requires --start-date)",
    )

    # Timezone
    parser.add_argument(
        "--timezone",
        default="US/Eastern",
        help="Timezone for earnings times [default: US/Eastern]",
    )

    # Output
    parser.add_argument(
        "--output-dir",
        default="exports",
        help="Output directory for reports [default: exports]",
    )

    args = parser.parse_args()

    # Handle special actions
    if args.list_indices:
        list_indices()
        sys.exit(0)

    # Validate date arguments
    if args.end_date and not args.start_date and not args.timeframe:
        parser.error("--end-date requires --start-date")

    # Determine date range
    if args.timeframe:
        start_date, end_date = get_date_range_for_timeframe(args.timeframe)
    elif args.start_date:
        start_date = args.start_date
        end_date = args.end_date if args.end_date else args.start_date
    else:
        # Default: current week
        start_date, end_date = get_date_range_for_timeframe("week")

    # Validate index
    available_indices = LsegClient.get_available_indices()
    if args.index.upper() not in available_indices:
        print(f"\nWarning: '{args.index}' not in standard index list.")
        print("It may still work if it's a valid LSEG index symbol.")
        print("Use --list-indices to see all supported indices.\n")

    # Create specific earnings report config
    config = EarningsConfig(
        index=args.index.upper(),
        min_market_cap=args.min_cap,
        max_market_cap=args.max_cap,
        start_date=start_date,
        end_date=end_date,
        timezone=args.timezone,
        output_dir=args.output_dir,
    )

    # Display configuration
    print("=" * 80)
    print("EARNINGS REPORT CONFIGURATION")
    print("=" * 80)
    print(f"Index:        {config.index}")
    if config.min_market_cap or config.max_market_cap:
        min_cap = (
            f"${config.min_market_cap:,.0f}M" if config.min_market_cap else "No limit"
        )
        max_cap = (
            f"${config.max_market_cap:,.0f}M" if config.max_market_cap else "No limit"
        )
        print(f"Market Cap:   {min_cap} to {max_cap}")
    print(
        f"Date Range:   {config.start_date.strftime('%Y-%m-%d')} to {config.end_date.strftime('%Y-%m-%d')}"
    )
    print(f"Timezone:     {config.timezone}")
    print(f"Output Dir:   {config.output_dir}")
    print("=" * 80)
    print()

    # Run pipeline
    try:
        pipeline = EarningsReportPipeline(config)
        output_path = pipeline.run()
        print(f"\n{'=' * 80}")
        print("SUCCESS!")
        print(f"{'=' * 80}")
        print(f"Report saved to: {output_path}")

    except LsegError as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nReport generation interrupted by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"\nUnexpected error: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
