"""
Earnings Report Generator - Export company earnings data to Excel.

This module generates comprehensive earnings reports with financial data,
consensus estimates, and analyst metrics.
"""

from lseg_toolkit.earnings.config import EarningsConfig
from lseg_toolkit.earnings.pipeline import EarningsReportPipeline

__all__ = ["EarningsReportPipeline", "EarningsConfig"]
