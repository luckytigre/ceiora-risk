"""
LSEG Toolkit - Financial data extraction and reporting tools using LSEG API.

Core utilities for accessing LSEG/Refinitiv data and generating reports.
"""

from lseg_toolkit.client import LsegClient
from lseg_toolkit.data import DataProcessor
from lseg_toolkit.excel import ExcelExporter
from lseg_toolkit.exceptions import (
    ConfigurationError,
    DataRetrievalError,
    DataValidationError,
    LsegError,
    SessionError,
)

__version__ = "0.1.0"
__all__ = [
    "LsegClient",
    "DataProcessor",
    "ExcelExporter",
    "LsegError",
    "SessionError",
    "DataRetrievalError",
    "DataValidationError",
    "ConfigurationError",
]
