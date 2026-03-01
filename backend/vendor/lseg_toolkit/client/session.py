"""
Session management for LSEG API.

Handles opening, closing, and context management for LSEG data sessions.
"""

import logging
import warnings

import lseg.data as rd

from ..exceptions import SessionError
from .config import load_app_key

logger = logging.getLogger(__name__)

# Suppress FutureWarnings from LSEG library about pandas replace() downcasting
# These warnings come from lseg.data._tools._dataframe.py:192 and are not in our control
warnings.filterwarnings(
    "ignore", category=FutureWarning, module="lseg.data._tools._dataframe"
)


class SessionManager:
    """
    Manages LSEG Data session lifecycle.

    Provides context manager support for automatic session cleanup.
    """

    def __init__(self, auto_open: bool = True):
        """
        Initialize session manager.

        Args:
            auto_open: Automatically open LSEG session (default: True)
        """
        self._session_opened = False
        if auto_open:
            self.open_session()

    def open_session(self):
        """
        Open LSEG Data session.

        Automatically loads app key from config files (if present):
        1. .lseg-config.json (local project config)
        2. ~/.lseg/config.json (global user config)
        3. Falls back to LSEG default if no config found

        Requires LSEG Workspace Desktop to be running and authenticated.
        WSL2 users must have mirrored networking enabled (see WSL_SETUP.md).

        To create a config file with your app key, run:
            uv run lseg-setup
        """
        if not self._session_opened:
            try:
                # Load app key from config files (returns None if not found)
                app_key = load_app_key()

                # Open session with app key if found, otherwise use default
                if app_key:
                    rd.open_session(app_key=app_key)
                else:
                    rd.open_session()

                self._session_opened = True
            except Exception as e:
                error_msg = (
                    f"Failed to open LSEG session: {e}\n\n"
                    "Possible solutions:\n"
                    "1. Ensure LSEG Workspace Desktop is running and logged in\n"
                    "2. Create an app key in LSEG Workspace and configure it:\n"
                    "   Run: uv run lseg-setup\n"
                    "3. On WSL2: Ensure mirrored networking is enabled (see WSL_SETUP.md)"
                )
                raise SessionError(error_msg) from e

    def close_session(self):
        """Close LSEG Data session."""
        if self._session_opened:
            try:
                rd.close_session()
                self._session_opened = False
            except Exception as e:
                logger.warning(f"Error closing LSEG session: {e}")

    def __enter__(self):
        """Context manager entry."""
        self.open_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_session()

    def __del__(self):
        """Cleanup on deletion."""
        self.close_session()

    @property
    def is_open(self) -> bool:
        """Check if session is currently open."""
        return self._session_opened
