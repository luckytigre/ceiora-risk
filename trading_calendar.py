"""Compatibility shim for scripts run from the repository root.

The project source of truth lives in ``backend/trading_calendar.py``.
This module re-exports those symbols so existing imports like
``from trading_calendar import ...`` work regardless of current directory.
"""

from backend.trading_calendar import *  # noqa: F401,F403

