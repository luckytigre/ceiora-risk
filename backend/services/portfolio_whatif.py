"""Compatibility shim for cUSE4 portfolio what-if preview semantics.

Prefer importing ``backend.services.cuse4_portfolio_whatif`` from the default
cUSE4 route family. This module remains only for older callers and direct
service tests that still import the legacy path.
"""

from __future__ import annotations

from backend.services.cuse4_portfolio_whatif import (
    PortfolioWhatIfDependencies,
    config,
    get_portfolio_whatif_dependencies,
    preview_portfolio_whatif,
)

__all__ = [
    "PortfolioWhatIfDependencies",
    "config",
    "get_portfolio_whatif_dependencies",
    "preview_portfolio_whatif",
]
