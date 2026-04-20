"""Compatibility shim for cUSE4 portfolio what-if preview semantics.

Prefer importing ``backend.services.cuse4_portfolio_whatif`` from the default
cUSE4 route family. This module remains only for older callers and direct
service tests that still import the legacy path.
"""

from __future__ import annotations

from backend.services import cuse4_portfolio_whatif as _owner


PortfolioWhatIfDependencies = _owner.PortfolioWhatIfDependencies
config = _owner.config


def get_portfolio_whatif_dependencies() -> PortfolioWhatIfDependencies:
    return _owner.get_portfolio_whatif_dependencies()


def preview_portfolio_whatif(
    *,
    scenario_rows: list[dict[str, object]],
    requested_exposure_modes: tuple[str, ...] | list[str] | None = None,
    dependencies: PortfolioWhatIfDependencies | None = None,
) -> dict[str, object]:
    resolved_dependencies = (
        dependencies
        if dependencies is not None
        else get_portfolio_whatif_dependencies()
    )
    return _owner.preview_portfolio_whatif(
        scenario_rows=scenario_rows,
        requested_exposure_modes=requested_exposure_modes,
        dependencies=resolved_dependencies,
    )

__all__ = [
    "PortfolioWhatIfDependencies",
    "config",
    "get_portfolio_whatif_dependencies",
    "preview_portfolio_whatif",
]
