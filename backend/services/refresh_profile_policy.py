"""Runtime-role policy helpers for refresh profiles."""

from __future__ import annotations

from backend import config
from backend.orchestration.profiles import PROFILE_CONFIG


def default_refresh_profile() -> str:
    return "serve-refresh" if config.cloud_mode() else "source-daily-plus-core-if-due"


def runtime_allowed_profiles() -> set[str]:
    if config.cloud_mode():
        return {"serve-refresh"}
    return set(PROFILE_CONFIG.keys())


def assert_refresh_profile_allowed(profile: str) -> None:
    allowed = runtime_allowed_profiles()
    if profile in allowed:
        return
    raise ValueError(
        f"Profile '{profile}' is not allowed when APP_RUNTIME_ROLE={config.APP_RUNTIME_ROLE}. "
        f"Allowed profiles: {', '.join(sorted(allowed))}"
    )
