"""Shared request-shape validation for refresh entrypoints."""

from __future__ import annotations

from typing import Any

from backend.orchestration.profiles import (
    PROFILE_CONFIG,
    STAGES,
    planned_stages_for_profile,
    resolve_profile_name,
)
from backend.services.refresh_profile_policy import assert_refresh_profile_allowed
from backend.services.refresh_profile_policy import default_refresh_profile


def _resolve_profile(profile: str | None) -> str:
    prof = str(profile or "").strip().lower()
    if prof:
        prof = resolve_profile_name(prof)
        if prof not in PROFILE_CONFIG:
            raise ValueError(
                f"Invalid profile '{profile}'. Valid profiles: {', '.join(sorted(PROFILE_CONFIG.keys()))}"
            )
        return prof
    return default_refresh_profile()


def _normalize_stage(name: str | None) -> str | None:
    if name is None:
        return None
    clean = str(name).strip().lower()
    if not clean:
        return None
    if clean not in STAGES:
        raise ValueError(f"Invalid stage '{name}'. Valid stages: {', '.join(STAGES)}")
    return clean


def _validate_stage_window(from_stage: str | None, to_stage: str | None) -> None:
    if from_stage is None or to_stage is None:
        return
    if STAGES.index(from_stage) > STAGES.index(to_stage):
        raise ValueError("--from-stage must be before or equal to --to-stage")


def _validate_profile_stage_policy(
    *,
    profile: str,
    from_stage: str | None,
    to_stage: str | None,
) -> None:
    if str(profile or "").strip().lower() == "cold-core" and (from_stage is not None or to_stage is not None):
        raise ValueError("cold-core does not support partial stage windows; use core-weekly or a narrower profile.")


def resolve_refresh_request(
    *,
    profile: str | None = None,
    from_stage: str | None = None,
    to_stage: str | None = None,
    force_core: bool = False,
    force_risk_recompute: bool = False,
) -> dict[str, Any]:
    resolved_profile = _resolve_profile(profile)
    assert_refresh_profile_allowed(resolved_profile)
    mode = str(PROFILE_CONFIG.get(resolved_profile, {}).get("serving_mode") or "full")
    stage_from = _normalize_stage(from_stage)
    stage_to = _normalize_stage(to_stage)
    _validate_stage_window(stage_from, stage_to)
    _validate_profile_stage_policy(
        profile=resolved_profile,
        from_stage=stage_from,
        to_stage=stage_to,
    )
    force_core_effective = bool(force_core or force_risk_recompute)
    planned_stages_for_profile(
        profile=resolved_profile,
        from_stage=stage_from,
        to_stage=stage_to,
        force_core=force_core_effective,
    )
    return {
        "profile": resolved_profile,
        "mode": mode,
        "from_stage": stage_from,
        "to_stage": stage_to,
        "force_core": force_core_effective,
    }
