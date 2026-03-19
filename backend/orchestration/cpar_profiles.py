"""Profile and package-date resolution for the dedicated cPAR pipeline."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from backend.cpar.weekly_anchors import weekly_anchor_for_date

STAGES = [
    "source_read",
    "package_build",
    "persist_package",
]

PROFILE_CONFIG: dict[str, dict[str, Any]] = {
    "cpar-weekly": {
        "label": "cPAR Weekly",
        "description": "Build the latest completed cPAR weekly package from the local source archive.",
        "package_date_mode": "latest_completed_weekly_anchor",
        "package_date_required": False,
        "default_stages": list(STAGES),
    },
    "cpar-package-date": {
        "label": "cPAR Package Date",
        "description": "Build one explicit cPAR package for the requested XNYS weekly package date.",
        "package_date_mode": "explicit_anchor",
        "package_date_required": True,
        "default_stages": list(STAGES),
    },
}


def _parse_iso_date(value: str | None) -> date:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("package date is required")
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid ISO date: {raw}") from exc


def _today_utc() -> date:
    return datetime.now(timezone.utc).date()


def resolve_profile_name(profile: str) -> str:
    clean = str(profile or "").strip().lower()
    if not clean:
        raise ValueError("profile is required")
    if clean not in PROFILE_CONFIG:
        raise ValueError(
            f"Unsupported cPAR profile '{profile}'. Expected one of: {', '.join(sorted(PROFILE_CONFIG))}"
        )
    return clean


def _validate_stage_name(stage: str | None, *, option: str) -> None:
    if stage is None:
        return
    if stage not in STAGES:
        raise ValueError(f"{option} must be one of: {', '.join(STAGES)}")


def _stage_window(from_stage: str | None, to_stage: str | None) -> list[str]:
    _validate_stage_name(from_stage, option="from_stage")
    _validate_stage_name(to_stage, option="to_stage")
    start = STAGES.index(from_stage) if from_stage else 0
    end = STAGES.index(to_stage) if to_stage else len(STAGES) - 1
    if start > end:
        raise ValueError("--from-stage must be before or equal to --to-stage")
    if start > 0:
        raise ValueError("cPAR stage windows must start at source_read because upstream stage state is in-memory only.")
    return STAGES[start : end + 1]


def planned_stages_for_profile(
    *,
    profile: str,
    from_stage: str | None = None,
    to_stage: str | None = None,
) -> tuple[str, dict[str, Any], list[str]]:
    profile_key = resolve_profile_name(profile)
    cfg = PROFILE_CONFIG[profile_key]
    if from_stage or to_stage:
        return profile_key, cfg, _stage_window(from_stage, to_stage)
    return profile_key, cfg, list(cfg.get("default_stages") or STAGES)


def package_date_required(profile: str) -> bool:
    profile_key = resolve_profile_name(profile)
    return bool(PROFILE_CONFIG[profile_key].get("package_date_required"))


def _latest_completed_package_date(as_of_date: str | None = None) -> str:
    target = _parse_iso_date(as_of_date) if as_of_date else _today_utc()
    containing_anchor = date.fromisoformat(weekly_anchor_for_date(target.isoformat()))
    if target > containing_anchor:
        return containing_anchor.isoformat()
    previous_anchor_probe = containing_anchor - timedelta(days=7)
    return weekly_anchor_for_date(previous_anchor_probe.isoformat())


def resolve_package_date(*, profile: str, as_of_date: str | None = None) -> str:
    profile_key = resolve_profile_name(profile)
    if profile_key == "cpar-weekly":
        return _latest_completed_package_date(as_of_date)
    if profile_key == "cpar-package-date":
        if not as_of_date:
            raise ValueError("cpar-package-date requires one explicit XNYS weekly package date.")
        parsed = _parse_iso_date(as_of_date)
        anchor = weekly_anchor_for_date(parsed.isoformat())
        if anchor != parsed.isoformat():
            raise ValueError("cpar-package-date requires one explicit XNYS weekly package date.")
        return anchor
    raise ValueError(f"Unsupported cPAR profile '{profile_key}'")


def profile_catalog() -> list[dict[str, Any]]:
    return [
        {
            "profile": profile,
            "label": str(cfg.get("label") or profile),
            "description": str(cfg.get("description") or ""),
            "package_date_mode": str(cfg.get("package_date_mode") or ""),
            "package_date_required": bool(cfg.get("package_date_required")),
            "default_stages": list(cfg.get("default_stages") or []),
        }
        for profile, cfg in PROFILE_CONFIG.items()
    ]
