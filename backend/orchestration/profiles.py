"""Profile and stage metadata for the model pipeline orchestrator."""

from __future__ import annotations

from typing import Any

from backend import config

STAGES = [
    "ingest",
    "source_sync",
    "neon_readiness",
    "raw_history",
    "feature_build",
    "estu_audit",
    "factor_returns",
    "risk_model",
    "serving_refresh",
]

PROFILE_CONFIG: dict[str, dict[str, Any]] = {
    "publish-only": {
        "label": "Publish Only",
        "description": "Republish already-current cached serving payloads without recomputing analytics.",
        "core_policy": "never",
        "serving_mode": "publish",
        "raw_history_policy": "none",
        "reset_core_cache": False,
        "default_stages": ["serving_refresh"],
        "enable_ingest": False,
        "ingest_policy": "none",
    },
    "serve-refresh": {
        "label": "Serve Refresh",
        "description": "Rebuild frontend-facing caches only.",
        "core_policy": "never",
        "serving_mode": "light",
        "raw_history_policy": "none",
        "reset_core_cache": False,
        "default_stages": ["serving_refresh"],
        "enable_ingest": False,
        "ingest_policy": "none",
    },
    "source-daily": {
        "label": "Source Daily",
        "description": "Pull latest source-of-truth data locally, publish the retained operating window, and rebuild serving caches.",
        "core_policy": "never",
        "serving_mode": "light",
        "raw_history_policy": "none",
        "reset_core_cache": False,
        "default_stages": ["ingest", "serving_refresh"],
        "enable_ingest": True,
        "ingest_policy": "local_lseg",
    },
    "source-daily-plus-core-if-due": {
        "label": "Source Daily + Core If Due",
        "description": "Daily source refresh plus a core recompute only when cadence or policy says it is due.",
        "core_policy": "due",
        "serving_mode": "light",
        "raw_history_policy": "recent-daily",
        "reset_core_cache": False,
        "default_stages": ["ingest", "raw_history", "factor_returns", "risk_model", "serving_refresh"],
        "enable_ingest": True,
        "ingest_policy": "local_lseg",
    },
    "core-weekly": {
        "label": "Core Weekly",
        "description": "Recompute factor returns, covariance, and specific risk from the current authoritative rebuild store.",
        "core_policy": "always",
        "serving_mode": "full",
        "raw_history_policy": "recent-daily",
        "reset_core_cache": False,
        "default_stages": ["raw_history", "factor_returns", "risk_model", "serving_refresh"],
        "enable_ingest": False,
        "ingest_policy": "none",
    },
    "cold-core": {
        "label": "Cold Core",
        "description": "Structural rebuild of raw history and core model state from the current authoritative rebuild store.",
        "core_policy": "always",
        "serving_mode": "full",
        "raw_history_policy": "full-daily",
        "reset_core_cache": True,
        "default_stages": ["raw_history", "feature_build", "estu_audit", "factor_returns", "risk_model", "serving_refresh"],
        "enable_ingest": False,
        "ingest_policy": "none",
    },
    "universe-add": {
        "label": "Universe Add",
        "description": "Post-universe-onboarding serving refresh after targeted source backfills.",
        "core_policy": "never",
        "serving_mode": "full",
        "raw_history_policy": "none",
        "reset_core_cache": False,
        "default_stages": ["serving_refresh"],
        "enable_ingest": False,
        "ingest_policy": "none",
    },
}

_NEON_BROAD_MIRROR_PROFILES = {
    "source-daily",
    "source-daily-plus-core-if-due",
    "core-weekly",
    "cold-core",
    "universe-add",
}

_CORE_REBUILD_PROFILES = {
    "source-daily-plus-core-if-due",
    "core-weekly",
    "cold-core",
}


def _data_backend(*, cfg: dict[str, Any] | None = None) -> str:
    spec = cfg or {}
    value = str(spec.get("data_backend") or config.DATA_BACKEND or "").strip().lower()
    return value or "sqlite"


def _neon_authoritative_rebuilds_enabled(*, cfg: dict[str, Any] | None = None) -> bool:
    spec = cfg or {}
    if _data_backend(cfg=spec) != "neon":
        return False
    if "neon_authoritative_rebuilds" in spec:
        return bool(spec.get("neon_authoritative_rebuilds"))
    return bool(config.NEON_AUTHORITATIVE_REBUILDS)


def resolve_profile_name(profile: str) -> str:
    clean = str(profile or "").strip().lower()
    if not clean:
        raise ValueError("profile is required")
    return clean


def profile_source_sync_required(profile: str, *, cfg: dict[str, Any] | None = None) -> bool:
    clean = str(profile or "").strip().lower()
    return bool(
        _data_backend(cfg=cfg) == "neon"
        and clean in _NEON_BROAD_MIRROR_PROFILES
        and clean not in {"serve-refresh", "publish-only"}
    )


def profile_rebuild_backend(profile: str, *, cfg: dict[str, Any] | None = None) -> str:
    clean = str(profile or "").strip().lower()
    spec = cfg or PROFILE_CONFIG.get(clean) or {}
    if str(spec.get("core_policy") or "never") == "never":
        return "none"
    return "neon" if _neon_authoritative_rebuilds_enabled(cfg=spec) else "local"


def profile_requires_neon_sync_before_core(profile: str, *, cfg: dict[str, Any] | None = None) -> bool:
    clean = str(profile or "").strip().lower()
    if clean not in _CORE_REBUILD_PROFILES:
        return False
    return profile_rebuild_backend(clean, cfg=cfg) == "neon"


def profile_neon_readiness_required(profile: str, *, cfg: dict[str, Any] | None = None) -> bool:
    clean = str(profile or "").strip().lower()
    if clean not in _CORE_REBUILD_PROFILES:
        return False
    return profile_rebuild_backend(clean, cfg=cfg) == "neon"


def profile_catalog() -> list[dict[str, Any]]:
    return [
        {
            "profile": profile,
            "label": str(cfg.get("label") or profile),
            "description": str(cfg.get("description") or ""),
            "core_policy": str(cfg.get("core_policy") or ""),
            "serving_mode": str(cfg.get("serving_mode") or ""),
            "raw_history_policy": str(cfg.get("raw_history_policy") or "none"),
            "reset_core_cache": bool(cfg.get("reset_core_cache")),
            "default_stages": list(cfg.get("default_stages") or []),
            "enable_ingest": bool(cfg.get("enable_ingest")),
            "ingest_policy": str(cfg.get("ingest_policy") or "none"),
            "rebuild_backend": profile_rebuild_backend(profile, cfg=cfg),
            "requires_neon_sync_before_core": bool(profile_requires_neon_sync_before_core(profile, cfg=cfg)),
            "source_sync_required": bool(profile_source_sync_required(profile, cfg=cfg)),
            "neon_readiness_required": bool(profile_neon_readiness_required(profile, cfg=cfg)),
        }
        for profile, cfg in PROFILE_CONFIG.items()
    ]


def _stage_window(from_stage: str | None, to_stage: str | None) -> list[str]:
    start = STAGES.index(from_stage) if from_stage else 0
    end = STAGES.index(to_stage) if to_stage else len(STAGES) - 1
    if start > end:
        raise ValueError("--from-stage must be before or equal to --to-stage")
    return STAGES[start : end + 1]


def _default_stage_selection(cfg: dict[str, Any], from_stage: str | None, to_stage: str | None) -> list[str]:
    if from_stage or to_stage:
        return _stage_window(from_stage, to_stage)
    selected = [str(stage) for stage in (cfg.get("default_stages") or []) if str(stage) in STAGES]
    return selected or list(STAGES)


def _apply_neon_authority_stage_selection(
    *,
    profile: str,
    cfg: dict[str, Any],
    selected: list[str],
    from_stage: str | None,
    to_stage: str | None,
    skip_source_sync: bool,
) -> list[str]:
    if from_stage or to_stage:
        return selected
    wanted = set(selected)
    rebuild_stages = {"raw_history", "feature_build", "estu_audit", "factor_returns", "risk_model"}
    if (
        not skip_source_sync
        and profile_source_sync_required(profile, cfg=cfg)
        and wanted.intersection(rebuild_stages | {"serving_refresh"})
    ):
        wanted.add("source_sync")
    if profile_neon_readiness_required(profile, cfg=cfg) and wanted.intersection(rebuild_stages):
        wanted.add("neon_readiness")
    return [stage for stage in STAGES if stage in wanted]


def _apply_force_core_stage_selection(
    *,
    selected: list[str],
    force_core: bool,
    from_stage: str | None,
    to_stage: str | None,
) -> list[str]:
    if not force_core:
        return selected
    required = {"factor_returns", "risk_model"}
    if required.issubset(set(selected)):
        return selected
    if from_stage or to_stage:
        raise ValueError(
            "force_core requires a stage window that includes factor_returns and risk_model, "
            "or no explicit --from-stage/--to-stage."
        )
    wanted = set(selected) | required
    return [stage for stage in STAGES if stage in wanted]


def planned_stages_for_profile(
    *,
    profile: str,
    from_stage: str | None = None,
    to_stage: str | None = None,
    force_core: bool = False,
    skip_source_sync: bool = False,
) -> tuple[str, dict[str, Any], list[str]]:
    profile_key = resolve_profile_name(profile)
    if profile_key not in PROFILE_CONFIG:
        raise ValueError(
            f"Unsupported profile '{profile}'. Expected one of: {', '.join(sorted(PROFILE_CONFIG))}"
        )
    cfg = PROFILE_CONFIG[profile_key]
    skip_source_sync = skip_source_sync or config.cloud_job_mode()
    if skip_source_sync:
        if from_stage or to_stage:
            raise ValueError("--skip-source-sync cannot be combined with explicit --from-stage/--to-stage.")
        if not profile_source_sync_required(profile_key, cfg=cfg):
            raise ValueError("--skip-source-sync is only valid for Neon-authoritative profiles that require source_sync.")
        if bool(cfg.get("enable_ingest")):
            raise ValueError("--skip-source-sync is not valid for ingest-capable profiles.")
        if not profile_neon_readiness_required(profile_key, cfg=cfg):
            raise ValueError("--skip-source-sync requires a Neon-authoritative core rebuild profile.")
    selected = _default_stage_selection(cfg, from_stage, to_stage)
    selected = _apply_neon_authority_stage_selection(
        profile=profile_key,
        cfg=cfg,
        selected=selected,
        from_stage=from_stage,
        to_stage=to_stage,
        skip_source_sync=bool(skip_source_sync),
    )
    selected = _apply_force_core_stage_selection(
        selected=selected,
        force_core=bool(force_core),
        from_stage=from_stage,
        to_stage=to_stage,
    )
    return profile_key, cfg, selected
