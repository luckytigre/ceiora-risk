"""Pipeline orchestration package."""

from __future__ import annotations

__all__ = ["PROFILE_CONFIG", "STAGES", "run_model_pipeline"]


def __getattr__(name: str):
    if name in __all__:
        from backend.orchestration.run_model_pipeline import PROFILE_CONFIG, STAGES, run_model_pipeline

        mapping = {
            "PROFILE_CONFIG": PROFILE_CONFIG,
            "STAGES": STAGES,
            "run_model_pipeline": run_model_pipeline,
        }
        return mapping[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
