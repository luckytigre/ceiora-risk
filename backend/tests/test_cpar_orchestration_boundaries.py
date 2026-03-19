from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
CPAR_ORCH_FILES = [
    REPO_ROOT / "backend" / "orchestration" / "cpar_profiles.py",
    REPO_ROOT / "backend" / "orchestration" / "cpar_stages.py",
    REPO_ROOT / "backend" / "orchestration" / "run_cpar_pipeline.py",
]
FORBIDDEN_PREFIXES = (
    "backend.api",
    "backend.data.job_runs",
    "backend.services",
    "backend.data.runtime_state",
    "backend.orchestration.profiles",
    "backend.orchestration.run_model_pipeline",
    "backend.orchestration.stage_core",
    "backend.orchestration.stage_serving",
    "backend.orchestration.stage_source",
    "frontend",
    "fastapi",
)


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(str(alias.name))
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(str(node.module))
    return modules


def test_cpar_orchestration_modules_do_not_import_forbidden_layers() -> None:
    offenders: list[str] = []
    for path in CPAR_ORCH_FILES:
        imported = _imported_modules(path)
        if any(
            name == prefix or name.startswith(f"{prefix}.")
            for name in imported
            for prefix in FORBIDDEN_PREFIXES
        ):
            offenders.append(str(path.relative_to(REPO_ROOT)))
    assert offenders == []
