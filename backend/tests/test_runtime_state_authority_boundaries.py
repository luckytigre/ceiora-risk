from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RUNTIME_STATE = REPO_ROOT / "backend" / "data" / "runtime_state.py"
RUNTIME_STATE_AUTHORITY = REPO_ROOT / "backend" / "data" / "runtime_state_authority.py"
REFRESH_STATUS_SERVICE = REPO_ROOT / "backend" / "services" / "refresh_status_service.py"
OPERATOR_STATUS_SERVICE = REPO_ROOT / "backend" / "services" / "operator_status_service.py"
POST_RUN_PUBLISH = REPO_ROOT / "backend" / "orchestration" / "post_run_publish.py"
FORBIDDEN_AUTHORITY_TOKENS = (
    "publish_active_snapshot(",
    "persist_runtime_state(",
    "load_runtime_state(",
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


def test_runtime_state_authority_does_not_import_runtime_state() -> None:
    imported = _imported_modules(RUNTIME_STATE_AUTHORITY)
    assert "backend.data.runtime_state" not in imported


def test_runtime_state_remains_the_public_runtime_state_boundary() -> None:
    text = RUNTIME_STATE.read_text(encoding="utf-8")
    assert "runtime_state_authority" in text
    assert "def load_runtime_state(" in text
    assert "def persist_runtime_state(" in text
    assert "def publish_active_snapshot(" in text


def test_runtime_state_authority_stays_lower_level_only() -> None:
    text = RUNTIME_STATE_AUTHORITY.read_text(encoding="utf-8")
    assert all(token not in text for token in FORBIDDEN_AUTHORITY_TOKENS)


def test_higher_layers_keep_importing_runtime_state_facade() -> None:
    for path in (REFRESH_STATUS_SERVICE, OPERATOR_STATUS_SERVICE, POST_RUN_PUBLISH):
        imported = _imported_modules(path)
        assert "backend.data.runtime_state_authority" not in imported
        text = path.read_text(encoding="utf-8")
        assert "runtime_state" in text
