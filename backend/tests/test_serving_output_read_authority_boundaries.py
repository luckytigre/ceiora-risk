from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SERVING_OUTPUT_READ_AUTHORITY = REPO_ROOT / "backend" / "data" / "serving_output_read_authority.py"
SERVING_OUTPUTS = REPO_ROOT / "backend" / "data" / "serving_outputs.py"
DASHBOARD_PAYLOAD_SERVICE = REPO_ROOT / "backend" / "services" / "cuse4_dashboard_payload_service.py"
FORBIDDEN_READ_AUTHORITY_TOKENS = (
    "persist_current_payloads",
    "_verify_current_payloads_neon",
    "compare_current_payload_manifests",
    "load_current_payload_rows_sqlite",
    "load_current_payload_rows_neon",
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


def test_serving_output_read_authority_does_not_import_public_serving_outputs() -> None:
    imported = _imported_modules(SERVING_OUTPUT_READ_AUTHORITY)
    assert "backend.data.serving_outputs" not in imported


def test_serving_outputs_stays_the_public_serving_payload_boundary() -> None:
    text = SERVING_OUTPUTS.read_text(encoding="utf-8")
    assert "serving_output_read_authority" in text
    assert "def load_current_payload(" in text
    assert "def load_runtime_payload(" in text


def test_dashboard_service_continues_to_depend_on_serving_outputs_boundary_module() -> None:
    imported = _imported_modules(DASHBOARD_PAYLOAD_SERVICE)
    assert "backend.data.serving_output_read_authority" not in imported
    text = DASHBOARD_PAYLOAD_SERVICE.read_text(encoding="utf-8")
    assert "serving_outputs.load_runtime_payload" in text


def test_serving_output_read_authority_stays_read_only() -> None:
    text = SERVING_OUTPUT_READ_AUTHORITY.read_text(encoding="utf-8")
    assert all(token not in text for token in FORBIDDEN_READ_AUTHORITY_TOKENS)
