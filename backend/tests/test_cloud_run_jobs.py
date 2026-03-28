from __future__ import annotations

import json
import urllib.error

import pytest

from backend.ops import cloud_run_jobs


class _FakeCreds:
    token = ""

    def refresh(self, _request) -> None:
        self.token = "token-123"


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


def test_dispatch_serve_refresh_builds_expected_request(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cloud_run_jobs.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(cloud_run_jobs.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(cloud_run_jobs.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(cloud_run_jobs.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(cloud_run_jobs.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(cloud_run_jobs, "_google_auth_default", lambda *, scopes: (_FakeCreds(), "proj"))
    monkeypatch.setattr(cloud_run_jobs, "_google_auth_request", lambda: object())

    def _urlopen(req, timeout: int):
        captured["url"] = req.full_url
        captured["headers"] = dict(req.header_items())
        captured["body"] = json.loads(req.data.decode("utf-8"))
        captured["timeout"] = timeout
        return _FakeResponse({"name": "projects/proj/locations/us-east4/jobs/x/executions/exec-1"})

    monkeypatch.setattr(cloud_run_jobs.urllib.request, "urlopen", _urlopen)

    out = cloud_run_jobs.dispatch_serve_refresh(
        pipeline_run_id="crj_123",
        profile="serve-refresh",
        as_of_date=None,
        from_stage=None,
        to_stage=None,
        force_core=False,
        refresh_scope="holdings_only",
    )

    assert captured["url"].endswith("/jobs/ceiora-prod-serve-refresh:run")
    env = captured["body"]["overrides"]["containerOverrides"][0]["env"]
    assert {"name": "REFRESH_PIPELINE_RUN_ID", "value": "crj_123"} in env
    assert {"name": "REFRESH_PROFILE", "value": "serve-refresh"} in env
    assert {"name": "REFRESH_SCOPE", "value": "holdings_only"} in env
    assert captured["timeout"] == 30
    assert out["execution_name"].endswith("exec-1")


def test_describe_execution_uses_v2_execution_resource(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(cloud_run_jobs.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(cloud_run_jobs.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(cloud_run_jobs.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(cloud_run_jobs.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(cloud_run_jobs.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(cloud_run_jobs, "_google_auth_default", lambda *, scopes: (_FakeCreds(), "proj"))
    monkeypatch.setattr(cloud_run_jobs, "_google_auth_request", lambda: object())

    def _urlopen(req, timeout: int):
        captured["url"] = req.full_url
        captured["timeout"] = timeout
        return _FakeResponse({"name": "projects/proj/locations/us-east4/jobs/x/executions/exec-1"})

    monkeypatch.setattr(cloud_run_jobs.urllib.request, "urlopen", _urlopen)

    out = cloud_run_jobs.describe_execution("exec-1")

    assert captured["url"].endswith("/jobs/ceiora-prod-serve-refresh/executions/exec-1")
    assert captured["timeout"] == 30
    assert out["name"].endswith("exec-1")


def test_describe_execution_raises_file_not_found_on_404(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cloud_run_jobs.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(cloud_run_jobs.config, "CLOUD_RUN_JOBS_ENABLED", True)
    monkeypatch.setattr(cloud_run_jobs.config, "CLOUD_RUN_PROJECT_ID", "proj")
    monkeypatch.setattr(cloud_run_jobs.config, "CLOUD_RUN_REGION", "us-east4")
    monkeypatch.setattr(cloud_run_jobs.config, "SERVE_REFRESH_CLOUD_RUN_JOB_NAME", "ceiora-prod-serve-refresh")
    monkeypatch.setattr(cloud_run_jobs, "_google_auth_default", lambda *, scopes: (_FakeCreds(), "proj"))
    monkeypatch.setattr(cloud_run_jobs, "_google_auth_request", lambda: object())

    def _urlopen(req, timeout: int):
        raise urllib.error.HTTPError(req.full_url, 404, "not found", hdrs=None, fp=None)

    monkeypatch.setattr(cloud_run_jobs.urllib.request, "urlopen", _urlopen)

    with pytest.raises(FileNotFoundError):
        cloud_run_jobs.describe_execution("exec-1")


def test_execution_terminal_summary_recognizes_failed_completion() -> None:
    out = cloud_run_jobs.execution_terminal_summary(
        {
            "completionTime": "2026-03-23T17:00:00Z",
            "conditions": [
                {
                    "type": "Completed",
                    "state": "CONDITION_FAILED",
                    "message": "The configured memory limit was reached.",
                }
            ],
        }
    )

    assert out == {
        "terminal": True,
        "status": "failed",
        "finished_at": "2026-03-23T17:00:00Z",
        "message": "The configured memory limit was reached.",
    }
