"""Cloud Run Jobs dispatch adapter for control-plane refreshes."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from backend import config

_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def dispatch_enabled() -> bool:
    return config.serve_refresh_cloud_job_configured()


def _execution_url() -> str:
    if not dispatch_enabled():
        raise RuntimeError("Cloud Run Jobs dispatch is not configured for serve-refresh.")
    return (
        "https://run.googleapis.com/v2/"
        f"projects/{config.CLOUD_RUN_PROJECT_ID}/locations/{config.CLOUD_RUN_REGION}/"
        f"jobs/{config.SERVE_REFRESH_CLOUD_RUN_JOB_NAME}:run"
    )


def _execution_resource_name(execution_name: str) -> str:
    clean = str(execution_name or "").strip()
    if not clean:
        raise ValueError("Cloud Run execution name is required.")
    if clean.startswith("projects/"):
        return clean
    if not dispatch_enabled():
        raise RuntimeError("Cloud Run Jobs dispatch is not configured for serve-refresh.")
    return (
        f"projects/{config.CLOUD_RUN_PROJECT_ID}/locations/{config.CLOUD_RUN_REGION}/"
        f"jobs/{config.SERVE_REFRESH_CLOUD_RUN_JOB_NAME}/executions/{clean}"
    )


def _google_auth_default(*, scopes: list[str]) -> tuple[Any, str | None]:
    import google.auth

    return google.auth.default(scopes=scopes)


def _google_auth_request() -> Any:
    from google.auth.transport.requests import Request

    return Request()


def _access_token() -> str:
    credentials, _ = _google_auth_default(scopes=[_SCOPE])
    credentials.refresh(_google_auth_request())
    token = str(getattr(credentials, "token", "") or "").strip()
    if not token:
        raise RuntimeError("Google application credentials did not return an access token.")
    return token


def _request_json(url: str, *, method: str = "GET", payload: dict[str, Any] | None = None) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        data=(json.dumps(payload).encode("utf-8") if payload is not None else None),
        headers={
            "Authorization": f"Bearer {_access_token()}",
            "Content-Type": "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8") or "{}")


def _env_overrides(
    *,
    pipeline_run_id: str,
    profile: str,
    as_of_date: str | None,
    from_stage: str | None,
    to_stage: str | None,
    force_core: bool,
    refresh_scope: str | None,
) -> list[dict[str, str]]:
    env = [
        {"name": "REFRESH_PIPELINE_RUN_ID", "value": pipeline_run_id},
        {"name": "REFRESH_PROFILE", "value": profile},
    ]
    optional = {
        "REFRESH_AS_OF_DATE": as_of_date,
        "REFRESH_FROM_STAGE": from_stage,
        "REFRESH_TO_STAGE": to_stage,
        "REFRESH_SCOPE": refresh_scope,
    }
    for name, value in optional.items():
        clean = str(value or "").strip()
        if clean:
            env.append({"name": name, "value": clean})
    if force_core:
        env.append({"name": "REFRESH_FORCE_CORE", "value": "true"})
    return env


def dispatch_serve_refresh(
    *,
    pipeline_run_id: str,
    profile: str,
    as_of_date: str | None,
    from_stage: str | None,
    to_stage: str | None,
    force_core: bool,
    refresh_scope: str | None,
) -> dict[str, Any]:
    payload = {
        "overrides": {
            "containerOverrides": [
                {
                    "env": _env_overrides(
                        pipeline_run_id=pipeline_run_id,
                        profile=profile,
                        as_of_date=as_of_date,
                        from_stage=from_stage,
                        to_stage=to_stage,
                        force_core=force_core,
                        refresh_scope=refresh_scope,
                    ),
                }
            ]
        }
    }
    body = _request_json(_execution_url(), method="POST", payload=payload)
    return {
        "execution_name": body.get("name"),
        "metadata": body,
    }


def describe_execution(execution_name: str) -> dict[str, Any]:
    resource_name = _execution_resource_name(execution_name)
    try:
        return _request_json(f"https://run.googleapis.com/v2/{resource_name}")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise FileNotFoundError(f"Cloud Run execution not found: {resource_name}") from exc
        raise


def execution_terminal_summary(execution: dict[str, Any]) -> dict[str, Any]:
    conditions = execution.get("conditions") or execution.get("status", {}).get("conditions") or []
    completed = next(
        (cond for cond in conditions if str(cond.get("type") or "").strip() == "Completed"),
        {},
    )
    state = str(completed.get("state") or completed.get("status") or "").strip()
    finished_at = (
        str(execution.get("completionTime") or execution.get("status", {}).get("completionTime") or "").strip()
        or None
    )
    message = str(completed.get("message") or "").strip() or None
    if state in {"CONDITION_SUCCEEDED", "True", "true"}:
        return {
            "terminal": True,
            "status": "ok",
            "finished_at": finished_at,
            "message": message,
        }
    if state in {"CONDITION_FAILED", "False", "false"}:
        return {
            "terminal": True,
            "status": "failed",
            "finished_at": finished_at,
            "message": message,
        }
    return {
        "terminal": False,
        "status": "running",
        "finished_at": finished_at,
        "message": message,
    }
