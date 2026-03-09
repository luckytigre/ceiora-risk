"""Small role-based API auth helpers for cloud-safe mutating endpoints."""

from __future__ import annotations

import secrets

from fastapi import HTTPException, status

from backend import config


def _candidate_tokens(
    *,
    x_operator_token: str | None = None,
    x_editor_token: str | None = None,
    x_refresh_token: str | None = None,
    authorization: str | None = None,
) -> list[str]:
    candidates: list[str] = []
    for value in (x_operator_token, x_editor_token, x_refresh_token):
        clean = str(value or "").strip()
        if clean:
            candidates.append(clean)
    auth = str(authorization or "").strip()
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
        if token:
            candidates.append(token)
    return candidates


def _token_matches(expected: str, candidates: list[str]) -> bool:
    if not expected:
        return False
    return any(secrets.compare_digest(expected, candidate) for candidate in candidates if candidate)


def require_role(
    role: str,
    *,
    x_operator_token: str | None = None,
    x_editor_token: str | None = None,
    x_refresh_token: str | None = None,
    authorization: str | None = None,
) -> None:
    if not config.cloud_mode():
        return

    candidates = _candidate_tokens(
        x_operator_token=x_operator_token,
        x_editor_token=x_editor_token,
        x_refresh_token=x_refresh_token,
        authorization=authorization,
    )
    operator_ok = _token_matches(config.OPERATOR_API_TOKEN or config.REFRESH_API_TOKEN, candidates)
    editor_ok = _token_matches(config.EDITOR_API_TOKEN, candidates) or operator_ok

    if role == "operator":
        if operator_ok:
            return
        detail = (
            "Unauthorized: operator token required."
            if (config.OPERATOR_API_TOKEN or config.REFRESH_API_TOKEN)
            else "Unauthorized: cloud-serve mode requires OPERATOR_API_TOKEN or REFRESH_API_TOKEN."
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)

    if role == "editor":
        if editor_ok:
            return
        detail = (
            "Unauthorized: editor or operator token required."
            if (config.EDITOR_API_TOKEN or config.OPERATOR_API_TOKEN or config.REFRESH_API_TOKEN)
            else "Unauthorized: cloud-serve mode requires EDITOR_API_TOKEN or OPERATOR_API_TOKEN."
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)

    raise ValueError(f"unknown auth role: {role}")
