"""Small role-based API auth helpers for cloud-safe mutating endpoints."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import hmac
import json
import os
import secrets
import time

from fastapi import HTTPException, status

from backend import config


@dataclass(frozen=True)
class AppPrincipal:
    provider: str
    subject: str
    is_admin: bool
    email: str | None = None
    display_name: str | None = None


def _base64url_decode(value: str) -> bytes:
    padded = value + ("=" * ((4 - len(value) % 4) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def _shared_session_secret() -> str:
    return str(os.getenv("CEIORA_SESSION_SECRET", "")).strip()


def _verify_app_session_token(token: str) -> AppPrincipal | None:
    secret = _shared_session_secret()
    if not token or not secret:
        return None
    parts = token.split(".")
    if len(parts) != 2:
        return None
    payload, signature = parts
    expected = base64.urlsafe_b64encode(
        hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    ).decode("ascii").rstrip("=")
    if not secrets.compare_digest(expected, signature):
        return None
    try:
        decoded = json.loads(_base64url_decode(payload).decode("utf-8"))
    except Exception:
        return None
    provider = str(decoded.get("authProvider") or "shared").strip().lower()
    username = str(decoded.get("username") or "").strip()
    subject = str(decoded.get("subject") or "").strip()
    email = str(decoded.get("email") or "").strip() or None
    display_name = str(decoded.get("displayName") or "").strip() or None
    expires_at = int(decoded.get("expiresAt") or 0)
    if provider not in {"shared", "neon"} or expires_at <= int(time.time()):
        return None
    if provider == "shared":
        subject = subject or username
        if not username:
            return None
    elif not subject:
        return None
    return AppPrincipal(
        provider=provider,
        subject=subject,
        is_admin=bool(decoded.get("isAdmin") or decoded.get("primary")),
        email=email,
        display_name=display_name,
    )


def _candidate_tokens(
    *,
    x_operator_token: str | None = None,
    x_editor_token: str | None = None,
    authorization: str | None = None,
) -> list[str]:
    candidates: list[str] = []
    for value in (x_operator_token, x_editor_token):
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
    authorization: str | None = None,
) -> None:
    if not config.cloud_mode():
        return

    candidates = _candidate_tokens(
        x_operator_token=x_operator_token,
        x_editor_token=x_editor_token,
        authorization=authorization,
    )
    operator_ok = _token_matches(config.OPERATOR_API_TOKEN, candidates)
    editor_ok = _token_matches(config.EDITOR_API_TOKEN, candidates) or operator_ok

    if role == "operator":
        if operator_ok:
            return
        detail = (
            "Unauthorized: operator token required."
            if config.OPERATOR_API_TOKEN
            else "Unauthorized: cloud-serve mode requires OPERATOR_API_TOKEN."
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)

    if role == "editor":
        if editor_ok:
            return
        detail = (
            "Unauthorized: editor or operator token required."
            if (config.EDITOR_API_TOKEN or config.OPERATOR_API_TOKEN)
            else "Unauthorized: cloud-serve mode requires EDITOR_API_TOKEN or OPERATOR_API_TOKEN."
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)

    raise ValueError(f"unknown auth role: {role}")


def require_operator_or_authenticated_session(
    *,
    x_app_session_token: str | None = None,
    x_operator_token: str | None = None,
    x_editor_token: str | None = None,
    authorization: str | None = None,
) -> AppPrincipal | None:
    principal = parse_app_principal(x_app_session_token=x_app_session_token)
    if principal is not None:
        return principal
    require_role(
        "operator",
        x_operator_token=x_operator_token,
        authorization=authorization,
    )
    return None


def require_authenticated_session(
    *,
    x_app_session_token: str | None = None,
) -> AppPrincipal:
    principal = parse_app_principal(x_app_session_token=x_app_session_token)
    if principal is not None:
        return principal
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unauthorized: authenticated app session required.",
    )


def parse_app_principal(
    *,
    x_app_session_token: str | None = None,
) -> AppPrincipal | None:
    token = str(x_app_session_token or "").strip()
    if not token:
        return None
    return _verify_app_session_token(token)
