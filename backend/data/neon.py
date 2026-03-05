"""Shared Neon/Postgres connection helpers."""

from __future__ import annotations

import os
from typing import Any

import psycopg

from backend import config


def resolve_dsn(explicit_dsn: str | None = None) -> str:
    """Resolve Neon DSN from explicit arg or environment."""
    dsn = str(explicit_dsn or config.neon_dsn() or os.getenv("DATABASE_URL", "")).strip()
    if not dsn:
        raise ValueError("missing DSN: set NEON_DATABASE_URL or pass --dsn")
    return dsn


def connect(
    *,
    dsn: str,
    autocommit: bool = False,
    connect_timeout: int = 15,
    options: dict[str, Any] | None = None,
) -> psycopg.Connection:
    kwargs: dict[str, Any] = {
        "autocommit": bool(autocommit),
        "connect_timeout": int(connect_timeout),
    }
    if options:
        kwargs.update(options)
    return psycopg.connect(dsn, **kwargs)
