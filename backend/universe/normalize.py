"""Shared universe normalization helpers."""

from __future__ import annotations


def normalize_ric(value: str | None) -> str:
    return str(value or "").strip().upper()


def normalize_ticker(value: str | None) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def normalize_optional_text(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none"}:
        return None
    return text


def ticker_from_ric(ric: str | None) -> str | None:
    text = normalize_ric(ric)
    if not text:
        return None
    return text.split(".", 1)[0]
