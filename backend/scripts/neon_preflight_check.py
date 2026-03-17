#!/usr/bin/env python3
"""Stage-1 Neon preflight checks (dsn validation + optional connectivity test)."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import parse_qs, urlparse

import psycopg

from backend.data.neon import resolve_dsn


@dataclass
class DsnReport:
    ok: bool
    reason: str
    scheme: str
    host: str
    port: int | None
    dbname: str
    sslmode: str | None
    is_neon_host: bool


def _sanitize_dsn_for_log(dsn: str) -> str:
    parsed = urlparse(dsn)
    if not parsed.scheme:
        return "<invalid-dsn>"
    netloc = parsed.hostname or ""
    if parsed.port:
        netloc = f"{netloc}:{parsed.port}"
    user = parsed.username or ""
    if user:
        netloc = f"{user}:***@{netloc}"
    return parsed._replace(netloc=netloc).geturl()


def _validate_dsn(dsn: str) -> DsnReport:
    p = urlparse(dsn)
    q = parse_qs(p.query)
    sslmode = (q.get("sslmode") or [None])[0]
    scheme = str(p.scheme or "")
    host = str(p.hostname or "")
    dbname = str(p.path.lstrip("/") or "")

    if scheme not in {"postgres", "postgresql"}:
        return DsnReport(
            ok=False,
            reason="scheme must be postgres:// or postgresql://",
            scheme=scheme,
            host=host,
            port=p.port,
            dbname=dbname,
            sslmode=sslmode,
            is_neon_host=("neon.tech" in host.lower()),
        )
    if not host:
        return DsnReport(
            ok=False,
            reason="host missing",
            scheme=scheme,
            host=host,
            port=p.port,
            dbname=dbname,
            sslmode=sslmode,
            is_neon_host=False,
        )
    if not dbname:
        return DsnReport(
            ok=False,
            reason="database name missing in path",
            scheme=scheme,
            host=host,
            port=p.port,
            dbname=dbname,
            sslmode=sslmode,
            is_neon_host=("neon.tech" in host.lower()),
        )
    if sslmode not in {"require", "verify-ca", "verify-full"}:
        return DsnReport(
            ok=False,
            reason="sslmode should be require (or stricter) for Neon",
            scheme=scheme,
            host=host,
            port=p.port,
            dbname=dbname,
            sslmode=sslmode,
            is_neon_host=("neon.tech" in host.lower()),
        )
    return DsnReport(
        ok=True,
        reason="ok",
        scheme=scheme,
        host=host,
        port=p.port,
        dbname=dbname,
        sslmode=sslmode,
        is_neon_host=("neon.tech" in host.lower()),
    )


def _connectivity_check(
    dsn: str,
    *,
    check_write: bool,
    connect_timeout: int,
) -> dict[str, Any]:
    start = time.perf_counter()
    with psycopg.connect(dsn, connect_timeout=connect_timeout, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    current_database(),
                    current_user,
                    version(),
                    current_setting('server_version_num')
                """
            )
            row = cur.fetchone()
            out = {
                "current_database": str(row[0]),
                "current_user": str(row[1]),
                "server_version": str(row[2]),
                "server_version_num": str(row[3]),
            }
            if check_write:
                cur.execute("CREATE TEMP TABLE IF NOT EXISTS _neon_preflight_tmp (id INT)")
                cur.execute("TRUNCATE _neon_preflight_tmp")
                cur.execute("INSERT INTO _neon_preflight_tmp (id) VALUES (1)")
                cur.execute("SELECT COUNT(*) FROM _neon_preflight_tmp")
                out["temp_write_count"] = int(cur.fetchone()[0])
    out["connectivity_ok"] = True
    out["latency_ms"] = round((time.perf_counter() - start) * 1000.0, 2)
    return out


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--dsn",
        default=None,
        help="Neon Postgres DSN. Defaults to NEON_DATABASE_URL then DATABASE_URL env vars.",
    )
    p.add_argument(
        "--skip-connectivity",
        action="store_true",
        help="Only validate DSN format/settings without connecting.",
    )
    p.add_argument(
        "--check-write",
        action="store_true",
        help="Create/insert into a TEMP table to validate write capability.",
    )
    p.add_argument(
        "--connect-timeout",
        type=int,
        default=10,
        help="Postgres connection timeout seconds.",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON output.",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        dsn = resolve_dsn(args.dsn)
    except ValueError:
        msg = "missing DSN: set NEON_DATABASE_URL or pass --dsn"
        if args.json:
            print(json.dumps({"ok": False, "error": msg}, indent=2))
        else:
            print(msg)
        return 2

    dsn_report = _validate_dsn(dsn)
    out: dict[str, Any] = {
        "ok": bool(dsn_report.ok),
        "dsn": asdict(dsn_report),
        "sanitized_dsn": _sanitize_dsn_for_log(dsn),
        "connectivity": None,
    }
    if not dsn_report.ok:
        if args.json:
            print(json.dumps(out, indent=2))
        else:
            print(f"DSN invalid: {dsn_report.reason}")
        return 2

    if not args.skip_connectivity:
        try:
            out["connectivity"] = _connectivity_check(
                dsn,
                check_write=bool(args.check_write),
                connect_timeout=int(args.connect_timeout),
            )
        except Exception as exc:  # noqa: BLE001
            out["ok"] = False
            out["connectivity"] = {
                "connectivity_ok": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
            if args.json:
                print(json.dumps(out, indent=2))
            else:
                print(f"connectivity failed: {type(exc).__name__}: {exc}")
            return 1

    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print("Neon preflight ok")
        print(f"Host: {dsn_report.host} (neon_host={dsn_report.is_neon_host})")
        if out["connectivity"]:
            print(
                "Connected:",
                out["connectivity"]["current_database"],
                out["connectivity"]["current_user"],
                f"latency_ms={out['connectivity']['latency_ms']}",
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
