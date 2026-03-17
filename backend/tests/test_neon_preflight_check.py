from __future__ import annotations

import json

from backend.scripts import neon_preflight_check


def test_preflight_uses_config_backed_dsn(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        neon_preflight_check,
        "_parse_args",
        lambda: type(
            "Args",
            (),
            {
                "dsn": None,
                "skip_connectivity": True,
                "check_write": False,
                "connect_timeout": 10,
                "json": True,
            },
        )(),
    )
    monkeypatch.setattr(
        neon_preflight_check,
        "resolve_dsn",
        lambda _explicit=None: "postgresql://user:pass@example.neon.tech/db?sslmode=require",
    )

    rc = neon_preflight_check.main()

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is True
    assert payload["dsn"]["dbname"] == "db"
    assert payload["sanitized_dsn"] == "postgresql://user:***@example.neon.tech/db?sslmode=require"


def test_preflight_reports_missing_dsn_as_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        neon_preflight_check,
        "_parse_args",
        lambda: type(
            "Args",
            (),
            {
                "dsn": None,
                "skip_connectivity": True,
                "check_write": False,
                "connect_timeout": 10,
                "json": True,
            },
        )(),
    )
    monkeypatch.setattr(
        neon_preflight_check,
        "resolve_dsn",
        lambda _explicit=None: (_ for _ in ()).throw(ValueError("missing DSN")),
    )

    rc = neon_preflight_check.main()

    assert rc == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "ok": False,
        "error": "missing DSN: set NEON_DATABASE_URL or pass --dsn",
    }
