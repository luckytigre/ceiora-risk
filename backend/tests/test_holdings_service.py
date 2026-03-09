from __future__ import annotations

from backend.services import holdings_service


class _FakeConn:
    def close(self) -> None:
        return None

    def rollback(self) -> None:
        return None


def test_trigger_light_refresh_passes_holdings_only_scope(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        holdings_service,
        "start_refresh",
        lambda **kwargs: captured.update(kwargs) or (True, {"status": "running"}),
    )

    out = holdings_service.trigger_light_refresh_if_requested(True)

    assert out == {"started": True, "state": {"status": "running"}}
    assert captured["mode"] == "light"
    assert captured["force_risk_recompute"] is False
    assert captured["refresh_scope"] == "holdings_only"


def test_run_position_upsert_noop_skips_dirty_and_refresh(monkeypatch) -> None:
    calls = {"dirty": 0, "refresh": 0}

    monkeypatch.setattr(holdings_service, "resolve_dsn", lambda _dsn=None: "postgres://example")
    monkeypatch.setattr(holdings_service, "connect", lambda **kwargs: _FakeConn())
    monkeypatch.setattr(
        holdings_service,
        "apply_single_position_edit",
        lambda *args, **kwargs: {
            "status": "ok",
            "action": "none",
            "account_id": "main",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 10.0,
            "import_batch_id": "batch_1",
        },
    )
    monkeypatch.setattr(
        holdings_service,
        "record_holdings_dirty",
        lambda **kwargs: calls.__setitem__("dirty", calls["dirty"] + 1),
    )
    monkeypatch.setattr(
        holdings_service,
        "trigger_light_refresh_if_requested",
        lambda trigger: calls.__setitem__("refresh", calls["refresh"] + int(bool(trigger))) or None,
    )

    out = holdings_service.run_position_upsert(
        account_id="main",
        ric="AAPL.OQ",
        quantity=10,
        trigger_refresh=False,
    )

    assert out["action"] == "none"
    assert calls["dirty"] == 0
    assert calls["refresh"] == 0


def test_run_holdings_import_dry_run_skips_dirty_and_refresh(monkeypatch) -> None:
    calls = {"dirty": 0, "refresh": 0}

    monkeypatch.setattr(holdings_service, "resolve_dsn", lambda _dsn=None: "postgres://example")
    monkeypatch.setattr(holdings_service, "connect", lambda **kwargs: _FakeConn())
    monkeypatch.setattr(
        holdings_service,
        "parse_holdings_rows",
        lambda *args, **kwargs: {"rows": [{"ric": "AAPL.OQ", "quantity": 10.0}], "rejected": []},
    )
    monkeypatch.setattr(
        holdings_service,
        "apply_holdings_import",
        lambda *args, **kwargs: {
            "status": "ok",
            "applied_upserts": 1,
            "applied_deletes": 0,
            "import_batch_id": "batch_1",
        },
    )
    monkeypatch.setattr(
        holdings_service,
        "record_holdings_dirty",
        lambda **kwargs: calls.__setitem__("dirty", calls["dirty"] + 1),
    )
    monkeypatch.setattr(
        holdings_service,
        "trigger_light_refresh_if_requested",
        lambda trigger: calls.__setitem__("refresh", calls["refresh"] + int(bool(trigger))) or None,
    )

    out = holdings_service.run_holdings_import(
        account_id="main",
        mode="replace_account",
        rows=[{"ric": "AAPL.OQ", "quantity": 10.0}],
        dry_run=True,
        trigger_refresh=True,
    )

    assert out["status"] == "ok"
    assert out["refresh"] is None
    assert calls["dirty"] == 0
    assert calls["refresh"] == 0


def test_run_position_remove_records_dirty_and_refresh(monkeypatch) -> None:
    calls = {"dirty": 0, "refresh": 0}

    monkeypatch.setattr(holdings_service, "resolve_dsn", lambda _dsn=None: "postgres://example")
    monkeypatch.setattr(holdings_service, "connect", lambda **kwargs: _FakeConn())
    monkeypatch.setattr(
        holdings_service,
        "remove_single_position",
        lambda *args, **kwargs: {
            "status": "ok",
            "action": "removed",
            "account_id": "main",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 0.0,
            "import_batch_id": "batch_1",
        },
    )
    monkeypatch.setattr(
        holdings_service,
        "record_holdings_dirty",
        lambda **kwargs: calls.__setitem__("dirty", calls["dirty"] + 1),
    )
    monkeypatch.setattr(
        holdings_service,
        "trigger_light_refresh_if_requested",
        lambda trigger: calls.__setitem__("refresh", calls["refresh"] + int(bool(trigger))) or {"started": True},
    )

    out = holdings_service.run_position_remove(
        account_id="main",
        ric="AAPL.OQ",
        trigger_refresh=True,
    )

    assert out["action"] == "removed"
    assert calls["dirty"] == 1
    assert calls["refresh"] == 1
