from __future__ import annotations

from backend.services import holdings_service


class _FakeConn:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def rollback(self) -> None:
        return None


def _deps(
    *,
    conn: _FakeConn | None = None,
    accounts_loader=None,
    positions_loader=None,
    rows_parser=None,
    import_applier=None,
    position_upserter=None,
    position_remover=None,
    scenario_applier=None,
    dirty_recorder=None,
    refresh_requester=None,
) -> holdings_service.HoldingsDependencies:
    fake_conn = conn or _FakeConn()
    return holdings_service.HoldingsDependencies(
        dsn_resolver=lambda _dsn=None: "postgres://example",
        connection_factory=lambda **kwargs: fake_conn,
        accounts_loader=accounts_loader or (lambda _conn: []),
        positions_loader=positions_loader or (lambda _conn, account_id=None: []),
        rows_parser=rows_parser or (lambda *_args, **_kwargs: {"rows": [], "rejected": []}),
        import_applier=import_applier or (lambda *_args, **_kwargs: {"status": "ok"}),
        position_upserter=position_upserter or (lambda *_args, **_kwargs: {"status": "ok"}),
        position_remover=position_remover or (lambda *_args, **_kwargs: {"status": "ok"}),
        scenario_applier=scenario_applier or (lambda *_args, **_kwargs: {"status": "ok"}),
        dirty_recorder=dirty_recorder or (lambda **kwargs: None),
        refresh_requester=refresh_requester or (lambda trigger: None),
    )


def test_trigger_light_refresh_passes_holdings_only_scope() -> None:
    out = holdings_service.trigger_light_refresh_if_requested(
        True,
        refresh_requester=lambda **kwargs: {"started": True, "state": {"status": "running"}, "dispatch": "in_process"},
    )

    assert out == {"started": True, "state": {"status": "running"}, "dispatch": "in_process"}


def test_run_position_upsert_noop_skips_dirty_and_refresh() -> None:
    calls = {"dirty": 0, "refresh": 0}

    out = holdings_service.run_position_upsert(
        account_id="main",
        ric="AAPL.OQ",
        quantity=10,
        trigger_refresh=False,
        dependencies=_deps(
            position_upserter=lambda *args, **kwargs: {
                "status": "ok",
                "action": "none",
                "account_id": "main",
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "quantity": 10.0,
                "import_batch_id": "batch_1",
            },
            dirty_recorder=lambda **kwargs: calls.__setitem__("dirty", calls["dirty"] + 1),
            refresh_requester=lambda trigger: calls.__setitem__("refresh", calls["refresh"] + int(bool(trigger))) or None,
        ),
    )

    assert out["action"] == "none"
    assert calls["dirty"] == 0
    assert calls["refresh"] == 0


def test_run_holdings_import_dry_run_skips_dirty_and_refresh() -> None:
    calls = {"dirty": 0, "refresh": 0}

    out = holdings_service.run_holdings_import(
        account_id="main",
        mode="replace_account",
        rows=[{"ric": "AAPL.OQ", "quantity": 10.0}],
        dry_run=True,
        trigger_refresh=True,
        dependencies=_deps(
            rows_parser=lambda *args, **kwargs: {"rows": [{"ric": "AAPL.OQ", "quantity": 10.0}], "rejected": []},
            import_applier=lambda *args, **kwargs: {
                "status": "ok",
                "applied_upserts": 1,
                "applied_deletes": 0,
                "import_batch_id": "batch_1",
            },
            dirty_recorder=lambda **kwargs: calls.__setitem__("dirty", calls["dirty"] + 1),
            refresh_requester=lambda trigger: calls.__setitem__("refresh", calls["refresh"] + int(bool(trigger))) or None,
        ),
    )

    assert out["status"] == "ok"
    assert out["refresh"] is None
    assert calls["dirty"] == 0
    assert calls["refresh"] == 0


def test_run_position_remove_records_dirty_and_refresh() -> None:
    calls = {"dirty": 0, "refresh": 0}

    out = holdings_service.run_position_remove(
        account_id="main",
        ric="AAPL.OQ",
        trigger_refresh=True,
        dependencies=_deps(
            position_remover=lambda *args, **kwargs: {
                "status": "ok",
                "action": "removed",
                "account_id": "main",
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "quantity": 0.0,
                "import_batch_id": "batch_1",
            },
            dirty_recorder=lambda **kwargs: calls.__setitem__("dirty", calls["dirty"] + 1),
            refresh_requester=lambda trigger: calls.__setitem__("refresh", calls["refresh"] + int(bool(trigger))) or {"started": True},
        ),
    )

    assert out["action"] == "removed"
    assert calls["dirty"] == 1
    assert calls["refresh"] == 1


def test_record_holdings_dirty_logs_and_does_not_raise(monkeypatch) -> None:
    errors: list[str] = []

    monkeypatch.setattr(
        holdings_service.logger,
        "exception",
        lambda message, *args, **kwargs: errors.append(str(message)),
    )

    holdings_service.record_holdings_dirty(
        action="holdings_position_edit",
        account_id="main",
        summary="edit",
        import_batch_id="batch_1",
        change_count=1,
        dirty_marker=lambda **kwargs: (_ for _ in ()).throw(RuntimeError("runtime-state down")),
    )

    assert errors == ["Failed to persist holdings dirty state"]


def test_run_whatif_apply_records_dirty_without_runtime_compat_shim() -> None:
    conn = _FakeConn()

    out = holdings_service.run_whatif_apply(
        scenario_rows=[{"account_id": "acct_a", "ticker": "AAA", "quantity": 10.0}],
        dependencies=_deps(
            conn=conn,
            scenario_applier=lambda *args, **kwargs: {
                "status": "ok",
                "applied_upserts": 1,
                "applied_deletes": 0,
                "import_batch_ids": {"acct_a": "batch_1"},
            },
            dirty_recorder=lambda **kwargs: None,
        ),
    )

    assert out["status"] == "ok"
    assert conn.closed is True
