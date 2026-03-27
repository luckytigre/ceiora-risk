from __future__ import annotations

import pytest

from backend.data import holdings_reads


class _FakeCursor:
    def __init__(self, *, rows=None, execute_error: Exception | None = None, capture: dict[str, object] | None = None):
        self._rows = list(rows or [])
        self._execute_error = execute_error
        self._capture = capture if capture is not None else {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self._capture["sql"] = sql
        self._capture["params"] = params
        if self._execute_error is not None:
            raise self._execute_error

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


def test_load_holdings_accounts_shapes_rows_and_uses_expected_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    connect_calls: dict[str, object] = {}
    cursor = _FakeCursor(
        rows=[
            ("acct_alpha", None, 1, 2, 15.25, None),
            ("acct_beta", "Beta Account", 0, 0, 0.0, "2026-03-15T10:30:00Z"),
        ],
        capture=captured,
    )
    conn = _FakeConn(cursor)

    monkeypatch.setattr(holdings_reads, "resolve_dsn", lambda _explicit=None: "postgresql://example")

    def _connect(*, dsn: str, autocommit: bool):
        connect_calls["dsn"] = dsn
        connect_calls["autocommit"] = autocommit
        return conn

    monkeypatch.setattr(holdings_reads, "connect", _connect)

    rows = holdings_reads.load_holdings_accounts()

    assert connect_calls == {"dsn": "postgresql://example", "autocommit": True}
    assert "FROM holdings_accounts a" in str(captured["sql"])
    assert "ORDER BY a.account_id ASC" in str(captured["sql"])
    assert rows == [
        {
            "account_id": "acct_alpha",
            "account_name": "acct_alpha",
            "is_active": True,
            "positions_count": 2,
            "gross_quantity": 15.25,
            "last_position_updated_at": None,
        },
        {
            "account_id": "acct_beta",
            "account_name": "Beta Account",
            "is_active": False,
            "positions_count": 0,
            "gross_quantity": 0.0,
            "last_position_updated_at": "2026-03-15T10:30:00Z",
        },
    ]
    assert conn.closed is True


def test_load_holdings_positions_normalizes_request_and_shapes_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    cursor = _FakeCursor(
        rows=[
            ("acct_main", "aapl.oq", " aapl ", 10.5, "seed", None),
            ("acct_main", "msft.oq", None, 5, None, "2026-03-15T11:00:00Z"),
        ],
        capture=captured,
    )
    conn = _FakeConn(cursor)

    monkeypatch.setattr(holdings_reads, "resolve_dsn", lambda _explicit=None: "postgresql://example")
    monkeypatch.setattr(
        holdings_reads,
        "connect",
        lambda *, dsn, autocommit: conn,
    )

    rows = holdings_reads.load_holdings_positions(account_id="  ACCT_MAIN  ")

    assert captured["params"] == ("acct_main",)
    assert "NULLIF(TRIM(reg.ticker), '')" in str(captured["sql"])
    assert "NULLIF(TRIM(comp.ticker), '')" in str(captured["sql"])
    assert "LEFT JOIN security_registry reg" in str(captured["sql"])
    assert "LEFT JOIN security_master_compat_current comp" in str(captured["sql"])
    assert "LEFT JOIN security_master sm" not in str(captured["sql"])
    assert "ORDER BY p.account_id, COALESCE(" in str(captured["sql"])
    assert rows == [
        {
            "account_id": "acct_main",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 10.5,
            "source": "seed",
            "updated_at": None,
        },
        {
            "account_id": "acct_main",
            "ric": "MSFT.OQ",
            "ticker": None,
            "quantity": 5.0,
            "source": "",
            "updated_at": "2026-03-15T11:00:00Z",
        },
    ]
    assert conn.closed is True


def test_load_all_holdings_positions_shapes_rows_without_account_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    cursor = _FakeCursor(
        rows=[
            ("acct_alpha", "aapl.oq", " aapl ", 10.5, "seed", None),
            ("acct_beta", "msft.oq", None, 5, None, "2026-03-15T11:00:00Z"),
        ],
        capture=captured,
    )
    conn = _FakeConn(cursor)

    monkeypatch.setattr(holdings_reads, "resolve_dsn", lambda _explicit=None: "postgresql://example")
    monkeypatch.setattr(
        holdings_reads,
        "connect",
        lambda *, dsn, autocommit: conn,
    )

    rows = holdings_reads.load_all_holdings_positions()

    assert captured["params"] is None
    assert "FROM holdings_positions_current p" in str(captured["sql"])
    assert "WHERE p.account_id = %s" not in str(captured["sql"])
    assert "LEFT JOIN security_registry reg" in str(captured["sql"])
    assert "LEFT JOIN security_master_compat_current comp" in str(captured["sql"])
    assert "LEFT JOIN security_master sm" not in str(captured["sql"])
    assert "ORDER BY p.account_id, COALESCE(" in str(captured["sql"])
    assert rows == [
        {
            "account_id": "acct_alpha",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 10.5,
            "source": "seed",
            "updated_at": None,
        },
        {
            "account_id": "acct_beta",
            "ric": "MSFT.OQ",
            "ticker": None,
            "quantity": 5.0,
            "source": "",
            "updated_at": "2026-03-15T11:00:00Z",
        },
    ]
    assert conn.closed is True


def test_load_contributing_holdings_accounts_shapes_rows_and_orders_by_account(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    cursor = _FakeCursor(
        rows=[
            ("acct_alpha", None),
            ("acct_beta", "Beta Account"),
        ],
        capture=captured,
    )
    conn = _FakeConn(cursor)

    monkeypatch.setattr(holdings_reads, "resolve_dsn", lambda _explicit=None: "postgresql://example")
    monkeypatch.setattr(
        holdings_reads,
        "connect",
        lambda *, dsn, autocommit: conn,
    )

    rows = holdings_reads.load_contributing_holdings_accounts()

    assert captured["params"] is None
    assert "FROM holdings_positions_current p" in str(captured["sql"])
    assert "LEFT JOIN holdings_accounts a" in str(captured["sql"])
    assert "ORDER BY p.account_id ASC" in str(captured["sql"])
    assert rows == [
        {
            "account_id": "acct_alpha",
            "account_name": "acct_alpha",
        },
        {
            "account_id": "acct_beta",
            "account_name": "Beta Account",
        },
    ]
    assert conn.closed is True


def test_load_aggregate_holdings_positions_nets_quantities_and_uses_expected_sql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    cursor = _FakeCursor(
        rows=[
            ("all_accounts", "aapl.oq", " aapl ", 6.0, "aggregate", "2026-03-15T11:00:00Z"),
            ("all_accounts", "msft.oq", None, 5.0, "aggregate", "2026-03-15T09:00:00Z"),
        ],
        capture=captured,
    )
    conn = _FakeConn(cursor)

    monkeypatch.setattr(holdings_reads, "resolve_dsn", lambda _explicit=None: "postgresql://example")
    monkeypatch.setattr(
        holdings_reads,
        "connect",
        lambda *, dsn, autocommit: conn,
    )

    rows = holdings_reads.load_aggregate_holdings_positions()

    assert captured["params"] == (1e-12,)
    assert "ARRAY_AGG(" in str(captured["sql"])
    assert "SUM(CAST(p.quantity AS DOUBLE PRECISION)) AS quantity" in str(captured["sql"])
    assert "HAVING ABS(SUM(CAST(p.quantity AS DOUBLE PRECISION))) > %s" in str(captured["sql"])
    assert "ORDER BY COALESCE(agg.ticker, agg.ric), agg.ric" in str(captured["sql"])
    assert rows == [
        {
            "account_id": "all_accounts",
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "quantity": 6.0,
            "source": "aggregate",
            "updated_at": "2026-03-15T11:00:00Z",
        },
        {
            "account_id": "all_accounts",
            "ric": "MSFT.OQ",
            "ticker": None,
            "quantity": 5.0,
            "source": "aggregate",
            "updated_at": "2026-03-15T09:00:00Z",
        },
    ]
    assert conn.closed is True


def test_load_holdings_accounts_wraps_connect_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(holdings_reads, "resolve_dsn", lambda _explicit=None: "postgresql://example")
    monkeypatch.setattr(
        holdings_reads,
        "connect",
        lambda *, dsn, autocommit: (_ for _ in ()).throw(RuntimeError("dsn boom")),
    )

    with pytest.raises(holdings_reads.HoldingsReadError, match="Shared holdings account read failed: RuntimeError: dsn boom"):
        holdings_reads.load_holdings_accounts()


def test_load_holdings_positions_wraps_query_failures_and_closes_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(execute_error=RuntimeError("query boom"))
    conn = _FakeConn(cursor)

    monkeypatch.setattr(holdings_reads, "resolve_dsn", lambda _explicit=None: "postgresql://example")
    monkeypatch.setattr(
        holdings_reads,
        "connect",
        lambda *, dsn, autocommit: conn,
    )

    with pytest.raises(
        holdings_reads.HoldingsReadError,
        match="Shared holdings position read failed for account_id=acct_main: RuntimeError: query boom",
    ):
        holdings_reads.load_holdings_positions(account_id="ACCT_MAIN")

    assert conn.closed is True


def test_load_all_holdings_positions_wraps_query_failures_and_closes_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(execute_error=RuntimeError("query boom"))
    conn = _FakeConn(cursor)

    monkeypatch.setattr(holdings_reads, "resolve_dsn", lambda _explicit=None: "postgresql://example")
    monkeypatch.setattr(
        holdings_reads,
        "connect",
        lambda *, dsn, autocommit: conn,
    )

    with pytest.raises(
        holdings_reads.HoldingsReadError,
        match="Shared holdings position read failed for all accounts: RuntimeError: query boom",
    ):
        holdings_reads.load_all_holdings_positions()

    assert conn.closed is True


def test_load_contributing_holdings_accounts_wraps_query_failures_and_closes_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(execute_error=RuntimeError("query boom"))
    conn = _FakeConn(cursor)

    monkeypatch.setattr(holdings_reads, "resolve_dsn", lambda _explicit=None: "postgresql://example")
    monkeypatch.setattr(
        holdings_reads,
        "connect",
        lambda *, dsn, autocommit: conn,
    )

    with pytest.raises(
        holdings_reads.HoldingsReadError,
        match="Shared holdings contributing-account read failed: RuntimeError: query boom",
    ):
        holdings_reads.load_contributing_holdings_accounts()

    assert conn.closed is True


def test_load_aggregate_holdings_positions_wraps_query_failures_and_closes_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor(execute_error=RuntimeError("query boom"))
    conn = _FakeConn(cursor)

    monkeypatch.setattr(holdings_reads, "resolve_dsn", lambda _explicit=None: "postgresql://example")
    monkeypatch.setattr(
        holdings_reads,
        "connect",
        lambda *, dsn, autocommit: conn,
    )

    with pytest.raises(
        holdings_reads.HoldingsReadError,
        match="Shared holdings aggregate-position read failed: RuntimeError: query boom",
    ):
        holdings_reads.load_aggregate_holdings_positions()

    assert conn.closed is True
