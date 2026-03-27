from __future__ import annotations

from backend.services import neon_holdings_identifiers


class _FakeCursor:
    def __init__(self, route):
        self._route = route
        self._rows = []
        self.executed: list[tuple[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self.executed.append((sql, params))
        self._rows = list(self._route(sql, params))

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    def __init__(self, route):
        self.cursor_obj = _FakeCursor(route)

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj


def test_ric_exists_prefers_registry_surfaces_when_available() -> None:
    def _route(sql: str, params):
        if "information_schema.tables" in sql:
            assert params in {
                ("security_registry",),
                ("security_master_compat_current",),
            }
            return [(1,)] if params == ("security_registry",) else []
        if "FROM security_registry reg" in sql:
            assert params == ("AAPL.OQ",)
            return [("AAPL",)]
        raise AssertionError(f"unexpected SQL: {sql}")

    conn = _FakeConn(_route)

    ok, ticker = neon_holdings_identifiers.ric_exists(conn, " aapl.oq ")

    assert ok is True
    assert ticker == "AAPL"
    assert any("FROM security_registry reg" in sql for sql, _ in conn.cursor_obj.executed)


def test_resolve_ticker_to_ric_internal_ranks_active_native_core_ahead_of_projection_rows() -> None:
    def _route(sql: str, params):
        if "information_schema.tables" in sql:
            assert params in {
                ("security_registry",),
                ("security_policy_current",),
                ("security_taxonomy_current",),
                ("security_master_compat_current",),
            }
            return [(1,)] if params != ("security_master_compat_current",) else []
        if "FROM security_registry reg" in sql:
            assert params == ("SPY",)
            return [
                ("SPY.P", "SPY", 0, 0, 1, 0, 0),
                ("SPY.N", "SPY", 1, 1, 0, 1, 1),
            ]
        raise AssertionError(f"unexpected SQL: {sql}")

    conn = _FakeConn(_route)

    picked, alternatives = neon_holdings_identifiers.resolve_ticker_to_ric_internal(conn, " spy ")

    assert picked == "SPY.N"
    assert alternatives == ["SPY.P"]


def test_ric_exists_does_not_fall_back_to_legacy_when_registry_marks_name_non_current() -> None:
    def _route(sql: str, params):
        if "information_schema.tables" in sql:
            assert params in {
                ("security_registry",),
                ("security_master_compat_current",),
            }
            return [(1,)] if params == ("security_registry",) else []
        if "FROM security_registry reg" in sql:
            assert params == ("OLD.OQ",)
            return []
        raise AssertionError(f"unexpected SQL: {sql}")

    conn = _FakeConn(_route)

    ok, ticker = neon_holdings_identifiers.ric_exists(conn, "old.oq")

    assert ok is False
    assert ticker is None
    assert not any("FROM security_master" in sql for sql, _ in conn.cursor_obj.executed)


def test_resolve_ticker_to_ric_internal_rejects_historical_only_registry_matches() -> None:
    def _route(sql: str, params):
        if "information_schema.tables" in sql:
            assert params in {
                ("security_registry",),
                ("security_policy_current",),
                ("security_taxonomy_current",),
                ("security_master_compat_current",),
            }
            return [(1,)] if params != ("security_master_compat_current",) else []
        if "FROM security_registry reg" in sql:
            assert params == ("OLD",)
            return []
        raise AssertionError(f"unexpected SQL: {sql}")

    conn = _FakeConn(_route)

    picked, alternatives = neon_holdings_identifiers.resolve_ticker_to_ric_internal(conn, "old")

    assert picked is None
    assert alternatives == []


def test_resolve_ticker_to_ric_internal_falls_back_to_compat_surface_when_registry_missing() -> None:
    def _route(sql: str, params):
        if "information_schema.tables" in sql:
            assert params in {
                ("security_registry",),
                ("security_master_compat_current",),
            }
            return [] if params == ("security_registry",) else [(1,)]
        if "FROM security_master_compat_current" in sql:
            assert params == ("QQQ",)
            return [
                ("QQQ.OQ", "QQQ", "native_equity", 1, 1),
            ]
        raise AssertionError(f"unexpected SQL: {sql}")

    conn = _FakeConn(_route)

    picked, alternatives = neon_holdings_identifiers.resolve_ticker_to_ric_internal(conn, "qqq")

    assert picked == "QQQ.OQ"
    assert alternatives == []
    assert not any("FROM security_master " in sql for sql, _ in conn.cursor_obj.executed)


def test_resolve_ticker_to_ric_internal_stays_registry_first_when_companion_surfaces_are_incomplete() -> None:
    def _route(sql: str, params):
        if "information_schema.tables" in sql:
            if params == ("security_registry",):
                return [(1,)]
            if params == ("security_policy_current",):
                return []
            if params == ("security_taxonomy_current",):
                return [(1,)]
            if params == ("security_master_compat_current",):
                return []
        if "FROM security_registry reg" in sql:
            assert params == ("QQQ",)
            return [
                ("QQQ.OQ", "QQQ", 0, 0, 0, 1, 1),
            ]
        raise AssertionError(f"unexpected SQL: {sql}")

    conn = _FakeConn(_route)

    picked, alternatives = neon_holdings_identifiers.resolve_ticker_to_ric_internal(conn, "qqq")

    assert picked == "QQQ.OQ"
    assert alternatives == []
    assert not any("FROM security_master" in sql for sql, _ in conn.cursor_obj.executed)
