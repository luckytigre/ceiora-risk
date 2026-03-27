from __future__ import annotations

from pathlib import Path

import lseg.data as rd
import pytest

from backend.vendor.lseg_toolkit.client import session


def test_resolve_workspace_base_url_prefers_env(monkeypatch) -> None:
    monkeypatch.setenv("LSEG_WORKSPACE_BASE_URL", "http://localhost:9999")
    monkeypatch.setenv("LSEG_WORKSPACE_PORT", "1234")
    assert session.resolve_workspace_base_url() == "http://localhost:9999"


def test_latest_workspace_api_port_from_logs_uses_latest_log_dir(tmp_path: Path, monkeypatch) -> None:
    older = tmp_path / "Desktop.20260325.100000.p1"
    newer = tmp_path / "Desktop.20260326.100000.p2"
    older.mkdir()
    newer.mkdir()
    (older / "node-sxs.older.log").write_text("API Proxy is listening to port: 9000\n", encoding="utf-8")
    (newer / "node-sxs.newer.log").write_text("API Proxy is listening to port: 9001\n", encoding="utf-8")
    monkeypatch.setattr(session, "_WORKSPACE_LOG_ROOT", tmp_path)

    assert session._latest_workspace_api_port_from_logs() == 9001
    assert session.resolve_workspace_base_url() == "http://localhost:9001"


def test_open_managed_session_overrides_base_url_and_timeout(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeSession:
        def __init__(self) -> None:
            self.config = {"sessions.desktop.workspace.base-url": "http://localhost:9000"}
            self.timeout = None
            self.opened = False
            self.closed = False

        def set_timeout(self, seconds: int) -> None:
            self.timeout = int(seconds)

        def open(self) -> None:
            self.opened = True

        def close(self) -> None:
            self.closed = True

    fake_session = FakeSession()

    class FakeDefinition:
        def __init__(self, name: str = "workspace", app_key: str | None = None, app_name: str | None = None) -> None:
            captured["name"] = name
            captured["app_key"] = app_key
            captured["app_name"] = app_name

        def get_session(self):
            return fake_session

    monkeypatch.setattr(session, "_desktop_definition_type", lambda: FakeDefinition)
    monkeypatch.setattr(session, "resolve_workspace_base_url", lambda: "http://localhost:9001")
    monkeypatch.setenv("LSEG_SESSION_TIMEOUT_SECONDS", "75")
    monkeypatch.setattr(rd.session, "set_default", lambda sess: captured.setdefault("default_session", sess))

    opened = session.open_managed_session(app_key="demo-key")
    assert opened is fake_session
    assert fake_session.opened is True
    assert fake_session.config["sessions.desktop.workspace.base-url"] == "http://localhost:9001"
    assert fake_session.timeout == 75
    assert captured["app_key"] == "demo-key"
    assert captured["default_session"] is fake_session

    session.close_managed_session(opened)
    assert fake_session.closed is True


def test_open_managed_session_rolls_back_when_open_fails(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeSession:
        def __init__(self) -> None:
            self.config = {}
            self.closed = False

        def set_timeout(self, seconds: int) -> None:
            captured["timeout"] = int(seconds)

        def open(self) -> None:
            raise RuntimeError("boom")

        def close(self) -> None:
            self.closed = True

    fake_session = FakeSession()

    class FakeDefinition:
        def __init__(self, name: str = "workspace", app_key: str | None = None, app_name: str | None = None) -> None:
            captured["name"] = name
            captured["app_key"] = app_key
            captured["app_name"] = app_name

        def get_session(self):
            return fake_session

    default_calls: list[object] = []
    monkeypatch.setattr(session, "_desktop_definition_type", lambda: FakeDefinition)
    monkeypatch.setattr(session, "resolve_workspace_base_url", lambda: "http://localhost:9001")
    monkeypatch.setattr(rd.session, "set_default", lambda sess: default_calls.append(sess))

    with pytest.raises(RuntimeError, match="boom"):
        session.open_managed_session(app_key="demo-key")

    assert fake_session.closed is True
    assert default_calls == []
    assert captured["app_key"] == "demo-key"


def test_open_managed_session_falls_back_to_rd_open_session_when_definition_is_unavailable(monkeypatch) -> None:
    captured: dict[str, object] = {}
    fake_session = object()

    def _open_session(**kwargs):
        captured["kwargs"] = dict(kwargs)
        return fake_session

    monkeypatch.setattr(session, "_desktop_definition_type", lambda: None)
    monkeypatch.setattr(session, "resolve_workspace_base_url", lambda: "http://localhost:9001")
    monkeypatch.setattr(rd, "open_session", _open_session)

    opened = session.open_managed_session(app_key="demo-key")

    assert opened is fake_session
    assert captured["kwargs"] == {"name": "desktop.workspace", "app_key": "demo-key"}


def test_close_managed_session_clears_default_even_when_close_raises(monkeypatch) -> None:
    closed_default: list[bool] = []

    class FakeSession:
        def close(self) -> None:
            raise RuntimeError("close boom")

    fake_session = FakeSession()
    monkeypatch.setattr(session, "_default_session_matches", lambda sess: sess is fake_session)
    monkeypatch.setattr(session, "_close_default_session", lambda: closed_default.append(True))

    session.close_managed_session(fake_session)

    assert closed_default == [True]


def test_session_manager_uses_managed_session(monkeypatch) -> None:
    closed: list[object] = []
    fake_session = object()

    monkeypatch.setattr(session, "load_app_key", lambda: "cfg-key")
    monkeypatch.setattr(session, "open_managed_session", lambda app_key=None: fake_session)
    monkeypatch.setattr(session, "close_managed_session", lambda sess: closed.append(sess))

    manager = session.SessionManager(auto_open=False)
    assert manager.is_open is False

    manager.open_session()
    assert manager.is_open is True
    assert manager._session is fake_session

    manager.close_session()
    assert manager.is_open is False
    assert closed == [fake_session]
