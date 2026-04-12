from __future__ import annotations

import sqlite3

from backend.universe import estu


def test_load_security_frame_scopes_runtime_rows_to_selected_as_of_date(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_load_security_runtime_rows(conn, **kwargs):
        captured["as_of_date"] = kwargs.get("as_of_date")
        captured["include_disabled"] = kwargs.get("include_disabled")
        return [
            {
                "ric": "AAA.N",
                "ticker": "AAA",
                "classification_ready": 1,
                "is_single_name_equity": 1,
                "allow_cuse_native_core": 1,
            }
        ]

    monkeypatch.setattr(estu, "load_security_runtime_rows", _fake_load_security_runtime_rows)

    conn = sqlite3.connect(":memory:")
    try:
        frame = estu._load_security_frame(conn, as_of_date="2026-03-31")
    finally:
        conn.close()

    assert captured == {
        "as_of_date": "2026-03-31",
        "include_disabled": False,
    }
    assert len(frame) == 1
    assert int(frame.loc[0, "classification_ready"]) == 1
