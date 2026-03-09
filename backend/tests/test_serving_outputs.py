from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from backend.data import serving_outputs


def test_persist_current_payloads_roundtrip(tmp_path: Path, monkeypatch) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(serving_outputs, "DATA_DB", data_db)
    monkeypatch.setattr(serving_outputs.config, "neon_surface_enabled", lambda _surface: False)

    out = serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id="run_1",
        snapshot_id="snap_1",
        refresh_mode="serve-refresh",
        payloads={
            "portfolio": {"positions": [{"ticker": "AAPL"}], "position_count": 1},
            "risk": {"risk_shares": {"style": 50.0, "industry": 50.0, "country": 0.0, "idio": 0.0}},
        },
    )

    assert out["status"] == "ok"
    assert out["row_count"] == 2
    assert serving_outputs.load_current_payload("portfolio") == {
        "positions": [{"ticker": "AAPL"}],
        "position_count": 1,
    }

    conn = sqlite3.connect(str(data_db))
    try:
        row = conn.execute(
            "SELECT snapshot_id, run_id, refresh_mode, payload_json FROM serving_payload_current WHERE payload_name = ?",
            ("risk",),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == "snap_1"
    assert row[1] == "run_1"
    assert row[2] == "serve-refresh"
    assert json.loads(str(row[3]))["risk_shares"]["style"] == 50.0


def test_persist_current_payloads_dual_writes_to_neon_when_surface_enabled(tmp_path: Path, monkeypatch) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(serving_outputs, "DATA_DB", data_db)
    monkeypatch.setattr(serving_outputs.config, "neon_surface_enabled", lambda surface: surface == "serving_outputs")
    monkeypatch.setattr(
        serving_outputs,
        "_persist_current_payloads_neon",
        lambda rows: {"status": "ok", "row_count": len(rows)},
    )

    out = serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id="run_2",
        snapshot_id="snap_2",
        refresh_mode="serve-refresh",
        payloads={"portfolio": {"positions": [], "position_count": 0}},
    )

    assert out["status"] == "ok"
    assert out["neon_write"]["status"] == "ok"
    assert out["neon_write"]["row_count"] == 1
