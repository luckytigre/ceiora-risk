from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from backend.data import serving_outputs


def _canonical_payloads(**overrides):
    payloads = {
        "eligibility": {},
        "exposures": {"modes": {}},
        "health_diagnostics": {"status": "ok"},
        "model_sanity": {"status": "ok"},
        "portfolio": {"positions": [], "position_count": 0},
        "refresh_meta": {"risk_engine": {}},
        "risk": {"risk_shares": {}},
        "risk_engine_cov": {"factors": [], "matrix": []},
        "risk_engine_specific_risk": {},
        "universe_factors": {"factors": []},
        "universe_loadings": {"by_ticker": {}},
    }
    payloads.update(overrides)
    return payloads


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
            "risk": {"risk_shares": {"style": 50.0, "industry": 50.0, "market": 0.0, "idio": 0.0}},
        },
    )

    assert out["status"] == "ok"
    assert out["authority_store"] == "sqlite"
    assert out["row_count"] == 2
    assert out["sqlite_mirror_write"]["status"] == "ok"
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


def test_load_runtime_payload_state_reports_missing_sqlite(tmp_path: Path, monkeypatch) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(serving_outputs, "DATA_DB", data_db)
    monkeypatch.setattr(serving_outputs.config, "SERVING_OUTPUTS_PRIMARY_READS", False)
    monkeypatch.setattr(serving_outputs.config, "APP_RUNTIME_ROLE", "local-ingest")
    monkeypatch.setattr(serving_outputs.config, "DATA_BACKEND", "sqlite")

    out = serving_outputs.load_runtime_payload_state("health_diagnostics")

    assert out["status"] == "missing"
    assert out["source"] == "sqlite"
    assert out["value"] is None


def test_load_runtime_payload_state_reports_neon_error(monkeypatch) -> None:
    monkeypatch.setattr(serving_outputs.config, "SERVING_OUTPUTS_PRIMARY_READS", True)
    monkeypatch.setattr(serving_outputs.config, "APP_RUNTIME_ROLE", "cloud-serve")
    monkeypatch.setattr(serving_outputs.config, "DATA_BACKEND", "neon")
    monkeypatch.setattr(serving_outputs.config, "neon_surface_enabled", lambda surface: surface == "serving_outputs")
    monkeypatch.setattr(
        serving_outputs,
        "_load_current_payload_states_neon",
        lambda _names: {
            "health_diagnostics": {
                "status": "error",
                "source": "neon",
                "value": None,
                "error": {"type": "OperationalError", "message": "timed out"},
            }
        },
    )

    out = serving_outputs.load_runtime_payload_state("health_diagnostics")

    assert out["status"] == "error"
    assert out["source"] == "neon"
    assert out["error"]["type"] == "OperationalError"


def test_persist_current_payloads_partial_write_preserves_existing_rows(tmp_path: Path, monkeypatch) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(serving_outputs, "DATA_DB", data_db)
    monkeypatch.setattr(serving_outputs.config, "neon_surface_enabled", lambda _surface: False)

    serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id="run_1",
        snapshot_id="snap_1",
        refresh_mode="serve-refresh",
        payloads=_canonical_payloads(
            portfolio={"positions": [{"ticker": "AAPL"}]},
            risk={"risk_shares": {"style": 50.0}},
        ),
        replace_all=True,
    )
    out = serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id="run_2",
        snapshot_id="snap_2",
        refresh_mode="serve-refresh",
        payloads={"portfolio": {"positions": [{"ticker": "MSFT"}]}},
    )

    assert out["status"] == "ok"
    assert out["replace_all"] is False
    assert serving_outputs.load_current_payload("portfolio") == {"positions": [{"ticker": "MSFT"}]}
    assert serving_outputs.load_current_payload("risk") == {"risk_shares": {"style": 50.0}}


def test_persist_current_payloads_replace_all_requires_canonical_payload_set(tmp_path: Path, monkeypatch) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(serving_outputs, "DATA_DB", data_db)
    monkeypatch.setattr(serving_outputs.config, "neon_surface_enabled", lambda _surface: False)

    with pytest.raises(ValueError, match="canonical serving payload set"):
        serving_outputs.persist_current_payloads(
            data_db=data_db,
            run_id="run_1",
            snapshot_id="snap_1",
            refresh_mode="serve-refresh",
            payloads={"portfolio": {"positions": [{"ticker": "AAPL"}]}},
            replace_all=True,
        )


def test_persist_current_payloads_replace_all_deletes_absent_rows(tmp_path: Path, monkeypatch) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(serving_outputs, "DATA_DB", data_db)
    monkeypatch.setattr(serving_outputs.config, "neon_surface_enabled", lambda _surface: False)

    serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id="run_1",
        snapshot_id="snap_1",
        refresh_mode="serve-refresh",
        payloads=_canonical_payloads(
            portfolio={"positions": [{"ticker": "AAPL"}]},
            risk={"risk_shares": {"style": 50.0}},
        ),
        replace_all=True,
    )
    conn = sqlite3.connect(str(data_db))
    try:
        conn.execute(
            """
            INSERT INTO serving_payload_current (
                payload_name,
                snapshot_id,
                run_id,
                refresh_mode,
                payload_json,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy_payload",
                "snap_1",
                "run_1",
                "serve-refresh",
                json.dumps({"status": "legacy"}),
                "2026-03-24T00:00:00+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()
    out = serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id="run_2",
        snapshot_id="snap_2",
        refresh_mode="serve-refresh",
        payloads=_canonical_payloads(portfolio={"positions": [{"ticker": "MSFT"}]}),
        replace_all=True,
    )

    assert out["status"] == "ok"
    assert out["replace_all"] is True
    assert serving_outputs.load_current_payload("portfolio") == {"positions": [{"ticker": "MSFT"}]}
    assert serving_outputs.load_current_payload("legacy_payload") is None


def test_persist_current_payloads_dual_writes_to_neon_when_surface_enabled(tmp_path: Path, monkeypatch) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(serving_outputs, "DATA_DB", data_db)
    monkeypatch.setattr(serving_outputs.config, "neon_surface_enabled", lambda surface: surface == "serving_outputs")
    monkeypatch.setattr(
        serving_outputs,
        "_persist_current_payloads_neon",
        lambda rows, *, replace_all, write_mode: {
            "status": "ok",
            "row_count": len(rows),
            "replace_all": bool(replace_all),
            "write_mode": str(write_mode),
        },
    )

    out = serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id="run_2",
        snapshot_id="snap_2",
        refresh_mode="serve-refresh",
        payloads={"portfolio": {"positions": [], "position_count": 0}},
        replace_all=False,
    )

    assert out["status"] == "ok"
    assert out["authority_store"] == "neon"
    assert out["neon_write"]["status"] == "ok"
    assert out["neon_write"]["row_count"] == 1
    assert out["neon_write"]["replace_all"] is False
    assert out["neon_write"]["write_mode"] == "bulk"
    assert out["sqlite_mirror_write"]["status"] == "ok"


def test_persist_current_payloads_neon_verification_failure_bubbles_up(tmp_path: Path, monkeypatch) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(serving_outputs, "DATA_DB", data_db)
    monkeypatch.setattr(serving_outputs.config, "neon_surface_enabled", lambda surface: surface == "serving_outputs")
    monkeypatch.setattr(serving_outputs.config, "serving_payload_neon_write_required", lambda: False)
    monkeypatch.setattr(
        serving_outputs,
        "_persist_current_payloads_neon",
        lambda rows, *, replace_all, write_mode: {
            "status": "error",
            "row_count": len(rows),
            "replace_all": bool(replace_all),
            "verification": {
                "status": "error",
                "issues": ["missing_payload:risk"],
            },
        },
    )

    out = serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id="run_2",
        snapshot_id="snap_2",
        refresh_mode="serve-refresh",
        payloads={"portfolio": {"positions": [], "position_count": 0}},
        replace_all=False,
    )

    assert out["status"] == "ok"
    assert out["authority_store"] == "sqlite"
    assert out["neon_write"]["status"] == "error"
    assert out["neon_write"]["verification"]["issues"] == ["missing_payload:risk"]
    assert out["sqlite_mirror_write"]["status"] == "ok"


def test_verify_current_payloads_neon_detects_payload_json_mismatch() -> None:
    class _FakeCursor:
        def __init__(self) -> None:
            self._rows = [
                (
                    "portfolio",
                    "snap_1",
                    "run_1",
                    "serve-refresh",
                    '{"positions":[{"ticker":"MSFT"}]}',
                )
            ]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def execute(self, *_args, **_kwargs) -> None:
            return None

        def fetchall(self):
            return list(self._rows)

    class _FakeConn:
        def cursor(self):
            return _FakeCursor()

    out = serving_outputs._verify_current_payloads_neon(
        _FakeConn(),
        rows=[
            (
                "portfolio",
                "snap_1",
                "run_1",
                "serve-refresh",
                '{"positions":[{"ticker":"AAPL"}]}',
                "2026-03-23T00:00:00+00:00",
            )
        ],
        replace_all=False,
    )

    assert out["status"] == "error"
    assert any(
        issue.startswith("metadata_mismatch:portfolio:payload_json_sha256:")
        for issue in out["issues"]
    )


def test_persist_current_payloads_raises_before_sqlite_mirror_when_neon_required(tmp_path: Path, monkeypatch) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(serving_outputs, "DATA_DB", data_db)
    monkeypatch.setattr(serving_outputs.config, "neon_surface_enabled", lambda surface: surface == "serving_outputs")
    monkeypatch.setattr(serving_outputs.config, "serving_payload_neon_write_required", lambda: True)
    monkeypatch.setattr(
        serving_outputs,
        "_persist_current_payloads_neon",
        lambda rows, *, replace_all, write_mode: {
            "status": "error",
            "error": {"type": "RuntimeError", "message": "boom"},
            "row_count": len(rows),
            "replace_all": bool(replace_all),
        },
    )

    with pytest.raises(RuntimeError, match="Neon serving payload persistence failed"):
            serving_outputs.persist_current_payloads(
                data_db=data_db,
                run_id="run_2",
                snapshot_id="snap_2",
                refresh_mode="serve-refresh",
                payloads={"portfolio": {"positions": [], "position_count": 0}},
                replace_all=False,
            )

    conn = sqlite3.connect(str(data_db))
    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'serving_payload_current'"
        ).fetchall()
    finally:
        conn.close()
    assert tables == []


def test_load_current_payload_does_not_fallback_to_sqlite_when_neon_is_primary(tmp_path: Path, monkeypatch) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(serving_outputs, "DATA_DB", data_db)
    monkeypatch.setattr(serving_outputs.config, "neon_surface_enabled", lambda _surface: False)
    monkeypatch.setattr(serving_outputs, "_use_neon_reads", lambda: True)
    monkeypatch.setattr(serving_outputs.config, "serving_outputs_cache_fallback_enabled", lambda: False)
    monkeypatch.setattr(
        serving_outputs,
        "_load_current_payloads_neon",
        lambda payload_names: {str(name): None for name in payload_names},
    )
    monkeypatch.setattr(
        serving_outputs,
        "_persist_current_payloads_neon",
        lambda rows, *, replace_all, write_mode: (_ for _ in ()).throw(AssertionError("test should not hit real Neon writes")),
    )

    serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id="run_1",
        snapshot_id="snap_1",
        refresh_mode="serve-refresh",
        payloads=_canonical_payloads(
            portfolio={"positions": [{"ticker": "AAPL"}], "position_count": 1},
        ),
        replace_all=True,
    )

    out = serving_outputs.load_current_payload("portfolio")

    assert out is None


def test_load_current_payloads_reads_multiple_rows_in_one_surface(tmp_path: Path, monkeypatch) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(serving_outputs, "DATA_DB", data_db)
    monkeypatch.setattr(serving_outputs.config, "neon_surface_enabled", lambda _surface: False)

    serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id="run_1",
        snapshot_id="snap_1",
        refresh_mode="serve-refresh",
        payloads=_canonical_payloads(
            portfolio={"positions": [{"ticker": "AAPL"}]},
            risk={"risk_shares": {"style": 50.0}},
        ),
        replace_all=True,
    )

    out = serving_outputs.load_current_payloads(("portfolio", "risk", "missing"))

    assert out == {
        "portfolio": {"positions": [{"ticker": "AAPL"}]},
        "risk": {"risk_shares": {"style": 50.0}},
        "missing": None,
    }


def test_load_runtime_payloads_only_calls_fallback_for_missing_keys(monkeypatch) -> None:
    fallback_calls: list[str] = []

    monkeypatch.setattr(
        serving_outputs,
        "load_current_payload_states",
        lambda names: {
            "risk": {
                "status": "ok",
                "source": "neon",
                "value": {"risk_shares": {"style": 50.0}},
            },
            "model_sanity": {"status": "missing", "source": "neon", "value": None},
        },
    )
    monkeypatch.setattr(serving_outputs.config, "serving_outputs_cache_fallback_enabled", lambda: True)

    out = serving_outputs.load_runtime_payloads(
        ("risk", "model_sanity"),
        fallback_loader=lambda key: (fallback_calls.append(key), {"status": "ok"})[1],
    )

    assert out == {
        "risk": {"risk_shares": {"style": 50.0}},
        "model_sanity": {"status": "ok"},
    }
    assert fallback_calls == ["model_sanity"]


def test_collect_current_payload_manifest_and_compare_detects_snapshot_drift(tmp_path: Path, monkeypatch) -> None:
    data_db = tmp_path / "data.db"
    monkeypatch.setattr(serving_outputs, "DATA_DB", data_db)
    monkeypatch.setattr(serving_outputs.config, "neon_surface_enabled", lambda _surface: False)

    serving_outputs.persist_current_payloads(
        data_db=data_db,
        run_id="run_1",
        snapshot_id="snap_1",
        refresh_mode="serve-refresh",
        payloads={
            "portfolio": {"positions": [{"ticker": "AAPL"}]},
            "risk": {"risk_shares": {"style": 50.0}},
        },
    )

    sqlite_manifest = serving_outputs.collect_current_payload_manifest(
        store="sqlite",
        payload_names=("portfolio", "risk"),
        data_db=data_db,
    )
    assert sqlite_manifest["row_count"] == 2
    assert sqlite_manifest["missing_requested_payloads"] == []

    monkeypatch.setattr(
        serving_outputs,
        "_load_current_payload_rows_neon",
        lambda payload_names=None: [
            (
                "portfolio",
                "snap_2",
                "run_2",
                "serve-refresh",
                '{"positions":[{"ticker":"AAPL"}]}',
                "2026-03-24T00:00:00+00:00",
            ),
            (
                "risk",
                "snap_2",
                "run_2",
                "serve-refresh",
                '{"risk_shares":{"style":50.0}}',
                "2026-03-24T00:00:00+00:00",
            ),
        ],
    )
    neon_manifest = serving_outputs.collect_current_payload_manifest(
        store="neon",
        payload_names=("portfolio", "risk"),
        data_db=data_db,
    )

    diff = serving_outputs.compare_current_payload_manifests(sqlite_manifest, neon_manifest)

    assert diff["status"] == "error"
    assert "manifest_mismatch:distinct_snapshot_ids:['snap_1']!=['snap_2']" in diff["issues"]
    assert "mismatch:portfolio:snapshot_id:snap_1!=snap_2" in diff["issues"]
