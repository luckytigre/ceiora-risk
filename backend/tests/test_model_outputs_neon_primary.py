from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from backend.data import model_outputs


class _DummyPgConn:
    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None


def _factor_returns_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": "2026-03-03",
                "factor_name": "Beta",
                "factor_return": 0.01,
                "robust_se": 0.001,
                "t_stat": 10.0,
                "r_squared": 0.3,
                "residual_vol": 0.2,
                "cross_section_n": 100,
                "eligible_n": 95,
                "coverage": 0.95,
            }
        ]
    )


def _cov() -> pd.DataFrame:
    return pd.DataFrame([[1.0]], index=["Beta"], columns=["Beta"])


def _spec() -> dict[str, dict[str, float | int | str]]:
    return {
        "AAPL.OQ": {
            "ric": "AAPL.OQ",
            "ticker": "AAPL",
            "specific_var": 0.02,
            "specific_vol": 0.14,
            "obs": 60,
            "trbc_business_sector": "Tech",
        }
    }


def test_persist_model_outputs_writes_neon_first_when_configured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(model_outputs.config, "neon_dsn", lambda: "postgresql://example")
    monkeypatch.setattr(model_outputs.config, "neon_primary_model_data_enabled", lambda: True)
    monkeypatch.setattr(model_outputs, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(model_outputs, "resolve_dsn", lambda _dsn=None: "postgresql://example")
    monkeypatch.setattr(model_outputs.writers, "ensure_postgres_schema", lambda _pg_conn: None)
    monkeypatch.setattr(
        model_outputs.state,
        "factor_returns_load_start_postgres",
        lambda _pg_conn, *, as_of_date, risk_engine_state: ("2026-03-03", "incremental"),
    )
    monkeypatch.setattr(model_outputs.payloads, "load_factor_returns", lambda *_args, **_kwargs: _factor_returns_df())

    def _fake_pg_write(_pg_conn, **kwargs):
        calls.append("neon")
        assert kwargs["factor_returns_min_date"] == "2026-03-03"
        assert len(kwargs["factor_returns_payload"]) == 1
        return {"status": "ok"}

    def _fake_sqlite_write(_conn, **kwargs):
        calls.append("sqlite")
        assert kwargs["as_of_date"] == "2026-03-03"
        return {"status": "ok"}

    monkeypatch.setattr(model_outputs.writers, "write_model_outputs_postgres", _fake_pg_write)
    monkeypatch.setattr(model_outputs.writers, "write_model_outputs_sqlite", _fake_sqlite_write)

    out = model_outputs.persist_model_outputs(
        data_db=tmp_path / "data.db",
        cache_db=tmp_path / "cache.db",
        run_id="run_1",
        refresh_mode="full",
        status="ok",
        started_at="2026-03-04T00:00:00Z",
        completed_at="2026-03-04T00:01:00Z",
        source_dates={"exposures_asof": "2026-03-03", "fundamentals_asof": "2026-03-03"},
        params={},
        risk_engine_state={"factor_returns_latest_date": "2026-03-03"},
        cov=_cov(),
        specific_risk_by_ticker=_spec(),
    )

    assert calls == ["neon", "sqlite"]
    assert out["status"] == "ok"
    assert out["authority_store"] == "neon"
    assert out["factor_returns_persistence_mode"] == "incremental"
    assert out["factor_returns_reload_from"] == "2026-03-03"
    assert out["row_counts"] == {
        "model_factor_returns_daily": 1,
        "model_factor_covariance_daily": 1,
        "model_specific_risk_daily": 1,
    }
    assert out["neon_write"]["status"] == "ok"
    assert out["sqlite_mirror_write"]["status"] == "ok"


def test_persist_model_outputs_raises_when_required_neon_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    monkeypatch.setattr(model_outputs.config, "neon_dsn", lambda: "postgresql://example")
    monkeypatch.setattr(model_outputs.config, "neon_primary_model_data_enabled", lambda: True)
    monkeypatch.setattr(model_outputs, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(model_outputs, "resolve_dsn", lambda _dsn=None: "postgresql://example")
    monkeypatch.setattr(model_outputs.writers, "ensure_postgres_schema", lambda _pg_conn: None)
    monkeypatch.setattr(
        model_outputs.state,
        "factor_returns_load_start_postgres",
        lambda _pg_conn, *, as_of_date, risk_engine_state: (None, "full"),
    )
    monkeypatch.setattr(model_outputs.payloads, "load_factor_returns", lambda *_args, **_kwargs: _factor_returns_df())
    monkeypatch.setattr(
        model_outputs.writers,
        "write_model_outputs_postgres",
        lambda _pg_conn, **_kwargs: calls.append("neon") or (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        model_outputs.writers,
        "write_model_outputs_sqlite",
        lambda _conn, **_kwargs: calls.append("sqlite") or {"status": "ok"},
    )

    with pytest.raises(RuntimeError, match="Neon model output persistence failed"):
        model_outputs.persist_model_outputs(
            data_db=tmp_path / "data.db",
            cache_db=tmp_path / "cache.db",
            run_id="run_1",
            refresh_mode="full",
            status="ok",
            started_at="2026-03-04T00:00:00Z",
            completed_at="2026-03-04T00:01:00Z",
            source_dates={"exposures_asof": "2026-03-03", "fundamentals_asof": "2026-03-03"},
            params={},
            risk_engine_state={"factor_returns_latest_date": "2026-03-03"},
            cov=_cov(),
            specific_risk_by_ticker=_spec(),
        )
    assert calls == ["neon"]


def test_persist_model_outputs_falls_back_to_sqlite_when_neon_optional(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(model_outputs.config, "neon_dsn", lambda: "postgresql://example")
    monkeypatch.setattr(model_outputs.config, "neon_primary_model_data_enabled", lambda: False)
    monkeypatch.setattr(model_outputs, "connect", lambda **_kwargs: _DummyPgConn())
    monkeypatch.setattr(model_outputs, "resolve_dsn", lambda _dsn=None: "postgresql://example")
    monkeypatch.setattr(model_outputs.writers, "ensure_postgres_schema", lambda _pg_conn: None)
    monkeypatch.setattr(
        model_outputs.state,
        "factor_returns_load_start_postgres",
        lambda _pg_conn, *, as_of_date, risk_engine_state: (None, "full"),
    )
    monkeypatch.setattr(model_outputs.payloads, "load_factor_returns", lambda *_args, **_kwargs: _factor_returns_df())
    monkeypatch.setattr(
        model_outputs.writers,
        "write_model_outputs_postgres",
        lambda _pg_conn, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        model_outputs.writers,
        "write_model_outputs_sqlite",
        lambda _conn, **_kwargs: {"status": "ok"},
    )

    out = model_outputs.persist_model_outputs(
        data_db=tmp_path / "data.db",
        cache_db=tmp_path / "cache.db",
        run_id="run_1",
        refresh_mode="full",
        status="ok",
        started_at="2026-03-04T00:00:00Z",
        completed_at="2026-03-04T00:01:00Z",
        source_dates={"exposures_asof": "2026-03-03", "fundamentals_asof": "2026-03-03"},
        params={},
        risk_engine_state={"factor_returns_latest_date": "2026-03-03"},
        cov=_cov(),
        specific_risk_by_ticker=_spec(),
    )

    assert out["status"] == "ok"
    assert out["authority_store"] == "sqlite"
    assert out["neon_write"]["status"] == "error"
    assert out["sqlite_mirror_write"]["status"] == "ok"
