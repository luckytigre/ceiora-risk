from __future__ import annotations

from pathlib import Path

from backend.services import neon_mirror, neon_source_sync_cycle


def test_run_neon_source_sync_cycle_runs_schema_then_sync_and_summarizes_factor_returns(
    monkeypatch,
    tmp_path: Path,
) -> None:
    sqlite_path = tmp_path / "snapshot.db"
    sqlite_path.touch()
    calls: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(
        neon_source_sync_cycle,
        "ensure_neon_canonical_schema",
        lambda **kwargs: calls.append(("schema", dict(kwargs))) or {"status": "ok"},
    )
    monkeypatch.setattr(
        neon_source_sync_cycle,
        "sync_from_sqlite_to_neon",
        lambda **kwargs: calls.append(("sync", dict(kwargs)))
        or {
            "status": "ok",
            "tables": {
                "model_factor_returns_daily": {
                    "status": "ok",
                    "rows_written": 42,
                }
            },
        },
    )

    out = neon_source_sync_cycle.run_neon_source_sync_cycle(
        sqlite_path=sqlite_path,
        dsn="postgresql://example",
        mode="incremental",
        tables=["model_factor_returns_daily"],
        batch_size=500,
    )

    assert calls == [
        ("schema", {"dsn": "postgresql://example"}),
        (
            "sync",
            {
                "sqlite_path": sqlite_path,
                "dsn": "postgresql://example",
                "tables": ["model_factor_returns_daily"],
                "mode": "incremental",
                "batch_size": 500,
            },
        ),
    ]
    assert out["status"] == "ok"
    assert out["schema_ensure"] == {"status": "ok"}
    assert out["sync"]["status"] == "ok"
    assert out["factor_returns_sync"] == {
        "status": "ok",
        "source_table": "model_factor_returns_daily",
        "rows_written": 42,
    }


def test_run_neon_mirror_cycle_delegates_source_sync_then_appends_prune_and_parity(
    monkeypatch,
    tmp_path: Path,
) -> None:
    sqlite_path = tmp_path / "workspace.db"
    cache_path = tmp_path / "cache.db"
    sqlite_path.touch()
    cache_path.touch()
    captured: dict[str, dict[str, object]] = {}

    def _fake_source_sync(**kwargs):
        captured["source_sync"] = dict(kwargs)
        return {
            "status": "ok",
            "mode": "incremental",
            "tables": ["security_registry"],
            "schema_ensure": {"status": "ok"},
            "sync": {"status": "ok"},
            "factor_returns_sync": {"status": "skipped"},
        }

    def _fake_prune(**kwargs):
        captured["prune"] = dict(kwargs)
        return {"status": "ok"}

    def _fake_parity(**kwargs):
        captured["parity"] = dict(kwargs)
        return {"status": "ok", "issues": []}

    monkeypatch.setattr(neon_mirror, "run_neon_source_sync_cycle", _fake_source_sync)
    monkeypatch.setattr(neon_mirror, "prune_neon_history", _fake_prune)
    monkeypatch.setattr(neon_mirror, "run_bounded_parity_audit", _fake_parity)

    out = neon_mirror.run_neon_mirror_cycle(
        sqlite_path=sqlite_path,
        cache_path=cache_path,
        dsn="postgresql://example",
        tables=["security_registry"],
        parity_enabled=True,
        prune_enabled=True,
        source_years=7,
        analytics_years=3,
    )

    assert captured["source_sync"] == {
        "sqlite_path": sqlite_path,
        "dsn": "postgresql://example",
        "mode": "incremental",
        "tables": ["security_registry"],
        "batch_size": 25_000,
    }
    assert captured["prune"] == {
        "dsn": "postgresql://example",
        "source_years": 7,
        "analytics_years": 3,
    }
    assert captured["parity"] == {
        "sqlite_path": sqlite_path,
        "cache_path": cache_path,
        "dsn": "postgresql://example",
        "source_years": 7,
        "analytics_years": 3,
    }
    assert out["status"] == "ok"
    assert out["prune"] == {"status": "ok"}
    assert out["parity"] == {"status": "ok", "issues": []}
