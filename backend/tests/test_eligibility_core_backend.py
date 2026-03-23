from __future__ import annotations

from pathlib import Path

from backend.risk_model import eligibility


def test_load_exposure_snapshots_uses_core_backend_when_neon_enabled(
    monkeypatch,
) -> None:
    calls: list[tuple[str, list[object] | None]] = []

    monkeypatch.setattr(eligibility, "_use_neon_reads", lambda: True)

    def _fake_fetch_rows(sql: str, params: list[object] | None = None, *, data_db: Path, neon_enabled: bool):
        calls.append((sql, params))
        assert neon_enabled is True
        if "information_schema.columns" in sql and "barra_raw_cross_section_history" in str(params):
            return [
                {"column_name": "ric"},
                {"column_name": "ticker"},
                {"column_name": "as_of_date"},
                {"column_name": "style_beta_score"},
                {"column_name": "trbc_business_sector"},
            ]
        if "SELECT DISTINCT as_of_date" in sql:
            return [{"as_of_date": "2026-03-07"}]
        if "FROM barra_raw_cross_section_history" in sql:
            return [
                {
                    "ric": "AAPL.OQ",
                    "ticker": "AAPL",
                    "as_of_date": "2026-03-07",
                    "style_beta_score": 1.2,
                    "trbc_business_sector": "Technology",
                }
            ]
        raise AssertionError(sql)

    monkeypatch.setattr(eligibility.core_backend, "fetch_rows", _fake_fetch_rows)

    dates, snapshots = eligibility.load_exposure_snapshots(Path("/tmp/unused.db"), dates=["2026-03-07"])

    assert dates == ["2026-03-07"]
    assert "AAPL.OQ" in snapshots["2026-03-07"].index
    assert any("barra_raw_cross_section_history" in sql for sql, _ in calls)


def test_load_market_cap_panel_uses_core_backend_when_neon_enabled(
    monkeypatch,
) -> None:
    monkeypatch.setattr(eligibility, "_use_neon_reads", lambda: True)

    def _fake_fetch_rows(sql: str, params: list[object] | None = None, *, data_db: Path, neon_enabled: bool):
        assert "security_fundamentals_pit" in sql
        assert neon_enabled is True
        return [
            {"ric": "AAPL.OQ", "fetch_date": "2026-03-06", "market_cap": 100.0},
            {"ric": "AAPL.OQ", "fetch_date": "2026-03-07", "market_cap": 105.0},
        ]

    monkeypatch.setattr(eligibility.core_backend, "fetch_rows", _fake_fetch_rows)

    panel = eligibility._load_market_cap_panel(Path("/tmp/unused.db"), ["2026-03-06", "2026-03-07"])

    assert panel.loc["2026-03-07", "AAPL.OQ"] == 105.0


def test_load_trbc_classification_panel_uses_core_backend_when_neon_enabled(
    monkeypatch,
) -> None:
    monkeypatch.setattr(eligibility, "_use_neon_reads", lambda: True)

    def _fake_fetch_rows(sql: str, params: list[object] | None = None, *, data_db: Path, neon_enabled: bool):
        assert neon_enabled is True
        if "information_schema.columns" in sql and "security_classification_pit" in str(params):
            return [
                {"column_name": "ric"},
                {"column_name": "as_of_date"},
                {"column_name": "trbc_economic_sector_short"},
                {"column_name": "trbc_business_sector"},
                {"column_name": "trbc_industry_group"},
                {"column_name": "hq_country_code"},
            ]
        if "FROM security_classification_pit h" in sql:
            return [
                {
                    "ric": "AAPL.OQ",
                    "ref_date": "2026-03-07",
                    "trbc_economic_sector_short": "Technology",
                    "trbc_business_sector": "Technology",
                    "trbc_industry_group": "Computers",
                    "hq_country_code": "US",
                }
            ]
        raise AssertionError(sql)

    monkeypatch.setattr(eligibility.core_backend, "fetch_rows", _fake_fetch_rows)

    sector, business, industry, country = eligibility._load_trbc_classification_panel(
        Path("/tmp/unused.db"),
        ["2026-03-07"],
    )

    assert sector.loc["2026-03-07", "AAPL.OQ"] == "Technology"
    assert business.loc["2026-03-07", "AAPL.OQ"] == "Technology"
    assert industry.loc["2026-03-07", "AAPL.OQ"] == "Computers"
    assert country.loc["2026-03-07", "AAPL.OQ"] == "US"
