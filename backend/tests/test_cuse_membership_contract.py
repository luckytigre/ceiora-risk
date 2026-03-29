from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from backend.analytics.services.risk_views import build_positions_from_snapshot
from backend.analytics.services.universe_loadings import build_universe_ticker_loadings
from backend.data.cuse_membership_reads import load_cuse_membership_rows, load_cuse_stage_result_rows
from backend.data.model_outputs import persist_model_outputs
from backend.risk_model.risk_attribution import STYLE_COLUMN_TO_LABEL
from backend.universe.schema import ensure_cuse4_schema


def _ensure_estu_membership_daily_table(conn: sqlite3.Connection) -> None:
    ensure_cuse4_schema(conn)


def _seed_factor_returns(cache_db: Path) -> None:
    conn = sqlite3.connect(str(cache_db))
    conn.execute(
        """
        CREATE TABLE daily_factor_returns (
            date TEXT NOT NULL,
            factor_name TEXT NOT NULL,
            factor_return REAL NOT NULL,
            robust_se REAL NOT NULL DEFAULT 0.0,
            t_stat REAL NOT NULL DEFAULT 0.0,
            r_squared REAL NOT NULL,
            residual_vol REAL NOT NULL,
            cross_section_n INTEGER,
            eligible_n INTEGER,
            coverage REAL,
            PRIMARY KEY (date, factor_name)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO daily_factor_returns (
            date, factor_name, factor_return, robust_se, t_stat, r_squared, residual_vol,
            cross_section_n, eligible_n, coverage
        ) VALUES ('2026-03-13', 'Market', 0.01, 0.0, 0.0, 0.5, 0.2, 100, 95, 0.95)
        """
    )
    conn.commit()
    conn.close()


def _seed_source_context(data_db: Path) -> None:
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.executemany(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL.OQ", "AAPL", 1, 1, "native_equity", "seed", "2026-03-01T00:00:00+00:00"),
            ("SPY.P", "SPY", 0, 0, "projection_only", "seed", "2026-03-01T00:00:00+00:00"),
        ],
    )
    _seed_eligibility_inputs(
        conn,
        [
            {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "as_of_date": "2026-03-13",
                "market_cap": 100.0,
                "trbc_economic_sector": "Technology",
                "trbc_business_sector": "Technology Equipment",
                "hq_country_code": "US",
            }
        ],
    )
    _ensure_estu_membership_daily_table(conn)
    conn.execute(
        """
        INSERT INTO estu_membership_daily (
            date, ric, estu_flag, drop_reason, drop_reason_detail, source, updated_at
        ) VALUES ('2026-03-13', 'AAPL.OQ', 1, '', '', 'test', '2026-03-13T00:00:00Z')
        """
    )
    conn.commit()
    conn.close()


def _seed_eligibility_inputs(
    conn: sqlite3.Connection,
    rows: list[dict[str, object]],
) -> None:
    def _table_columns(table: str) -> set[str]:
        return {
            str(record[1])
            for record in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }

    style_cols = list(STYLE_COLUMN_TO_LABEL.keys())
    style_sql = ",\n            ".join(f"{column} REAL" for column in style_cols)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS barra_raw_cross_section_history (
            ric TEXT NOT NULL,
            ticker TEXT,
            as_of_date TEXT NOT NULL,
            {style_sql}
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS security_fundamentals_pit (
            ric TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            market_cap REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS security_classification_pit (
            ric TEXT NOT NULL,
            as_of_date TEXT NOT NULL,
            trbc_economic_sector TEXT,
            trbc_business_sector TEXT,
            hq_country_code TEXT,
            updated_at TEXT
        )
        """
    )
    fundamentals_cols = _table_columns("security_fundamentals_pit")
    classification_cols = _table_columns("security_classification_pit")
    raw_placeholders = ", ".join("?" for _ in range(3 + len(style_cols)))
    raw_columns = ", ".join(style_cols)
    for row in rows:
        as_of_date = str(row["as_of_date"])
        ric = str(row["ric"])
        ticker = str(row.get("ticker") or ric.split(".")[0])
        style_values = [1.0 for _ in style_cols]
        conn.execute(
            f"""
            INSERT INTO barra_raw_cross_section_history (
                ric, ticker, as_of_date, {raw_columns}
            ) VALUES ({raw_placeholders})
            """,
            [ric, ticker, as_of_date, *style_values],
        )
        if {"stat_date", "period_end_date"}.issubset(fundamentals_cols):
            conn.execute(
                """
                INSERT INTO security_fundamentals_pit (
                    ric, as_of_date, stat_date, period_end_date, market_cap, source, job_run_id, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ric,
                    as_of_date,
                    as_of_date,
                    as_of_date,
                    float(row.get("market_cap") or 100.0),
                    "test",
                    "job_1",
                    f"{as_of_date}T00:00:00Z",
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO security_fundamentals_pit (
                    ric, as_of_date, market_cap
                ) VALUES (?, ?, ?)
                """,
                (ric, as_of_date, float(row.get("market_cap") or 100.0)),
            )
        if {"source", "job_run_id"}.issubset(classification_cols):
            conn.execute(
                """
                INSERT INTO security_classification_pit (
                    ric, as_of_date, trbc_economic_sector, trbc_business_sector, hq_country_code, source, job_run_id, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ric,
                    as_of_date,
                    row.get("trbc_economic_sector"),
                    row.get("trbc_business_sector"),
                    row.get("hq_country_code"),
                    "test",
                    "job_1",
                    f"{as_of_date}T00:00:00Z",
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO security_classification_pit (
                    ric, as_of_date, trbc_economic_sector, trbc_business_sector, hq_country_code, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    ric,
                    as_of_date,
                    row.get("trbc_economic_sector"),
                    row.get("trbc_business_sector"),
                    row.get("hq_country_code"),
                    f"{as_of_date}T00:00:00Z",
                ),
            )


def test_persist_model_outputs_writes_cuse_membership_and_stage_rows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("backend.data.model_outputs.config.neon_dsn", lambda: "")
    monkeypatch.setattr("backend.data.model_outputs.config.neon_primary_model_data_enabled", lambda: False)

    data_db = tmp_path / "data.db"
    cache_db = tmp_path / "cache.db"
    _seed_factor_returns(cache_db)
    _seed_source_context(data_db)

    out = persist_model_outputs(
        data_db=data_db,
        cache_db=cache_db,
        run_id="run_1",
        refresh_mode="light",
        status="ok",
        started_at="2026-03-16T00:00:00Z",
        completed_at="2026-03-16T00:01:00Z",
        source_dates={"exposures_asof": "2026-03-13"},
        params={},
        risk_engine_state={"factor_returns_latest_date": "2026-03-13", "core_state_through_date": "2026-03-13"},
        cov=pd.DataFrame([[1.0]], index=["Market"], columns=["Market"]),
        specific_risk_by_ticker={
            "AAPL.OQ": {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "specific_var": 0.02,
                "specific_vol": 0.14,
                "obs": 60,
                "trbc_business_sector": "Technology Equipment",
            }
        },
        persisted_payloads={
            "universe_loadings": {
                "as_of_date": "2026-03-13",
                "by_ticker": {
                    "AAPL": {
                        "ticker": "AAPL",
                        "ric": "AAPL.OQ",
                        "as_of_date": "2026-03-13",
                        "model_status": "core_estimated",
                        "model_status_reason": "",
                        "exposure_origin": "native",
                        "exposures": {"market": 1.0},
                    },
                    "SPY": {
                        "ticker": "SPY",
                        "ric": "SPY.P",
                        "as_of_date": "2026-03-13",
                        "model_status": "ineligible",
                        "model_status_reason": "projection_unavailable",
                        "exposure_origin": "projected",
                        "projection_asof": "2026-03-13",
                        "exposures": {},
                    },
                },
            }
        },
    )

    assert out["row_counts"]["cuse_security_membership_daily"] == 2
    assert out["row_counts"]["cuse_security_stage_results_daily"] == 22

    membership_rows = load_cuse_membership_rows(data_db=data_db, as_of_dates=["2026-03-13"])
    membership_by_ticker = {str(row["ticker"]): row for row in membership_rows}
    assert membership_by_ticker["AAPL"]["realized_role"] == "core_estimated"
    assert membership_by_ticker["AAPL"]["policy_path"] == "native_core_candidate"
    assert membership_by_ticker["AAPL"]["output_status"] == "served"
    assert int(membership_by_ticker["AAPL"]["served_exposure_available"]) == 1
    assert membership_by_ticker["SPY"]["realized_role"] == "ineligible"
    assert membership_by_ticker["SPY"]["policy_path"] == "returns_projection_candidate"
    assert membership_by_ticker["SPY"]["output_status"] == "projection_unavailable"
    assert membership_by_ticker["SPY"]["projection_candidate_status"] == "candidate"
    assert membership_by_ticker["SPY"]["projection_output_status"] == "unavailable"

    stage_rows = load_cuse_stage_result_rows(data_db=data_db, as_of_dates=["2026-03-13"])
    spy_stage_map = {
        str(row["stage_name"]): row
        for row in stage_rows
        if str(row["ric"]) == "SPY.P"
    }
    assert spy_stage_map["returns_projection_candidate"]["stage_state"] == "passed"
    assert spy_stage_map["served_output_available"]["stage_state"] == "failed"
    assert spy_stage_map["served_output_available"]["reason_code"] == "projection_unavailable"


def test_build_universe_ticker_loadings_overlays_persisted_membership_truth(tmp_path: Path) -> None:
    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute(
        """
        CREATE TABLE cuse_security_membership_daily (
            as_of_date TEXT NOT NULL,
            ric TEXT,
            ticker TEXT NOT NULL,
            policy_path TEXT NOT NULL,
            realized_role TEXT NOT NULL,
            output_status TEXT NOT NULL,
            projection_candidate_status TEXT NOT NULL,
            projection_output_status TEXT NOT NULL,
            reason_code TEXT,
            quality_label TEXT NOT NULL,
            source_snapshot_status TEXT NOT NULL,
            projection_method TEXT,
            projection_basis_status TEXT NOT NULL,
            projection_source_package_date TEXT,
            served_exposure_available INTEGER NOT NULL DEFAULT 0,
            run_id TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO cuse_security_membership_daily (
            as_of_date, ric, ticker, policy_path, realized_role, output_status,
            projection_candidate_status, projection_output_status, reason_code,
            quality_label, source_snapshot_status, projection_method,
            projection_basis_status, projection_source_package_date,
            served_exposure_available, run_id, updated_at
        ) VALUES (
            '2026-03-13', 'SPY.P', 'SPY', 'returns_projection_candidate', 'projected_returns', 'served',
            'candidate', 'available', 'returns_projection',
            'returns_projection', 'served_snapshot', 'ols_returns_regression',
            'available', '2026-03-13',
            1, 'run_1', '2026-03-16T00:01:00Z'
        )
        """
    )
    conn.commit()
    conn.close()

    out = build_universe_ticker_loadings(
        exposures_df=pd.DataFrame(),
        fundamentals_df=pd.DataFrame(),
        prices_df=pd.DataFrame([{"ric": "SPY.P", "ticker": "SPY", "close": 500.0}]),
        cov=pd.DataFrame(),
        data_db=data_db,
        projected_loadings={},
        projection_universe_rows=[{"ric": "SPY.P", "ticker": "SPY"}],
        projection_core_state_through_date="2026-03-13",
    )

    spy = out["by_ticker"]["SPY"]
    assert spy["model_status"] == "projected_only"
    assert spy["model_status_reason"] == "returns_projection"
    assert spy["cuse_realized_role"] == "projected_returns"
    assert spy["cuse_output_status"] == "served"
    assert spy["projection_basis_status"] == "available"
    assert spy["served_exposure_available"] is True


def test_build_positions_from_snapshot_preserves_persisted_membership_truth_without_exposures() -> None:
    positions, _ = build_positions_from_snapshot(
        universe_by_ticker={
            "SPY": {
                "ticker": "SPY",
                "name": "SPDR S&P 500 ETF Trust",
                "price": 510.0,
                "model_status": "projected_only",
                "model_status_reason": "returns_projection",
                "cuse_realized_role": "projected_returns",
                "cuse_output_status": "served",
                "cuse_reason_code": "returns_projection",
                "projection_candidate_status": "candidate",
                "projection_output_status": "available",
                "projection_basis_status": "available",
                "served_exposure_available": True,
                "exposure_origin": "projected",
                "exposures": {},
            }
        },
        shares_map={"SPY": 10.0},
    )

    assert positions[0]["model_status"] == "projected_only"
    assert positions[0]["model_status_reason"] == "returns_projection"
    assert positions[0]["cuse_realized_role"] == "projected_returns"
    assert positions[0]["cuse_output_status"] == "served"
    assert positions[0]["projection_output_status"] == "available"


def test_build_cuse_membership_payloads_use_runtime_state_by_row_as_of_date(tmp_path: Path) -> None:
    from backend.risk_model.cuse_membership import build_cuse_membership_payloads

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    ensure_cuse4_schema(conn)
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role, source, updated_at
        ) VALUES ('AAPL.OQ', 'AAPL', 1, 1, 'native_equity', 'seed', '2026-03-01T00:00:00+00:00')
        """
    )
    _seed_eligibility_inputs(
        conn,
        [
            {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "as_of_date": "2026-03-06",
                "market_cap": 100.0,
                "trbc_economic_sector": "Technology",
                "trbc_business_sector": "Technology Equipment",
                "hq_country_code": "US",
            },
            {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "as_of_date": "2026-03-13",
                "market_cap": 110.0,
                "trbc_economic_sector": "Technology",
                "trbc_business_sector": "Technology Equipment",
                "hq_country_code": "NL",
            },
        ],
    )
    _ensure_estu_membership_daily_table(conn)
    conn.executemany(
        """
        INSERT INTO estu_membership_daily (
            date, ric, estu_flag, drop_reason, drop_reason_detail, source, updated_at
        ) VALUES (?, 'AAPL.OQ', 1, '', '', 'test', '2026-03-13T00:00:00Z')
        """,
        [("2026-03-06",), ("2026-03-13",)],
    )
    conn.commit()
    conn.close()

    membership_payload, _stage_payload = build_cuse_membership_payloads(
        data_db=data_db,
        universe_payload={
            "by_ticker": {
                "AAPL_old": {
                    "ticker": "AAPL",
                    "ric": "AAPL.OQ",
                    "as_of_date": "2026-03-06",
                    "model_status": "core_estimated",
                    "model_status_reason": "",
                    "exposure_origin": "native",
                    "exposures": {"market": 1.0},
                },
                "AAPL_new": {
                    "ticker": "AAPL",
                    "ric": "AAPL.OQ",
                    "as_of_date": "2026-03-13",
                    "model_status": "projected_only",
                    "model_status_reason": "",
                    "exposure_origin": "native",
                    "exposures": {"market": 1.0},
                },
            }
        },
        risk_engine_state={"core_state_through_date": "2026-03-13"},
        run_id="run_1",
        updated_at="2026-03-16T00:01:00Z",
    )

    membership_by_date = {row[0]: row for row in membership_payload}
    assert membership_by_date["2026-03-06"][3] == "native_core_candidate"
    assert membership_by_date["2026-03-06"][4] == "core_estimated"
    assert membership_by_date["2026-03-13"][3] == "fundamental_projection_candidate"
    assert membership_by_date["2026-03-13"][4] == "projected_fundamental"


def test_build_cuse_membership_payloads_uses_eligibility_reason_for_structural_failure(tmp_path: Path) -> None:
    from backend.risk_model.cuse_membership import build_cuse_membership_payloads

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute(
        """
        CREATE TABLE security_master (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            classification_ok INTEGER NOT NULL DEFAULT 0,
            is_equity_eligible INTEGER NOT NULL DEFAULT 0,
            coverage_role TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role
        ) VALUES ('AAPL.OQ', 'AAPL', 1, 1, 'native_equity')
        """
    )
    _seed_eligibility_inputs(
        conn,
        [
            {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "as_of_date": "2026-03-13",
                "market_cap": 100.0,
                "trbc_economic_sector": "Technology",
                "trbc_business_sector": None,
                "hq_country_code": "US",
            }
        ],
    )
    _ensure_estu_membership_daily_table(conn)
    conn.commit()
    conn.close()

    membership_payload, stage_payload = build_cuse_membership_payloads(
        data_db=data_db,
        universe_payload={
            "by_ticker": {
                "AAPL": {
                    "ticker": "AAPL",
                    "ric": "AAPL.OQ",
                    "as_of_date": "2026-03-13",
                    "model_status": "core_estimated",
                    "model_status_reason": "",
                    "exposure_origin": "native",
                    "exposures": {"market": 1.0},
                }
            }
        },
        risk_engine_state={"core_state_through_date": "2026-03-13"},
        run_id="run_1",
        updated_at="2026-03-16T00:01:00Z",
    )

    assert membership_payload[0][8] == "missing_trbc_industry"
    stage_map = {row[2]: row for row in stage_payload if row[1] == "AAPL.OQ"}
    assert stage_map["structural_eligible"][3] == "failed"
    assert stage_map["structural_eligible"][4] == "missing_trbc_industry"


def test_build_cuse_membership_payloads_force_local_eligibility_reads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from backend.risk_model.cuse_membership import build_cuse_membership_payloads

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute(
        """
        CREATE TABLE security_master (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            classification_ok INTEGER NOT NULL DEFAULT 0,
            is_equity_eligible INTEGER NOT NULL DEFAULT 0,
            coverage_role TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role
        ) VALUES ('AAPL.OQ', 'AAPL', 1, 1, 'native_equity')
        """
    )
    _seed_eligibility_inputs(
        conn,
        [
            {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "as_of_date": "2026-03-13",
                "market_cap": 100.0,
                "trbc_economic_sector": "Technology",
                "trbc_business_sector": "Technology Equipment",
                "hq_country_code": "US",
            }
        ],
    )
    _ensure_estu_membership_daily_table(conn)
    conn.execute(
        """
        INSERT INTO estu_membership_daily (
            date, ric, estu_flag, drop_reason, drop_reason_detail, source, updated_at
        ) VALUES ('2026-03-13', 'AAPL.OQ', 1, '', '', 'test', '2026-03-13T00:00:00Z')
        """
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr("backend.risk_model.eligibility.core_backend.use_neon_core_reads", lambda: True)

    membership_payload, _stage_payload = build_cuse_membership_payloads(
        data_db=data_db,
        universe_payload={
            "by_ticker": {
                "AAPL": {
                    "ticker": "AAPL",
                    "ric": "AAPL.OQ",
                    "as_of_date": "2026-03-13",
                    "model_status": "core_estimated",
                    "model_status_reason": "",
                    "exposure_origin": "native",
                    "exposures": {"market": 1.0},
                }
            }
        },
        risk_engine_state={"core_state_through_date": "2026-03-13"},
        run_id="run_1",
        updated_at="2026-03-16T00:01:00Z",
    )

    assert membership_payload[0][3] == "native_core_candidate"


def test_build_cuse_membership_payloads_do_not_fallback_to_compat_when_registry_empty(
    tmp_path: Path,
) -> None:
    from backend.risk_model.cuse_membership import build_cuse_membership_payloads

    data_db = tmp_path / "data.db"
    conn = sqlite3.connect(str(data_db))
    conn.execute(
        """
        CREATE TABLE security_registry (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            isin TEXT,
            exchange_name TEXT,
            tracking_status TEXT,
            source TEXT,
            job_run_id TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE security_master (
            ric TEXT PRIMARY KEY,
            ticker TEXT,
            classification_ok INTEGER NOT NULL DEFAULT 0,
            is_equity_eligible INTEGER NOT NULL DEFAULT 0,
            coverage_role TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO security_master (
            ric, ticker, classification_ok, is_equity_eligible, coverage_role
        ) VALUES (
            'AAPL.OQ', 'AAPL', 1, 1, 'projection_only'
        )
        """
    )
    _seed_eligibility_inputs(
        conn,
        [
            {
                "ric": "AAPL.OQ",
                "ticker": "AAPL",
                "as_of_date": "2026-03-13",
                "market_cap": 100.0,
                "trbc_economic_sector": "Technology",
                "trbc_business_sector": "Technology Equipment",
                "hq_country_code": "US",
            },
        ],
    )
    _ensure_estu_membership_daily_table(conn)
    conn.execute(
        """
        INSERT INTO estu_membership_daily (
            date, ric, estu_flag, drop_reason, drop_reason_detail, source, updated_at
        ) VALUES ('2026-03-13', 'AAPL.OQ', 1, '', '', 'test', '2026-03-13T00:00:00Z')
        """
    )
    conn.commit()
    conn.close()

    membership_payload, _stage_payload = build_cuse_membership_payloads(
        data_db=data_db,
        universe_payload={
            "by_ticker": {
                "AAPL": {
                    "ticker": "AAPL",
                    "ric": "AAPL.OQ",
                    "as_of_date": "2026-03-13",
                    "model_status": "core_estimated",
                    "model_status_reason": "",
                    "exposure_origin": "native",
                    "exposures": {"market": 1.0},
                },
            }
        },
        risk_engine_state={"core_state_through_date": "2026-03-13"},
        run_id="run_compat_off",
        updated_at="2026-03-16T00:01:00Z",
    )

    assert membership_payload[0][3] == "native_core_candidate"
    assert membership_payload[0][4] == "core_estimated"
