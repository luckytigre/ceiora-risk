"""Unit tests for OLS projection math with synthetic data."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

import backend.risk_model.projected_loadings as projected_loadings_module
from backend.risk_model.projected_loadings import (
    ProjectedLoadingResult,
    _run_ols,
    compute_projected_loadings,
    latest_persisted_projection_asof,
    load_persisted_projected_loadings,
)


def _create_synthetic_factor_returns(
    n_days: int = 300,
    factors: list[str] | None = None,
    seed: int = 42,
) -> pd.DataFrame:
    """Create synthetic daily factor returns."""
    rng = np.random.default_rng(seed)
    if factors is None:
        factors = ["Market", "Size", "Value"]
    dates = pd.bdate_range("2024-01-02", periods=n_days, freq="B")
    data = rng.normal(0.0, 0.01, size=(n_days, len(factors)))
    df = pd.DataFrame(data, index=[str(d.date()) for d in dates], columns=factors)
    return df


def _create_synthetic_instrument_returns(
    factor_returns: pd.DataFrame,
    betas: dict[str, float],
    noise_std: float = 0.005,
    seed: int = 123,
) -> pd.Series:
    """Create instrument returns = sum(beta_k * factor_k) + noise."""
    rng = np.random.default_rng(seed)
    dates = factor_returns.index
    y = np.zeros(len(dates))
    for factor, beta in betas.items():
        y += beta * factor_returns[factor].to_numpy()
    y += rng.normal(0.0, noise_std, size=len(dates))
    return pd.Series(y, index=dates)


class TestRunOLS:
    def test_recovers_known_betas(self):
        """OLS should recover known factor loadings from synthetic data."""
        factor_returns = _create_synthetic_factor_returns(n_days=300)
        true_betas = {"Market": 1.0, "Size": 0.3, "Value": -0.2}
        instrument_returns = _create_synthetic_instrument_returns(
            factor_returns, true_betas, noise_std=0.001
        )

        result = _run_ols(
            instrument_returns,
            factor_returns,
            core_state_through_date=str(factor_returns.index[-1]),
            lookback_days=300,
            min_obs=60,
        )

        assert result is not None
        assert result.status == "ok"
        assert result.obs_count >= 60

        # Betas should be close to true values
        for factor, true_beta in true_betas.items():
            estimated = result.exposures[factor]
            assert abs(estimated - true_beta) < 0.15, (
                f"{factor}: expected ~{true_beta}, got {estimated}"
            )

        # R-squared should be very high with low noise
        assert result.r_squared > 0.90

    def test_insufficient_data_returns_none(self):
        """Should return None when fewer observations than min_obs."""
        factor_returns = _create_synthetic_factor_returns(n_days=30)
        instrument_returns = _create_synthetic_instrument_returns(
            factor_returns, {"Market": 1.0, "Size": 0.0, "Value": 0.0}
        )
        result = _run_ols(
            instrument_returns,
            factor_returns,
            core_state_through_date=str(factor_returns.index[-1]),
            lookback_days=300,
            min_obs=60,
        )
        assert result is None

    def test_lookback_trims_observations(self):
        """Lookback should limit the number of observations used."""
        factor_returns = _create_synthetic_factor_returns(n_days=300)
        instrument_returns = _create_synthetic_instrument_returns(
            factor_returns, {"Market": 1.0, "Size": 0.0, "Value": 0.0}
        )
        result = _run_ols(
            instrument_returns,
            factor_returns,
            core_state_through_date=str(factor_returns.index[-1]),
            lookback_days=100,
            min_obs=60,
        )
        assert result is not None
        assert result.obs_count <= 100

    def test_specific_risk_is_annualized(self):
        """Specific variance should be annualized (multiplied by 252)."""
        factor_returns = _create_synthetic_factor_returns(n_days=300)
        instrument_returns = _create_synthetic_instrument_returns(
            factor_returns,
            {"Market": 1.0, "Size": 0.0, "Value": 0.0},
            noise_std=0.01,
        )
        result = _run_ols(
            instrument_returns,
            factor_returns,
            core_state_through_date=str(factor_returns.index[-1]),
            lookback_days=300,
            min_obs=60,
        )
        assert result is not None
        # Daily variance ~0.01^2 = 0.0001, annualized ~0.0252
        assert result.specific_var > 0.0
        assert result.specific_vol > 0.0
        assert abs(result.specific_vol - np.sqrt(result.specific_var)) < 1e-6


class TestComputeProjectedLoadings:
    def _setup_dbs(self, tmp_path: Path):
        """Create data.db with durable factor returns and ETF price history."""
        data_db = tmp_path / "data.db"

        # Create factor returns in durable model outputs.
        factor_returns = _create_synthetic_factor_returns(n_days=300)
        conn_data = sqlite3.connect(str(data_db))
        conn_data.execute("""
            CREATE TABLE IF NOT EXISTS model_factor_returns_daily (
                date TEXT NOT NULL,
                factor_name TEXT NOT NULL,
                factor_return REAL NOT NULL,
                robust_se REAL NOT NULL DEFAULT 0.0,
                t_stat REAL NOT NULL DEFAULT 0.0,
                r_squared REAL NOT NULL DEFAULT 0.0,
                residual_vol REAL NOT NULL DEFAULT 0.0,
                cross_section_n INTEGER NOT NULL DEFAULT 0,
                eligible_n INTEGER NOT NULL DEFAULT 0,
                coverage REAL NOT NULL DEFAULT 0.0,
                run_id TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (date, factor_name)
            )
        """)
        rows = []
        for date_str, row in factor_returns.iterrows():
            for factor in factor_returns.columns:
                rows.append((str(date_str), factor, float(row[factor])))
        conn_data.executemany(
            "INSERT INTO model_factor_returns_daily (date, factor_name, factor_return, run_id, updated_at) VALUES (?, ?, ?, 'run_1', '2024-01-01T00:00:00Z')",
            rows,
        )
        conn_data.commit()

        # Create price history in data.db for a synthetic ETF
        conn_data.execute("""
            CREATE TABLE IF NOT EXISTS security_prices_eod (
                ric TEXT NOT NULL,
                date TEXT NOT NULL,
                close REAL,
                open REAL, high REAL, low REAL, adj_close REAL,
                volume REAL, currency TEXT, source TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (ric, date)
            )
        """)
        # Generate prices from known betas
        true_betas = {"Market": 1.0, "Size": 0.3, "Value": -0.2}
        instrument_returns = _create_synthetic_instrument_returns(
            factor_returns, true_betas, noise_std=0.001
        )
        price = 100.0
        price_rows = []
        for date_str, ret in instrument_returns.items():
            price *= 1.0 + ret
            price_rows.append(("SPY.P", str(date_str), price, "2024-01-01T00:00:00Z"))
        conn_data.executemany(
            "INSERT INTO security_prices_eod (ric, date, close, updated_at) VALUES (?, ?, ?, ?)",
            price_rows,
        )
        conn_data.commit()
        conn_data.close()

        return data_db, str(factor_returns.index[-1]), true_betas

    def test_end_to_end_projection(self, tmp_path):
        """Full pipeline should recover known betas and persist results."""
        data_db, core_state_through_date, true_betas = self._setup_dbs(tmp_path)

        results = compute_projected_loadings(
            data_db=data_db,
            projection_rics=[{"ric": "SPY.P", "ticker": "SPY"}],
            core_state_through_date=core_state_through_date,
            lookback_days=300,
            min_obs=60,
        )

        assert "SPY" in results
        spy = results["SPY"]
        assert spy.status == "ok"
        assert spy.ric == "SPY.P"
        assert spy.ticker == "SPY"
        assert spy.projection_asof == core_state_through_date

        for factor, true_beta in true_betas.items():
            estimated = spy.exposures[factor]
            assert abs(estimated - true_beta) < 0.15

        assert spy.r_squared > 0.90

        # Verify persistence
        conn = sqlite3.connect(str(data_db))
        loadings = conn.execute(
            "SELECT factor_name, exposure FROM projected_instrument_loadings WHERE ric = 'SPY.P'"
        ).fetchall()
        meta = conn.execute(
            "SELECT projection_method, r_squared FROM projected_instrument_meta WHERE ric = 'SPY.P'"
        ).fetchall()
        conn.close()

        assert len(loadings) == 3
        assert len(meta) == 1
        assert meta[0][0] == "ols_returns_regression"

    def test_missing_prices_returns_insufficient_data(self, tmp_path):
        """Instruments without price history should get insufficient_data status."""
        data_db = tmp_path / "data.db"

        # Create durable factor returns.
        factor_returns = _create_synthetic_factor_returns(n_days=100)
        conn_data = sqlite3.connect(str(data_db))
        conn_data.execute("""
            CREATE TABLE IF NOT EXISTS model_factor_returns_daily (
                date TEXT NOT NULL,
                factor_name TEXT NOT NULL,
                factor_return REAL NOT NULL,
                run_id TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (date, factor_name)
            )
        """)
        rows = []
        for date_str, row in factor_returns.iterrows():
            for factor in factor_returns.columns:
                rows.append((str(date_str), factor, float(row[factor])))
        conn_data.executemany(
            "INSERT INTO model_factor_returns_daily (date, factor_name, factor_return, run_id, updated_at) VALUES (?, ?, ?, 'run_1', '2024-01-01T00:00:00Z')",
            rows,
        )

        # Create empty data.db
        conn_data.execute("""
            CREATE TABLE IF NOT EXISTS security_prices_eod (
                ric TEXT NOT NULL, date TEXT NOT NULL, close REAL,
                updated_at TEXT NOT NULL, PRIMARY KEY (ric, date)
            )
        """)
        conn_data.commit()
        conn_data.close()

        results = compute_projected_loadings(
            data_db=data_db,
            projection_rics=[{"ric": "FAKE.P", "ticker": "FAKE"}],
            core_state_through_date=str(factor_returns.index[-1]),
        )

        assert "FAKE" in results
        assert results["FAKE"].status == "insufficient_data"

    def test_empty_projection_rics_returns_empty(self, tmp_path):
        """Empty projection list should return empty dict."""
        results = compute_projected_loadings(
            data_db=tmp_path / "data.db",
            projection_rics=[],
            core_state_through_date="2024-12-31",
        )
        assert results == {}

    def test_persisted_loadings_are_loaded_by_core_state_date(self, tmp_path):
        data_db, core_state_through_date, _ = self._setup_dbs(tmp_path)
        compute_projected_loadings(
            data_db=data_db,
            projection_rics=[{"ric": "SPY.P", "ticker": "SPY"}],
            core_state_through_date=core_state_through_date,
        )

        loaded = load_persisted_projected_loadings(
            data_db=data_db,
            projection_rics=[{"ric": "SPY.P", "ticker": "SPY"}],
            as_of_date=core_state_through_date,
        )

        assert "SPY" in loaded
        assert loaded["SPY"].projection_asof == core_state_through_date

    def test_recompute_clears_stale_persisted_projection_rows_for_same_core_date(self, tmp_path):
        data_db, core_state_through_date, _ = self._setup_dbs(tmp_path)
        projection_rics = [{"ric": "SPY.P", "ticker": "SPY"}]

        compute_projected_loadings(
            data_db=data_db,
            projection_rics=projection_rics,
            core_state_through_date=core_state_through_date,
        )

        conn = sqlite3.connect(str(data_db))
        conn.execute("DELETE FROM security_prices_eod WHERE ric = 'SPY.P'")
        conn.commit()
        conn.close()

        compute_projected_loadings(
            data_db=data_db,
            projection_rics=projection_rics,
            core_state_through_date=core_state_through_date,
        )

        loaded = load_persisted_projected_loadings(
            data_db=data_db,
            projection_rics=projection_rics,
            as_of_date=core_state_through_date,
        )

        assert loaded == {}

    def test_compute_projected_loadings_persists_to_neon_authority_when_enabled(self, monkeypatch, tmp_path):
        data_db, core_state_through_date, _ = self._setup_dbs(tmp_path)

        executed_many: list[tuple[str, list[tuple[object, ...]]]] = []
        executed: list[tuple[str, tuple[object, ...]]] = []

        class _Cursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, query, params=None):
                executed.append((str(query), tuple(params or ())))

            def executemany(self, query, rows):
                executed_many.append((str(query), [tuple(row) for row in rows]))

        class _Conn:
            def __init__(self):
                self.committed = False

            def cursor(self):
                return _Cursor()

            def commit(self):
                self.committed = True

            def close(self):
                return None

        pg_conn = _Conn()
        monkeypatch.setattr(projected_loadings_module, "_neon_projection_reads_enabled", lambda: True)
        monkeypatch.setattr(projected_loadings_module, "connect", lambda **_kwargs: pg_conn)
        monkeypatch.setattr(projected_loadings_module, "resolve_dsn", lambda _dsn: "postgresql://example")
        monkeypatch.setattr("backend.data.model_output_writers.ensure_postgres_schema", lambda _conn: None)

        results = compute_projected_loadings(
            data_db=data_db,
            projection_rics=[{"ric": "SPY.P", "ticker": "SPY"}],
            core_state_through_date=core_state_through_date,
            lookback_days=300,
            min_obs=60,
        )

        assert results["SPY"].status == "ok"
        assert pg_conn.committed is True
        assert len(executed) == 2
        assert "DELETE FROM projected_instrument_loadings" in executed[0][0]
        assert executed[0][1][0] == core_state_through_date
        assert len(executed_many) == 2
        assert "INSERT INTO projected_instrument_loadings" in executed_many[0][0]
        assert "INSERT INTO projected_instrument_meta" in executed_many[1][0]
        assert len(executed_many[0][1]) == 3
        assert executed_many[1][1][0][0] == "SPY.P"
        assert executed_many[1][1][0][1] == core_state_through_date

    def test_load_persisted_projected_loadings_prefers_neon_authority_when_enabled(self, monkeypatch):
        class _Cursor:
            def __init__(self):
                self.rows = [
                    ("SPY.P", "SPY", "Market", 1.0, 252, 180, 0.97, 0.01, 0.1),
                    ("SPY.P", "SPY", "Beta", 0.2, 252, 180, 0.97, 0.01, 0.1),
                ]

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, _query, _params):
                return None

            def fetchall(self):
                return list(self.rows)

        class _Conn:
            def cursor(self):
                return _Cursor()

            def close(self):
                return None

        monkeypatch.setattr("backend.risk_model.projected_loadings._neon_projection_reads_enabled", lambda: True)
        monkeypatch.setattr("backend.risk_model.projected_loadings.connect", lambda **_kwargs: _Conn())
        monkeypatch.setattr("backend.risk_model.projected_loadings.resolve_dsn", lambda _dsn: "postgresql://example")

        loaded = load_persisted_projected_loadings(
            data_db=Path("/tmp/unused.db"),
            projection_rics=[{"ric": "SPY.P", "ticker": "SPY"}],
            as_of_date="2026-03-26",
        )

        assert sorted(loaded["SPY"].exposures.items()) == [("Beta", 0.2), ("Market", 1.0)]
        assert loaded["SPY"].projection_asof == "2026-03-26"
        assert loaded["SPY"].obs_count == 180

    def test_latest_persisted_projection_asof_prefers_neon_authority_when_enabled(self, monkeypatch):
        class _Cursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, _query, _params):
                return None

            def fetchone(self):
                return ("2026-03-26",)

        class _Conn:
            def cursor(self):
                return _Cursor()

            def close(self):
                return None

        monkeypatch.setattr("backend.risk_model.projected_loadings._neon_projection_reads_enabled", lambda: True)
        monkeypatch.setattr("backend.risk_model.projected_loadings.connect", lambda **_kwargs: _Conn())
        monkeypatch.setattr("backend.risk_model.projected_loadings.resolve_dsn", lambda _dsn: "postgresql://example")

        latest = latest_persisted_projection_asof(
            data_db=Path("/tmp/unused.db"),
            projection_rics=[{"ric": "SPY.P", "ticker": "SPY"}],
        )

        assert latest == "2026-03-26"
