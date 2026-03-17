from __future__ import annotations

from pathlib import Path

from backend.services import neon_stage2


def test_canonical_tables_include_durable_model_outputs() -> None:
    tables = neon_stage2.canonical_tables()

    assert "model_factor_returns_daily" in tables
    assert "model_factor_covariance_daily" in tables
    assert "model_specific_risk_daily" in tables
    assert "model_run_metadata" in tables


def test_canonical_schema_defines_durable_model_tables() -> None:
    schema_path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "migrations"
        / "neon"
        / "NEON_CANONICAL_SCHEMA.sql"
    )
    schema_sql = schema_path.read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS model_factor_returns_daily" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS model_factor_covariance_daily" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS model_specific_risk_daily" in schema_sql
    assert "CREATE TABLE IF NOT EXISTS model_run_metadata" in schema_sql
    assert "ADD COLUMN IF NOT EXISTS run_id TEXT" in schema_sql
