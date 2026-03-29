"""Canonical source-read facade for the cUSE dashboard."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from backend import config
from backend.data import core_read_backend as core_backend, source_dates, source_reads

DATA_DB = Path(config.DATA_DB_PATH)
logger = logging.getLogger(__name__)


def _use_neon_core_reads() -> bool:
    return core_backend.use_neon_core_reads()


def core_read_backend_name() -> str:
    return core_backend.core_read_backend_name()


def core_read_backend(backend: str):
    return core_backend.core_read_backend(backend)


def neon_core_read_session():
    return core_backend.neon_core_read_session()


def _to_pg_sql(query: str) -> str:
    return core_backend.to_pg_sql(query)


def _resolve_data_db(data_db: Path | None = None) -> Path:
    return Path(data_db or DATA_DB).expanduser().resolve()


def _fetch_rows(
    sql: str,
    params: list[Any] | None = None,
    *,
    data_db: Path | None = None,
) -> list[dict[str, Any]]:
    return core_backend.fetch_rows(
        sql,
        params,
        data_db=_resolve_data_db(data_db),
        neon_enabled=_use_neon_core_reads(),
    )


def _table_exists(table: str, *, data_db: Path | None = None) -> bool:
    return core_backend.table_exists(
        table,
        fetch_rows_fn=lambda sql, params=None: _fetch_rows(sql, params, data_db=data_db),
        neon_enabled=_use_neon_core_reads(),
    )


def _missing_tables(*tables: str, data_db: Path | None = None) -> list[str]:
    return core_backend.missing_tables(
        *tables,
        table_exists_fn=lambda table: _table_exists(table, data_db=data_db),
    )


def _load_latest_prices_sqlite(
    tickers: list[str] | None = None,
    *,
    data_db: Path | None = None,
) -> pd.DataFrame:
    return source_reads.load_latest_prices_sqlite(
        data_db=_resolve_data_db(data_db),
        tickers=tickers,
        missing_tables_fn=lambda *tables: _missing_tables(*tables, data_db=data_db),
    )


def _resolve_latest_model_tuple(*, data_db: Path | None = None) -> dict[str, str] | None:
    return source_dates.resolve_latest_model_tuple(
        fetch_rows_fn=lambda sql, params=None: _fetch_rows(sql, params, data_db=data_db),
        exposure_source_table_required_fn=lambda: _exposure_source_table_required(data_db=data_db),
    )


def _exposure_source_table_required(*, data_db: Path | None = None) -> str:
    return source_reads.exposure_source_table_required(
        table_exists_fn=lambda table: _table_exists(table, data_db=data_db),
    )


def _resolve_latest_well_covered_exposure_asof(table: str, *, data_db: Path | None = None) -> str | None:
    return source_reads.resolve_latest_well_covered_exposure_asof(
        table,
        fetch_rows_fn=lambda sql, params=None: _fetch_rows(sql, params, data_db=data_db),
    )


def load_raw_cross_section_latest(
    tickers: list[str] | None = None,
    *,
    data_db: Path | None = None,
) -> pd.DataFrame:
    with core_backend.neon_core_read_session():
        return source_reads.load_raw_cross_section_latest(
            tickers=tickers,
            fetch_rows_fn=lambda sql, params=None: _fetch_rows(sql, params, data_db=data_db),
            exposure_source_table_required_fn=lambda: _exposure_source_table_required(data_db=data_db),
            resolve_latest_well_covered_exposure_asof_fn=(
                lambda table: _resolve_latest_well_covered_exposure_asof(table, data_db=data_db)
            ),
        )


def load_latest_fundamentals(
    tickers: list[str] | None = None,
    as_of_date: str | None = None,
    *,
    data_db: Path | None = None,
) -> pd.DataFrame:
    with core_backend.neon_core_read_session():
        return source_reads.load_latest_fundamentals(
            tickers=tickers,
            as_of_date=as_of_date,
            fetch_rows_fn=lambda sql, params=None: _fetch_rows(sql, params, data_db=data_db),
            missing_tables_fn=lambda *tables: _missing_tables(*tables, data_db=data_db),
        )


def load_latest_prices(
    tickers: list[str] | None = None,
    *,
    data_db: Path | None = None,
) -> pd.DataFrame:
    if not _use_neon_core_reads():
        return _load_latest_prices_sqlite(tickers, data_db=data_db)
    with core_backend.neon_core_read_session():
        return source_reads.load_latest_prices(
            tickers=tickers,
            fetch_rows_fn=lambda sql, params=None: _fetch_rows(sql, params, data_db=data_db),
            missing_tables_fn=lambda *tables: _missing_tables(*tables, data_db=data_db),
        )


def load_source_dates(*, data_db: Path | None = None) -> dict[str, str | None]:
    with core_backend.neon_core_read_session():
        return source_dates.load_source_dates(
            fetch_rows_fn=lambda sql, params=None: _fetch_rows(sql, params, data_db=data_db),
            table_exists_fn=lambda table: _table_exists(table, data_db=data_db),
            exposure_source_table_required_fn=lambda: _exposure_source_table_required(data_db=data_db),
        )
