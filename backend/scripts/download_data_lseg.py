"""Canonical LSEG ingest: direct writes into RIC-keyed source-of-truth tables.

Volume policy:
- Ingest writes `security_prices_eod.volume` from `TR.Volume`.
- Historical volume-repair runs use the same metric via
  `backfill_prices_range_lseg.py --volume-only`.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


from backend.universe.schema import (
    FUNDAMENTALS_HISTORY_TABLE,
    PRICES_TABLE,
    TRBC_HISTORY_TABLE,
    ensure_cuse4_schema,
)
from backend.universe.runtime_rows import load_security_runtime_map, load_security_runtime_rows
from backend.universe.registry_sync import reconcile_default_security_policy_rows
from backend.universe.source_observation import refresh_security_source_observation_daily
from backend.universe.security_master_sync import (
    derive_security_master_flags,
    load_default_source_universe_rows,
    load_price_ingest_universe_rows,
    ticker_from_ric,
    upsert_security_master_rows,
)
from backend.universe.taxonomy_builder import (
    materialize_security_master_compat_current,
    refresh_security_taxonomy_current,
)
from backend.trading_calendar import previous_or_same_xnys_session

_DB_RAW = Path(os.getenv("DATA_DB_PATH", "data.db")).expanduser()
DEFAULT_DB = _DB_RAW if _DB_RAW.is_absolute() else (Path(__file__).resolve().parent.parent / _DB_RAW)
LSEG_BATCH_SIZE = max(1, int(os.getenv("LSEG_BATCH_SIZE", "500")))
SQLITE_TIMEOUT_SECONDS = 120
SQLITE_BUSY_TIMEOUT_MS = 120000
SQLITE_MAX_RETRIES = 6
SQLITE_RETRY_SLEEP_SECONDS = 2.0
PRICE_VOLUME_FIELD = "TR.Volume"

LSEG_FIELDS_ALL = [
    "TR.TickerSymbol",
    "TR.ISIN",
    "TR.ExchangeName",
    "TR.CommonName",
    "TR.TRBCEconomicSector",
    "TR.TRBCBusinessSector",
    "TR.TRBCIndustryGroup",
    "TR.TRBCIndustry",
    "TR.TRBCActivity",
    "TR.HQCountryCode",
    "TR.PriceOpen",
    "TR.PriceHigh",
    "TR.PriceLow",
    "TR.PriceClose",
    PRICE_VOLUME_FIELD,
    "TR.PriceClose.currency",
    "TR.CompanyMarketCap",
    "TR.SharesOutstanding",
    "TR.DividendYield",
    "TR.BookValuePerShare",
    "TR.EPSMean",
    "TR.EPSActValue",
    "TR.TotalDebt",
    "TR.CashAndEquivalents",
    "TR.LongTermDebt",
    "TR.FreeCashFlow",
    "TR.GrossProfit",
    "TR.NetIncome",
    "TR.CashFromOperatingActivities",
    "TR.CapitalExpenditures",
    "TR.BasicWeightedAverageShares",
    "TR.DilutedWeightedAverageShares",
    "TR.FreeFloat",
    "TR.FreeFloatPct",
    "TR.Revenue",
    "TR.Revenue.fperiod",
    "TR.Revenue.currency",
    "TR.Revenue.periodenddate",
    "TR.EBITDA",
    "TR.EBIT",
    "TR.TotalAssets",
    "TR.TotalLiabilities",
    "TR.ROEPercent",
    "TR.OperatingMarginPercent",
]

FUNDAMENTALS_FIELD_SET = {
    "TR.CommonName",
    "TR.CompanyMarketCap",
    "TR.SharesOutstanding",
    "TR.DividendYield",
    "TR.BookValuePerShare",
    "TR.EPSMean",
    "TR.EPSActValue",
    "TR.TotalDebt",
    "TR.CashAndEquivalents",
    "TR.LongTermDebt",
    "TR.CashFromOperatingActivities",
    "TR.CapitalExpenditures",
    "TR.Revenue",
    "TR.Revenue.fperiod",
    "TR.Revenue.currency",
    "TR.Revenue.periodenddate",
    "TR.EBITDA",
    "TR.EBIT",
    "TR.TotalAssets",
    "TR.ROEPercent",
    "TR.OperatingMarginPercent",
}

CLASSIFICATION_FIELD_SET = {
    "TR.TRBCEconomicSector",
    "TR.TRBCBusinessSector",
    "TR.TRBCIndustryGroup",
    "TR.TRBCIndustry",
    "TR.TRBCActivity",
    "TR.HQCountryCode",
}

PRICE_FIELD_SET = {
    "TR.PriceOpen",
    "TR.PriceHigh",
    "TR.PriceLow",
    "TR.PriceClose",
    PRICE_VOLUME_FIELD,
    "TR.PriceClose.currency",
}

SECURITY_MASTER_FIELD_SET = {
    "TR.TickerSymbol",
    "TR.ISIN",
    "TR.ExchangeName",
    *CLASSIFICATION_FIELD_SET,
}


def _select_lseg_fields(
    *,
    write_fundamentals: bool,
    write_prices: bool,
    write_classification: bool,
) -> list[str]:
    if not any((write_fundamentals, write_prices, write_classification)):
        return []
    wanted: set[str] = set(SECURITY_MASTER_FIELD_SET)
    if write_fundamentals:
        wanted.update(FUNDAMENTALS_FIELD_SET)
    if write_prices:
        wanted.update(PRICE_FIELD_SET)
    if write_classification:
        wanted.update(CLASSIFICATION_FIELD_SET)
    if not wanted:
        return []
    return [f for f in LSEG_FIELDS_ALL if f in wanted]


def _load_lseg_client():
    try:
        from backend.vendor.lseg_toolkit import LsegClient
    except Exception as exc:
        raise RuntimeError(
            "Unable to import lseg_toolkit/LSEG runtime. "
            "Ensure vendored toolkit is present and `lseg-data` is installed."
        ) from exc
    return LsegClient


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        got = cols.get(c.lower())
        if got:
            return got
    return None


def _iso_date(value: Any) -> str | None:
    if isinstance(value, pd.Series):
        for item in value.tolist():
            if item is None or pd.isna(item):
                continue
            value = item
            break
        else:
            return None
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return str(value.date())
    s = str(value).strip()
    if not s or s.lower() in {"nan", "nat", "none"}:
        return None
    return s


def _float_or_none(value: Any) -> float | None:
    if isinstance(value, pd.Series):
        for item in value.tolist():
            if item is None or pd.isna(item):
                continue
            value = item
            break
        else:
            return None
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _parse_fiscal_period(value: Any) -> tuple[int | None, str | None, str | None]:
    if value is None or pd.isna(value):
        return None, None, None
    s = str(value).strip().upper()
    if not s:
        return None, None, None

    m_fy = re.match(r"^FY\D*(\d{4})$", s)
    if m_fy:
        return int(m_fy.group(1)), None, "FY"

    m_fq = re.match(r"^F?Q([1-4])\D*(\d{4})$", s)
    if m_fq:
        return int(m_fq.group(2)), f"Q{m_fq.group(1)}", "FQ"

    m_year = re.search(r"(\d{4})", s)
    if m_year:
        return int(m_year.group(1)), None, None
    return None, None, None


def _connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=SQLITE_TIMEOUT_SECONDS)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    return conn


def _existing_cols(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [str(r[1]) for r in cur.fetchall()]


def _insert_rows(
    conn: sqlite3.Connection,
    table: str,
    rows: list[dict[str, Any]],
    *,
    replace: bool = False,
) -> int:
    if not rows:
        return 0
    cols = _existing_cols(conn, table)
    use_cols = [c for c in cols if c in rows[0]]
    if not use_cols:
        return 0
    placeholders = ",".join("?" for _ in use_cols)
    insert_kw = "INSERT OR REPLACE" if replace else "INSERT"
    sql = f'{insert_kw} INTO {table} ({",".join(use_cols)}) VALUES ({placeholders})'
    payload = [tuple(r.get(c) for c in use_cols) for r in rows]
    for attempt in range(SQLITE_MAX_RETRIES):
        try:
            conn.executemany(sql, payload)
            break
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt + 1 >= SQLITE_MAX_RETRIES:
                raise
            time.sleep(SQLITE_RETRY_SLEEP_SECONDS * (attempt + 1))
    return len(rows)


def _to_local_ticker(ric: str) -> str:
    base = str(ric or "").strip().upper()
    if not base:
        return base
    return base.split(".", 1)[0]


def _load_runtime_ingest_scope(
    conn: sqlite3.Connection,
    *,
    tickers: list[str] | None,
    rics: list[str] | None,
    write_fundamentals: bool,
    write_prices: bool,
    write_classification: bool,
) -> list[dict[str, str]]:
    explicit_request = bool(tickers or rics)
    if not explicit_request:
        rows_by_ric: dict[str, dict[str, str]] = {}
        if write_prices:
            for row in load_price_ingest_universe_rows(conn, include_pending_seed=True):
                ric = str(row.get("ric") or "").strip().upper()
                ticker = str(row.get("ticker") or "").strip().upper()
                if ric and ticker:
                    rows_by_ric[ric] = {"ticker": ticker, "ric": ric}
        if write_fundamentals or write_classification:
            for row in load_default_source_universe_rows(conn, include_pending_seed=True):
                ric = str(row.get("ric") or "").strip().upper()
                ticker = str(row.get("ticker") or "").strip().upper()
                if ric and ticker:
                    rows_by_ric[ric] = {"ticker": ticker, "ric": ric}
        return sorted(rows_by_ric.values(), key=lambda row: (row["ticker"], row["ric"]))
    rows = load_security_runtime_rows(
        conn,
        tickers=[str(t).strip().upper() for t in (tickers or [])],
        rics=[str(r).strip().upper() for r in (rics or [])],
        include_disabled=False,
    )
    out: list[dict[str, str]] = []
    for row in sorted(rows, key=lambda row: (str(row.get("ticker") or ""), str(row.get("ric") or ""))):
        ticker = str(row.get("ticker") or "").strip().upper()
        ric = str(row.get("ric") or "").strip().upper()
        if not ticker or not ric:
            continue
        pending_seed_structural = (
            int(row.get("classification_ready") or 0) == 0
            and str(row.get("source") or "").strip().lower().endswith("_seed")
            and int(row.get("allow_cuse_returns_projection") or 0) != 1
        )
        allow_price = int(row.get("price_ingest_enabled") or 0) == 1
        allow_fundamentals = int(row.get("pit_fundamentals_enabled") or 0) == 1 or pending_seed_structural
        allow_classification = int(row.get("pit_classification_enabled") or 0) == 1 or pending_seed_structural
        if not (
            (write_prices and allow_price)
            or (write_fundamentals and allow_fundamentals)
            or (write_classification and allow_classification)
        ):
            continue
        out.append({"ticker": ticker, "ric": ric})
    return out


def _resolve_requested_tickers(
    *,
    tickers_csv: str | None,
    index: str | None,
) -> list[str] | None:
    if tickers_csv:
        return [t.strip().upper() for t in tickers_csv.split(",") if t.strip()]
    if not index:
        return None
    LsegClient = _load_lseg_client()
    with LsegClient() as client:
        rics = client.get_index_constituents(index=index)
    return [_to_local_ticker(str(r)) for r in rics if str(r).strip()]


def download_from_lseg(
    *,
    db_path: Path = DEFAULT_DB,
    index: str | None = None,
    tickers_csv: str | None = None,
    rics_csv: str | None = None,
    as_of_date: str | None = None,
    shard_count: int = 1,
    shard_index: int = 0,
    write_fundamentals: bool = True,
    write_prices: bool = True,
    write_classification: bool = True,
) -> dict[str, Any]:
    LsegClient = _load_lseg_client()

    raw_as_of = str(as_of_date or datetime.now(timezone.utc).date().isoformat())
    as_of = previous_or_same_xnys_session(raw_as_of)
    updated_at = datetime.now(timezone.utc).isoformat()
    selected_fields = _select_lseg_fields(
        write_fundamentals=bool(write_fundamentals),
        write_prices=bool(write_prices),
        write_classification=bool(write_classification),
    )
    if not selected_fields:
        return {
            "status": "no-op",
            "as_of": as_of,
            "reason": "all_write_targets_skipped",
            "shard_index": int(shard_index),
            "shard_count": int(shard_count),
        }

    conn = _connect_db(db_path)
    ensure_cuse4_schema(conn)

    requested_tickers = _resolve_requested_tickers(tickers_csv=tickers_csv, index=index)
    requested_rics = (
        [r.strip().upper() for r in str(rics_csv).split(",") if str(r).strip()]
        if rics_csv
        else None
    )
    requested_ticker_set = set(requested_tickers or [])
    requested_ric_set = set(requested_rics or [])
    universe_rows = _load_runtime_ingest_scope(
        conn,
        tickers=requested_tickers,
        rics=requested_rics,
        write_fundamentals=bool(write_fundamentals),
        write_prices=bool(write_prices),
        write_classification=bool(write_classification),
    )
    available_ticker_set = {str(row.get("ticker") or "").strip().upper() for row in universe_rows if row.get("ticker")}
    available_ric_set = {str(row.get("ric") or "").strip().upper() for row in universe_rows if row.get("ric")}
    matched_ticker_set = available_ticker_set & requested_ticker_set
    matched_ric_set = available_ric_set & requested_ric_set
    missing_requested_tickers = sorted(requested_ticker_set - matched_ticker_set)
    missing_requested_rics = sorted(requested_ric_set - matched_ric_set)

    shard_count = max(1, int(shard_count))
    shard_index = int(shard_index)
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError(f"shard_index must be in [0, {shard_count - 1}]")
    if shard_count > 1:
        universe_rows = [
            r
            for r in universe_rows
            if int(hashlib.md5(r["ticker"].encode("utf-8")).hexdigest(), 16) % shard_count == shard_index
        ]

    if not universe_rows:
        conn.close()
        return {
            "status": "no-universe",
            "as_of": as_of,
            "shard_index": int(shard_index),
            "shard_count": int(shard_count),
            "requested_ticker_count": int(len(requested_ticker_set)),
            "requested_ric_count": int(len(requested_ric_set)),
            "matched_requested_ticker_count": int(len(matched_ticker_set)),
            "matched_requested_ric_count": int(len(matched_ric_set)),
            "missing_requested_tickers": missing_requested_tickers,
            "missing_requested_rics": missing_requested_rics,
            "requires_seeded_runtime_universe": bool(requested_ticker_set or requested_ric_set),
            "requires_seeded_security_master": bool(requested_ticker_set or requested_ric_set),
        }

    ric_universe = sorted({r["ric"] for r in universe_rows})
    runtime_policy_by_ric = load_security_runtime_map(
        conn,
        rics=ric_universe,
        as_of_date=as_of,
        include_disabled=True,
    )

    print(f"Fetching LSEG data for {len(ric_universe)} instruments...")
    company_parts: list[pd.DataFrame] = []
    bad_instruments = 0

    with LsegClient() as client:

        def _fetch_company_data_robust(batch: list[str]) -> tuple[pd.DataFrame, int]:
            if not batch:
                return pd.DataFrame(), 0
            try:
                part = client.get_company_data(batch, fields=selected_fields, as_of_date=as_of)
                return (part if part is not None else pd.DataFrame(), 0)
            except Exception as exc:
                if len(batch) <= 1:
                    bad = batch[0] if batch else "UNKNOWN"
                    print(f"  skipped bad instrument: {bad} ({exc})")
                    return pd.DataFrame(), 1
                mid = len(batch) // 2
                left_df, left_bad = _fetch_company_data_robust(batch[:mid])
                right_df, right_bad = _fetch_company_data_robust(batch[mid:])
                frames = [df for df in (left_df, right_df) if df is not None and not df.empty]
                merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                return merged, left_bad + right_bad

        for i in range(0, len(ric_universe), LSEG_BATCH_SIZE):
            batch = ric_universe[i : i + LSEG_BATCH_SIZE]
            part, bad_n = _fetch_company_data_robust(batch)
            bad_instruments += int(bad_n)
            if part is not None and not part.empty:
                company_parts.append(part)
            done = min(i + LSEG_BATCH_SIZE, len(ric_universe))
            print(f"  company_data: {done:,}/{len(ric_universe):,}")

    company = pd.concat(company_parts, ignore_index=True) if company_parts else pd.DataFrame()
    if company is None or company.empty:
        conn.close()
        return {
            "status": "no-data",
            "as_of": as_of,
            "universe": len(universe_rows),
            "requested_ticker_count": int(len(requested_ticker_set)),
            "requested_ric_count": int(len(requested_ric_set)),
            "matched_requested_ticker_count": int(len(matched_ticker_set)),
            "matched_requested_ric_count": int(len(matched_ric_set)),
            "missing_requested_tickers": missing_requested_tickers,
            "missing_requested_rics": missing_requested_rics,
        }

    instrument_col = _pick_col(company, ["Instrument"])
    if not instrument_col:
        conn.close()
        raise RuntimeError("LSEG response missing Instrument column")

    col = {
        "ticker_symbol": _pick_col(company, ["Ticker Symbol", "TR.TickerSymbol"]),
        "isin": _pick_col(company, ["ISIN", "TR.ISIN"]),
        "exchange_name": _pick_col(company, ["Exchange Name", "TR.ExchangeName"]),
        "price_open": _pick_col(company, ["Price Open"]),
        "price_high": _pick_col(company, ["Price High"]),
        "price_low": _pick_col(company, ["Price Low"]),
        "price": _pick_col(company, ["Price Close"]),
        "price_volume": _pick_col(
            company,
            [
                "Volume",
                "TR.Volume",
            ],
        ),
        "price_currency": _pick_col(company, ["Price Close Currency", "Currency"]),
        "market_cap": _pick_col(company, ["Company Market Cap"]),
        "shares_outstanding": _pick_col(company, ["Outstanding Shares", "Shares Outstanding", "Shares Outstanding - Common Stock"]),
        "dividend_yield": _pick_col(company, ["Dividend yield", "Dividend Yield"]),
        "common_name": _pick_col(company, ["Company Common Name", "Common Name", "TR.CommonName"]),
        "trbc_economic_sector": _pick_col(company, ["TRBC Economic Sector Name", "TRBC Economic Sector"]),
        "trbc_business_sector": _pick_col(company, ["TRBC Business Sector Name", "TRBC Business Sector"]),
        "trbc_industry_group": _pick_col(company, ["TRBC Industry Group Name", "TRBC Industry Group"]),
        "trbc_industry": _pick_col(company, ["TRBC Industry Name", "TRBC Industry"]),
        "trbc_activity": _pick_col(company, ["TRBC Activity Name", "TRBC Activity"]),
        "hq_country_code": _pick_col(
            company,
            [
                "Country ISO Code of Headquarters",
                "Headquarters Country Code",
                "HQ Country Code",
                "Country Code",
            ],
        ),
        "book_value": _pick_col(company, ["Book Value Per Share"]),
        "forward_eps": _pick_col(company, ["Earnings Per Share - Mean"]),
        "trailing_eps": _pick_col(company, ["Earnings Per Share - Actual"]),
        "total_debt": _pick_col(company, ["Total Debt"]),
        "cash_and_equivalents": _pick_col(company, ["Cash and Equivalents"]),
        "long_term_debt": _pick_col(company, ["Long Term Debt"]),
        "free_cash_flow": _pick_col(company, ["Free Cash Flow"]),
        "net_income": _pick_col(company, ["Net Income Incl Extra Before Distributions", "Net Income"]),
        "operating_cashflow": _pick_col(company, ["Cash from Operating Activities"]),
        "capital_expenditures": _pick_col(company, ["Capital Expenditures, Cumulative"]),
        "revenue": _pick_col(company, ["Revenue"]),
        "financial_period_abs": _pick_col(company, ["Financial Period Absolute"]),
        "report_currency": _pick_col(company, ["Currency"]),
        "ebitda": _pick_col(company, ["EBITDA"]),
        "ebit": _pick_col(company, ["EBIT"]),
        "total_assets": _pick_col(company, ["Total Assets"]),
        "total_liabilities": _pick_col(company, ["Total Liabilities"]),
        "return_on_equity": _pick_col(company, ["Pretax ROE Total Equity %", "ROE"]),
        "operating_margins": _pick_col(company, ["Operating Margin, Percent", "Operating Margin %"]),
        "fundamental_period_end_date": _pick_col(company, ["Period End Date"]),
    }

    job_run_id = f"lseg_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    compat_rows: list[dict[str, Any]] = []
    fundamentals_rows: list[dict[str, Any]] = []
    prices_rows: list[dict[str, Any]] = []
    classification_rows: list[dict[str, Any]] = []
    prices_rows_skipped_missing_close = 0

    def _txt(v: Any) -> str | None:
        if isinstance(v, pd.Series):
            for item in v.tolist():
                if item is None or pd.isna(item):
                    continue
                v = item
                break
            else:
                return None
        if v is None or pd.isna(v):
            return None
        s = str(v).strip()
        if not s or s.lower() in {"nan", "none"}:
            return None
        return s

    for _, row in company.iterrows():
        ric = str(row.get(instrument_col) or "").strip().upper()
        if ric not in ric_universe:
            continue
        runtime_row = runtime_policy_by_ric.get(ric, {})
        pending_seed_structural = (
            int(runtime_row.get("classification_ready") or 0) == 0
            and str(runtime_row.get("source") or "").strip().lower().endswith("_seed")
            and int(runtime_row.get("allow_cuse_returns_projection") or 0) != 1
        )
        allow_fundamentals = bool(int(runtime_row.get("pit_fundamentals_enabled") or 0) == 1 or pending_seed_structural)
        allow_classification = bool(
            int(runtime_row.get("pit_classification_enabled") or 0) == 1 or pending_seed_structural
        )
        trbc_economic_sector = _txt(row.get(col["trbc_economic_sector"]) if col["trbc_economic_sector"] else None)
        trbc_business_sector = _txt(row.get(col["trbc_business_sector"]) if col["trbc_business_sector"] else None)
        trbc_industry_group = _txt(row.get(col["trbc_industry_group"]) if col["trbc_industry_group"] else None)
        trbc_industry = _txt(row.get(col["trbc_industry"]) if col["trbc_industry"] else None)
        trbc_activity = _txt(row.get(col["trbc_activity"]) if col["trbc_activity"] else None)
        hq_country_code = (_txt(row.get(col["hq_country_code"]) if col["hq_country_code"] else None) or "").upper() or None
        classification_ok, is_equity_eligible = derive_security_master_flags(
            trbc_economic_sector=trbc_economic_sector,
            trbc_business_sector=trbc_business_sector,
            trbc_industry_group=trbc_industry_group,
            trbc_industry=trbc_industry,
            trbc_activity=trbc_activity,
            hq_country_code=hq_country_code,
        )
        compat_rows.append(
            {
                "ric": ric,
                "ticker": _txt(row.get(col["ticker_symbol"]) if col["ticker_symbol"] else None) or ticker_from_ric(ric),
                "isin": _txt(row.get(col["isin"]) if col["isin"] else None),
                "exchange_name": _txt(row.get(col["exchange_name"]) if col["exchange_name"] else None),
                "classification_ok": classification_ok,
                "is_equity_eligible": is_equity_eligible,
                "source": "lseg_toolkit",
                "job_run_id": job_run_id,
                "updated_at": updated_at,
            }
        )

        period_end_date = _iso_date(row.get(col["fundamental_period_end_date"]) if col["fundamental_period_end_date"] else None)
        fiscal_year, _, period_type = _parse_fiscal_period(
            row.get(col["financial_period_abs"]) if col["financial_period_abs"] else None
        )
        stat_date = period_end_date or as_of

        if allow_fundamentals:
            fundamentals_rows.append(
                {
                    "ric": ric,
                    "as_of_date": as_of,
                    "stat_date": stat_date,
                    "period_end_date": period_end_date,
                    "fiscal_year": fiscal_year,
                    "period_type": period_type,
                    "report_currency": _txt(row.get(col["report_currency"]) if col["report_currency"] else None),
                    "market_cap": _float_or_none(row.get(col["market_cap"]) if col["market_cap"] else None),
                    "shares_outstanding": _float_or_none(row.get(col["shares_outstanding"]) if col["shares_outstanding"] else None),
                    "dividend_yield": _float_or_none(row.get(col["dividend_yield"]) if col["dividend_yield"] else None),
                    "book_value_per_share": _float_or_none(row.get(col["book_value"]) if col["book_value"] else None),
                    "total_assets": _float_or_none(row.get(col["total_assets"]) if col["total_assets"] else None),
                    "total_debt": _float_or_none(row.get(col["total_debt"]) if col["total_debt"] else None),
                    "cash_and_equivalents": _float_or_none(row.get(col["cash_and_equivalents"]) if col["cash_and_equivalents"] else None),
                    "long_term_debt": _float_or_none(row.get(col["long_term_debt"]) if col["long_term_debt"] else None),
                    "operating_cashflow": _float_or_none(row.get(col["operating_cashflow"]) if col["operating_cashflow"] else None),
                    "capital_expenditures": _float_or_none(row.get(col["capital_expenditures"]) if col["capital_expenditures"] else None),
                    "trailing_eps": _float_or_none(row.get(col["trailing_eps"]) if col["trailing_eps"] else None),
                    "forward_eps": _float_or_none(row.get(col["forward_eps"]) if col["forward_eps"] else None),
                    "revenue": _float_or_none(row.get(col["revenue"]) if col["revenue"] else None),
                    "ebitda": _float_or_none(row.get(col["ebitda"]) if col["ebitda"] else None),
                    "ebit": _float_or_none(row.get(col["ebit"]) if col["ebit"] else None),
                    "roe_pct": _float_or_none(row.get(col["return_on_equity"]) if col["return_on_equity"] else None),
                    "operating_margin_pct": _float_or_none(row.get(col["operating_margins"]) if col["operating_margins"] else None),
                    "common_name": _txt(row.get(col["common_name"]) if col["common_name"] else None),
                    "source": "lseg_toolkit",
                    "job_run_id": job_run_id,
                    "updated_at": updated_at,
                }
            )

        close = _float_or_none(row.get(col["price"]) if col["price"] else None)
        open_px = _float_or_none(row.get(col["price_open"]) if col["price_open"] else None)
        high_px = _float_or_none(row.get(col["price_high"]) if col["price_high"] else None)
        low_px = _float_or_none(row.get(col["price_low"]) if col["price_low"] else None)
        volume_px = _float_or_none(row.get(col["price_volume"]) if col["price_volume"] else None)
        price_ccy = _txt(row.get(col["price_currency"]) if col["price_currency"] else None)
        report_ccy = _txt(row.get(col["report_currency"]) if col["report_currency"] else None)
        if close is None:
            prices_rows_skipped_missing_close += 1
        else:
            prices_rows.append(
                {
                    "ric": ric,
                    "date": as_of,
                    "open": open_px if open_px is not None else close,
                    "high": high_px if high_px is not None else close,
                    "low": low_px if low_px is not None else close,
                    "close": close,
                    "adj_close": close,
                    "volume": volume_px,
                    "currency": (price_ccy or report_ccy),
                    "source": "lseg_toolkit",
                    "updated_at": updated_at,
                }
            )

        if allow_classification:
            classification_rows.append(
                {
                    "ric": ric,
                    "as_of_date": as_of,
                    "trbc_economic_sector": trbc_economic_sector,
                    "trbc_business_sector": trbc_business_sector,
                    "trbc_industry_group": trbc_industry_group,
                    "trbc_industry": trbc_industry,
                    "trbc_activity": trbc_activity,
                    "hq_country_code": hq_country_code,
                    "source": "lseg_toolkit",
                    "job_run_id": job_run_id,
                    "updated_at": updated_at,
                }
            )

    n_f = 0
    n_p = 0
    n_c = 0
    n_runtime_rows = 0
    touched_rics = sorted({str(row.get("ric") or "").strip().upper() for row in compat_rows if row.get("ric")})
    try:
        n_runtime_rows = upsert_security_master_rows(
            conn,
            compat_rows,
            refresh_runtime_surfaces=False,
        )
        conn.commit()
        if write_fundamentals:
            n_f = _insert_rows(conn, FUNDAMENTALS_HISTORY_TABLE, fundamentals_rows, replace=True)
            conn.commit()
        if write_prices:
            n_p = _insert_rows(conn, PRICES_TABLE, prices_rows, replace=True)
            conn.commit()
        if write_classification:
            n_c = _insert_rows(conn, TRBC_HISTORY_TABLE, classification_rows, replace=True)
            conn.commit()
        if touched_rics:
            refresh_security_taxonomy_current(conn, rics=touched_rics)
            reconcile_default_security_policy_rows(conn, rics=touched_rics)
            refresh_security_source_observation_daily(conn, rics=touched_rics)
            materialize_security_master_compat_current(conn, rics=touched_rics)
            conn.commit()
    finally:
        conn.close()

    projection_price_rows = 0

    out = {
        "status": "ok",
        "as_of": as_of,
        "universe": len(universe_rows),
        "price_volume_metric": PRICE_VOLUME_FIELD,
        "registry_rows_upserted": int(n_runtime_rows),
        "compat_rows_upserted": int(n_runtime_rows),
        # Retained as a compatibility alias for existing callers; live ingest no longer
        # writes physical security_master in the runtime sync path.
        "security_master_rows_upserted": 0,
        "fundamental_rows_inserted": int(n_f),
        "price_rows_inserted": int(n_p),
        "price_rows_skipped_missing_close": int(prices_rows_skipped_missing_close),
        "classification_rows_inserted": int(n_c),
        "projection_only_price_rows_inserted": int(projection_price_rows),
        "db_path": str(db_path),
        "shard_index": int(shard_index),
        "shard_count": int(shard_count),
        "bad_instruments_skipped": int(bad_instruments),
        "requested_ticker_count": int(len(requested_ticker_set)),
        "requested_ric_count": int(len(requested_ric_set)),
        "matched_requested_ticker_count": int(len(matched_ticker_set)),
        "matched_requested_ric_count": int(len(matched_ric_set)),
        "missing_requested_tickers": missing_requested_tickers,
        "missing_requested_rics": missing_requested_rics,
    }
    print(out)
    return out


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Canonical LSEG ingest into RIC-keyed source-of-truth tables.")
    p.add_argument("--db-path", default=str(DEFAULT_DB), help="Path to target SQLite DB")
    p.add_argument("--index", default=None, help="Index code (e.g. SPX, NDX). Constituents are filtered to the runtime ingest scope")
    p.add_argument("--tickers", default=None, help="Comma-separated tickers to ingest (must exist in the runtime ingest scope)")
    p.add_argument("--rics", default=None, help="Comma-separated RICs to ingest (must exist in the runtime ingest scope)")
    p.add_argument("--as-of-date", default=None, help="Override as-of date (YYYY-MM-DD)")
    p.add_argument("--shard-count", type=int, default=1, help="Total number of ticker shards")
    p.add_argument("--shard-index", type=int, default=0, help="Zero-based shard index to process")
    p.add_argument("--skip-fundamentals", action="store_true", help="Skip writing fundamentals table")
    p.add_argument("--skip-prices", action="store_true", help="Skip writing prices table")
    p.add_argument("--skip-classification", action="store_true", help="Skip writing classification table")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    download_from_lseg(
        db_path=Path(args.db_path),
        index=args.index,
        tickers_csv=args.tickers,
        rics_csv=args.rics,
        as_of_date=args.as_of_date,
        shard_count=args.shard_count,
        shard_index=args.shard_index,
        write_fundamentals=not bool(args.skip_fundamentals),
        write_prices=not bool(args.skip_prices),
        write_classification=not bool(args.skip_classification),
    )
