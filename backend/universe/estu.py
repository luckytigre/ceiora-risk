"""ESTU membership construction and audit persistence for cUSE4."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sqlite3
from typing import Any

import numpy as np
import pandas as pd

from backend.universe.schema import (
    ESTU_MEMBERSHIP_TABLE,
    FUNDAMENTALS_HISTORY_TABLE,
    PRICES_TABLE,
    SECURITY_MASTER_TABLE,
    TRBC_HISTORY_TABLE,
    ensure_cuse4_schema,
)
from backend.universe.settings import EstuPolicy, estu_policy_from_env
from backend.trading_calendar import previous_or_same_xnys_session


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (table,),
    ).fetchone()
    return row is not None


def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null", "<na>"}:
        return ""
    return s


def _infer_country_from_ric(ric: str) -> str:
    if "." not in ric:
        return ""
    suffix = ric.split(".")[-1].upper()
    if suffix in {"N", "O", "A", "K", "P", "PK", "Q"}:
        return "US"
    return ""


def _load_security_frame(conn: sqlite3.Connection) -> pd.DataFrame:
    if not _table_exists(conn, SECURITY_MASTER_TABLE):
        return pd.DataFrame()
    df = pd.read_sql_query(
        f"""
        SELECT
            UPPER(TRIM(ric)) AS ric,
            UPPER(TRIM(ticker)) AS ticker,
            COALESCE(permid, '') AS permid,
            COALESCE(classification_ok, 0) AS classification_ok,
            COALESCE(is_equity_eligible, 0) AS is_equity_eligible
        FROM {SECURITY_MASTER_TABLE}
        WHERE ric IS NOT NULL
          AND TRIM(ric) <> ''
        """,
        conn,
    )
    if df.empty:
        return df
    df["ric"] = df["ric"].astype(str).str.upper()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["classification_ok"] = pd.to_numeric(df["classification_ok"], errors="coerce").fillna(0).astype(int)
    df["is_equity_eligible"] = pd.to_numeric(df["is_equity_eligible"], errors="coerce").fillna(0).astype(int)
    return df.drop_duplicates(subset=["ric"], keep="last")


def _load_price_features(
    conn: sqlite3.Connection,
    *,
    as_of_date: str,
    min_history_days: int,
) -> pd.DataFrame:
    if not _table_exists(conn, PRICES_TABLE):
        return pd.DataFrame()

    lookback_days = max(120, int(min_history_days * 2.2))
    start_date = (pd.Timestamp(as_of_date) - pd.Timedelta(days=lookback_days)).date().isoformat()

    prices = pd.read_sql_query(
        f"""
        SELECT
            UPPER(TRIM(p.ric)) AS ric,
            p.date,
            CAST(p.close AS REAL) AS close,
            CAST(p.volume AS REAL) AS volume
        FROM {PRICES_TABLE} p
        WHERE p.date >= ?
          AND p.date <= ?
        ORDER BY UPPER(TRIM(p.ric)), p.date
        """,
        conn,
        params=(start_date, as_of_date),
    )
    if prices.empty:
        return pd.DataFrame(columns=["ric", "price_close", "adv_20d", "price_obs"])

    prices["close"] = pd.to_numeric(prices["close"], errors="coerce")
    prices["volume"] = pd.to_numeric(prices["volume"], errors="coerce")
    prices = prices.dropna(subset=["ric", "date", "close"])
    if prices.empty:
        return pd.DataFrame(columns=["ric", "price_close", "adv_20d", "price_obs"])

    prices = prices.sort_values(["ric", "date"])
    prices["dollar_volume"] = prices["close"] * prices["volume"].fillna(0.0)

    latest = prices.groupby("ric", sort=False).tail(1).set_index("ric")
    obs = prices.groupby("ric", sort=False)["close"].count().rename("price_obs")
    adv20 = (
        prices.groupby("ric", sort=False)
        .tail(20)
        .groupby("ric", sort=False)["dollar_volume"]
        .mean()
        .rename("adv_20d")
    )

    out = pd.concat(
        [
            latest[["close"]].rename(columns={"close": "price_close"}),
            obs,
            adv20,
        ],
        axis=1,
    ).reset_index()
    out["price_close"] = pd.to_numeric(out["price_close"], errors="coerce")
    out["adv_20d"] = pd.to_numeric(out["adv_20d"], errors="coerce")
    out["price_obs"] = pd.to_numeric(out["price_obs"], errors="coerce").fillna(0).astype(int)
    return out


def _load_latest_fundamentals(conn: sqlite3.Connection, *, as_of_date: str) -> pd.DataFrame:
    if not _table_exists(conn, FUNDAMENTALS_HISTORY_TABLE):
        return pd.DataFrame()
    df = pd.read_sql_query(
        f"""
        WITH ranked AS (
            SELECT
                ric,
                as_of_date,
                market_cap,
                ROW_NUMBER() OVER (
                    PARTITION BY ric
                    ORDER BY as_of_date DESC, stat_date DESC
                ) AS rn
            FROM {FUNDAMENTALS_HISTORY_TABLE}
            WHERE as_of_date <= ?
        )
        SELECT ric, as_of_date, CAST(market_cap AS REAL) AS market_cap
        FROM ranked
        WHERE rn = 1
        """,
        conn,
        params=(as_of_date,),
    )
    if df.empty:
        return pd.DataFrame(columns=["ric", "market_cap", "has_required_fundamentals"])
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
    df["has_required_fundamentals"] = np.isfinite(df["market_cap"]) & (df["market_cap"] > 0.0)
    return df[["ric", "market_cap", "has_required_fundamentals"]]


def _load_latest_trbc(conn: sqlite3.Connection, *, as_of_date: str) -> pd.DataFrame:
    if not _table_exists(conn, TRBC_HISTORY_TABLE):
        return pd.DataFrame()
    df = pd.read_sql_query(
        f"""
        WITH ranked AS (
            SELECT
                ric,
                as_of_date,
                COALESCE(trbc_industry_group, '') AS trbc_industry_group,
                COALESCE(hq_country_code, '') AS hq_country_code,
                ROW_NUMBER() OVER (
                    PARTITION BY ric
                    ORDER BY as_of_date DESC
                ) AS rn
            FROM {TRBC_HISTORY_TABLE}
            WHERE as_of_date <= ?
        )
        SELECT ric, trbc_industry_group, hq_country_code
        FROM ranked
        WHERE rn = 1
        """,
        conn,
        params=(as_of_date,),
    )
    if df.empty:
        return pd.DataFrame(columns=["ric", "trbc_industry_group", "hq_country_code", "has_required_trbc"])
    df["trbc_industry_group"] = df["trbc_industry_group"].fillna("").astype(str).str.strip()
    df["hq_country_code"] = df["hq_country_code"].fillna("").astype(str).str.strip().str.upper()
    df["has_required_trbc"] = (
        df["trbc_industry_group"].str.len().gt(0) & df["hq_country_code"].str.len().gt(0)
    )
    return df[["ric", "trbc_industry_group", "hq_country_code", "has_required_trbc"]]


def _drop_reason(row: pd.Series) -> tuple[str, str]:
    if not bool(row.get("is_equity_eligible", False)):
        return "not_equity_eligible", "security_master.is_equity_eligible != 1"
    if not bool(row.get("has_required_price_history", False)):
        return "missing_price_history", "insufficient trailing price observations"
    if not bool(row.get("has_required_fundamentals", False)):
        return "missing_fundamentals", "no positive market cap fundamentals row"
    if not bool(row.get("has_required_trbc", False)):
        return "missing_trbc", "missing industry or country classification"
    if not bool(row.get("passes_price_floor", False)):
        return "price_floor", "price below minimum threshold"
    if not bool(row.get("passes_microcap_guard", False)):
        return "microcap", "market cap below minimum threshold"
    if not bool(row.get("passes_liquidity_guard", False)):
        return "illiquid", "ADV20 below minimum threshold"
    return "", ""


def build_and_persist_estu_membership(
    *,
    db_path: Path,
    as_of_date: str | None = None,
    policy: EstuPolicy | None = None,
) -> dict[str, Any]:
    selected_date = previous_or_same_xnys_session(as_of_date or datetime.now(timezone.utc).date().isoformat())
    estu_policy = policy or estu_policy_from_env()

    conn = sqlite3.connect(str(db_path), timeout=120)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=120000")

    try:
        ensure_cuse4_schema(conn)
        security = _load_security_frame(conn)
        if security.empty:
            return {
                "status": "no-security-master",
                "date": selected_date,
                "rows_written": 0,
            }

        prices = _load_price_features(
            conn,
            as_of_date=selected_date,
            min_history_days=estu_policy.min_price_history_days,
        )
        fundamentals = _load_latest_fundamentals(conn, as_of_date=selected_date)
        trbc = _load_latest_trbc(conn, as_of_date=selected_date)

        frame = security.copy()
        frame = frame.merge(prices, on="ric", how="left")
        frame = frame.merge(fundamentals, on="ric", how="left")
        frame = frame.merge(trbc, on="ric", how="left")

        frame["hq_country_code"] = frame.get("hq_country_code", "").fillna("").astype(str).str.strip().str.upper()
        frame["hq_country_code"] = np.where(
            frame["hq_country_code"].str.len().gt(0),
            frame["hq_country_code"],
            frame["ric"].fillna("").astype(str).map(_infer_country_from_ric),
        )
        frame["has_required_trbc"] = (
            frame.get("trbc_industry_group", "").fillna("").astype(str).str.strip().str.len().gt(0)
            & frame["hq_country_code"].astype(str).str.len().gt(0)
        )

        frame["price_obs"] = pd.to_numeric(frame.get("price_obs"), errors="coerce").fillna(0).astype(int)
        frame["price_close"] = pd.to_numeric(frame.get("price_close"), errors="coerce")
        frame["adv_20d"] = pd.to_numeric(frame.get("adv_20d"), errors="coerce")
        frame["market_cap"] = pd.to_numeric(frame.get("market_cap"), errors="coerce")

        frame["has_required_price_history"] = frame["price_obs"] >= int(estu_policy.min_price_history_days)
        has_required_fundamentals = frame.get("has_required_fundamentals")
        if has_required_fundamentals is None:
            frame["has_required_fundamentals"] = False
        else:
            frame["has_required_fundamentals"] = (
                has_required_fundamentals.astype("boolean").fillna(False).astype(bool)
            )
        frame["passes_price_floor"] = frame["price_close"].fillna(-np.inf) >= float(estu_policy.min_price_floor)
        frame["passes_microcap_guard"] = frame["market_cap"].fillna(-np.inf) >= float(estu_policy.min_market_cap)
        frame["passes_liquidity_guard"] = frame["adv_20d"].fillna(-np.inf) >= float(estu_policy.min_adv_20d)

        eligibility_conditions = (
            frame["is_equity_eligible"].astype(int).eq(1)
            & frame["has_required_price_history"].astype(bool)
            & frame["has_required_fundamentals"].astype(bool)
            & frame["has_required_trbc"].astype(bool)
            & frame["passes_price_floor"].astype(bool)
            & frame["passes_microcap_guard"].astype(bool)
            & frame["passes_liquidity_guard"].astype(bool)
        )
        frame["estu_flag"] = eligibility_conditions.astype(int)

        reasons = frame.apply(_drop_reason, axis=1)
        frame["drop_reason"] = [r[0] for r in reasons]
        frame["drop_reason_detail"] = [r[1] for r in reasons]

        now_iso = datetime.now(timezone.utc).isoformat()
        job_run_id = f"cuse4_estu_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

        conn.execute(f"DELETE FROM {ESTU_MEMBERSHIP_TABLE} WHERE date = ?", (selected_date,))
        payload = [
            (
                selected_date,
                str(row.ric),
                int(row.estu_flag),
                _norm_text(row.drop_reason) or None,
                _norm_text(row.drop_reason_detail) or None,
                float(row.market_cap) if np.isfinite(row.market_cap) else None,
                float(row.price_close) if np.isfinite(row.price_close) else None,
                float(row.adv_20d) if np.isfinite(row.adv_20d) else None,
                int(bool(row.has_required_price_history)),
                int(bool(row.has_required_fundamentals)),
                int(bool(row.has_required_trbc)),
                "cuse4_estu_builder",
                job_run_id,
                now_iso,
            )
            for row in frame.itertuples(index=False)
        ]
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {ESTU_MEMBERSHIP_TABLE} (
                date,
                ric,
                estu_flag,
                drop_reason,
                drop_reason_detail,
                mcap,
                price_close,
                adv_20d,
                has_required_price_history,
                has_required_fundamentals,
                has_required_trbc,
                source,
                job_run_id,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        conn.commit()

        drop_counts = (
            frame.loc[frame["estu_flag"] == 0, "drop_reason"]
            .fillna("")
            .astype(str)
            .value_counts()
            .to_dict()
        )

        return {
            "status": "ok",
            "date": selected_date,
            "rows_written": int(len(payload)),
            "estu_count": int(frame["estu_flag"].sum()),
            "ineligible_count": int((frame["estu_flag"] == 0).sum()),
            "drop_reason_counts": {str(k): int(v) for k, v in drop_counts.items() if str(k)},
            "policy": {
                "min_price_history_days": int(estu_policy.min_price_history_days),
                "min_price_floor": float(estu_policy.min_price_floor),
                "min_market_cap": float(estu_policy.min_market_cap),
                "min_adv_20d": float(estu_policy.min_adv_20d),
            },
            "job_run_id": job_run_id,
        }
    finally:
        conn.close()
