"""Universe loadings and coverage builders used by analytics pipeline."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.analytics.contracts import (
    FactorCoveragePayload,
    SpecificRiskPayload,
    UniverseLoadingsPayload,
    UniverseTickerPayload,
)
from backend.analytics.refresh_metadata import finite_float as _finite_float
from backend.data.cuse_membership_reads import load_cuse_membership_lookup
from backend.analytics.trbc_economic_sector_short import abbreviate_trbc_economic_sector_short
from backend.risk_model.descriptors import (
    FULL_STYLE_ORTH_RULES,
    apply_style_canonicalization,
    fit_and_apply_style_canonicalization,
)
from backend.risk_model.cuse_membership import membership_row_to_overlay
from backend.risk_model.eligibility import build_eligibility_context, structural_eligibility_for_date
from backend.risk_model.factor_catalog import (
    MARKET_FACTOR,
    STYLE_LABEL_TO_COLUMN,
    STYLE_COLUMN_TO_LABEL,
    build_factor_catalog_for_factors,
    factor_id_for_name,
    factor_id_to_entry_map,
    factor_name_to_id_map,
    infer_factor_family,
    serialize_factor_catalog,
)
from backend.risk_model.model_status import derive_model_status
from backend.universe.runtime_rows import load_security_runtime_rows

logger = logging.getLogger(__name__)

_RIC_SUFFIX_RANK = {
    ".N": 0,
    ".OQ": 1,
    ".O": 2,
    ".K": 3,
    ".P": 4,
}


def _ric_priority_key(ric: str) -> tuple[int, str]:
    ric_txt = str(ric or "").upper().strip()
    for suffix, rank in _RIC_SUFFIX_RANK.items():
        if ric_txt.endswith(suffix):
            return int(rank), ric_txt
    return 99, ric_txt


def _overlay_persisted_cuse_membership(
    *,
    data_db: Path,
    universe_by_ticker: dict[str, UniverseTickerPayload],
) -> None:
    relevant_dates = sorted(
        {
            str(row.get("as_of_date") or "").strip()
            for row in universe_by_ticker.values()
            if str(row.get("as_of_date") or "").strip()
        }
    )
    if not relevant_dates:
        return
    membership_lookup = load_cuse_membership_lookup(
        data_db=data_db,
        as_of_dates=relevant_dates,
    )
    if not membership_lookup:
        return

    for ticker, row in universe_by_ticker.items():
        as_of_date = str(row.get("as_of_date") or "").strip()
        if not as_of_date:
            continue
        ric = str(row.get("ric") or "").strip().upper()
        membership_row = membership_lookup.get((as_of_date, ticker))
        if membership_row is None and ric:
            membership_row = membership_lookup.get((as_of_date, ric))
        if membership_row is None:
            continue
        row.update(
            membership_row_to_overlay(
                membership_row,
                payload_exposures=dict(row.get("exposures") or {}),
            )
        )


def _load_admitted_runtime_tickers_neon() -> set[str] | None:
    """Load admitted tickers from Neon security_registry when local SQLite is unavailable."""
    from backend.data.neon import connect, resolve_dsn
    try:
        dsn = resolve_dsn(None)
    except ValueError as exc:
        raise RuntimeError(
            "Unable to resolve Neon DSN while loading admitted runtime tickers."
        ) from exc
    try:
        pg_conn = connect(dsn=dsn, autocommit=True)
        try:
            with pg_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ticker
                    FROM security_registry
                    WHERE (tracking_status IS NULL OR tracking_status != 'disabled')
                    AND ticker IS NOT NULL AND ticker != ''
                    """
                )
                rows = cur.fetchall()
        finally:
            pg_conn.close()
    except Exception as exc:
        raise RuntimeError(
            "Failed to read admitted runtime tickers from Neon security_registry."
        ) from exc
    if not rows:
        return set()
    return {str(row[0]).strip().upper() for row in rows if str(row[0]).strip()}


def _load_admitted_runtime_tickers(data_db: Path) -> set[str] | None:
    db_path = Path(data_db)
    if not db_path.exists():
        # Local SQLite unavailable — try Neon registry when it is the configured backend.
        from backend.data.core_read_backend import use_neon_core_reads
        if use_neon_core_reads():
            return _load_admitted_runtime_tickers_neon()
        return None
    conn = sqlite3.connect(str(db_path))
    try:
        registry_exists = conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type IN ('table', 'view') AND name='security_registry'
            LIMIT 1
            """
        ).fetchone()
        if registry_exists is None:
            # Registry absent from local SQLite — try Neon fallback.
            from backend.data.core_read_backend import use_neon_core_reads
            if use_neon_core_reads():
                return _load_admitted_runtime_tickers_neon()
            return None
        registry_count = conn.execute("SELECT COUNT(*) FROM security_registry").fetchone()
        if not registry_count or int(registry_count[0] or 0) <= 0:
            return set()
        runtime_rows = load_security_runtime_rows(
            conn,
            include_disabled=False,
            allow_empty_registry_fallback=False,
        )
    except sqlite3.DatabaseError:
        from backend.data.core_read_backend import use_neon_core_reads
        if use_neon_core_reads():
            return _load_admitted_runtime_tickers_neon()
        return None
    finally:
        conn.close()
    return {
        str(row.get("ticker") or "").strip().upper()
        for row in runtime_rows
        if str(row.get("ticker") or "").strip()
    }


def build_universe_ticker_loadings(
    exposures_df: pd.DataFrame,
    fundamentals_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    cov: pd.DataFrame,
    *,
    data_db: Path,
    specific_risk_by_ticker: dict[str, SpecificRiskPayload] | None = None,
    factor_catalog_by_name: dict[str, object] | None = None,
    projected_loadings: dict | None = None,
    projection_universe_rows: list[dict[str, str]] | None = None,
    projection_core_state_through_date: str | None = None,
) -> UniverseLoadingsPayload:
    """Build full-universe cached loadings/risk context keyed by ticker."""
    exposures_df = exposures_df.copy() if exposures_df is not None else pd.DataFrame()
    fundamentals_df = fundamentals_df.copy() if fundamentals_df is not None else pd.DataFrame()
    prices_df = prices_df.copy() if prices_df is not None else pd.DataFrame()

    if not exposures_df.empty:
        exposures_df["ticker"] = exposures_df["ticker"].astype(str).str.upper()
        if "ric" in exposures_df.columns:
            exposures_df["ric"] = exposures_df["ric"].astype(str).str.upper()
    if not fundamentals_df.empty:
        fundamentals_df["ticker"] = fundamentals_df["ticker"].astype(str).str.upper()
        if "ric" in fundamentals_df.columns:
            fundamentals_df["ric"] = fundamentals_df["ric"].astype(str).str.upper()
    if not prices_df.empty:
        prices_df["ticker"] = prices_df["ticker"].astype(str).str.upper()
        if "ric" in prices_df.columns:
            prices_df["ric"] = prices_df["ric"].astype(str).str.upper()

    ric_by_ticker: dict[str, str] = {}
    ticker_by_ric: dict[str, str] = {}

    def _collect_maps(df: pd.DataFrame) -> None:
        if df.empty or "ric" not in df.columns or "ticker" not in df.columns:
            return
        for _, row in df.iterrows():
            ric = str(row.get("ric") or "").upper().strip()
            ticker = str(row.get("ticker") or "").upper().strip()
            if not ric or not ticker:
                continue
            existing = ric_by_ticker.get(ticker)
            if existing is None or _ric_priority_key(ric) < _ric_priority_key(existing):
                ric_by_ticker[ticker] = ric
            if ric not in ticker_by_ric:
                ticker_by_ric[ric] = ticker

    _collect_maps(exposures_df)
    _collect_maps(fundamentals_df)
    _collect_maps(prices_df)

    # Latest price map for whole universe
    price_map: dict[str, float] = {}
    if not prices_df.empty:
        for _, row in prices_df.iterrows():
            ticker = str(row.get("ticker", "")).upper()
            if ticker:
                price_map[ticker] = _finite_float(row.get("close"), 0.0)

    # Fundamentals maps for whole universe
    mcap_map: dict[str, float] = {}
    trbc_economic_sector_short_map: dict[str, str] = {}
    trbc_business_sector_map: dict[str, str] = {}
    trbc_industry_map: dict[str, str] = {}
    name_map: dict[str, str] = {}
    if not fundamentals_df.empty:
        name_col = None
        for col in ("company_name", "common_name", "name", "Company Common Name"):
            if col in fundamentals_df.columns:
                name_col = col
                break

        for _, row in fundamentals_df.iterrows():
            ticker = str(row.get("ticker", "")).upper()
            if not ticker:
                continue
            mcap = _finite_float(row.get("market_cap"), np.nan)
            if np.isfinite(mcap) and mcap > 0:
                mcap_map[ticker] = mcap
            trbc_economic_sector_short = str(
                row.get("trbc_economic_sector_short")
                or row.get("trbc_sector")
                or ""
            ).strip()
            if trbc_economic_sector_short:
                trbc_economic_sector_short_map[ticker] = trbc_economic_sector_short
            trbc_business_sector = str(row.get("trbc_business_sector") or "").strip()
            if trbc_business_sector:
                trbc_business_sector_map[ticker] = trbc_business_sector
            trbc_industry = str(row.get("trbc_industry_group") or "").strip()
            if trbc_industry:
                trbc_industry_map[ticker] = trbc_industry
            if name_col:
                raw_name = row.get(name_col)
                if raw_name is not None:
                    s = str(raw_name).strip()
                    if s and s.lower() != "nan":
                        name_map[ticker] = s

    latest_asof = ""
    latest_available_asof = ""
    if "as_of_date" in exposures_df.columns and not exposures_df.empty:
        asof_series = exposures_df["as_of_date"].astype(str).str.strip()
        asof_counts = asof_series.value_counts()
        if not asof_counts.empty:
            latest_available_asof = str(asof_series.max())
            max_count = int(asof_counts.max())
            # Guard against thin end-of-day snapshots (for example, incomplete latest date).
            min_coverage = max(100, int(0.50 * max_count))
            well_covered_dates = sorted(
                str(dt) for dt, cnt in asof_counts.items()
                if int(cnt) >= min_coverage
            )
            if well_covered_dates:
                latest_asof = well_covered_dates[-1]
            else:
                latest_asof = latest_available_asof
            if latest_asof != latest_available_asof:
                logger.warning(
                    "Using well-covered exposure as-of date %s instead of sparse latest %s "
                    "(coverage threshold=%s, max_count=%s)",
                    latest_asof,
                    latest_available_asof,
                    min_coverage,
                    max_count,
                )

    effective_exposures_df = exposures_df
    if latest_asof and not exposures_df.empty and "as_of_date" in exposures_df.columns:
        effective_exposures_df = exposures_df[
            exposures_df["as_of_date"].astype(str).str.strip() == latest_asof
        ].copy()

    eligibility_df = pd.DataFrame()
    if latest_asof:
        elig_ctx = build_eligibility_context(data_db, dates=[latest_asof])
        _, eligibility_df = structural_eligibility_for_date(elig_ctx, latest_asof)

    eligible_mask = eligibility_df.get("is_structural_eligible", pd.Series(dtype=bool)).astype(bool)
    eligible_rics = set(eligibility_df.index[eligible_mask].astype(str).str.upper())
    core_eligible_rics = {
        str(ric).upper()
        for ric in eligibility_df.index[
            eligible_mask
            & eligibility_df.get("hq_country_code", pd.Series(index=eligibility_df.index, dtype="string"))
            .fillna("")
            .astype(str)
            .str.upper()
            .eq("US")
        ]
    }
    eligible_tickers = {
        ticker for ticker, ric in ric_by_ticker.items()
        if ric in eligible_rics
    }
    ineligible_reason: dict[str, str] = {}
    for ticker, ric in ric_by_ticker.items():
        if ric in eligibility_df.index:
            ineligible_reason[ticker] = str(eligibility_df.loc[ric, "exclusion_reason"] or "")

    # Canonicalize style scores on the US-core cross-section, then apply that transform to
    # the full projectable universe so projected-only non-US names remain on the same scale.
    canonical_style_map: dict[str, dict[str, float]] = {}
    style_cols_present = (
        [c for c in STYLE_COLUMN_TO_LABEL if c in effective_exposures_df.columns]
        if not effective_exposures_df.empty
        else []
    )
    if (
        not effective_exposures_df.empty
        and style_cols_present
        and eligible_rics
        and "ric" in effective_exposures_df.columns
    ):
        style_names = [STYLE_COLUMN_TO_LABEL[c] for c in style_cols_present]
        style_scores = effective_exposures_df[["ric", *style_cols_present]].copy()
        style_scores["ric"] = style_scores["ric"].astype(str).str.upper()
        style_scores = style_scores[style_scores["ric"].isin(eligible_rics)]
        style_scores = style_scores.drop_duplicates(subset=["ric"], keep="last").set_index("ric")
        style_scores.columns = style_names
        if not style_scores.empty and core_eligible_rics:
            style_scores_core = style_scores.loc[style_scores.index.intersection(sorted(core_eligible_rics))].copy()
            caps_from_elig = pd.to_numeric(
                eligibility_df.reindex(style_scores_core.index)["market_cap"],
                errors="coerce",
            )
            industries_from_elig = (
                eligibility_df.reindex(style_scores_core.index)["trbc_business_sector"]
                .fillna("")
                .astype(str)
            )
            valid = (
                style_scores_core.notna().all(axis=1).to_numpy(dtype=bool)
                & np.isfinite(style_scores_core.to_numpy(dtype=float)).all(axis=1)
                & np.isfinite(caps_from_elig.to_numpy(dtype=float))
                & (caps_from_elig.to_numpy(dtype=float) > 0.0)
                & (industries_from_elig.str.len().to_numpy(dtype=float) > 0)
            )
            if int(valid.sum()) > 0:
                valid_idx = style_scores_core.index[valid]
                style_scores_core = style_scores_core.loc[valid_idx]
                caps_from_elig = caps_from_elig.loc[valid_idx]
                industries_from_elig = industries_from_elig.loc[valid_idx]
                industry_dummies = pd.get_dummies(industries_from_elig, dtype=float)
                canonical_scores_core, canonical_model = fit_and_apply_style_canonicalization(
                    style_scores=style_scores_core,
                    market_caps=caps_from_elig,
                    orth_rules=FULL_STYLE_ORTH_RULES,
                    industry_exposures=industry_dummies,
                )
                projectable_industries = (
                    eligibility_df.reindex(style_scores.index)["trbc_business_sector"]
                    .fillna("")
                    .astype(str)
                )
                projectable_dummies = (
                    pd.get_dummies(projectable_industries, dtype=float)
                    .reindex(columns=industry_dummies.columns, fill_value=0.0)
                    .reindex(style_scores.index, fill_value=0.0)
                )
                canonical_scores = apply_style_canonicalization(
                    style_scores=style_scores,
                    model=canonical_model,
                    industry_exposures=projectable_dummies,
                )
                for ric, row in canonical_scores.iterrows():
                    ticker = ticker_by_ric.get(str(ric).upper(), str(ric).upper())
                    canonical_style_map[ticker] = {
                        factor: _finite_float(row.get(factor), 0.0)
                        for factor in canonical_scores_core.columns
                    }

    catalog_by_name = dict(factor_catalog_by_name or {})
    catalog_by_id = factor_id_to_entry_map(catalog_by_name) if catalog_by_name else {}
    catalog_method_version = ""
    if catalog_by_name:
        first_entry = next(iter(catalog_by_name.values()))
        catalog_method_version = str(getattr(first_entry, "method_version", "") or "")
    catalog_factor_tokens = {str(name).strip() for name in catalog_by_name}
    if style_cols_present:
        catalog_factor_tokens.update(STYLE_COLUMN_TO_LABEL[col] for col in style_cols_present)
    if eligible_rics:
        catalog_factor_tokens.add(MARKET_FACTOR)
    if cov is None or cov.empty:
        catalog_factor_tokens.update(
            str(value).strip()
            for value in eligibility_df.get("trbc_business_sector", pd.Series(dtype=str)).fillna("").astype(str)
            if str(value).strip()
        )
    if cov is not None and not cov.empty:
        catalog_factor_tokens.update(str(name).strip() for name in cov.columns if str(name).strip())
    known_catalog_tokens = set(catalog_by_name) | set(catalog_by_id)
    missing_catalog_names = sorted(catalog_factor_tokens - known_catalog_tokens)
    if not catalog_by_name:
        catalog_by_name = build_factor_catalog_for_factors(
            sorted(catalog_factor_tokens),
            method_version=catalog_method_version,
        )
    elif missing_catalog_names:
        catalog_by_name.update(
            build_factor_catalog_for_factors(
                missing_catalog_names,
                method_version=catalog_method_version,
            )
        )
    catalog_by_id = factor_id_to_entry_map(catalog_by_name)
    factor_name_to_id = factor_name_to_id_map(catalog_by_name)

    def _resolve_projected_factor_id(token: str) -> str | None:
        clean = str(token or "").strip()
        if not clean:
            return None
        if clean in catalog_by_id:
            return clean
        direct = factor_name_to_id.get(clean)
        if direct:
            return direct
        family = infer_factor_family(clean, structural_factor_names=catalog_by_name.keys())
        source_column = STYLE_LABEL_TO_COLUMN.get(clean)
        factor_id = factor_id_for_name(
            clean,
            family=family,
            source_column=source_column,
        )
        return factor_id if factor_id in catalog_by_id else None

    # Factor vol map from full-universe covariance
    factor_vol_map: dict[str, float] = {}
    if cov is not None and not cov.empty:
        for factor in cov.columns:
            factor_token = str(factor)
            factor_id = (
                factor_token
                if factor_token in catalog_by_id
                else factor_name_to_id.get(factor_token)
            )
            if not factor_id:
                continue
            factor_vol_map[factor_id] = float(np.sqrt(max(0.0, _finite_float(cov.loc[factor, factor], 0.0))))

    source_tickers = sorted(
        {
            *exposures_df.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().tolist(),
            *fundamentals_df.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().tolist(),
            *prices_df.get("ticker", pd.Series(dtype=str)).astype(str).str.upper().tolist(),
        }
    )
    admitted_runtime_tickers = _load_admitted_runtime_tickers(data_db)
    if admitted_runtime_tickers is None:
        all_tickers = source_tickers
    else:
        all_tickers = sorted(set(source_tickers).intersection(admitted_runtime_tickers))
        dropped_tickers = sorted(set(source_tickers).difference(admitted_runtime_tickers))
        if dropped_tickers:
            logger.info(
                "Excluded %d non-runtime tickers from universe loadings search surface: %s",
                len(dropped_tickers),
                ", ".join(dropped_tickers[:20]),
            )
    universe_by_ticker: dict[str, UniverseTickerPayload] = {}
    downgraded_missing_exposures: list[str] = []
    for ticker in all_tickers:
        if not ticker:
            continue

        ric = str(ric_by_ticker.get(ticker, "")).upper()
        structurally_eligible = bool(ric in eligible_rics) if ric else bool(ticker in eligible_tickers)
        trbc_economic_sector_short = str(
            (
                eligibility_df.loc[ric, "trbc_economic_sector_short"]
                if ric in eligibility_df.index and "trbc_economic_sector_short" in eligibility_df.columns
                else (
                    eligibility_df.loc[ric, "trbc_sector"]
                    if ric in eligibility_df.index and "trbc_sector" in eligibility_df.columns
                    else ""
                )
            )
            or trbc_economic_sector_short_map.get(ticker, "")
        )
        trbc_industry_group = str(
            (eligibility_df.loc[ric, "trbc_industry_group"] if ric in eligibility_df.index else "")
            or trbc_industry_map.get(ticker, "")
        )
        trbc_business_sector = str(
            (eligibility_df.loc[ric, "trbc_business_sector"] if ric in eligibility_df.index else "")
            or trbc_business_sector_map.get(ticker, "")
        )
        hq_country_code = str(
            (eligibility_df.loc[ric, "hq_country_code"] if ric in eligibility_df.index else "")
            or ""
        ).upper()
        market_cap = _finite_float(
            eligibility_df.loc[ric, "market_cap"] if ric in eligibility_df.index else mcap_map.get(ticker),
            np.nan,
        )

        exposures_by_name: dict[str, float] = {}
        if structurally_eligible and ticker in canonical_style_map:
            exposures_by_name.update(canonical_style_map[ticker])
            exposures_by_name[MARKET_FACTOR] = 1.0
            if trbc_business_sector:
                exposures_by_name[trbc_business_sector] = 1.0

        exposures = {
            factor_name_to_id[factor_name]: value
            for factor_name, value in exposures_by_name.items()
            if factor_name in factor_name_to_id
        }

        has_factor_exposures = bool(exposures)
        model_status = derive_model_status(
            is_core_regression_member=bool(structurally_eligible and has_factor_exposures and hq_country_code == "US"),
            is_projectable=bool(structurally_eligible and has_factor_exposures),
        )
        if structurally_eligible and not has_factor_exposures:
            downgraded_missing_exposures.append(ticker)

        sensitivities = {
            factor: round(_finite_float(exposures.get(factor), 0.0) * _finite_float(vol, 0.0), 6)
            for factor, vol in factor_vol_map.items()
        }
        risk_loading = round(float(sum(abs(v) for v in sensitivities.values())), 6) if has_factor_exposures else None
        spec = (specific_risk_by_ticker or {}).get(ric, {}) if ric else {}
        if not spec:
            spec = (specific_risk_by_ticker or {}).get(ticker, {})
        spec_var = _finite_float(spec.get("specific_var"), np.nan) if has_factor_exposures else np.nan
        spec_vol = _finite_float(spec.get("specific_vol"), np.nan) if has_factor_exposures else np.nan

        omitted_unmodeled_sector = bool(
            structurally_eligible
            and trbc_business_sector
            and trbc_business_sector not in factor_name_to_id
        )

        if has_factor_exposures:
            model_status_reason = ""
            model_warning = (
                "Business sector is outside the current modeled factor set; sector effect is carried in specific risk."
                if omitted_unmodeled_sector
                else ""
            )
        elif structurally_eligible:
            model_status_reason = "missing_factor_exposures"
            selected_date = latest_asof or "current"
            model_warning = (
                "Ticker is structurally eligible but missing factor exposures "
                f"on selected as-of date {selected_date}."
            )
        else:
            model_status_reason = ineligible_reason.get(ticker, "ineligible")
            model_warning = "Ticker is ineligible for strict equity model; analytics shown as N/A."

        universe_by_ticker[ticker] = {
            "ticker": ticker,
            "ric": ric or None,
            "name": name_map.get(ticker, ""),
            "trbc_economic_sector_short": trbc_economic_sector_short,
            "trbc_economic_sector_short_abbr": abbreviate_trbc_economic_sector_short(trbc_economic_sector_short),
            "trbc_business_sector": trbc_business_sector,
            "trbc_industry_group": trbc_industry_group,
            "market_cap": round(float(market_cap), 2) if np.isfinite(market_cap) else None,
            "price": round(_finite_float(price_map.get(ticker), 0.0), 4),
            "exposures": exposures,
            "sensitivities": sensitivities,
            "risk_loading": risk_loading,
            "specific_var": round(spec_var, 8) if np.isfinite(spec_var) else None,
            "specific_vol": round(spec_vol, 6) if np.isfinite(spec_vol) else None,
            "model_status": model_status,
            "model_status_reason": model_status_reason,
            "eligibility_reason": model_status_reason,
            "model_warning": model_warning,
            "as_of_date": latest_asof,
            "exposure_origin": "native",
        }

    if downgraded_missing_exposures:
        sample = ", ".join(sorted(downgraded_missing_exposures)[:10])
        logger.warning(
            "Downgraded %s structurally eligible tickers missing factor exposures on %s: %s",
            len(downgraded_missing_exposures),
            latest_asof or "<unknown>",
            sample,
        )

    # Inject projection-only instruments (e.g. ETFs) into the universe from the
    # persisted projection surface bound to the active core package date.
    if projected_loadings:
        for proj_ticker, proj in projected_loadings.items():
            if proj.status != "ok":
                continue
            # Map factor names to factor IDs for exposures
            proj_exposures = {
                resolved_factor_id: v
                for k, v in proj.exposures.items()
                if (resolved_factor_id := _resolve_projected_factor_id(k))
            }
            proj_sensitivities = {
                factor: round(_finite_float(proj_exposures.get(factor), 0.0) * _finite_float(vol, 0.0), 6)
                for factor, vol in factor_vol_map.items()
            }
            proj_risk_loading = round(float(sum(abs(v) for v in proj_sensitivities.values())), 6)
            proj_spec_var = proj.specific_var
            proj_spec_vol = proj.specific_vol

            universe_by_ticker[proj_ticker] = {
                "ticker": proj_ticker,
                "ric": proj.ric or None,
                "name": name_map.get(proj_ticker, ""),
                "trbc_economic_sector_short": "",
                "trbc_economic_sector_short_abbr": "",
                "trbc_business_sector": "",
                "trbc_industry_group": "",
                "market_cap": None,
                "price": round(_finite_float(price_map.get(proj_ticker), 0.0), 4),
                "exposures": proj_exposures,
                "sensitivities": proj_sensitivities,
                "risk_loading": proj_risk_loading,
                "specific_var": round(proj_spec_var, 8) if np.isfinite(proj_spec_var) else None,
                "specific_vol": round(proj_spec_vol, 6) if np.isfinite(proj_spec_vol) else None,
                "model_status": "projected_only",
                "model_status_reason": "returns_projection",
                "eligibility_reason": "returns_projection",
                "model_warning": (
                    "Projected exposures and specific risk are approximate returns-regression "
                    "outputs derived from the stable cUSE core package."
                ),
                "as_of_date": proj.projection_asof or projection_core_state_through_date or latest_asof,
                "exposure_origin": "projected",
                "projection_method": "ols_returns_regression",
                "projection_r_squared": round(proj.r_squared, 6),
                "projection_obs_count": proj.obs_count,
                "projection_asof": proj.projection_asof or projection_core_state_through_date or None,
            }
        logger.info(
            "Injected %d projection-only instruments into universe loadings.",
            sum(1 for p in projected_loadings.values() if p.status == "ok"),
        )

    if projection_universe_rows:
        active_projection_asof = projection_core_state_through_date or latest_asof
        for row in projection_universe_rows:
            proj_ticker = str(row.get("ticker") or "").upper().strip()
            proj_ric = str(row.get("ric") or "").upper().strip()
            if not proj_ticker:
                continue
            existing = universe_by_ticker.get(proj_ticker)
            if existing is not None and str(existing.get("exposure_origin") or "") == "projected" and bool(existing.get("exposures")):
                continue
            universe_by_ticker[proj_ticker] = {
                "ticker": proj_ticker,
                "ric": proj_ric or None,
                "name": name_map.get(proj_ticker, ""),
                "trbc_economic_sector_short": "",
                "trbc_economic_sector_short_abbr": "",
                "trbc_business_sector": "",
                "trbc_industry_group": "",
                "market_cap": None,
                "price": round(_finite_float(price_map.get(proj_ticker), 0.0), 4),
                "exposures": {},
                "sensitivities": {},
                "risk_loading": None,
                "specific_var": None,
                "specific_vol": None,
                "model_status": "ineligible",
                "model_status_reason": "projection_unavailable",
                "eligibility_reason": "projection_unavailable",
                "model_warning": (
                    "Projection-only instrument has no persisted projected loadings for "
                    f"active core package {active_projection_asof or 'unknown'}."
                ),
                "as_of_date": active_projection_asof,
                "exposure_origin": "projected",
                "projection_method": None,
                "projection_r_squared": None,
                "projection_obs_count": None,
                "projection_asof": active_projection_asof,
            }

    _overlay_persisted_cuse_membership(
        data_db=data_db,
        universe_by_ticker=universe_by_ticker,
    )

    # Lightweight search index for instant lookup
    search_index = [
        {
            "ticker": t,
            "ric": d.get("ric", None),
            "name": d.get("name", ""),
            "trbc_economic_sector_short": d.get("trbc_economic_sector_short", ""),
            "trbc_economic_sector_short_abbr": d.get(
                "trbc_economic_sector_short_abbr",
                d.get("trbc_sector_abbr", ""),
            ),
            "trbc_business_sector": d.get("trbc_business_sector", ""),
            "trbc_industry_group": d.get("trbc_industry_group", ""),
            "risk_loading": d.get("risk_loading", 0.0),
            "specific_vol": d.get("specific_vol", None),
            "model_status": str(d.get("model_status") or "ineligible"),
            "model_status_reason": str(d.get("model_status_reason") or d.get("eligibility_reason") or ""),
            "eligibility_reason": str(d.get("model_status_reason") or d.get("eligibility_reason") or ""),
            "exposure_origin": d.get("exposure_origin"),
            "projection_method": d.get("projection_method"),
            "projection_output_status": d.get("projection_output_status"),
            "served_exposure_available": bool(d.get("served_exposure_available")),
            "cuse_realized_role": str(d.get("cuse_realized_role") or ""),
            "cuse_output_status": str(d.get("cuse_output_status") or ""),
            "cuse_reason_code": str(d.get("cuse_reason_code") or ""),
        }
        for t, d in universe_by_ticker.items()
    ]
    search_index.sort(key=lambda x: str(x["ticker"]))

    core_estimated_count = int(
        sum(1 for d in universe_by_ticker.values() if str(d.get("model_status") or "") == "core_estimated")
    )
    projected_only_count = int(
        sum(1 for d in universe_by_ticker.values() if str(d.get("model_status") or "") == "projected_only")
    )
    ineligible_count = int(
        sum(1 for d in universe_by_ticker.values() if str(d.get("model_status") or "") == "ineligible")
    )
    eligible_count = core_estimated_count + projected_only_count
    return {
        "ticker_count": len(universe_by_ticker),
        "eligible_ticker_count": eligible_count,
        "core_estimated_ticker_count": core_estimated_count,
        "projected_only_ticker_count": projected_only_count,
        "ineligible_ticker_count": ineligible_count,
        "as_of_date": latest_asof or None,
        "latest_available_asof": latest_available_asof or None,
        "factor_count": len(factor_vol_map),
        "factors": sorted(factor_vol_map.keys()),
        "factor_vols": {k: round(v, 6) for k, v in factor_vol_map.items()},
        "factor_catalog": serialize_factor_catalog(catalog_by_name),
        "index": search_index,
        "by_ticker": universe_by_ticker,
    }


def _load_factor_coverage_rows(
    db_path: Path,
    *,
    table: str,
    date_col: str,
) -> tuple[str | None, dict[str, FactorCoveragePayload]]:
    conn = sqlite3.connect(str(db_path))
    try:
        latest_row = conn.execute(f"SELECT MAX({date_col}) FROM {table}").fetchone()
        latest = str(latest_row[0]) if latest_row and latest_row[0] else None
        if latest is None:
            return None, {}
        rows = conn.execute(
            f"""
            SELECT factor_name, cross_section_n, eligible_n, coverage
            FROM {table}
            WHERE {date_col} = ?
            """,
            (latest,),
        ).fetchall()
    except sqlite3.OperationalError:
        return None, {}
    finally:
        conn.close()

    out: dict[str, FactorCoveragePayload] = {}
    for factor_name, cross_n, eligible_n, coverage in rows:
        out[str(factor_name)] = {
            "cross_section_n": int(cross_n or 0),
            "eligible_n": int(eligible_n or 0),
            "coverage_pct": float(coverage or 0.0),
        }
    return latest, out


def load_latest_factor_coverage(
    cache_db: Path,
    *,
    data_db: Path | None = None,
) -> tuple[str | None, dict[str, FactorCoveragePayload]]:
    """Load latest factor coverage, preferring durable model outputs over legacy cache history."""
    if data_db is not None and Path(data_db).exists():
        latest, out = _load_factor_coverage_rows(
            Path(data_db),
            table="model_factor_returns_daily",
            date_col="date",
        )
        if latest is not None and out:
            return latest, out

    return _load_factor_coverage_rows(
        Path(cache_db),
        table="daily_factor_returns",
        date_col="date",
    )
