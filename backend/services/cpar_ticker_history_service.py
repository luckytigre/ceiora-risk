"""Read-only cPAR single-name price-history payload service."""

from __future__ import annotations

from datetime import date, datetime, timedelta

from backend.data import cpar_source_reads
from backend.services import cpar_meta_service, cpar_ticker_service


def _week_ending_friday(day: date) -> date:
    delta = 4 - day.weekday()
    if delta < 0:
        delta += 7
    return day + timedelta(days=delta)


def load_cpar_ticker_history_payload(
    *,
    ticker: str,
    ric: str | None = None,
    years: int,
    data_db=None,
) -> dict[str, object]:
    package, _fit, _registry_row, clean_ticker, resolved_ric = cpar_ticker_service.resolve_cpar_ticker_identity(
        ticker=ticker,
        ric=ric,
        data_db=data_db,
    )

    package_date = str(package.get("package_date") or "")
    try:
        latest_date = datetime.fromisoformat(package_date).date()
    except (TypeError, ValueError) as exc:
        raise cpar_meta_service.CparReadUnavailable(
            f"Active cPAR package has an invalid package_date: {package_date or 'missing'}"
        ) from exc
    date_from = latest_date - timedelta(days=max(int(years), 1) * 366)

    try:
        rows = cpar_source_reads.load_price_rows_for_rics(
            [resolved_ric],
            date_from=date_from.isoformat(),
            date_to=latest_date.isoformat(),
            data_db=data_db,
        )
    except cpar_source_reads.CparSourceReadError as exc:
        raise cpar_meta_service.CparReadUnavailable(f"Shared-source read failed: {exc}") from exc

    week_close: dict[str, float] = {}
    for row in rows:
        raw_date = str(row.get("date") or "").strip()
        raw_close = row.get("adj_close")
        if raw_close is None:
            raw_close = row.get("close")
        if not raw_date or raw_close is None:
            continue
        try:
            close = float(raw_close)
            day = datetime.fromisoformat(raw_date).date()
        except (TypeError, ValueError):
            continue
        week_close[_week_ending_friday(day).isoformat()] = close

    if not week_close:
        raise cpar_ticker_service.CparTickerNotFound(
            f"No price history found for {clean_ticker or ticker}."
        )

    return {
        "ticker": clean_ticker,
        "ric": resolved_ric,
        "years": int(years),
        "points": [
            {"date": week_end, "close": round(float(close), 4)}
            for week_end, close in sorted(week_close.items())
        ],
        "_cached": True,
    }
