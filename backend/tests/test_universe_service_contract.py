from __future__ import annotations

from pathlib import Path

from backend.services import cuse4_universe_service
from backend.services import universe_service


def _patch_universe_payload(monkeypatch, payload) -> None:
    monkeypatch.setattr(
        universe_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: payload if name == "universe_loadings" else None,
    )
    monkeypatch.setattr(
        cuse4_universe_service,
        "load_runtime_payload_field",
        lambda name, field_name, *, fallback_loader=None: (
            payload.get(field_name)
            if name == "universe_loadings" and isinstance(payload, dict)
            else None
        ),
    )
    monkeypatch.setattr(universe_service, "cache_get", lambda key: None)


def test_universe_legacy_shim_reexports_supported_contract() -> None:
    assert universe_service.UniversePayloadNotReady is cuse4_universe_service.UniversePayloadNotReady
    assert sorted(universe_service.__all__) == [
        "DATA_DB",
        "UniversePayloadNotReady",
        "cache_get",
        "load_price_history_rows",
        "load_runtime_payload",
        "load_universe_factors_payload",
        "load_universe_payload",
        "load_universe_ticker_history_payload",
        "load_universe_ticker_payload",
        "search_universe_payload",
    ]


def test_universe_legacy_shim_search_uses_legacy_module_globals(monkeypatch) -> None:
    payload = {
        "index": [{"ticker": "JPM", "name": "JPMORGAN CHASE", "ric": "JPM.N", "exposures": {"market": 1.0}}],
        "by_ticker": {"JPM": {"ticker": "JPM", "ric": "JPM.N", "model_status": "core_estimated", "exposures": {"market": 1.0}}},
    }
    _patch_universe_payload(monkeypatch, payload)
    monkeypatch.setattr(
        cuse4_universe_service.registry_quote_reads,
        "search_registry_quote_rows",
        lambda *args, **kwargs: [],
    )

    out = universe_service.search_universe_payload(
        q="jpm",
        limit=20,
        row_normalizer=lambda row: row,
    )

    assert out["total"] == 1
    assert out["results"][0]["ticker"] == "JPM"
    assert out["results"][0]["ric"] == "JPM.N"
    assert out["results"][0]["whatif_ready"] is True


def test_universe_legacy_shim_history_uses_legacy_data_db_and_loader(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_db = tmp_path / "legacy-data.db"
    monkeypatch.setattr(universe_service, "DATA_DB", data_db)
    monkeypatch.setattr(
        universe_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: {
            "by_ticker": {"ABC": {"ticker": "ABC", "ric": "ABC.N"}},
        } if name == "universe_loadings" else None,
    )
    monkeypatch.setattr(universe_service, "cache_get", lambda key: None)

    def _history_loader(path: Path, *, ric: str, years: int):
        assert path == data_db
        assert ric == "ABC.N"
        assert years == 1
        return "2026-01-16", [
            ("2026-01-12", 13.0),
            ("2026-01-16", 15.0),
        ]

    monkeypatch.setattr(universe_service, "load_price_history_rows", _history_loader)

    out = universe_service.load_universe_ticker_history_payload("ABC", years=1)

    assert out["ticker"] == "ABC"
    assert out["ric"] == "ABC.N"
    assert out["points"] == [{"date": "2026-01-16", "close": 15.0}]


def test_universe_search_includes_registry_only_rows(monkeypatch) -> None:
    payload = {
        "index": [{"ticker": "JPM", "name": "JPMORGAN CHASE", "ric": "JPM.N"}],
        "by_ticker": {"JPM": {"ticker": "JPM", "ric": "JPM.N", "model_status": "core_estimated"}},
    }
    _patch_universe_payload(monkeypatch, payload)
    monkeypatch.setattr(
        cuse4_universe_service.registry_quote_reads,
        "search_registry_quote_rows",
        lambda *args, **kwargs: [
            {
                "ric": "SPY.P",
                "ticker": "SPY",
                "common_name": "SPDR S&P 500 ETF",
                "trbc_economic_sector": "Funds",
                "trbc_industry_group": "Exchange Traded Funds",
                "price": 610.0,
                "allow_cuse_returns_projection": 1,
            }
        ],
    )

    out = universe_service.search_universe_payload(q="sp", limit=20, row_normalizer=lambda row: row)

    assert {row["ticker"] for row in out["results"]} == {"SPY"}
    assert out["results"][0]["risk_tier_label"] == "Projected (Returns Candidate)"
    assert out["results"][0]["quote_source_label"] == "Registry Runtime"
    assert out["results"][0]["whatif_ready"] is False


def test_universe_search_marks_served_projection_index_row_preview_ready(monkeypatch) -> None:
    payload = {
        "index": [
            {
                "ticker": "XLF",
                "name": "Financial Select Sector SPDR Fund",
                "ric": "XLF.P",
            }
        ],
        "by_ticker": {
            "XLF": {
                "ticker": "XLF",
                "ric": "XLF.P",
                "model_status": "projected_only",
                "exposure_origin": "projected_returns",
                "projection_method": "ols_returns_regression",
                "projection_output_status": "available",
                "served_exposure_available": True,
                "exposures": {"market": 1.0},
            }
        },
    }
    _patch_universe_payload(monkeypatch, payload)
    monkeypatch.setattr(
        cuse4_universe_service.registry_quote_reads,
        "search_registry_quote_rows",
        lambda *args, **kwargs: [],
    )

    out = universe_service.search_universe_payload(
        q="xlf",
        limit=20,
        row_normalizer=lambda row: row,
    )

    assert out["total"] == 1
    assert out["results"][0]["ticker"] == "XLF"
    assert out["results"][0]["quote_source_label"] == "Live cUSE Payload"
    assert out["results"][0]["whatif_ready"] is True
    assert out["results"][0]["whatif_ready_label"] == "Preview Ready"


def test_universe_search_skips_registry_lookup_when_served_hits_fill_limit(monkeypatch) -> None:
    payload = {
        "index": [
            {"ticker": "XLF", "name": "Financial Select Sector SPDR Fund", "ric": "XLF.P"},
            {"ticker": "XLE", "name": "Energy Select Sector SPDR Fund", "ric": "XLE.P"},
        ],
        "by_ticker": {
            "XLF": {
                "ticker": "XLF",
                "ric": "XLF.P",
                "model_status": "projected_only",
                "exposure_origin": "projected_returns",
                "projection_output_status": "available",
                "served_exposure_available": True,
                "exposures": {"market": 1.0},
            },
            "XLE": {
                "ticker": "XLE",
                "ric": "XLE.P",
                "model_status": "projected_only",
                "exposure_origin": "projected_returns",
                "projection_output_status": "available",
                "served_exposure_available": True,
                "exposures": {"market": 1.0},
            },
        },
    }
    _patch_universe_payload(monkeypatch, payload)

    def _unexpected_registry_call(*args, **kwargs):
        raise AssertionError("registry fallback should not run when served hits already fill the limit")

    monkeypatch.setattr(
        cuse4_universe_service.registry_quote_reads,
        "search_registry_quote_rows",
        _unexpected_registry_call,
    )

    out = universe_service.search_universe_payload(
        q="xl",
        limit=2,
        row_normalizer=lambda row: row,
    )

    assert [row["ticker"] for row in out["results"]] == ["XLE", "XLF"]
    assert all(row["whatif_ready"] is True for row in out["results"])


def test_universe_ticker_payload_falls_back_to_registry_runtime(monkeypatch) -> None:
    monkeypatch.setattr(
        universe_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: {"index": [], "by_ticker": {}} if name == "universe_loadings" else None,
    )
    monkeypatch.setattr(universe_service, "cache_get", lambda key: None)
    monkeypatch.setattr(
        cuse4_universe_service.registry_quote_reads,
        "load_registry_quote_rows_for_tickers",
        lambda *args, **kwargs: [
            {
                "ric": "URA.P",
                "ticker": "URA",
                "common_name": "Global X Uranium ETF",
                "trbc_economic_sector": "Funds",
                "trbc_industry_group": "Exchange Traded Funds",
                "price": 32.5,
                "allow_cuse_returns_projection": 1,
            }
        ],
    )

    out = universe_service.load_universe_ticker_payload("URA", row_normalizer=lambda row: row)

    assert out["_cached"] is False
    assert out["item"]["ticker"] == "URA"
    assert out["item"]["risk_tier_label"] == "Projected (Returns Candidate)"
    assert out["item"]["quote_source_label"] == "Registry Runtime"
    assert out["item"]["quote_source_detail"] == (
        "This quote is coming from registry/runtime authority because the current published cUSE snapshot does not contain this ticker."
    )
    assert out["item"]["whatif_ready"] is False


def test_universe_ticker_payload_uses_published_portfolio_overlay_for_held_name(monkeypatch) -> None:
    payload = {
        "index": [],
        "by_ticker": {},
    }
    monkeypatch.setattr(
        universe_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: (
            payload if name == "universe_loadings" else {
                "positions": [
                    {
                        "ticker": "COST",
                        "name": "Costco Wholesale Corp",
                        "price": 980.85,
                        "exposures": {"market": 1.0, "industry_retailers": 1.0},
                        "model_status": "core_estimated",
                        "exposure_origin": "native",
                        "served_exposure_available": True,
                    }
                ]
            } if name == "portfolio" else None
        ),
    )
    monkeypatch.setattr(universe_service, "cache_get", lambda key: None)

    out = universe_service.load_universe_ticker_payload("COST", row_normalizer=lambda row: row)

    assert out["_cached"] is True
    assert out["item"]["ticker"] == "COST"
    assert out["item"]["quote_source_label"] == "Live cUSE Payload"
    assert out["item"]["whatif_ready"] is True
    assert out["item"]["exposures"]["market"] == 1.0


def test_universe_search_includes_published_portfolio_overlay_when_universe_missing_ticker(monkeypatch) -> None:
    payload = {
        "index": [],
        "by_ticker": {},
    }
    monkeypatch.setattr(
        universe_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: (
            payload if name == "universe_loadings" else {
                "positions": [
                    {
                        "ticker": "COST",
                        "name": "Costco Wholesale Corp",
                        "price": 980.85,
                        "exposures": {"market": 1.0, "industry_retailers": 1.0},
                        "model_status": "core_estimated",
                        "exposure_origin": "native",
                        "served_exposure_available": True,
                    }
                ]
            } if name == "portfolio" else None
        ),
    )
    monkeypatch.setattr(universe_service, "cache_get", lambda key: None)
    monkeypatch.setattr(
        cuse4_universe_service.registry_quote_reads,
        "search_registry_quote_rows",
        lambda *args, **kwargs: [],
    )

    out = universe_service.search_universe_payload(
        q="cost",
        limit=20,
        row_normalizer=lambda row: row,
    )

    assert out["total"] == 1
    assert out["results"][0]["ticker"] == "COST"
    assert out["results"][0]["whatif_ready"] is True
    assert out["results"][0]["quote_source_label"] == "Live cUSE Payload"


def test_universe_history_can_resolve_registry_runtime_ticker(monkeypatch, tmp_path: Path) -> None:
    data_db = tmp_path / "legacy-data.db"
    monkeypatch.setattr(universe_service, "DATA_DB", data_db)
    monkeypatch.setattr(
        universe_service,
        "load_runtime_payload",
        lambda name, *, fallback_loader=None: {"by_ticker": {}} if name == "universe_loadings" else None,
    )
    monkeypatch.setattr(universe_service, "cache_get", lambda key: None)
    monkeypatch.setattr(
        cuse4_universe_service.registry_quote_reads,
        "load_registry_quote_rows_for_tickers",
        lambda *args, **kwargs: [{"ric": "QQQ.P", "ticker": "QQQ", "allow_cuse_returns_projection": 1}],
    )

    def _history_loader(path: Path, *, ric: str, years: int):
        assert path == data_db
        assert ric == "QQQ.P"
        assert years == 1
        return "2026-01-16", [("2026-01-16", 500.0)]

    monkeypatch.setattr(universe_service, "load_price_history_rows", _history_loader)

    out = universe_service.load_universe_ticker_history_payload("QQQ", years=1)

    assert out["ticker"] == "QQQ"
    assert out["ric"] == "QQQ.P"
