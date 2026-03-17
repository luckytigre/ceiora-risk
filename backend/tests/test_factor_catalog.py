from backend.risk_model.factor_catalog import (
    MARKET_FACTOR,
    MARKET_FACTOR_ID,
    STYLE_COLUMN_TO_LABEL,
    build_factor_catalog,
    build_factor_catalog_for_factors,
    factor_id_for_name,
    factor_name_from_token,
    infer_factor_family,
    serialize_factor_catalog,
)


def test_build_factor_catalog_assigns_stable_ids_and_orders() -> None:
    catalog = build_factor_catalog(
        market_factor_name=MARKET_FACTOR,
        industry_names=["Technology Equipment", "Industrial Goods"],
        method_version="test_v1",
    )

    assert catalog[MARKET_FACTOR].factor_id == "market"
    assert catalog["Beta"].factor_id == "style_beta_score"
    assert catalog["Technology Equipment"].factor_id == "industry_technology_equipment"
    assert catalog["Industrial Goods"].factor_id == "industry_industrial_goods"

    serialized = serialize_factor_catalog(catalog)
    names = [str(row["factor_name"]) for row in serialized]
    assert names[0] == MARKET_FACTOR
    assert names[1:3] == ["Industrial Goods", "Technology Equipment"]


def test_infer_factor_family_supports_current_and_future_structural_factors() -> None:
    assert infer_factor_family("Beta") == "style"
    assert infer_factor_family(MARKET_FACTOR) == "market"
    assert infer_factor_family(MARKET_FACTOR_ID) == "market"
    assert infer_factor_family("style_beta_score") == "style"
    assert infer_factor_family("Technology Equipment") == "industry"


def test_factor_id_for_name_uses_source_column_for_styles() -> None:
    assert factor_id_for_name("Momentum", family="style", source_column="momentum_score") == "style_momentum_score"
    assert factor_id_for_name("Market", family="market") == "market"
    assert factor_id_for_name("Retailers", family="industry") == "industry_retailers"


def test_style_column_mapping_matches_catalog_source_names() -> None:
    catalog = build_factor_catalog(market_factor_name=MARKET_FACTOR)
    assert set(STYLE_COLUMN_TO_LABEL.values()).issubset(set(catalog.keys()))


def test_build_factor_catalog_for_factor_ids_normalizes_to_canonical_entries() -> None:
    catalog = build_factor_catalog_for_factors(
        ["market", "style_beta_score", "industry_technology_equipment"],
        method_version="test_v1",
    )

    assert catalog["Market"].factor_id == "market"
    assert catalog["Beta"].factor_id == "style_beta_score"
    assert catalog["Technology Equipment"].factor_id == "industry_technology_equipment"


def test_factor_name_from_token_resolves_ids_to_names() -> None:
    assert factor_name_from_token("market") == "Market"
    assert factor_name_from_token("style_beta_score") == "Beta"
    assert factor_name_from_token("industry_technology_equipment") == "Technology Equipment"
