from pathlib import Path

import pandas as pd

from nfe_importer.config import (
    CSVOutputConfig,
    ExportConfig,
    MetafieldsConfig,
    PathsConfig,
    PricingConfig,
    Settings,
    TagsConfig,
)
from nfe_importer.core.generator import CSVGenerator
from nfe_importer.core.models import CatalogProduct, MatchDecision, NFEItem
from nfe_importer.core.parser import CatalogLoader


def build_settings(
    tmp_path: Path,
    *,
    export_status: str = "draft",
    drop_short_codes: bool = False,
    min_alpha_len: int = 3,
) -> Settings:
    paths = PathsConfig(
        nfe_input_folder=tmp_path / "input",
        master_data_file=Path("example_docs/MART-Ficha-tecnica-Biblioteca-Virtual-08-08-2025.xlsx"),
        output_folder=tmp_path / "output",
        log_folder=tmp_path / "logs",
        synonym_cache_file=tmp_path / "synonyms.json",
    )
    csv_config = CSVOutputConfig(
        filename_prefix="test_",
        columns=[
            "Handle",
            "Title",
            "Body (HTML)",
            "Vendor",
            "Tags",
            "Published",
            "Option1 Name",
            "Option1 Value",
            "Option2 Name",
            "Option2 Value",
            "Option3 Name",
            "Option3 Value",
            "Variant SKU",
            "Variant Price",
            "Variant Compare At Price",
            "Variant Inventory Qty",
            "Variant Weight",
            "Variant Weight Unit",
            "Variant Requires Shipping",
            "Image Src",
            "Variant Barcode",
            "Variant Grams",
            "Variant Inventory Tracker",
            "Variant Inventory Policy",
            "Variant Fulfillment Service",
            "product.metafields.custom.unidade",
            "product.metafields.custom.catalogo",
            "product.metafields.custom.dimensoes_do_produto",
            "product.metafields.custom.composicao",
            "product.metafields.custom.capacidade",
            "product.metafields.custom.modo_de_uso",
            "product.metafields.custom.icms",
            "product.metafields.custom.ncm",
            "product.metafields.custom.pis",
            "product.metafields.custom.ipi",
            "product.metafields.custom.cofins",
            "product.metafields.custom.componente_de_kit",
            "product.metafields.custom.resistencia_a_agua",
            "Variant Taxable",
            "Cost per item",
            "Image Position",
            "Variant Image",
            "Product Category",
            "Type",
            "Collection",
            "Status",
        ],
    )
    metafields = MetafieldsConfig(
        namespace="custom",
        keys={
            "unidade": "unidade",
            "catalogo": "catalogo",
            "dimensoes_do_produto": "dimensoes_do_produto",
            "composicao": "composicao",
            "capacidade": "capacidade",
            "modo_de_uso": "modo_de_uso",
            "icms": "icms",
            "ncm": "ncm",
            "pis": "pis",
            "ipi": "ipi",
            "cofins": "cofins",
            "componente_de_kit": "componente_de_kit",
            "resistencia_a_agua": "resistencia_a_agua",
            "cfop": "cfop",
            "cest": "cest",
        },
    )
    settings = Settings(
        paths=paths,
        pricing=PricingConfig(strategy="markup_fixo", markup_factor=2.0),
        csv_output=csv_config,
        metafields=metafields,
        export=ExportConfig(status=export_status),
        tags=TagsConfig(drop_short_codes=drop_short_codes, min_alpha_len=min_alpha_len),
    )
    settings.ensure_folders()
    return settings


def make_decision() -> MatchDecision:
    product = CatalogProduct(
        sku="08158",
        title="BANDEJA EM METAL COM ESPELHO",
        barcode="7899525681589",
        vendor="MART",
        product_type="BANDEJAS",
        unit="PC",
        ncm="73239900",
        cest="",
        weight=0.4,
        metafields={"composition": "70% METAL"},
        extra={"features": "Detalhes decorativos", "textos": "Descricao detalhada do produto"},
    )
    item = NFEItem(
        invoice_key="TEST",
        item_number=1,
        sku="08158",
        description=product.title,
        barcode=product.barcode,
        ncm=product.ncm,
        cest=None,
        cfop="5102",
        unit="PC",
        quantity=2.0,
        unit_value=10.0,
        total_value=20.0,
    )
    return MatchDecision(item=item, product=product, confidence=1.0, match_source="sku")


def build_decision_for_product(product: CatalogProduct, *, quantity: float = 1.0, unit_value: float = 10.0) -> MatchDecision:
    item = NFEItem(
        invoice_key="TEST",
        item_number=1,
        sku=product.sku,
        description=product.title,
        barcode=product.barcode,
        ncm=product.ncm,
        cest=product.cest,
        cfop="5102",
        unit=product.unit or "UN",
        quantity=quantity,
        unit_value=unit_value,
        total_value=quantity * unit_value,
    )
    return MatchDecision(item=item, product=product, confidence=1.0, match_source="test")


def test_csv_generator_creates_file(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)
    decision = make_decision()

    csv_path, pendings_path, dataframe, pendings_df = generator.generate([decision], [], run_id="20240101T000000")

    assert csv_path.exists()
    assert pendings_path is None
    assert dataframe.iloc[0]["Variant SKU"] == "08158"
    assert float(dataframe.iloc[0]["Variant Price"]) == 20.0  # markup_fixo 2.0 * cost 10.0

    output_df = pd.read_csv(csv_path)
    assert output_df.loc[0, "Handle"] != ""
    assert str(output_df.loc[0, "product.metafields.custom.ncm"]).strip() == "73239900"


def test_body_removes_duplicate_composition(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)
    product = CatalogProduct(
        sku="SKU-BODY",
        title="Escultura Decorativa",
        vendor="MART",
        product_type="Decor",
        weight=0.6,
        metafields={"composition": "100% POLIRRESINA"},
        extra={"features": "Peça decorativa _x000D_\nCom acabamento brilhante"},
    )
    df = generator._build_dataframe([build_decision_for_product(product)])
    row = df.iloc[0]
    assert "100% POLIRRESINA" not in row["Body (HTML)"]
    assert "_x000D_" not in row["Body (HTML)"]
    assert row["product.metafields.custom.composicao"] == "100% POLIRRESINA"


def test_default_fiscal_and_metafield_defaults(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)
    product = CatalogProduct(sku="SKU-FISCAL", title="Produto Fiscal", vendor="MART")
    df = generator._build_dataframe([build_decision_for_product(product)])
    row = df.iloc[0]
    for column in (
        "product.metafields.custom.icms",
        "product.metafields.custom.ipi",
        "product.metafields.custom.pis",
        "product.metafields.custom.cofins",
    ):
        assert row[column] == "0"
    assert row["product.metafields.custom.componente_de_kit"] == "FALSE"
    assert row["product.metafields.custom.resistencia_a_agua"] == "Não se aplica"


def test_weight_and_grams_conversion(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)
    heavy = CatalogProduct(sku="SKU-HEAVY", title="Produto Pesado", vendor="MART", weight=1.25)
    light = CatalogProduct(sku="SKU-LIGHT", title="Produto Leve", vendor="MART", weight=0.3)
    df = generator._build_dataframe([
        build_decision_for_product(heavy),
        build_decision_for_product(light),
    ]).set_index("Variant SKU")

    heavy_row = df.loc["SKU-HEAVY"]
    assert heavy_row["Variant Weight"] == "1.25"
    assert heavy_row["Variant Weight Unit"] == "kg"
    assert heavy_row["Variant Grams"] == "1250"

    light_row = df.loc["SKU-LIGHT"]
    assert light_row["Variant Weight"] == "300"
    assert light_row["Variant Weight Unit"] == "g"
    assert light_row["Variant Grams"] == "300"


def test_status_respects_config_and_ui_flag(tmp_path):
    settings = build_settings(tmp_path, export_status="active")
    generator = CSVGenerator(settings)
    base_product = CatalogProduct(sku="SKU-ACTIVE", title="Produto Ativo", vendor="MART")
    draft_product = CatalogProduct(
        sku="SKU-DRAFT",
        title="Produto Draft",
        vendor="MART",
        extra={"create_as_draft": True},
    )
    df = generator._build_dataframe([
        build_decision_for_product(base_product),
        build_decision_for_product(draft_product),
    ]).set_index("Variant SKU")

    assert df.loc["SKU-ACTIVE", "Status"] == "active"
    assert df.loc["SKU-DRAFT", "Status"] == "draft"


def test_option1_value_unique_per_handle(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)
    product_a = CatalogProduct(sku="SKU-1", title="Produto Único", vendor="MART")
    product_b = CatalogProduct(sku="SKU-2", title="Produto Único", vendor="MART")
    df = generator._build_dataframe([
        build_decision_for_product(product_a),
        build_decision_for_product(product_b),
    ]).set_index("Variant SKU")

    assert df.loc["SKU-1", "Option1 Value"] == "Default Title"
    assert df.loc["SKU-2", "Option1 Value"] == "Default Title-2"


def test_tags_sanitization_when_enabled(tmp_path):
    settings = build_settings(tmp_path, drop_short_codes=True, min_alpha_len=3)
    generator = CSVGenerator(settings)
    product = CatalogProduct(
        sku="SKU-TAGS",
        title="Produto Tags",
        vendor="MART",
        product_type="Acessórios",
        tags=["1T24", "Coleção Nova", "A-01", "Decor"],
    )
    df = generator._build_dataframe([build_decision_for_product(product)])
    row = df.iloc[0]
    tags = row["Tags"].split(",") if row["Tags"] else []
    assert tags == ["Acessórios", "Coleção Nova", "Decor"]
