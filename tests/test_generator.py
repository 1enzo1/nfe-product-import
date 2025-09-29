from pathlib import Path

import pandas as pd

from nfe_importer.config import (
    CSVOutputConfig,
    MetafieldsConfig,
    PathsConfig,
    PricingConfig,
    Settings,
)
from nfe_importer.core.generator import CSVGenerator, SHOPIFY_HEADER
from nfe_importer.core.models import CatalogProduct, MatchDecision, NFEItem


def build_settings(tmp_path: Path) -> Settings:
    paths = PathsConfig(
        nfe_input_folder=tmp_path / "input",
        master_data_file=Path("example_docs/MART-Ficha-tecnica-Biblioteca-Virtual-08-08-2025.xlsx"),
        output_folder=tmp_path / "output",
        log_folder=tmp_path / "logs",
        synonym_cache_file=tmp_path / "synonyms.json",
    )
    csv_config = CSVOutputConfig(
        filename_prefix="test_",
        columns=list(SHOPIFY_HEADER),
    )
    metafields = MetafieldsConfig(
        namespace="custom",
        keys={
            "ncm": "ncm",
            "unidade": "unidade",
            "composicao": "composicao",
            "catalogo": "catalogo",
            "modo_de_uso": "modo_de_uso",
            "capacidade": "capacidade",
            "dimensoes_do_produto": "dimensoes_do_produto",
        },
        dynamic_mapping=MetafieldsConfig.DynamicMap(
            enabled=True,
            map={
                "unidade": "unit",
                "catalogo": "catalogo",
                "dimensoes_do_produto": "medidas_s_emb",
                "capacidade": "capacidade__ml_ou_peso_suportado",
                "ncm": "ncm",
                "modo_de_uso": "features",
            },
        ),
    )
    settings = Settings(
        paths=paths,
        pricing=PricingConfig(strategy="markup_fixo", markup_factor=2.0),
        csv_output=csv_config,
        metafields=metafields,
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


def test_csv_generator_creates_file(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)
    decision = make_decision()

    csv_path, pendings_path, dataframe, pendings_df = generator.generate([decision], [], run_id="20240101T000000")

    assert csv_path.exists()
    assert pendings_path is None
    row = dataframe.iloc[0]
    assert row["Variant SKU"] == "08158"
    assert float(row["Variant Price"]) == 20.0  # markup_fixo 2.0 * cost 10.0
    assert row["Variant Inventory Tracker"] == "shopify"
    assert row["Variant Weight"] == "400"
    assert row["Variant Weight Unit"] == "g"
    assert row["Variant Grams"] == "400"
    # Body should keep features but avoid duplicating composition metafield
    assert row["Body (HTML)"] == "Descricao detalhada do produto"
    assert "70% METAL" not in row["Body (HTML)"]
    assert row["product.metafields.custom.modo_de_uso"] == "Detalhes decorativos"

    output_df = pd.read_csv(csv_path)
    assert output_df.loc[0, "Handle"] != ""
    assert str(output_df.loc[0, "product.metafields.custom.ncm"]).strip() == "73239900"


def test_weight_and_metafields_mapping(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)

    product_light = CatalogProduct(
        sku="SKU-L",
        title="Produto Leve",
        vendor="Marca",
        product_type="Categoria A",
        unit="CX",
        ncm="1111",
        weight=0.3,
        collection="nan",
        metafields={"composition": "Fibra natural"},
        extra={
            "features": "Caracteristica leve",
            "catalogo": "Linha 1",
            "medidas_s_emb": "10x10x5",
            "capacidade__ml_ou_peso_suportado": "500 ml",
            "textos": "Uso diario",
        },
    )
    item_light = NFEItem(
        invoice_key="R1",
        item_number=1,
        sku="SKU-L",
        description="Produto leve",
        barcode=None,
        ncm=product_light.ncm,
        cest=None,
        cfop="5102",
        unit="CX",
        quantity=1.0,
        unit_value=15.0,
        total_value=15.0,
    )

    product_heavy = CatalogProduct(
        sku="SKU-H",
        title="Produto Pesado",
        vendor="Marca",
        product_type="Categoria B",
        unit="UN",
        ncm="2222",
        weight=1.25,
        collection="Colecao Premium",
        extra={
            "catalogo": "Linha 2",
            "medidas_s_emb": "30x20x15",
            "textos": "Limpar com pano seco",
        },
    )
    item_heavy = NFEItem(
        invoice_key="R2",
        item_number=1,
        sku="SKU-H",
        description="Produto pesado",
        barcode=None,
        ncm=product_heavy.ncm,
        cest=None,
        cfop="6102",
        unit="UN",
        quantity=1.0,
        unit_value=40.0,
        total_value=40.0,
    )

    df = generator._build_dataframe(
        [
            MatchDecision(item=item_light, product=product_light, confidence=1.0, match_source="sku"),
            MatchDecision(item=item_heavy, product=product_heavy, confidence=1.0, match_source="sku"),
        ]
    )

    row_light = df[df["Variant SKU"] == "SKU-L"].iloc[0]
    assert row_light["Variant Weight"] == "300"
    assert row_light["Variant Weight Unit"] == "g"
    assert row_light["Variant Grams"] == "300"
    assert row_light["Collection"] == "Categoria A"
    assert row_light["Body (HTML)"] == "Uso diario"
    assert row_light["product.metafields.custom.modo_de_uso"] == "Caracteristica leve"
    assert row_light["product.metafields.custom.catalogo"] == "Linha 1"
    assert row_light["product.metafields.custom.capacidade"] == "500 ml"
    assert row_light["product.metafields.custom.dimensoes_do_produto"] == "10x10x5"
    assert row_light["product.metafields.custom.unidade"] == "CX"
    assert row_light["product.metafields.custom.ncm"] == "1111"

    row_heavy = df[df["Variant SKU"] == "SKU-H"].iloc[0]
    assert row_heavy["Variant Weight"] == "1.25"
    assert row_heavy["Variant Weight Unit"] == "kg"
    assert row_heavy["Variant Grams"] == "1250"
    assert row_heavy["Collection"] == "Colecao Premium"
    assert row_heavy["product.metafields.custom.catalogo"] == "Linha 2"
    assert row_heavy["product.metafields.custom.unidade"] == "UN"
    assert row_heavy["product.metafields.custom.ncm"] == "2222"
    # When no features provided the body may remain empty
    assert row_heavy["Body (HTML)"] == "Limpar com pano seco"
    assert row_heavy["product.metafields.custom.modo_de_uso"] == ""


def test_body_html_excludes_composition_when_metafield_present(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)

    product = CatalogProduct(
        sku="SKU-C",
        title="Produto com composicao",
        vendor="Marca",
        product_type="Categoria",
        unit="UN",
        weight=0.8,
        ncm="3333",
        metafields={"composition": "100% Algodao"},
        extra={"features": "Toque macio", "textos": "Descricao concisa"},
    )
    item = NFEItem(
        invoice_key="R3",
        item_number=1,
        sku="SKU-C",
        description="Produto com composicao",
        barcode=None,
        ncm="3333",
        cest=None,
        cfop="5102",
        unit="UN",
        quantity=1.0,
        unit_value=25.0,
        total_value=25.0,
    )

    df = generator._build_dataframe(
        [MatchDecision(item=item, product=product, confidence=1.0, match_source="sku")]
    )

    row = df.iloc[0]
    assert row["product.metafields.custom.composicao"] == "100% Algodao"
    assert row["Body (HTML)"] == "Descricao concisa"
    assert row["product.metafields.custom.modo_de_uso"] == "Toque macio"
    assert "Algodao" not in row["Body (HTML)"]












