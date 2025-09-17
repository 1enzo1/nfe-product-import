from pathlib import Path

import pandas as pd

from nfe_importer.config import CSVOutputConfig, MetafieldsConfig, PathsConfig, PricingConfig, Settings
from nfe_importer.core.generator import CSVGenerator
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
        columns=[
            "Handle",
            "Title",
            "Vendor",
            "Product Type",
            "SKU",
            "Barcode",
            "Status",
            "Published",
            "Tags",
            "Price",
            "Compare At Price",
            "Cost per item",
            "Inventory Qty",
            "Weight",
            "Image Src",
        ],
    )
    metafields = MetafieldsConfig(namespace="custom", keys={"ncm": "ncm", "cfop": "cfop", "unidade": "unidade", "cest": "cest", "composicao": "composicao"})
    settings = Settings(paths=paths, pricing=PricingConfig(strategy="markup_fixo", markup_factor=2.0), csv_output=csv_config, metafields=metafields)
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
    assert dataframe.iloc[0]["SKU"] == "08158"
    assert dataframe.iloc[0]["Price"] == 20.0  # markup_fixo 2.0 * cost 10.0

    output_df = pd.read_csv(csv_path)
    assert output_df.loc[0, "Handle"] != ""
    assert str(output_df.loc[0, "product.metafields.custom.ncm"]).strip() == "73239900"
