from pathlib import Path

from nfe_importer.core.generator import CSVGenerator
from nfe_importer.core.models import CatalogProduct

from tests.test_generator import build_decision_for_product, build_settings


def test_options_value_without_name_gets_cleared(tmp_path):
    settings = build_settings(tmp_path)
    settings.variants.enabled = True
    settings.variants.option1.column = "modelo"
    settings.variants.option1.name = ""  # force inference

    generator = CSVGenerator(settings)
    product = CatalogProduct(
        sku="SKU-MODELO",
        title="Produto Modelo",
        vendor="MART",
        product_type="Decor",
        extra={"modelo": "Edição Limitada"},
    )
    df = generator._build_dataframe([build_decision_for_product(product)])
    row = df.iloc[0]
    for idx in range(1, 4):
        assert row[f"Option{idx} Name"] == ""
        assert row[f"Option{idx} Value"] == ""
