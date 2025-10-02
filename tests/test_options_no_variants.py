from nfe_importer.core.generator import CSVGenerator
from nfe_importer.core.models import CatalogProduct

from tests.test_generator import build_decision_for_product, build_settings


def test_options_empty_for_simple_product(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)
    product = CatalogProduct(
        sku="SKU-SIMPLE",
        title="Produto Simples",
        vendor="MART",
        product_type="Decor",
    )
    df = generator._build_dataframe([build_decision_for_product(product)])
    row = df.iloc[0]
    for idx in range(1, 4):
        assert row[f"Option{idx} Name"] == ""
        assert row[f"Option{idx} Value"] == ""
