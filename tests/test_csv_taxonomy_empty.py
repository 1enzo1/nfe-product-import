from pathlib import Path

from nfe_importer.core.generator import CSVGenerator
from nfe_importer.core.models import CatalogProduct
from tests.test_generator import build_decision_for_product, build_settings


def test_taxonomy_fields_forced_empty(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)
    product = CatalogProduct(
        sku="SKU-TAXONOMY",
        title="Produto Taxonomia",
        vendor="MART",
        product_type="Decor",
        collection="Colecao Teste",
        extra={"product_category": "Categoria Teste"},
    )
    df = generator._build_dataframe([build_decision_for_product(product)])
    row = df.iloc[0]
    assert row["Product Category"] == ""
    assert row["Type"] == ""
    assert row["Collection"] == ""
