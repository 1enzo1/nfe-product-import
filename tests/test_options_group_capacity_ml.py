
from nfe_importer.core.generator import CSVGenerator
from nfe_importer.core.models import CatalogProduct

from tests.test_generator import build_decision_for_product, build_settings


CAP_VARIANTS = (
    ("BOT-300ML", "300 ml"),
    ("BOT-500ML", "500 ml"),
    ("BOT-1L", "1 L"),
)


def test_options_group_capacity_ml(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)

    decisions = []
    for sku, _ in CAP_VARIANTS:
        product = CatalogProduct(
            sku=sku,
            title="Garrafa Termica",
            vendor="MART",
            product_type="Utilidades",
        )
        decisions.append(build_decision_for_product(product))

    df = generator._build_dataframe(decisions).set_index("Variant SKU")
    for sku, expected in CAP_VARIANTS:
        row = df.loc[sku]
        assert row["Option1 Name"] == "Capacidade"
        assert row["Option1 Value"] == expected
        assert row["Option2 Name"] == ""
        assert row["Option2 Value"] == ""
        assert row["Option3 Name"] == ""
        assert row["Option3 Value"] == ""
