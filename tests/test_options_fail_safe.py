
from nfe_importer.core.generator import CSVGenerator
from nfe_importer.core.models import CatalogProduct

from tests.test_generator import build_decision_for_product, build_settings


def test_options_fail_safe_blank_when_no_axis(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)

    decisions = []
    for suffix in ("alpha", "beta"):
        product = CatalogProduct(
            sku=f"PROD-{suffix}",
            title="Produto Generico",
            vendor="MART",
            product_type="Diversos",
        )
        decisions.append(build_decision_for_product(product))

    df = generator._build_dataframe(decisions)
    for _, row in df.iterrows():
        assert row["Option1 Name"] == ""
        assert row["Option1 Value"] == ""
        assert row["Option2 Name"] == ""
        assert row["Option2 Value"] == ""
        assert row["Option3 Name"] == ""
        assert row["Option3 Value"] == ""
        assert row["Option1 Value"].casefold() != "default title"
