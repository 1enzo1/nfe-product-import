
from nfe_importer.core.generator import CSVGenerator
from nfe_importer.core.models import CatalogProduct

from tests.test_generator import build_decision_for_product, build_settings


def test_options_group_size_pmg(tmp_path):
    settings = build_settings(tmp_path)
    generator = CSVGenerator(settings)

    decisions = []
    for code in ("P", "M", "G"):
        product = CatalogProduct(
            sku=f"CAM-{code}",
            title="Camiseta Basica",
            vendor="MART",
            product_type="Moda",
        )
        decisions.append(build_decision_for_product(product))

    df = generator._build_dataframe(decisions).set_index("Variant SKU")
    for code in ("P", "M", "G"):
        row = df.loc[f"CAM-{code}"]
        assert row["Option1 Name"] == "Tamanho"
        assert row["Option1 Value"] == code
        assert row["Option2 Name"] == ""
        assert row["Option2 Value"] == ""
        assert row["Option3 Name"] == ""
        assert row["Option3 Value"] == ""
