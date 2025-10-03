from pathlib import Path

from nfe_importer.core.generator import CSVGenerator
from nfe_importer.core.models import CatalogProduct

from tests.test_generator import build_decision_for_product, build_settings

SIZE_CODES = ["P", "M", "G"]


def _prepare_settings(tmp_path: Path):
    settings = build_settings(tmp_path)
    settings.variants.enabled = True
    settings.variants.option1.column = "tamanho"
    settings.variants.option1.name = ""
    return settings


def test_options_infer_size_axis(tmp_path):
    settings = _prepare_settings(tmp_path)
    generator = CSVGenerator(settings)

    decisions = []
    for code in SIZE_CODES:
        product = CatalogProduct(
            sku=f"SKU-{code}",
            title="Camiseta",
            vendor="MART",
            product_type="Moda",
            extra={"tamanho": code},
        )
        decisions.append(build_decision_for_product(product))

    df = generator._build_dataframe(decisions).set_index("Variant SKU")
    for code in SIZE_CODES:
        row = df.loc[f"SKU-{code}"]
        assert row["Option1 Name"] == "Tamanho"
        assert row["Option1 Value"] == code
        assert row["Option2 Name"] == ""
        assert row["Option2 Value"] == ""
        assert row["Option3 Name"] == ""
        assert row["Option3 Value"] == ""
