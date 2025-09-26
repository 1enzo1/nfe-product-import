from pathlib import Path

from nfe_importer.core.generator import CSVGenerator
from nfe_importer.core.models import CatalogProduct, NFEItem, MatchDecision
from nfe_importer.config import Settings


def make_settings(tmp_path: Path, dynamic=False, unit_col=None, default_unit="kg"):
    cfg = {
        "paths": {
            "nfe_input_folder": str(tmp_path),
            "master_data_file": str(tmp_path / "master.xlsx"),
            "output_folder": str(tmp_path),
            "log_folder": str(tmp_path),
            "synonym_cache_file": str(tmp_path / "synonyms.json"),
        },
        "csv_output": {
            "filename_prefix": "importacao_produtos_",
            "columns": [
                "Handle","Title","Body (HTML)","Vendor","Tags","Published",
                "Option1 Name","Option1 Value","Option2 Name","Option2 Value","Option3 Name","Option3 Value",
                "Variant SKU","Variant Price","Variant Compare At Price","Variant Inventory Qty","Variant Weight","Variant Weight Unit","Variant Requires Shipping","Image Src","Variant Barcode","Variant Grams","Variant Inventory Tracker","Variant Inventory Policy","Variant Fulfillment Service",
                "product.metafields.custom.unidade","product.metafields.custom.catalogo","product.metafields.custom.dimensoes_do_produto","product.metafields.custom.composicao","product.metafields.custom.capacidade","product.metafields.custom.modo_de_uso","product.metafields.custom.icms","product.metafields.custom.ncm","product.metafields.custom.pis","product.metafields.custom.ipi","product.metafields.custom.cofins","product.metafields.custom.componente_de_kit","product.metafields.custom.resistencia_a_agua",
                "Variant Taxable","Cost per item","Image Position","Variant Image","Product Category","Type","Collection","Status"
            ],
        },
        "weights": {
            "dynamic_unit": dynamic,
            "column": "peso",
            "unit_column": unit_col,
            "default_unit": default_unit,
        },
    }
    return Settings.parse_obj(cfg)


def make_decision(weight: float | None, unit_raw: str | None = None):
    extra = {"un_peso": unit_raw} if unit_raw else {}
    product = CatalogProduct(sku="S", title="P", weight=weight, extra=extra)
    item = NFEItem(
        invoice_key="k", item_number=1, sku="S", description="P",
        barcode=None, ncm=None, cest=None, cfop=None, unit="UN", quantity=1.0, unit_value=10.0, total_value=10.0,
    )
    return MatchDecision(item=item, product=product, confidence=1.0, match_source="test")


def test_weight_default_unit(tmp_path):
    settings = make_settings(tmp_path, dynamic=False)
    gen = CSVGenerator(settings)
    df = gen._build_dataframe([make_decision(0.5)])
    row = df.iloc[0]
    assert row["Variant Weight"] == 0.5
    assert row["Variant Weight Unit"] == "kg"


def test_weight_dynamic_g(tmp_path):
    settings = make_settings(tmp_path, dynamic=True, unit_col="un_peso")
    gen = CSVGenerator(settings)
    df = gen._build_dataframe([make_decision(500.0, unit_raw="g")])
    row = df.iloc[0]
    assert row["Variant Weight Unit"] == "g"

