from pathlib import Path

from nfe_importer.core.generator import CSVGenerator
from nfe_importer.core.models import CatalogProduct, NFEItem, MatchDecision
from nfe_importer.config import Settings


def make_settings(tmp_path: Path, enabled=True):
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
        "metafields": {
            "namespace": "custom",
            "dynamic_mapping": {
                "enabled": enabled,
                "map": {
                    "dimensoes_do_produto": "dimensoes",
                    "capacidade": "cap",
                },
            },
        },
    }
    return Settings.parse_obj(cfg)


def make_decision(extra: dict):
    product = CatalogProduct(sku="S", title="P", extra=extra)
    item = NFEItem(
        invoice_key="k", item_number=1, sku="S", description="P",
        barcode=None, ncm=None, cest=None, cfop=None, unit="UN", quantity=1.0, unit_value=10.0, total_value=10.0,
    )
    return MatchDecision(item=item, product=product, confidence=1.0, match_source="test")


def test_dynamic_metafields_mapping(tmp_path):
    settings = make_settings(tmp_path, enabled=True)
    gen = CSVGenerator(settings)
    df = gen._build_dataframe([make_decision({"dimensoes": "10x10x10", "cap": "2L"})])
    row = df.iloc[0]
    assert row["product.metafields.custom.dimensoes_do_produto"] == "10x10x10"
    assert row["product.metafields.custom.capacidade"] == "2L"

