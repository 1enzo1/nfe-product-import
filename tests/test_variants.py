from pathlib import Path

from nfe_importer.core.generator import CSVGenerator
from nfe_importer.core.models import CatalogProduct, NFEItem, MatchDecision
from nfe_importer.config import Settings


def make_settings(tmp_path: Path, option1_col=None, option2_col=None, enabled=True):
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
        "variants": {
            "enabled": enabled,
            "option1": {"name": "Color", "column": option1_col},
            "option2": {"name": "Tamanho", "column": option2_col},
        },
    }
    return Settings.parse_obj(cfg)


def make_decision(sku: str, title: str, extra: dict | None = None):
    product = CatalogProduct(sku=sku, title=title, extra=extra or {})
    item = NFEItem(
        invoice_key="k",
        item_number=1,
        sku=sku,
        description=title,
        barcode=None,
        ncm=None,
        cest=None,
        cfop=None,
        unit="UN",
        quantity=1.0,
        unit_value=10.0,
        total_value=10.0,
    )
    return MatchDecision(item=item, product=product, confidence=1.0, match_source="test")


def test_no_variation_defaults(tmp_path):
    settings = make_settings(tmp_path, enabled=False)
    gen = CSVGenerator(settings)
    df = gen._build_dataframe([make_decision("SKU1", "Produto X")])
    row = df.iloc[0]
    assert row["Option1 Name"] == "Title"
    assert row["Option1 Value"] == "Default Title"


def test_variation_only_color(tmp_path):
    settings = make_settings(tmp_path, option1_col="cor", enabled=True)
    gen = CSVGenerator(settings)
    d1 = make_decision("SKU-AZ", "Camiseta", extra={"cor": "Azul"})
    d2 = make_decision("SKU-VM", "Camiseta", extra={"cor": "Vermelha"})
    df = gen._build_dataframe([d1, d2])
    assert (df["Handle"] == "camiseta").all()
    assert set(df["Option1 Value"]) == {"Azul", "Vermelha"}
    assert (df["Option1 Name"] == "Color").all()


def test_variation_color_size(tmp_path):
    settings = make_settings(tmp_path, option1_col="cor", option2_col="tamanho", enabled=True)
    gen = CSVGenerator(settings)
    d1 = make_decision("SKU-AZ-P", "Camiseta", extra={"cor": "Azul", "tamanho": "P"})
    d2 = make_decision("SKU-AZ-M", "Camiseta", extra={"cor": "Azul", "tamanho": "M"})
    d3 = make_decision("SKU-VM-P", "Camiseta", extra={"cor": "Vermelha", "tamanho": "P"})
    df = gen._build_dataframe([d1, d2, d3])
    assert set(df["Option1 Value"]) == {"Azul", "Vermelha"}
    assert set(df["Option2 Value"]) == {"P", "M"}

