from pathlib import Path

from nfe_importer.core.generator import CSVGenerator
from nfe_importer.core.models import CatalogProduct, MatchDecision, NFEItem
from nfe_importer.config import Settings


def make_settings(tmp_path: Path, enabled: bool = True) -> Settings:
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
                "Handle", "Title", "Body (HTML)", "Vendor", "Tags", "Published",
                "Variant SKU", "Variant Weight", "Variant Weight Unit", "Variant Grams",
                "product.metafields.custom.unidade", "product.metafields.custom.catalogo",
                "product.metafields.custom.dimensoes_do_produto", "product.metafields.custom.capacidade",
            ],
        },
        "metafields": {
            "namespace": "custom",
            "keys": {
                "unidade": "unidade",
                "catalogo": "catalogo",
                "dimensoes_do_produto": "dimensoes_do_produto",
                "capacidade": "capacidade",
            },
            "dynamic_mapping": {
                "enabled": enabled,
                "map": {
                    "unidade": "unit",
                    "catalogo": "catalogo",
                    "dimensoes_do_produto": "medidas_s_emb",
                    "capacidade": "capacidade__ml_ou_peso_suportado",
                },
            },
        },
    }
    return Settings.parse_obj(cfg)


def make_decision(extra: dict) -> MatchDecision:
    product = CatalogProduct(
        sku="S",
        title="Produto",
        unit="CX",
        extra=extra,
    )
    item = NFEItem(
        invoice_key="k",
        item_number=1,
        sku="S",
        description="Produto",
        barcode=None,
        ncm=None,
        cest=None,
        cfop=None,
        unit="CX",
        quantity=1.0,
        unit_value=10.0,
        total_value=10.0,
    )
    return MatchDecision(item=item, product=product, confidence=1.0, match_source="test")


def test_dynamic_metafields_mapping(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, enabled=True)
    generator = CSVGenerator(settings)
    df = generator._build_dataframe(
        [
            make_decision(
                {
                    "catalogo": "Linha Casa",
                    "medidas_s_emb": "10x10x10",
                    "capacidade__ml_ou_peso_suportado": "2L",
                }
            )
        ]
    )
    row = df.iloc[0]
    assert row["product.metafields.custom.dimensoes_do_produto"] == "10x10x10"
    assert row["product.metafields.custom.capacidade"] == "2L"
    assert row["product.metafields.custom.catalogo"] == "Linha Casa"
    assert row["product.metafields.custom.unidade"] == "CX"
