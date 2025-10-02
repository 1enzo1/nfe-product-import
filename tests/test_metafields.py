from pathlib import Path

from nfe_importer.config import CSVOutputConfig, MetafieldsConfig, PathsConfig, Settings
from nfe_importer.core.generator import CSVGenerator, SHOPIFY_HEADER
from nfe_importer.core.models import CatalogProduct, MatchDecision, NFEItem


def make_settings(tmp_path: Path, enabled: bool = True) -> Settings:
    paths = PathsConfig(
        nfe_input_folder=tmp_path,
        master_data_file=tmp_path / "master.xlsx",
        output_folder=tmp_path,
        log_folder=tmp_path,
        synonym_cache_file=tmp_path / "synonyms.json",
    )
    csv_output = CSVOutputConfig(
        filename_prefix="importacao_produtos_",
        columns=list(SHOPIFY_HEADER),
    )
    metafields = MetafieldsConfig(
        namespace="custom",
        keys={
            "unidade": "unidade",
            "catalogo": "catalogo",
            "dimensoes_do_produto": "dimensoes_do_produto",
            "capacidade": "capacidade",
            "ipi": "ipi",
        },
        dynamic_mapping=MetafieldsConfig.DynamicMap(
            enabled=enabled,
            map={
                "unidade": "unit",
                "catalogo": "catalogo",
                "dimensoes_do_produto": "medidas_s_emb",
                "capacidade": "capacidade__ml_ou_peso_suportado",
                "ipi": "ipi",
            },
        ),
    )
    settings = Settings(paths=paths, csv_output=csv_output, metafields=metafields)
    settings.ensure_folders()
    return settings


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
                    "ipi": 12.5,
                }
            )
        ]
    )
    row = df.iloc[0]
    assert row["product.metafields.custom.dimensoes_do_produto"] == "10 x 10 x 10"
    assert row["product.metafields.custom.capacidade"] == "2L"
    assert row["product.metafields.custom.catalogo"] == "Linha Casa"
    assert row["product.metafields.custom.unidade"] == "CX"
    assert row["product.metafields.custom.ipi"] == "12.5"


def test_ipi_fallback_without_dynamic_mapping(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, enabled=False)
    generator = CSVGenerator(settings)
    df = generator._build_dataframe([make_decision({"ipi": 0})])
    row = df.iloc[0]
    assert row["product.metafields.custom.ipi"] == "0"
